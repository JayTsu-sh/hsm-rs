//! End-to-end test for the M2d.1 daemon path.
//!
//! Wires the real components together — `MockCopytool` (no Lustre),
//! `MemStore`, `FifoPerKind`, `InProcessAgent` running a `NoopMover`
//! — and checks the daemon delivers each enqueued action all the way
//! to a terminal store state.
//!
//! Validates the contract DESIGN.md §6 and §7 promise:
//! - recv → store insert → scheduler enqueue happens
//! - dispatch tick picks ready actions and pushes them to a capable agent
//! - mover progress flows back into store as updated `progress` extents
//! - terminal status flips the store row to `Succeed` (or `Failed` on
//!   error)
//! - cancel propagates from the daemon side through the agent's
//!   cancel channel into the mover's `CancellationToken`

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hsm_core::{ActionKind, ArState, ArchiveId, Cookie, Extent, Fid};
use hsm_plugin_noop::NoopMover;
use hsm_plugin_sdk::ProgressConfig;
use hsm_scheduler::{FifoPerKind, Scheduler};
use hsm_store::{ActionStore, MemStore};
use hsmd::{Daemon, DaemonConfig, InProcessAgent, MockRecvSource};
use lustre_llapi::{MockCopytool, ReceivedAction};
use parking_lot::Mutex;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

fn sample(cookie: u64, kind: ActionKind, archive: u32) -> ReceivedAction {
    ReceivedAction {
        cookie: Cookie::new(cookie),
        fid: Fid::new(0x200000401, cookie as u32, 0),
        dfid: Fid::new(0x200000401, cookie as u32, 0),
        archive_id: ArchiveId::new(archive),
        kind,
        extent: Extent::WHOLE,
        gid: 0,
        data: Bytes::new(),
        fsname: "mock".into(),
    }
}

/// Async wait_until — re-evaluates `predicate` on each tick.
///
/// `predicate` is `async FnMut()` so callers can `.await` store reads
/// without spinning a sub-task or blocking a runtime worker.
async fn wait_until<F, Fut>(timeout: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if predicate().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    predicate().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_action_flows_through_full_pipeline() {
    init_tracing();
    let store = Arc::new(MemStore::new());
    let scheduler = Arc::new(FifoPerKind::new());
    let ct = Arc::new(Mutex::new(MockCopytool::new()));
    let recv = MockRecvSource::new(ct.clone(), Duration::from_millis(5));

    // Build daemon and the agent before starting; the recv loop won't
    // see an action until the test enqueues one.
    let daemon = Daemon::new(
        store.clone(),
        scheduler.clone(),
        recv,
        DaemonConfig::default(),
    );
    let mover = Arc::new(NoopMover::default());
    let (conn, _agent) = InProcessAgent::spawn(
        hsm_core::AgentId::new("noop-0"),
        [ArchiveId::new(1)],
        mover.clone(),
        ProgressConfig::defaults(),
    );
    let _drain = daemon.register_agent(conn);
    let handle = daemon.start();

    // Now enqueue one Archive action.
    ct.lock().enqueue(sample(0xa1, ActionKind::Archive, 1));

    // Diagnostic: confirm recv → store insert actually happens.
    let store_for_present = store.clone();
    let present = wait_until(Duration::from_secs(1), || {
        let s = store_for_present.clone();
        async move { s.get(Cookie::new(0xa1)).await.unwrap().is_some() }
    })
    .await;
    assert!(present, "action never made it to store via recv loop");
    let initial = store.get(Cookie::new(0xa1)).await.unwrap().unwrap();
    eprintln!("[diag] after insert: state = {:?}", initial.state);

    // Wait for the store row to flip to Succeed.
    let store_for_check = store.clone();
    let ok = wait_until(Duration::from_secs(2), || {
        let store = store_for_check.clone();
        async move {
            match store.get(Cookie::new(0xa1)).await.unwrap() {
                Some(rec) => matches!(rec.state, ArState::Succeed { .. }),
                None => false,
            }
        }
    })
    .await;
    if !ok {
        let final_state = store.get(Cookie::new(0xa1)).await.unwrap().unwrap();
        eprintln!("[diag] final state = {:?}", final_state.state);
        eprintln!("[diag] mover invocations = {}", mover.invocations().len());
    }
    assert!(ok, "expected cookie 0xa1 to reach Succeed within 2s");

    // Mover should have run exactly once.
    let invs = mover.invocations();
    assert_eq!(invs.len(), 1);
    assert_eq!(invs[0].kind, ActionKind::Archive);
    assert!(!invs[0].cancelled);

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_actions_complete_concurrently() {
    let store = Arc::new(MemStore::new());
    let scheduler = Arc::new(FifoPerKind::new());
    let ct = Arc::new(Mutex::new(MockCopytool::new()));
    let recv = MockRecvSource::new(ct.clone(), Duration::from_millis(5));

    let daemon = Daemon::new(
        store.clone(),
        scheduler.clone(),
        recv,
        DaemonConfig::default(),
    );
    let mover = Arc::new(NoopMover::default());
    let (conn, _agent) = InProcessAgent::spawn(
        hsm_core::AgentId::new("noop-0"),
        [ArchiveId::new(1)],
        mover.clone(),
        ProgressConfig::defaults(),
    );
    let _drain = daemon.register_agent(conn);
    let handle = daemon.start();

    // 20 archive actions back-to-back.
    {
        let mut g = ct.lock();
        for i in 0..20u64 {
            g.enqueue(sample(0x100 + i, ActionKind::Archive, 1));
        }
    }

    // Wait for everything to land — easiest predicate: count
    // invocations on the mover (terminal-only, no Cancel). We expect 20.
    let mover_for_check = mover.clone();
    let ok = wait_until(Duration::from_secs(5), || {
        let mover = mover_for_check.clone();
        async move { mover.invocations().len() == 20 }
    })
    .await;
    assert!(
        ok,
        "expected 20 invocations, got {}",
        mover.invocations().len()
    );

    // None should have been cancelled.
    assert!(mover.invocations().iter().all(|i| !i.cancelled));

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_tx_propagates_through_agent_into_mover() {
    use std::time::Duration;
    init_tracing();

    let store = Arc::new(MemStore::new());
    let scheduler = Arc::new(FifoPerKind::new());
    let ct = Arc::new(Mutex::new(MockCopytool::new()));
    let recv = MockRecvSource::new(ct.clone(), Duration::from_millis(5));

    let daemon = Daemon::new(
        store.clone(),
        scheduler.clone(),
        recv,
        DaemonConfig::default(),
    );

    // Slow mover: 16 chunks of 1 MiB with 50 ms delay each → ~800 ms
    // unconstrained. Cancel after ~120 ms must abort with ECANCELED.
    let mover = Arc::new(NoopMover::new(
        1024 * 1024,
        16 * 1024 * 1024,
        Duration::from_millis(50),
    ));
    let agent_id = hsm_core::AgentId::new("noop-slow");
    let (conn, _agent) = InProcessAgent::spawn(
        agent_id.clone(),
        [ArchiveId::new(1)],
        mover.clone(),
        ProgressConfig::defaults(),
    );

    // Capture the cancel sender before register_agent consumes the conn.
    let cancel_tx = conn.cancel_tx.clone();
    let _drain = daemon.register_agent(conn);
    let handle = daemon.start();

    let cookie = Cookie::new(0xc1);
    ct.lock()
        .enqueue(sample(cookie.get(), ActionKind::Archive, 1));

    // Wait for the action to be dispatched (state = Started).
    let s2 = store.clone();
    let dispatched = wait_until(Duration::from_secs(1), || {
        let s = s2.clone();
        async move {
            matches!(
                s.get(cookie).await.unwrap().map(|r| r.state),
                Some(ArState::Started { .. })
            )
        }
    })
    .await;
    assert!(dispatched, "action never reached Started state");

    // Now cancel.
    cancel_tx.send(cookie).await.unwrap();

    // Wait for terminal Failed state (mover bails with Cancelled, daemon
    // records Failed { rc: 125 }).
    let s3 = store.clone();
    let terminal = wait_until(Duration::from_secs(2), || {
        let s = s3.clone();
        async move {
            matches!(
                s.get(cookie).await.unwrap().map(|r| r.state),
                Some(ArState::Failed { rc: 125 })
            )
        }
    })
    .await;
    assert!(terminal, "expected Failed{{rc:125}} after cancel");

    // Mover should report cancelled in its log.
    let invs = mover.invocations();
    assert_eq!(invs.len(), 1);
    assert!(invs[0].cancelled, "mover should record cancellation");

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn action_for_unmatched_archive_stays_queued() {
    let store = Arc::new(MemStore::new());
    let scheduler = Arc::new(FifoPerKind::new());
    let ct = Arc::new(Mutex::new(MockCopytool::new()));
    let recv = MockRecvSource::new(ct.clone(), Duration::from_millis(5));

    let daemon = Daemon::new(
        store.clone(),
        scheduler.clone(),
        recv,
        DaemonConfig::default(),
    );
    // Agent serves archive_id=2 but the action targets archive_id=1.
    let mover = Arc::new(NoopMover::default());
    let (conn, _agent) = InProcessAgent::spawn(
        hsm_core::AgentId::new("noop-2"),
        [ArchiveId::new(2)],
        mover.clone(),
        ProgressConfig::defaults(),
    );
    let _drain = daemon.register_agent(conn);
    let handle = daemon.start();

    ct.lock().enqueue(sample(0xb1, ActionKind::Archive, 1));

    // Give the daemon a generous window to NOT process this action.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Mover saw nothing.
    assert!(mover.invocations().is_empty());
    // Action sits in the scheduler queue.
    assert_eq!(scheduler.queue_depth(), 1);
    // Store row is still Waiting (the dispatch transition to Started
    // never happened because no agent matched).
    let row = store.get(Cookie::new(0xb1)).await.unwrap().unwrap();
    assert!(matches!(row.state, ArState::Waiting));

    handle.shutdown().await;
}
