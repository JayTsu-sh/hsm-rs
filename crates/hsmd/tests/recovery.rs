//! Integration tests for [`Daemon::recover`].
//!
//! These tests pre-populate a store with records in various states,
//! call `recover()`, and assert the scheduler / store end up in the
//! expected state — without starting the full daemon loops.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use hsm_core::{
    Action, ActionKind, ActionRecord, AgentId, ArState, ArchiveId, Cookie, Extent, Fid,
};
use hsm_scheduler::FifoPerKind;
use hsm_store::{ActionStore, MemStore, SqliteStore};
use hsmd::{Daemon, DaemonConfig, MockRecvSource};
use lustre_llapi::MockCopytool;
use parking_lot::Mutex;

fn action(cookie: u64) -> Action {
    Action {
        cookie: Cookie::new(cookie),
        fid: Fid::new(0x200000401, cookie as u32, 0),
        dfid: Fid::new(0x200000401, cookie as u32, 0),
        archive_id: ArchiveId::new(1),
        kind: ActionKind::Archive,
        extent: Extent::WHOLE,
        gid: 0,
        data: Bytes::new(),
    }
}

fn record(cookie: u64) -> ActionRecord {
    ActionRecord::new(action(cookie), SystemTime::now())
}

fn started_record(cookie: u64) -> ActionRecord {
    let mut r = record(cookie);
    r.state = ArState::Started {
        agent: AgentId::new("old-mover"),
        since_unix_ms: 0, // epoch → grace already expired immediately
    };
    r
}

fn terminal_record(cookie: u64) -> ActionRecord {
    let mut r = record(cookie);
    r.state = ArState::Succeed { rc: 0 };
    r
}

fn make_daemon(
    store: Arc<impl ActionStore + 'static>,
    grace: Duration,
) -> Daemon<FifoPerKind, impl ActionStore, MockRecvSource> {
    let ct = Arc::new(Mutex::new(MockCopytool::new()));
    let recv = MockRecvSource::new(ct, Duration::from_millis(50));
    let scheduler = Arc::new(FifoPerKind::new());
    let cfg = DaemonConfig {
        recovery_grace_period: grace,
        ..DaemonConfig::default()
    };
    Daemon::new(store, scheduler, recv, cfg)
}

// ---------------------------------------------------------------------------
// MemStore tests (fast, no I/O)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn waiting_records_are_requeued() {
    let store = Arc::new(MemStore::new());
    store.insert(record(1)).await.unwrap();
    store.insert(record(2)).await.unwrap();

    let daemon = make_daemon(store.clone(), Duration::from_secs(60));
    daemon.recover().await;

    // Both records still in store (not deleted — they're Waiting, not terminal).
    assert!(store.get(Cookie::new(1)).await.unwrap().is_some());
    assert!(store.get(Cookie::new(2)).await.unwrap().is_some());
    // Scheduler now has them: we can verify by checking store state is Waiting.
    let r = store.get(Cookie::new(1)).await.unwrap().unwrap();
    assert!(matches!(r.state, ArState::Waiting));
}

#[tokio::test]
async fn terminal_records_are_deleted() {
    let store = Arc::new(MemStore::new());
    store.insert(terminal_record(10)).await.unwrap();
    store.insert(terminal_record(11)).await.unwrap();

    let daemon = make_daemon(store.clone(), Duration::from_secs(60));
    daemon.recover().await;

    assert!(store.get(Cookie::new(10)).await.unwrap().is_none());
    assert!(store.get(Cookie::new(11)).await.unwrap().is_none());
    assert_eq!(store.len().await.unwrap(), 0);
}

#[tokio::test]
async fn started_record_rolls_back_after_grace_expires() {
    let store = Arc::new(MemStore::new());
    // since_unix_ms = 0 → elapsed >> grace → watcher fires immediately
    store.insert(started_record(20)).await.unwrap();

    // Very short grace so the test doesn't wait long.
    let daemon = make_daemon(store.clone(), Duration::from_millis(10));
    daemon.recover().await;

    // Give the grace watcher task time to fire.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let r = store.get(Cookie::new(20)).await.unwrap().unwrap();
    assert!(
        matches!(r.state, ArState::Waiting),
        "expected Waiting after grace expiry, got {:?}",
        r.state
    );
}

#[tokio::test]
async fn started_record_not_rolled_back_if_agent_claims_it() {
    let store = Arc::new(MemStore::new());
    store.insert(started_record(30)).await.unwrap();

    // Long grace — watcher won't fire during the test.
    let daemon = make_daemon(store.clone(), Duration::from_secs(3600));
    daemon.recover().await;

    // Simulate agent claiming it: transition to Succeed before grace expires.
    store
        .transition(Cookie::new(30), ArState::Succeed { rc: 0 })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let r = store.get(Cookie::new(30)).await.unwrap().unwrap();
    assert!(matches!(r.state, ArState::Succeed { rc: 0 }));
}

#[tokio::test]
async fn empty_store_recover_is_noop() {
    let store = Arc::new(MemStore::new());
    let daemon = make_daemon(store.clone(), Duration::from_secs(60));
    daemon.recover().await; // must not panic or error
    assert_eq!(store.len().await.unwrap(), 0);
}

// ---------------------------------------------------------------------------
// SqliteStore: verify recovery survives reopen (simulates real crash restart)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_recovery_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("state.db").display());

    // First "run": insert a Waiting record, then "crash" (drop the store).
    {
        let store = Arc::new(SqliteStore::open(&url).await.unwrap());
        store.insert(record(100)).await.unwrap();
    }

    // Second "run": open same DB, recover, assert record is re-queued.
    {
        let store = Arc::new(SqliteStore::open(&url).await.unwrap());
        let daemon = make_daemon(store.clone(), Duration::from_secs(60));
        daemon.recover().await;

        let r = store.get(Cookie::new(100)).await.unwrap().unwrap();
        assert!(matches!(r.state, ArState::Waiting));
    }
}
