//! End-to-end gRPC test: daemon ↔ plugin via a real UDS, no Lustre.
//!
//! Replays the M2d.1 scenarios (`e2e_mock.rs`) but routes the agent
//! plumbing through `hsmd::GrpcAgentService` + `hsm_plugin_sdk::run`.
//! Same `MockCopytool` recv source, same `NoopMover`, same `MemStore`
//! and `FifoPerKind` — only the agent transport changes.
//!
//! If anything between the daemon's `dispatch_one` and the mover's
//! `archive` differs in observable behavior compared to the in-process
//! agent, that's an IPC bug we want to catch here, not in production.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hsm_core::{ActionKind, AgentId, ArState, ArchiveId, Cookie, Extent, Fid};
use hsm_plugin_noop::NoopMover;
use hsm_plugin_sdk::{ProgressConfig, RunConfig, run_with_channel};
use hsm_proto::v1::data_mover_server::DataMoverServer;
use hsm_scheduler::FifoPerKind;
use hsm_store::{ActionStore, MemStore};
use hsmd::{AgentRegistrar, Daemon, DaemonConfig, DaemonHandle, GrpcAgentService, MockRecvSource};
use hyper_util::rt::TokioIo;
use lustre_llapi::{MockCopytool, ReceivedAction};
use parking_lot::Mutex;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Endpoint, Server};
use tower::service_fn;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Self-cleaning UDS path under /tmp (one per test).
struct TempUds(PathBuf);

impl TempUds {
    fn new(name: &str) -> Self {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = PathBuf::from(format!("/tmp/hsmd-e2e-{name}-{pid}-{nanos}.sock"));
        let _ = std::fs::remove_file(&path);
        Self(path)
    }
    fn path(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for TempUds {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
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

struct E2eRig {
    store: Arc<MemStore>,
    ct: Arc<Mutex<MockCopytool>>,
    // Tests grab the original Arc<NoopMover> from `spawn_rig`'s caller;
    // `mover` is kept here only to extend its lifetime to the rig's
    // shutdown, so prefix with `_` to silence dead_code.
    _mover: Arc<NoopMover>,
    handle: DaemonHandle,
    server_task: tokio::task::JoinHandle<()>,
    plugin_task: tokio::task::JoinHandle<hsm_plugin_sdk::RunResult<()>>,
    _uds: TempUds,
}

impl E2eRig {
    async fn shutdown(self) {
        // Plugin first: closes the gRPC stream, the server side sees
        // EOF and tears down its forwarders. Then stop the daemon.
        self.plugin_task.abort();
        let _ = self.plugin_task.await;
        self.handle.shutdown().await;
        self.server_task.abort();
        let _ = self.server_task.await;
    }
}

/// Spin up daemon + gRPC server + plugin, all over a temp UDS.
async fn spawn_rig(name: &str, mover: Arc<NoopMover>, plugin_archives: Vec<ArchiveId>) -> E2eRig {
    init_tracing();

    let store = Arc::new(MemStore::new());
    let scheduler = Arc::new(FifoPerKind::new());
    let ct = Arc::new(Mutex::new(MockCopytool::new()));
    let recv = MockRecvSource::new(ct.clone(), Duration::from_millis(5));
    let daemon = Daemon::new(store.clone(), scheduler, recv, DaemonConfig::default());

    // Start the daemon FIRST — DaemonHandle exposes registrar() which
    // gives us the gRPC service's enrollment callback. The handle stays
    // owned by `spawn_rig` (no Arc dance) since registrar() captures
    // only the mpsc sender it needs.
    let handle = daemon.start();
    let registrar: AgentRegistrar = handle.registrar();

    // Bind UDS server.
    let uds = TempUds::new(name);
    let listener = UnixListener::bind(uds.path()).expect("bind UDS");
    let incoming = UnixListenerStream::new(listener);
    let svc = GrpcAgentService::new(registrar);
    let server_task = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(DataMoverServer::new(svc))
            .serve_with_incoming(incoming)
            .await;
    });

    // Wait briefly for the server to be ready to accept connections.
    // tokio::net::UnixListener::bind is synchronous so the socket exists,
    // but tonic needs a tick to start serving; a tight retry loop on the
    // client side handles transient ECONNREFUSED.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Build client channel that dials the same UDS.
    let path = uds.path().clone();
    let channel = Endpoint::try_from("http://hsmd.uds.local")
        .expect("static endpoint")
        .connect_with_connector(service_fn(move |_| {
            let path = path.clone();
            async move {
                let stream = UnixStream::connect(&path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .expect("connect to UDS");

    let plugin_cfg = RunConfig {
        progress: ProgressConfig::defaults(),
        ..RunConfig::new("noop-grpc", plugin_archives)
    };
    let mover_for_plugin = mover.clone();
    let plugin_task =
        tokio::spawn(async move { run_with_channel(channel, mover_for_plugin, plugin_cfg).await });

    // Wait for the agent to register before returning. registry() lives
    // on the owned handle — we just borrow it.
    let registered = wait_until(Duration::from_secs(2), || {
        let h = &handle;
        async move { !h.registry().is_empty() }
    })
    .await;
    assert!(registered, "plugin failed to register within 2s");

    E2eRig {
        store,
        ct,
        _mover: mover,
        handle,
        server_task,
        plugin_task,
        _uds: uds,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_action_flows_through_grpc_pipeline() {
    let mover = Arc::new(NoopMover::default());
    let rig = spawn_rig("archive", mover.clone(), vec![ArchiveId::new(1)]).await;

    rig.ct.lock().enqueue(sample(0xa1, ActionKind::Archive, 1));

    let store = rig.store.clone();
    let ok = wait_until(Duration::from_secs(3), || {
        let store = store.clone();
        async move {
            matches!(
                store.get(Cookie::new(0xa1)).await.unwrap().map(|r| r.state),
                Some(ArState::Succeed { .. })
            )
        }
    })
    .await;
    assert!(
        ok,
        "expected cookie 0xa1 to reach Succeed within 3s over gRPC"
    );

    let invs = mover.invocations();
    assert_eq!(invs.len(), 1);
    assert_eq!(invs[0].kind, ActionKind::Archive);
    assert!(!invs[0].cancelled);

    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_actions_complete_concurrently_over_grpc() {
    let mover = Arc::new(NoopMover::default());
    let rig = spawn_rig("many", mover.clone(), vec![ArchiveId::new(1)]).await;

    {
        let mut g = rig.ct.lock();
        for i in 0..20u64 {
            g.enqueue(sample(0x100 + i, ActionKind::Archive, 1));
        }
    }

    let mover_check = mover.clone();
    let ok = wait_until(Duration::from_secs(10), || {
        let m = mover_check.clone();
        async move { m.invocations().len() == 20 }
    })
    .await;
    assert!(
        ok,
        "expected 20 invocations, got {}",
        mover.invocations().len()
    );
    assert!(mover.invocations().iter().all(|i| !i.cancelled));

    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_propagates_via_grpc_to_mover() {
    let mover = Arc::new(NoopMover::new(
        1024 * 1024,
        16 * 1024 * 1024,
        Duration::from_millis(50),
    ));
    let rig = spawn_rig("cancel", mover.clone(), vec![ArchiveId::new(1)]).await;

    let cookie = Cookie::new(0xc1);
    rig.ct
        .lock()
        .enqueue(sample(cookie.get(), ActionKind::Archive, 1));

    // Wait for dispatch before cancelling.
    let s2 = rig.store.clone();
    let dispatched = wait_until(Duration::from_secs(2), || {
        let s = s2.clone();
        async move {
            matches!(
                s.get(cookie).await.unwrap().map(|r| r.state),
                Some(ArState::Started { .. })
            )
        }
    })
    .await;
    assert!(dispatched, "action never reached Started");

    // Trip cancel via DaemonHandle::cancel — same path the future
    // hsmctl will use.
    let cancelled = rig.handle.cancel(&AgentId::new("noop-grpc"), cookie).await;
    assert!(cancelled, "cancel send returned false");

    let s3 = rig.store.clone();
    let terminal = wait_until(Duration::from_secs(3), || {
        let s = s3.clone();
        async move {
            matches!(
                s.get(cookie).await.unwrap().map(|r| r.state),
                Some(ArState::Failed { rc: 125 })
            )
        }
    })
    .await;
    assert!(terminal, "expected Failed{{rc:125}} after gRPC cancel");

    let invs = mover.invocations();
    assert_eq!(invs.len(), 1);
    assert!(invs[0].cancelled);

    rig.shutdown().await;
}
