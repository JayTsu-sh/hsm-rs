//! `hsmd` binary entry point.
//!
//! Wires:
//!   1. Config load (TOML).
//!   2. Tracing subscriber init (filter + format).
//!   3. RecvSource by mode (mock today, live in M2d.2c).
//!   4. Daemon::start (recv + dispatch + status drain + enroll loops).
//!   5. UDS-bound tonic server hosting GrpcAgentService.
//!   6. SIGINT / SIGTERM → graceful shutdown.
//!
//! Usage:
//!   hsmd --config /etc/hsmd.toml
//!
//! Mock-action file (optional, only honoured in `mode = "mock"`):
//!   each line of `mock_actions_file` is one MockAction in JSON form.
//!   The file is polled every 200ms; new lines (delta from last
//!   read) get enqueued via the in-process MockCopytool. Useful for
//!   local dev / smoke tests without a Lustre kernel module.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hsm_core::{ActionKind, ArchiveId, Cookie, Extent, Fid};
use hsm_proto::v1::data_mover_server::DataMoverServer;
use hsm_scheduler::FifoPerKind;
use hsm_store::MemStore;
use hsmd::config::{HsmdConfig, LogFormat, MockAction, Mode};
use hsmd::{Daemon, GrpcAgentService, MockRecvSource};
use lustre_llapi::{MockCopytool, ReceivedAction};
use parking_lot::Mutex;
use tokio::net::UnixListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tracing::{error, info, warn};

#[derive(Debug)]
struct Args {
    config_path: PathBuf,
}

fn parse_args() -> Result<Args, String> {
    // No clap dependency — we have one option, parse it by hand.
    let mut iter = std::env::args().skip(1);
    let mut config_path: Option<PathBuf> = None;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                config_path = iter.next().map(PathBuf::from).or(None);
                if config_path.is_none() {
                    return Err("--config requires a path argument".into());
                }
            }
            "--help" | "-h" => {
                eprintln!("usage: hsmd --config <path-to-hsmd.toml>");
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {other}"));
            }
        }
    }
    let config_path = config_path.ok_or_else(|| "missing --config".to_string())?;
    Ok(Args { config_path })
}

fn init_tracing(cfg: &hsmd::config::LogCfg) {
    let filter = cfg
        .filter
        .clone()
        .unwrap_or_else(|| std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()));
    let env_filter = tracing_subscriber::EnvFilter::try_new(&filter).unwrap_or_else(|_| {
        eprintln!("invalid log filter {filter:?}, falling back to 'info'");
        tracing_subscriber::EnvFilter::new("info")
    });
    // `with_writer(std::io::stderr)` is intentional: the default
    // tracing-subscriber writer routes through `termcolor` which does
    // not follow shell `2>file` redirections in spawned subprocesses
    // (records get dropped silently). Using std::io::stderr writes to
    // fd 2 directly so subprocess pipes / file redirects work.
    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr);
    let _ = match cfg.format {
        LogFormat::Pretty => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            eprintln!("usage: hsmd --config <path-to-hsmd.toml>");
            return std::process::ExitCode::from(2);
        }
    };

    let cfg = match HsmdConfig::load(&args.config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config load failed: {e}");
            return std::process::ExitCode::from(2);
        }
    };

    init_tracing(&cfg.log);
    info!(target: "hsmd", config = %args.config_path.display(), mode = ?cfg.mode, "starting");

    if matches!(cfg.mode, Mode::Live) {
        error!(
            target: "hsmd",
            "mode = \"live\" requires LiveCopytool integration, which lands in M2d.2c. \
             Use mode = \"mock\" for now (set mock_actions_file to drive synthetic actions)."
        );
        return std::process::ExitCode::from(2);
    }

    // --- recv source (mock) -------------------------------------------------
    let mock_ct = Arc::new(Mutex::new(MockCopytool::new()));
    let recv = MockRecvSource::new(mock_ct.clone(), Duration::from_millis(50));

    // --- daemon -------------------------------------------------------------
    let store = Arc::new(MemStore::new());
    let scheduler = Arc::new(FifoPerKind::new());
    let daemon = Daemon::new(store, scheduler, recv, cfg.to_daemon_config());
    let handle = daemon.start();
    info!(target: "hsmd", "daemon loops running");

    // --- gRPC server --------------------------------------------------------
    let socket_path = cfg.transport.socket_path.clone();
    if let Some(parent) = socket_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                error!(target: "hsmd", error = %e, dir = %parent.display(), "create socket dir");
                return std::process::ExitCode::from(1);
            }
        }
    }
    // Best-effort: remove any leftover socket from a previous (crashed) run.
    let _ = std::fs::remove_file(&socket_path);
    let listener = match UnixListener::bind(&socket_path) {
        Ok(l) => l,
        Err(e) => {
            error!(target: "hsmd", error = %e, path = %socket_path.display(), "bind UDS");
            return std::process::ExitCode::from(1);
        }
    };
    info!(target: "hsmd", path = %socket_path.display(), "UDS server bound");

    let registrar = handle.registrar();
    let svc = GrpcAgentService::new(registrar);
    let incoming = UnixListenerStream::new(listener);
    let server_shutdown = tokio_util::sync::CancellationToken::new();
    let server_shutdown_inner = server_shutdown.clone();
    let server_task = tokio::spawn(async move {
        let res = Server::builder()
            .add_service(DataMoverServer::new(svc))
            .serve_with_incoming_shutdown(incoming, async move {
                server_shutdown_inner.cancelled().await;
            })
            .await;
        if let Err(e) = res {
            error!(target: "hsmd.grpc", error = %e, "server exited with error");
        }
    });

    // --- mock-actions file watcher (optional) -------------------------------
    let mock_watcher_task = cfg
        .mock_actions_file
        .clone()
        .map(|path| spawn_mock_watcher(path, mock_ct.clone()));

    // --- shutdown signal handling ------------------------------------------
    info!(target: "hsmd", "ready (Ctrl-C or SIGTERM to stop)");
    wait_for_shutdown_signal().await;
    info!(target: "hsmd", "shutdown signal received");

    // 1) close server (stops accepting new connections, drains in-flight RPCs)
    server_shutdown.cancel();
    let _ = server_task.await;

    // 2) stop mock watcher
    if let Some(t) = mock_watcher_task {
        t.abort();
    }

    // 3) stop daemon (recv loop + dispatch tick + enroll task)
    handle.shutdown().await;

    // 4) cleanup socket
    let _ = std::fs::remove_file(&socket_path);

    info!(target: "hsmd", "exited cleanly");
    std::process::ExitCode::SUCCESS
}

async fn wait_for_shutdown_signal() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            warn!(target: "hsmd", error = %e, "SIGTERM handler unavailable");
            // Fall back to ctrl_c only.
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

// ---------------------------------------------------------------------------
// mock-actions JSONL watcher
// ---------------------------------------------------------------------------

fn spawn_mock_watcher(path: PathBuf, ct: Arc<Mutex<MockCopytool>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(target: "hsmd.mock", path = %path.display(), "watching for actions");
        let mut last_seen_lines = 0usize;
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let new_total = match read_actions(&path, &ct, last_seen_lines).await {
                Ok(n) => n,
                Err(e) => {
                    warn!(target: "hsmd.mock", error = %e, "read failed");
                    last_seen_lines
                }
            };
            last_seen_lines = new_total;
        }
    })
}

async fn read_actions(
    path: &Path,
    ct: &Arc<Mutex<MockCopytool>>,
    skip_lines: usize,
) -> std::io::Result<usize> {
    let text = match tokio::fs::read_to_string(path).await {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(skip_lines),
        Err(e) => return Err(e),
    };
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let total = lines.len();
    if total <= skip_lines {
        return Ok(total);
    }
    for (i, line) in lines.iter().enumerate().skip(skip_lines) {
        match serde_json::from_str::<MockAction>(line) {
            Ok(a) => match enqueue_mock_action(ct, a) {
                Ok(()) => {}
                Err(e) => warn!(target: "hsmd.mock", line = i + 1, error = %e, "skipped"),
            },
            Err(e) => warn!(target: "hsmd.mock", line = i + 1, error = %e, "skipped non-JSON"),
        }
    }
    Ok(total)
}

fn enqueue_mock_action(ct: &Arc<Mutex<MockCopytool>>, a: MockAction) -> Result<(), String> {
    let kind = match a.kind.as_str() {
        "archive" => ActionKind::Archive,
        "restore" => ActionKind::Restore,
        "remove" => ActionKind::Remove,
        "cancel" => ActionKind::Cancel,
        other => return Err(format!("unknown kind {other:?}")),
    };
    let extent = match a.length {
        None => Extent::WHOLE,
        Some(0) => Extent::WHOLE,
        Some(n) => Extent::new(0, n),
    };
    let received = ReceivedAction {
        cookie: Cookie::new(a.cookie),
        fid: Fid::new(a.fid_seq, a.fid_oid, a.fid_ver),
        dfid: Fid::new(a.fid_seq, a.fid_oid, a.fid_ver),
        archive_id: ArchiveId::new(a.archive_id),
        kind,
        extent,
        gid: 0,
        data: Bytes::new(),
        fsname: "mock".into(),
    };
    ct.lock().enqueue(received);
    Ok(())
}
