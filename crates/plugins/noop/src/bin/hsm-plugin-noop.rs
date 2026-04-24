//! `hsm-plugin-noop` binary.
//!
//! Stand-alone reference plugin: connects to `hsmd` over a Unix Domain
//! Socket, registers the configured `archive_ids`, and runs the
//! [`NoopMover`] until the daemon closes the stream (or SIGINT/SIGTERM
//! is delivered).
//!
//! Usage:
//!   hsm-plugin-noop --config /etc/hsm-plugin-noop.toml
//!
//! Config (TOML):
//!   socket_path  = "/var/run/hsmd/agent.sock"
//!   agent_id     = "noop-1"
//!   archive_ids  = [1, 2]
//!   chunk_size_mib       = 1
//!   whole_file_mib       = 16
//!   chunk_delay_ms       = 0
//!   log_filter   = "info"     # optional
//!
//! Exits 0 on graceful shutdown, 1 on transport / fatal errors,
//! 2 on bad arguments / config.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use hsm_core::ArchiveId;
use hsm_plugin_noop::NoopMover;
use hsm_plugin_sdk::{run_with_channel, RunConfig};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use thiserror::Error;
use tokio::net::UnixStream;
use tokio::signal::unix::{signal, SignalKind};
use tonic::transport::Endpoint;
use tower::service_fn;
use tracing::{error, info, warn};

#[derive(Debug)]
struct Args {
    config_path: PathBuf,
}

fn parse_args() -> Result<Args, String> {
    let mut iter = std::env::args().skip(1);
    let mut config_path: Option<PathBuf> = None;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                config_path = iter.next().map(PathBuf::from);
                if config_path.is_none() {
                    return Err("--config requires a path argument".into());
                }
            }
            "--help" | "-h" => {
                eprintln!("usage: hsm-plugin-noop --config <path>");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let config_path = config_path.ok_or_else(|| "missing --config".to_string())?;
    Ok(Args { config_path })
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginConfig {
    /// UDS path of the daemon's agent endpoint.
    socket_path: PathBuf,
    /// Plugin's identity, surfaced in `Hello.agent_id`.
    agent_id: String,
    /// Archive ids this plugin will serve.
    archive_ids: Vec<u32>,
    /// Chunk size in MiB (default 1).
    #[serde(default = "default_chunk_mib")]
    chunk_size_mib: u64,
    /// Cap on bytes "transferred" for whole-file actions (default 16).
    #[serde(default = "default_whole_mib")]
    whole_file_mib: u64,
    /// Optional sleep between simulated chunks, ms (default 0).
    #[serde(default)]
    chunk_delay_ms: u64,
    /// Optional `RUST_LOG`-style filter (default `info`).
    #[serde(default)]
    log_filter: Option<String>,
}

fn default_chunk_mib() -> u64 {
    1
}

fn default_whole_mib() -> u64 {
    16
}

#[derive(Debug, Error)]
enum CfgError {
    #[error("read config {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse config {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
}

fn load_config(path: &Path) -> Result<PluginConfig, CfgError> {
    let text = std::fs::read_to_string(path).map_err(|e| CfgError::Read {
        path: path.into(),
        source: e,
    })?;
    toml::from_str(&text).map_err(|e| CfgError::Parse {
        path: path.into(),
        source: e,
    })
}

fn init_tracing(filter: Option<&str>) {
    let filter = filter
        .map(|s| s.to_string())
        .unwrap_or_else(|| std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()));
    let env_filter = tracing_subscriber::EnvFilter::try_new(&filter).unwrap_or_else(|_| {
        eprintln!("invalid log filter {filter:?}, falling back to 'info'");
        tracing_subscriber::EnvFilter::new("info")
    });
    // See hsmd's init_tracing comment for why we set with_writer
    // explicitly: tracing-subscriber's default termcolor writer
    // silently drops records when stderr is redirected to a non-tty
    // (subprocess pipes, `2>file`, …).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .try_init();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            eprintln!("usage: hsm-plugin-noop --config <path>");
            return std::process::ExitCode::from(2);
        }
    };

    let cfg = match load_config(&args.config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config load failed: {e}");
            return std::process::ExitCode::from(2);
        }
    };

    init_tracing(cfg.log_filter.as_deref());
    info!(
        target: "hsm.plugin.noop",
        config = %args.config_path.display(),
        socket = %cfg.socket_path.display(),
        agent = %cfg.agent_id,
        archives = ?cfg.archive_ids,
        "starting"
    );

    if cfg.archive_ids.is_empty() {
        error!(target: "hsm.plugin.noop", "archive_ids must be non-empty");
        return std::process::ExitCode::from(2);
    }

    // --- connect to daemon UDS ---------------------------------------------
    let socket_path = cfg.socket_path.clone();
    let connect_path = socket_path.clone();
    let channel = match Endpoint::try_from("http://hsmd.uds.local")
        .expect("static endpoint")
        .connect_with_connector(service_fn(move |_| {
            let path = connect_path.clone();
            async move {
                let stream = UnixStream::connect(&path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
    {
        Ok(c) => c,
        Err(e) => {
            error!(target: "hsm.plugin.noop", error = %e, path = %socket_path.display(), "connect to daemon");
            return std::process::ExitCode::from(1);
        }
    };
    info!(target: "hsm.plugin.noop", "connected to daemon");

    // --- mover --------------------------------------------------------------
    let mib = 1024u64 * 1024;
    let mover = Arc::new(NoopMover::new(
        cfg.chunk_size_mib.max(1) * mib,
        cfg.whole_file_mib.max(1) * mib,
        Duration::from_millis(cfg.chunk_delay_ms),
    ));
    let mover_for_run = mover.clone();

    // --- run the SDK loop in a task so we can race it against signals ------
    let archive_ids: Vec<ArchiveId> = cfg.archive_ids.iter().copied().map(ArchiveId::new).collect();
    let run_cfg = RunConfig::new(cfg.agent_id.clone(), archive_ids);
    let run_task = tokio::spawn(async move { run_with_channel(channel, mover_for_run, run_cfg).await });

    // --- shutdown signal ----------------------------------------------------
    let signal_task = tokio::spawn(wait_for_shutdown_signal());

    let exit = tokio::select! {
        res = run_task => match res {
            Ok(Ok(())) => {
                info!(target: "hsm.plugin.noop", "stream closed by daemon; exiting");
                std::process::ExitCode::SUCCESS
            }
            Ok(Err(e)) => {
                error!(target: "hsm.plugin.noop", error = %e, "SDK run failed");
                std::process::ExitCode::from(1)
            }
            Err(e) => {
                error!(target: "hsm.plugin.noop", error = %e, "SDK task panicked");
                std::process::ExitCode::from(1)
            }
        },
        _ = signal_task => {
            info!(target: "hsm.plugin.noop", "shutdown signal received; exiting");
            std::process::ExitCode::SUCCESS
        }
    };

    let total = mover.invocations().len();
    info!(target: "hsm.plugin.noop", invocations = total, "exited");
    exit
}

async fn wait_for_shutdown_signal() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            warn!(target: "hsm.plugin.noop", error = %e, "SIGTERM handler unavailable");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}
