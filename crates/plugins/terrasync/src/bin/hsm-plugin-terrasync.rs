//! `hsm-plugin-terrasync` binary.
//!
//! Connects to `hsmd` over UDS, registers the configured archive_ids,
//! and runs [`TerrasyncMover`] until the daemon closes the stream or
//! SIGINT/SIGTERM arrives.
//!
//! Config (TOML):
//!   socket_path     = "/var/run/hsmd/agent.sock"
//!   agent_id        = "terrasync-1"
//!   archive_ids     = [1]
//!   archive_root    = "/var/hsm-archive"   # absolute filesystem path
//!   log_filter      = "info"               # optional
//!
//! Mirrors hsm-plugin-noop's CLI / signal / tracing plumbing —
//! single entrypoint, no clap dependency, explicit
//! `with_writer(std::io::stderr)` so logs survive subprocess pipes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use hsm_core::ArchiveId;
use hsm_plugin_sdk::{run_with_channel, RunConfig};
use hsm_plugin_terrasync::TerrasyncMover;
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
                eprintln!("usage: hsm-plugin-terrasync --config <path>");
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
    socket_path: PathBuf,
    agent_id: String,
    archive_ids: Vec<u32>,
    archive_root: PathBuf,
    #[serde(default)]
    log_filter: Option<String>,
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
    // See hsmd/hsm-plugin-noop: tracing-subscriber's default writer
    // (termcolor) silently drops records when stderr is redirected.
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
            eprintln!("usage: hsm-plugin-terrasync --config <path>");
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
        target: "hsm.plugin.terrasync",
        config = %args.config_path.display(),
        socket = %cfg.socket_path.display(),
        agent = %cfg.agent_id,
        archives = ?cfg.archive_ids,
        archive_root = %cfg.archive_root.display(),
        "starting"
    );

    if cfg.archive_ids.is_empty() {
        error!(target: "hsm.plugin.terrasync", "archive_ids must be non-empty");
        return std::process::ExitCode::from(2);
    }
    if !cfg.archive_root.is_absolute() {
        error!(
            target: "hsm.plugin.terrasync",
            archive_root = %cfg.archive_root.display(),
            "archive_root must be an absolute path"
        );
        return std::process::ExitCode::from(2);
    }
    if let Err(e) = std::fs::create_dir_all(&cfg.archive_root) {
        error!(
            target: "hsm.plugin.terrasync",
            archive_root = %cfg.archive_root.display(),
            error = %e,
            "create archive_root failed"
        );
        return std::process::ExitCode::from(1);
    }

    // --- connect to daemon --------------------------------------------------
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
            error!(
                target: "hsm.plugin.terrasync",
                error = %e, path = %socket_path.display(), "connect to daemon"
            );
            return std::process::ExitCode::from(1);
        }
    };
    info!(target: "hsm.plugin.terrasync", "connected to daemon");

    let mover = Arc::new(TerrasyncMover::new(cfg.archive_root.clone()));
    let archive_ids: Vec<ArchiveId> = cfg.archive_ids.iter().copied().map(ArchiveId::new).collect();
    let run_cfg = RunConfig::new(cfg.agent_id.clone(), archive_ids);
    let run_task = tokio::spawn(async move { run_with_channel(channel, mover, run_cfg).await });

    let signal_task = tokio::spawn(wait_for_shutdown_signal());

    let exit = tokio::select! {
        res = run_task => match res {
            Ok(Ok(())) => {
                info!(target: "hsm.plugin.terrasync", "stream closed by daemon; exiting");
                std::process::ExitCode::SUCCESS
            }
            Ok(Err(e)) => {
                error!(target: "hsm.plugin.terrasync", error = %e, "SDK run failed");
                std::process::ExitCode::from(1)
            }
            Err(e) => {
                error!(target: "hsm.plugin.terrasync", error = %e, "SDK task panicked");
                std::process::ExitCode::from(1)
            }
        },
        _ = signal_task => {
            info!(target: "hsm.plugin.terrasync", "shutdown signal received; exiting");
            std::process::ExitCode::SUCCESS
        }
    };

    info!(target: "hsm.plugin.terrasync", "exited");
    exit
}

async fn wait_for_shutdown_signal() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            warn!(target: "hsm.plugin.terrasync", error = %e, "SIGTERM handler unavailable");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}
