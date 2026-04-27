//! `hsm-plugin-terrasync` binary.
//!
//! Connects to `hsmd` over UDS, registers the configured archive_ids,
//! and runs [`TerrasyncMover`] until the daemon closes the stream or
//! SIGINT/SIGTERM arrives.
//!
//! Config (TOML):
//!   socket_path       = "/var/run/hsmd/agent.sock"
//!   agent_id          = "terrasync-1"
//!   archive_ids       = [1]
//!   archive_root_url  = "file:///var/hsm-archive"            # file backend
//!   # archive_root_url = "s3+https://ak:sk@bucket.host:port" # S3 backend
//!   log_filter        = "info"   # optional
//!
//! S3 URL format (compatible with storage_v2):
//!   s3+https://ACCESS_KEY:SECRET_KEY@BUCKET.HOST:PORT
//!   s3+http://ACCESS_KEY:SECRET_KEY@BUCKET.HOST:PORT   (for plaintext, e.g. RustFS)
//!   s3://ACCESS_KEY:SECRET_KEY@BUCKET.HOST:PORT         (defaults to HTTPS)
//!
//! Example for RustFS at 192.168.50.23:9090 with bucket "hsm-archive":
//!   archive_root_url = "s3+https://minioadmin:minioadmin@hsm-archive.192.168.50.23:9090"
//!
//! Mirrors hsm-plugin-noop's CLI / signal / tracing plumbing —
//! single entrypoint, no clap dependency, explicit
//! `with_writer(std::io::stderr)` so logs survive subprocess pipes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use hsm_core::ArchiveId;
use hsm_plugin_sdk::{RunConfig, run_with_channel};
use hsm_plugin_terrasync::{ArchiveLayout, BackendScheme, BackendUrl, TerrasyncMover};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use storage_v2::{LocalStorage, StorageEnum, create_storage};
use thiserror::Error;
use tokio::net::UnixStream;
use tokio::signal::unix::{SignalKind, signal};
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
    /// Backend URL (`file://<path>`, `s3://...`, `nfs://...`,
    /// `cifs://...`). M3a wires `file://` only; other schemes parse
    /// but error at startup with a clear "not yet wired" message.
    archive_root_url: String,
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
        archive_root_url = %cfg.archive_root_url,
        "starting"
    );

    if cfg.archive_ids.is_empty() {
        error!(target: "hsm.plugin.terrasync", "archive_ids must be non-empty");
        return std::process::ExitCode::from(2);
    }

    // Parse the URL up front so we fail at startup, not on the first
    // dispatched action.
    let parsed = match BackendUrl::parse(&cfg.archive_root_url) {
        Ok(p) => p,
        Err(e) => {
            error!(
                target: "hsm.plugin.terrasync",
                url = %cfg.archive_root_url, error = %e,
                "archive_root_url parse failed"
            );
            return std::process::ExitCode::from(2);
        }
    };

    let mover = match build_mover(&cfg.archive_root_url, &parsed).await {
        Ok(m) => Arc::new(m),
        Err(e) => {
            error!(
                target: "hsm.plugin.terrasync",
                url = %cfg.archive_root_url, error = %e,
                "destination storage construction failed"
            );
            return std::process::ExitCode::from(2);
        }
    };

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

    let archive_ids: Vec<ArchiveId> = cfg
        .archive_ids
        .iter()
        .copied()
        .map(ArchiveId::new)
        .collect();
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

/// Build a [`TerrasyncMover`] for the parsed backend URL. M3a wires
/// `file://` end-to-end. Other schemes (`s3://`, `nfs://`, `cifs://`)
/// parse cleanly upstream but fail here with a "not yet wired"
/// message so deployments don't silently fall back to an unintended
/// backend.
/// Build a [`TerrasyncMover`] for the parsed backend URL.
///
/// Supported backends:
/// - `file://<path>` — local POSIX filesystem (file:// mode)
/// - `s3://<ak>:<sk>@<bucket>.<host>:<port>` — AWS S3 / MinIO / RustFS
/// - `s3+https://<ak>:<sk>@<bucket>.<host>:<port>` — S3 over HTTPS (skip TLS verify)
/// - `s3+http://<ak>:<sk>@<bucket>.<host>:<port>` — S3 over plaintext HTTP
async fn build_mover(raw_url: &str, parsed: &BackendUrl) -> Result<TerrasyncMover, String> {
    match parsed.scheme {
        BackendScheme::File => {
            if !parsed.path.is_absolute() {
                return Err(format!(
                    "file:// URL path must be absolute, got {}",
                    parsed.path.display()
                ));
            }
            std::fs::create_dir_all(&parsed.path)
                .map_err(|e| format!("create archive_root {}: {e}", parsed.path.display()))?;
            let layout = ArchiveLayout::new(parsed.path.clone());
            let dst = Arc::new(StorageEnum::Local(LocalStorage::new(
                layout.root.clone(),
                None,
            )));
            Ok(TerrasyncMover::with_dst(dst, layout))
        }
        BackendScheme::S3 => {
            // Delegate URL parsing + client construction to storage_v2.
            // create_s3_storage() handles s3://, s3+http://, s3+https://,
            // including credentials extraction and TLS configuration.
            let storage: StorageEnum = create_storage(raw_url, None)
                .await
                .map_err(|e| format!("S3 storage init failed for {raw_url}: {e}"))?;

            // Derive the archive root path (for ArchiveLayout) from the URL path.
            // For `s3://ak:sk@bucket.host:port/prefix`, path is `/prefix`.
            // If no prefix, root is just "/" relative to bucket root.
            let archive_root = if parsed.path.as_os_str().is_empty()
                || parsed.path == std::path::Path::new("/")
            {
                std::path::PathBuf::from("/")
            } else {
                parsed.path.clone()
            };
            let layout = ArchiveLayout::new(archive_root);
            let dst = Arc::new(storage);
            Ok(TerrasyncMover::with_dst(dst, layout))
        }
        BackendScheme::Nfs | BackendScheme::Cifs => Err(format!(
            "scheme {:?} not yet implemented; use file:// or s3://",
            parsed.scheme.as_str()
        )),
    }
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
