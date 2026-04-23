//! `run(mover, addr)` — gRPC client side that drives a [`Mover`] from
//! a remote `hsmd`.
//!
//! Hosts the [`Mover`] inside a tokio process, opens a single bidi
//! stream to the daemon (`DataMover::open`), sends `Hello` first, then:
//!
//! - For each `ToPlugin::Action(item)` received:
//!   - If `kind == Cancel` → trip the cookie's [`CancellationToken`].
//!   - Otherwise → spawn a task running the matching `Mover` method,
//!     forwarding throttled progress + the terminal status back over
//!     the same bidi stream.
//!
//! Returns when the bidi stream ends (server closed, transport drop,
//! or explicit shutdown) or on a fatal protocol error.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use hsm_core::{ActionKind, ArchiveId, BackendObject, Cookie, Extent, Fid};
use hsm_proto::v1 as pb;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

use crate::ctx::ActionCtxBuilder;
use crate::error::{MoverError, MoverResult};
use crate::mover::Mover;
use crate::progress::{ProgressConfig, ProgressReporter};
use tokio_util::sync::CancellationToken;

/// Configuration for [`run`].
#[derive(Clone, Debug)]
pub struct RunConfig {
    /// Plugin's identity. Surfaced to the daemon via `Hello.agent_id`.
    pub agent_id: String,
    /// Archive ids this plugin is willing to serve.
    pub archive_ids: Vec<ArchiveId>,
    /// Plugin's own version string. Surfaced via `Hello.version`.
    pub version: String,
    /// Throttling tunables for the per-action [`ProgressReporter`].
    pub progress: ProgressConfig,
    /// Capacity of the inbound action work queue between the bidi
    /// stream reader and the per-action task spawner.
    pub action_buffer: usize,
    /// Capacity of the outbound status queue between the per-action
    /// tasks and the bidi stream writer.
    pub status_buffer: usize,
}

impl RunConfig {
    /// Sensible defaults — tweak `agent_id` / `archive_ids` / `version`
    /// then leave the rest alone unless you have a reason.
    pub fn new(agent_id: impl Into<String>, archive_ids: Vec<ArchiveId>) -> Self {
        Self {
            agent_id: agent_id.into(),
            archive_ids,
            version: env!("CARGO_PKG_VERSION").to_string(),
            progress: ProgressConfig::defaults(),
            action_buffer: 64,
            status_buffer: 256,
        }
    }
}

/// Errors `run` can return.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// Failed to connect to the daemon's gRPC endpoint.
    #[error("connect to daemon: {0}")]
    Connect(#[from] tonic::transport::Error),
    /// gRPC stream-level failure (server closed, network drop, etc.).
    #[error("grpc stream: {0}")]
    Stream(#[from] tonic::Status),
    /// Daemon's first reply was not the expected `Welcome`.
    #[error("expected Welcome from daemon, got {0:?}")]
    HandshakeFailed(Option<pb::to_plugin::Body>),
    /// The daemon closed the inbound stream before sending Welcome.
    #[error("daemon closed stream before Welcome")]
    NoWelcome,
}

/// Result alias for [`run`].
pub type RunResult<T> = Result<T, RunError>;

/// Run the plugin: connect to `daemon_endpoint`, register with
/// `config`, and drive `mover` until the daemon closes the stream.
///
/// `daemon_endpoint` is anything `tonic::transport::Endpoint` accepts.
/// For the production path it'll be a `unix://...` URI; tests use a
/// pre-built [`Channel`] via [`run_with_channel`].
pub async fn run<M: Mover>(
    daemon_endpoint: impl TryInto<Endpoint, Error = tonic::transport::Error>,
    mover: Arc<M>,
    config: RunConfig,
) -> RunResult<()> {
    let endpoint = daemon_endpoint.try_into()?;
    let channel = endpoint.connect().await?;
    run_with_channel(channel, mover, config).await
}

/// Variant of [`run`] taking a pre-built [`Channel`].
///
/// Useful for tests that wire an in-memory transport (or a custom UDS
/// connector that needs special configuration the [`Endpoint`] DSL
/// doesn't reach).
pub async fn run_with_channel<M: Mover>(
    channel: Channel,
    mover: Arc<M>,
    config: RunConfig,
) -> RunResult<()> {
    let mut client = pb::data_mover_client::DataMoverClient::new(channel);

    // Outbound channel: per-action tasks push FromPlugin envelopes here;
    // we drain it into the gRPC client stream.
    let (out_tx, out_rx) = mpsc::channel::<pb::FromPlugin>(config.status_buffer);

    // Send Hello as the first outbound message.
    out_tx
        .send(pb::FromPlugin {
            body: Some(pb::from_plugin::Body::Hello(pb::Hello {
                agent_id: config.agent_id.clone(),
                archive_ids: config.archive_ids.iter().map(|a| a.get()).collect(),
                version: config.version.clone(),
            })),
        })
        .await
        .map_err(|_| RunError::Stream(tonic::Status::aborted("outbound channel dropped")))?;

    let outbound = ReceiverStream::new(out_rx);
    let mut inbound = client.open(outbound).await?.into_inner();

    // First inbound must be Welcome.
    let welcome_msg = inbound.next().await.ok_or(RunError::NoWelcome)??;
    match welcome_msg.body {
        Some(pb::to_plugin::Body::Welcome(w)) => {
            info!(
                target: "hsm.sdk.run",
                agent = %config.agent_id, session = %w.session_id,
                "registered with daemon"
            );
        }
        other => return Err(RunError::HandshakeFailed(other)),
    }

    // Per-cookie cancellation token map. The action loop reads from it
    // when an Action(Cancel) arrives; per-action tasks insert/remove.
    let cancels: Arc<Mutex<HashMap<Cookie, CancellationToken>>> = Arc::default();

    // Action loop. Each ActionItem either spawns a per-action task or
    // (for Cancel) trips the matching token.
    while let Some(msg) = inbound.next().await {
        let env = match msg {
            Ok(e) => e,
            Err(e) => return Err(RunError::Stream(e)),
        };
        let item = match env.body {
            Some(pb::to_plugin::Body::Action(a)) => a,
            Some(pb::to_plugin::Body::Welcome(_)) => {
                warn!(target: "hsm.sdk.run", "duplicate Welcome ignored");
                continue;
            }
            None => {
                warn!(target: "hsm.sdk.run", "ToPlugin with empty body ignored");
                continue;
            }
        };
        handle_action_item(item, mover.clone(), out_tx.clone(), cancels.clone(), config.progress);
    }

    info!(target: "hsm.sdk.run", agent = %config.agent_id, "stream closed; exiting");
    Ok(())
}

fn handle_action_item<M: Mover>(
    item: pb::ActionItem,
    mover: Arc<M>,
    out_tx: mpsc::Sender<pb::FromPlugin>,
    cancels: Arc<Mutex<HashMap<Cookie, CancellationToken>>>,
    progress_config: ProgressConfig,
) {
    let cookie = Cookie::new(item.cookie);
    let kind_i = item.kind;
    let kind_enum = pb::ActionKind::try_from(kind_i).unwrap_or(pb::ActionKind::Unspecified);

    if matches!(kind_enum, pb::ActionKind::Cancel) {
        debug!(target: "hsm.sdk.run", %cookie, "cancel signal");
        if let Some(t) = cancels.lock().get(&cookie) {
            t.cancel();
        } else {
            // Could be a cancel for an action that already finished;
            // the daemon learns of completion before its cancel sweep.
            debug!(target: "hsm.sdk.run", %cookie, "cancel for unknown cookie (already done?)");
        }
        return;
    }

    let action_kind = match kind_enum {
        pb::ActionKind::Archive => ActionKind::Archive,
        pb::ActionKind::Restore => ActionKind::Restore,
        pb::ActionKind::Remove => ActionKind::Remove,
        _ => {
            warn!(target: "hsm.sdk.run", %cookie, "unknown action kind {kind_i}; reporting failure");
            send_failed_blocking(out_tx, cookie, 22 /* EINVAL */);
            return;
        }
    };

    // Per-action progress channel — events get translated to outbound
    // ActionStatus envelopes by a small forwarder task.
    let extent = Extent::new(item.offset, item.length);
    let (progress, mut prog_rx) = ProgressReporter::new(cookie, extent, progress_config);
    let cancel_token = CancellationToken::new();
    cancels.lock().insert(cookie, cancel_token.clone());

    let forwarder_out = out_tx.clone();
    let forwarder = tokio::spawn(async move {
        while let Some(ev) = prog_rx.recv().await {
            let status = pb::ActionStatus {
                cookie: ev.cookie.get(),
                bytes_advanced: ev.bytes_advanced,
                completed: false,
                errno: 0,
                result: None,
            };
            let _ = forwarder_out
                .send(pb::FromPlugin { body: Some(pb::from_plugin::Body::Status(status)) })
                .await;
        }
    });

    let ctx = build_ctx_from_proto(&item, action_kind, extent, progress.clone(), cancel_token);
    let existing_obj = item.existing.map(proto_to_backend_obj);

    tokio::spawn(async move {
        let result: MoverResult<Option<BackendObject>> = match action_kind {
            ActionKind::Archive => mover.archive(ctx).await.map(Some),
            ActionKind::Restore => match existing_obj.clone() {
                Some(obj) => mover.restore(ctx, obj).await.map(|()| None),
                None => Err(MoverError::Other(format!(
                    "restore for {cookie} missing existing BackendObject"
                ))),
            },
            ActionKind::Remove => match existing_obj.clone() {
                Some(obj) => mover.remove(ctx, obj).await.map(|()| None),
                None => Err(MoverError::Other(format!(
                    "remove for {cookie} missing existing BackendObject"
                ))),
            },
            ActionKind::Cancel => unreachable!("Cancel handled above"),
        };

        // Snapshot then drop the local progress so the forwarder loop
        // can exit. Same dance as inproc.rs (M2d.1 bug fix).
        let total_bytes = progress.total_advanced();
        drop(progress);
        let _ = forwarder.await;

        let terminal = match result {
            Ok(maybe_obj) => pb::ActionStatus {
                cookie: cookie.get(),
                bytes_advanced: total_bytes,
                completed: true,
                errno: 0,
                result: maybe_obj.map(backend_obj_to_proto),
            },
            Err(e) => {
                error!(target: "hsm.sdk.run", %cookie, error = %e, "mover failed");
                pb::ActionStatus {
                    cookie: cookie.get(),
                    bytes_advanced: total_bytes,
                    completed: true,
                    errno: e.errno(),
                    result: None,
                }
            }
        };

        cancels.lock().remove(&cookie);

        if out_tx
            .send(pb::FromPlugin { body: Some(pb::from_plugin::Body::Status(terminal)) })
            .await
            .is_err()
        {
            warn!(target: "hsm.sdk.run", %cookie, "outbound closed; terminal status dropped");
        }
    });
}

fn send_failed_blocking(out_tx: mpsc::Sender<pb::FromPlugin>, cookie: Cookie, errno: i32) {
    tokio::spawn(async move {
        let _ = out_tx
            .send(pb::FromPlugin {
                body: Some(pb::from_plugin::Body::Status(pb::ActionStatus {
                    cookie: cookie.get(),
                    bytes_advanced: 0,
                    completed: true,
                    errno,
                    result: None,
                })),
            })
            .await;
    });
}

fn build_ctx_from_proto(
    item: &pb::ActionItem,
    kind: ActionKind,
    extent: Extent,
    progress: ProgressReporter,
    cancel: CancellationToken,
) -> crate::ctx::ActionCtx {
    let mut b = ActionCtxBuilder::default()
        .cookie(Cookie::new(item.cookie))
        .fid(Fid::ZERO) // Daemon strips fid in M2d.2; FID resolution is M2d.2c.
        .archive_id(ArchiveId::new(item.archive_id))
        .kind(kind)
        .extent(extent)
        .primary_path(PathBuf::from(&item.primary_path))
        .hint(Bytes::from(item.data.clone()))
        .progress(progress)
        .cancel(cancel);
    if !item.write_path.is_empty() {
        b = b.write_path(PathBuf::from(&item.write_path));
    }
    if let Some(obj) = &item.existing {
        b = b.existing(BackendObject {
            uuid: obj.uuid.clone(),
            hash: hash_from_proto(&obj.hash),
            url: obj.url.clone(),
        });
    }
    b.build()
}

fn proto_to_backend_obj(o: pb::BackendObject) -> BackendObject {
    BackendObject {
        uuid: o.uuid,
        hash: hash_from_proto(&o.hash),
        url: o.url,
    }
}

fn backend_obj_to_proto(o: BackendObject) -> pb::BackendObject {
    pb::BackendObject {
        uuid: o.uuid,
        hash: o.hash.to_vec(),
        url: o.url,
    }
}

fn hash_from_proto(h: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    let n = h.len().min(32);
    out[..n].copy_from_slice(&h[..n]);
    out
}
