//! In-process [`Agent`] backed by a [`Mover`] running on tokio tasks.
//!
//! The daemon hands an [`AgentConn`] to its registry; the
//! [`AgentSink`] half is consumed by the agent task spawned here. From
//! the daemon's perspective an in-process agent is identical to a gRPC
//! one — same channels, same status semantics — which is the point of
//! the trait split.
//!
//! Used by:
//! - The end-to-end integration test (`tests/e2e_mock.rs`)
//! - Future "co-located" deployments where the daemon embeds a mover
//!   directly to skip IPC for trusted backends

use std::collections::HashMap;
use std::sync::Arc;

use hsm_core::{ActionKind, AgentId, ArchiveId, Cookie};
use hsm_plugin_sdk::{
    ActionCtx, ActionCtxBuilder, CancellationToken, MoverError, MoverResult, ProgressConfig,
    ProgressReporter,
};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use crate::agent::{ActionStatus, AgentConn, AgentSink, DispatchedAction, failed_from_error};

/// Spawn an in-process agent that dispatches work to `mover`. Returns a
/// pre-built [`AgentConn`] the daemon can register; the spawned task
/// keeps running until `action_tx` is dropped.
///
/// `progress_config` controls the throttling of `ProgressEvent` →
/// daemon `ActionStatus::Progress` translation. Defaults are usually
/// fine; tests use a tighter threshold to see more events.
pub struct InProcessAgent {
    /// JoinHandle for the agent's main task. Held so callers can `await`
    /// shutdown if they want a clean close.
    pub task: JoinHandle<()>,
    /// Cancel tokens per in-flight cookie — owned by the agent so the
    /// dispatcher's cancel signal can fire them.
    cancels: Arc<Mutex<HashMap<Cookie, CancellationToken>>>,
}

impl InProcessAgent {
    /// Spawn the agent; return the daemon-side handle and the
    /// `InProcessAgent` (which holds the agent's task).
    pub fn spawn<M>(
        id: AgentId,
        archives: impl IntoIterator<Item = ArchiveId>,
        mover: Arc<M>,
        progress_config: ProgressConfig,
    ) -> (AgentConn, Self)
    where
        M: hsm_plugin_sdk::Mover,
    {
        let (conn, sink) = AgentSink::pair(
            id.clone(),
            archives,
            /* action */ 64,
            /* status */ 256,
        );
        let cancels: Arc<Mutex<HashMap<Cookie, CancellationToken>>> = Arc::default();
        let task = tokio::spawn(run_agent(id, mover, sink, cancels.clone(), progress_config));
        (conn, Self { task, cancels })
    }

    /// Number of currently-in-flight cookies inside this agent. Useful
    /// for tests waiting for drain.
    pub fn inflight(&self) -> usize {
        self.cancels.lock().len()
    }
}

async fn run_agent<M: hsm_plugin_sdk::Mover>(
    id: AgentId,
    mover: Arc<M>,
    mut sink: AgentSink,
    cancels: Arc<Mutex<HashMap<Cookie, CancellationToken>>>,
    progress_config: ProgressConfig,
) {
    debug!(target: "hsm.inproc", agent = %id, "agent task started");
    loop {
        tokio::select! {
            // Dispatcher pushes new work.
            maybe_action = sink.action_rx.recv() => {
                let Some(action) = maybe_action else {
                    debug!(target: "hsm.inproc", agent = %id, "action_rx closed; exiting");
                    return;
                };
                spawn_action_task(
                    mover.clone(),
                    action,
                    sink.status_tx.clone(),
                    cancels.clone(),
                    progress_config,
                );
            }

            // Dispatcher signals cancel for some in-flight cookie.
            maybe_cookie = sink.cancel_rx.recv() => {
                let Some(cookie) = maybe_cookie else {
                    debug!(target: "hsm.inproc", agent = %id, "cancel_rx closed");
                    continue;
                };
                if let Some(token) = cancels.lock().get(&cookie) {
                    token.cancel();
                } else {
                    warn!(target: "hsm.inproc", agent = %id, %cookie, "cancel for unknown cookie");
                }
            }
        }
    }
}

fn spawn_action_task<M: hsm_plugin_sdk::Mover>(
    mover: Arc<M>,
    action: DispatchedAction,
    status_tx: mpsc::Sender<ActionStatus>,
    cancels: Arc<Mutex<HashMap<Cookie, CancellationToken>>>,
    progress_config: ProgressConfig,
) {
    let cookie = action.action.cookie;
    let extent = action.action.extent;
    let kind = action.action.kind;

    // Per-action progress channel; we forward each event into the agent's
    // single status_tx as ActionStatus::Progress.
    let (progress, mut prog_rx) = ProgressReporter::new(cookie, extent, progress_config);
    let cancel_token = CancellationToken::new();
    cancels.lock().insert(cookie, cancel_token.clone());

    // Forwarder: drains the per-action progress channel into the agent's
    // shared status channel until the action's progress reporter is
    // dropped (which happens when the mover task returns).
    let forwarder_status_tx = status_tx.clone();
    let forwarder = tokio::spawn(async move {
        while let Some(ev) = prog_rx.recv().await {
            let _ = forwarder_status_tx
                .send(ActionStatus::Progress {
                    cookie: ev.cookie,
                    bytes_advanced: ev.bytes_advanced,
                })
                .await;
        }
    });

    // Build the ActionCtx the mover sees.
    let ctx = build_ctx(&action, progress.clone(), cancel_token);

    // Run the mover on its own task so per-action work is independent.
    tokio::spawn(async move {
        let result: MoverResult<Option<hsm_core::BackendObject>> = match kind {
            ActionKind::Archive => mover.archive(ctx).await.map(Some),
            ActionKind::Restore => {
                let obj = action.existing.clone().ok_or_else(|| {
                    MoverError::Other(format!(
                        "restore for {cookie} missing existing BackendObject"
                    ))
                });
                match obj {
                    Ok(obj) => mover.restore(ctx, obj).await.map(|()| None),
                    Err(e) => Err(e),
                }
            }
            ActionKind::Remove => {
                let obj = action.existing.clone().ok_or_else(|| {
                    MoverError::Other(format!(
                        "remove for {cookie} missing existing BackendObject"
                    ))
                });
                match obj {
                    Ok(obj) => mover.remove(ctx, obj).await.map(|()| None),
                    Err(e) => Err(e),
                }
            }
            ActionKind::Cancel => {
                // Cancel doesn't get a Mover invocation — the daemon
                // routes Cancel through cancel_tx instead.
                Err(MoverError::Other(
                    "Cancel routed to mover (daemon bug)".into(),
                ))
            }
        };

        // Snapshot the total *before* dropping `progress` — once we drop
        // it, the reporter is gone. Then drop both senders we hold so the
        // forwarder's `prog_rx.recv()` returns None and its loop exits.
        // The `ctx` already moved into the mover and was dropped when the
        // mover returned; we control the only remaining sender here.
        let total_bytes = progress.total_advanced();
        drop(progress);

        // Wait for the forwarder to flush its queue, then signal terminal.
        let _ = forwarder.await;

        let terminal = match result {
            Ok(maybe_obj) => ActionStatus::Completed {
                cookie,
                total_bytes,
                result: maybe_obj,
            },
            Err(e) => {
                error!(target: "hsm.inproc", %cookie, error = %e, "mover failed");
                failed_from_error(cookie, &e)
            }
        };

        cancels.lock().remove(&cookie);

        if status_tx.send(terminal).await.is_err() {
            warn!(target: "hsm.inproc", %cookie, "status channel closed; terminal event dropped");
        }
    });
}

fn build_ctx(
    d: &DispatchedAction,
    progress: ProgressReporter,
    cancel: CancellationToken,
) -> ActionCtx {
    let mut b = ActionCtxBuilder::default()
        .cookie(d.action.cookie)
        .fid(d.action.fid)
        .archive_id(d.action.archive_id)
        .kind(d.action.kind)
        .extent(d.action.extent)
        .primary_path(d.primary_path.clone())
        .hint(d.action.data.clone())
        .progress(progress)
        .cancel(cancel);
    if let Some(p) = &d.write_path {
        b = b.write_path(p.clone());
    }
    if let Some(o) = &d.existing {
        b = b.existing(o.clone());
    }
    b.build()
}
