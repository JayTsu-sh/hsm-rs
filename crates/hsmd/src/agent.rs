//! [`AgentConn`] — the daemon's view of one connected mover.
//!
//! Channel-based abstraction: the daemon pushes `DispatchedAction`s into
//! `action_tx`, the agent (in-process Mover task in M2d.1, gRPC client in
//! M2d.2) consumes them, runs the work, and pushes back `ActionStatus`
//! events on `status_rx`. Cancellation rides a parallel `cancel_tx`.
//!
//! The daemon never assumes anything about *what* sits on the other end
//! of these channels — that's intentional. Swapping in-process for gRPC
//! is a change to the agent factory, not the dispatcher.

use std::collections::HashSet;

use hsm_core::{Action, AgentId, ArchiveId, BackendObject, Cookie};
use hsm_plugin_sdk::MoverError;
use tokio::sync::mpsc;

/// One unit of work the daemon hands to an agent.
///
/// Carries the pre-resolved Lustre paths so the mover doesn't need to
/// know how to translate FIDs to filesystem paths — that stays in the
/// daemon's recv pipeline (and eventually `lustre-llapi` helpers).
#[derive(Clone, Debug)]
pub struct DispatchedAction {
    /// The action to perform.
    pub action: Action,
    /// Filesystem path to read from on archive / remove (absolute).
    /// Empty for actions that don't need it.
    pub primary_path: std::path::PathBuf,
    /// Optional restore destination (absolute). `None` for non-restore.
    pub write_path: Option<std::path::PathBuf>,
    /// Pre-existing backend object metadata for restore / remove.
    pub existing: Option<BackendObject>,
    /// Real Lustre path of the file relative to the mount root
    /// (e.g. `"test/dir/file.txt"`). Populated by the daemon for
    /// Archive actions via `llapi_fid2path`; empty for other kinds or
    /// when resolution fails (mock mode, no Lustre mount).
    /// Used by the mover to write the shadow namespace entry.
    pub lustre_path: Option<std::path::PathBuf>,
}

/// Mover-reported status for one action.
///
/// `Progress` is non-terminal (just an offset advance); `Completed` /
/// `Failed` end the action. Modeled as an enum (rather than a single
/// struct with `bool completed`) so terminal events carry their own
/// payloads — a successful Archive must include the freshly-minted
/// [`BackendObject`] for the daemon to write into xattrs.
#[derive(Clone, Debug)]
pub enum ActionStatus {
    /// Non-terminal byte-level update.
    Progress {
        /// Action this update belongs to.
        cookie: Cookie,
        /// Furthest byte fully transferred (cumulative).
        bytes_advanced: u64,
    },
    /// Action finished successfully.
    Completed {
        /// Action that finished.
        cookie: Cookie,
        /// Total bytes moved.
        total_bytes: u64,
        /// `Some` for Archive (freshly minted); `None` for Restore /
        /// Remove (no new metadata to persist).
        result: Option<BackendObject>,
    },
    /// Action failed (or was cancelled — cancellation is one specific
    /// `MoverError::Cancelled` whose `errno()` is 125).
    Failed {
        /// Action that failed.
        cookie: Cookie,
        /// POSIX errno (`MoverError::errno()` mapping).
        errno: i32,
        /// Human-readable reason for logs.
        reason: String,
    },
}

impl ActionStatus {
    /// Cookie this status pertains to.
    pub fn cookie(&self) -> Cookie {
        match self {
            ActionStatus::Progress { cookie, .. }
            | ActionStatus::Completed { cookie, .. }
            | ActionStatus::Failed { cookie, .. } => *cookie,
        }
    }

    /// `true` if this is a terminal event (Completed / Failed).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ActionStatus::Completed { .. } | ActionStatus::Failed { .. }
        )
    }
}

/// Helper: convert a `MoverError` into the matching `Failed` status.
pub(crate) fn failed_from_error(cookie: Cookie, e: &MoverError) -> ActionStatus {
    ActionStatus::Failed {
        cookie,
        errno: e.errno(),
        reason: e.to_string(),
    }
}

/// What the daemon hands back when an agent registers.
///
/// `action_tx` and `cancel_tx` are owned by the daemon; the agent owns
/// the corresponding receivers (returned by [`AgentSink::take_endpoints`]).
/// `status_rx` is owned by the daemon (the agent has the sender).
pub struct AgentConn {
    /// Stable id (matches `hsm_scheduler::AgentRegistry`).
    pub id: AgentId,
    /// Archive ids this agent advertised.
    pub archives: HashSet<ArchiveId>,
    /// Push channel: dispatcher → agent.
    pub action_tx: mpsc::Sender<DispatchedAction>,
    /// Push channel: dispatcher → agent (out-of-band cancel signals).
    pub cancel_tx: mpsc::Sender<Cookie>,
    /// Pull channel: agent → dispatcher (status events).
    pub status_rx: mpsc::Receiver<ActionStatus>,
}

/// Endpoints the agent side keeps. Constructed by [`AgentSink::pair`]
/// alongside an [`AgentConn`] so both halves are wired to the same
/// channels.
pub struct AgentSink {
    /// Pull channel: action work the dispatcher sent.
    pub action_rx: mpsc::Receiver<DispatchedAction>,
    /// Pull channel: cookies the dispatcher wants cancelled.
    pub cancel_rx: mpsc::Receiver<Cookie>,
    /// Push channel: status events back to the dispatcher.
    pub status_tx: mpsc::Sender<ActionStatus>,
}

impl AgentSink {
    /// Build an [`AgentConn`] / [`AgentSink`] pair sharing the same
    /// underlying channels.
    ///
    /// `action_capacity` and `status_capacity` size the bounded mpsc
    /// queues — pick numbers larger than the agent's expected
    /// concurrency to avoid back-pressure spikes during normal traffic.
    pub fn pair(
        id: AgentId,
        archives: impl IntoIterator<Item = ArchiveId>,
        action_capacity: usize,
        status_capacity: usize,
    ) -> (AgentConn, AgentSink) {
        let (action_tx, action_rx) = mpsc::channel(action_capacity);
        let (status_tx, status_rx) = mpsc::channel(status_capacity);
        let (cancel_tx, cancel_rx) = mpsc::channel(action_capacity);
        let conn = AgentConn {
            id,
            archives: archives.into_iter().collect(),
            action_tx,
            cancel_tx,
            status_rx,
        };
        let sink = AgentSink {
            action_rx,
            cancel_rx,
            status_tx,
        };
        (conn, sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hsm_core::Cookie;

    #[test]
    fn status_classification() {
        let p = ActionStatus::Progress {
            cookie: Cookie::new(1),
            bytes_advanced: 1024,
        };
        let c = ActionStatus::Completed {
            cookie: Cookie::new(1),
            total_bytes: 4096,
            result: None,
        };
        let f = ActionStatus::Failed {
            cookie: Cookie::new(1),
            errno: 5,
            reason: "io".into(),
        };
        assert!(!p.is_terminal());
        assert!(c.is_terminal());
        assert!(f.is_terminal());
        assert_eq!(p.cookie(), Cookie::new(1));
    }

    #[test]
    fn failed_from_mover_error_maps_errno() {
        let s = failed_from_error(Cookie::new(7), &MoverError::Cancelled);
        match s {
            ActionStatus::Failed { errno, cookie, .. } => {
                assert_eq!(errno, 125);
                assert_eq!(cookie, Cookie::new(7));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn pair_wires_both_sides() {
        let (mut conn, mut sink) = AgentSink::pair(AgentId::new("m0"), [ArchiveId::new(1)], 16, 16);
        // Conn side -> sink side
        let action = DispatchedAction {
            action: hsm_core::Action {
                cookie: Cookie::new(1),
                fid: hsm_core::Fid::ZERO,
                dfid: hsm_core::Fid::ZERO,
                archive_id: ArchiveId::new(1),
                kind: hsm_core::ActionKind::Archive,
                extent: hsm_core::Extent::WHOLE,
                gid: 0,
                data: bytes::Bytes::new(),
            },
            primary_path: std::path::PathBuf::from("/x"),
            write_path: None,
            existing: None,
            lustre_path: None,
        };
        conn.action_tx.try_send(action.clone()).unwrap();
        let got = sink.action_rx.try_recv().unwrap();
        assert_eq!(got.action.cookie, Cookie::new(1));

        // Sink side -> conn side
        sink.status_tx
            .try_send(ActionStatus::Progress {
                cookie: Cookie::new(1),
                bytes_advanced: 100,
            })
            .unwrap();
        let got = conn.status_rx.try_recv().unwrap();
        assert_eq!(got.cookie(), Cookie::new(1));
    }
}
