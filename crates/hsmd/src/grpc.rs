//! Daemon-side gRPC bridge between [`crate::AgentConn`] channels and a
//! tonic [`DataMover`] service.
//!
//! Each plugin connection is a single bidirectional stream
//! ([`hsm_proto::v1::DataMover::open`]):
//!
//! - First message from the plugin must be a `Hello` carrying agent
//!   id + archive ids the plugin can serve.
//! - Daemon replies with `Welcome` (just a session id for diagnostics)
//!   and starts forwarding `ActionItem`s onto the outbound stream as
//!   the dispatcher pushes them to the agent's `action_tx`.
//! - Plugin streams `ActionStatus` back; we translate each into the
//!   internal [`ActionStatus`] enum and push it on the agent's
//!   `status_tx` for the daemon's status drain task.
//!
//! Cancellation is routed *as an action* — when the dispatcher pushes
//! a cookie onto the agent's `cancel_tx` we synthesize a
//! `ToPlugin::Action(ActionItem { kind: Cancel, cookie, … })` and
//! send it on the outbound stream. The plugin's SDK trips the
//! per-cookie token on receipt; no extra RPC needed.

use std::pin::Pin;
use std::sync::Arc;

use hsm_core::{ActionKind, AgentId, ArchiveId, BackendObject, Cookie};
use hsm_proto::v1 as pb;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use crate::agent::{ActionStatus, AgentConn, AgentSink, DispatchedAction};

/// Capacity of the daemon→plugin outbound stream channel. Picked
/// larger than `AgentConn`'s `action_tx` capacity (64) so the gRPC
/// layer never adds back-pressure on top of the dispatcher's bounded
/// queue.
const OUTBOUND_BUFFER: usize = 128;

/// Callback the gRPC service invokes on every successful Hello.
///
/// Receives the freshly-built [`AgentConn`] so the daemon can register
/// it with its scheduler / status drain. Returning `Err` causes the
/// gRPC service to reject the connection with `Status::failed_precondition`.
pub type AgentRegistrar =
    Arc<dyn Fn(AgentConn) -> Result<(), String> + Send + Sync + 'static>;

/// gRPC service implementing [`hsm_proto::v1::data_mover_server::DataMover`].
///
/// Construct with a registrar callback (the daemon's `register_agent`
/// wrapped in an `Arc<dyn Fn>`) and pass to a tonic server.
#[derive(Clone)]
pub struct GrpcAgentService {
    registrar: AgentRegistrar,
    /// Monotonic counter for diagnostic session ids.
    next_session: Arc<std::sync::atomic::AtomicU64>,
}

impl GrpcAgentService {
    /// Build the service with the given registrar.
    pub fn new(registrar: AgentRegistrar) -> Self {
        Self {
            registrar,
            next_session: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }
}

#[tonic::async_trait]
impl pb::data_mover_server::DataMover for GrpcAgentService {
    type OpenStream = Pin<Box<dyn Stream<Item = Result<pb::ToPlugin, Status>> + Send>>;

    async fn open(
        &self,
        request: Request<Streaming<pb::FromPlugin>>,
    ) -> Result<Response<Self::OpenStream>, Status> {
        let mut inbound = request.into_inner();
        let session_id = self.next_session.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug!(target: "hsmd.grpc", %session_id, "session opening");

        // First message must be Hello.
        let hello = match inbound.next().await {
            Some(Ok(pb::FromPlugin {
                body: Some(pb::from_plugin::Body::Hello(h)),
            })) => h,
            Some(Ok(other)) => {
                return Err(Status::failed_precondition(format!(
                    "first message must be Hello; got {other:?}"
                )));
            }
            Some(Err(e)) => return Err(e),
            None => return Err(Status::failed_precondition("stream closed before Hello")),
        };
        let agent_id = AgentId::new(hello.agent_id);
        let archives: Vec<ArchiveId> = hello.archive_ids.into_iter().map(ArchiveId::new).collect();
        info!(
            target: "hsmd.grpc",
            agent = %agent_id, %session_id, n_archives = archives.len(),
            "agent registered"
        );

        // Build the AgentConn / AgentSink pair. The Sink half stays on
        // this task — we drive `action_rx` → outbound stream and
        // inbound stream → `status_tx` here. The Conn half goes into
        // the daemon via the registrar.
        let (conn, sink) = AgentSink::pair(
            agent_id.clone(),
            archives.iter().copied(),
            /* action capacity */ 64,
            /* status capacity */ 256,
        );

        if let Err(reason) = (self.registrar)(conn) {
            warn!(target: "hsmd.grpc", %agent_id, %reason, "registrar rejected agent");
            return Err(Status::failed_precondition(reason));
        }

        // Prepare the outbound (daemon→plugin) stream. Every item is a
        // `ToPlugin` envelope. The first one is Welcome; subsequent
        // ones are ActionItems (work or Cancel control signals).
        let (out_tx, out_rx) = mpsc::channel::<Result<pb::ToPlugin, Status>>(OUTBOUND_BUFFER);
        let session_str = format!("session-{session_id}");
        if out_tx
            .send(Ok(pb::ToPlugin {
                body: Some(pb::to_plugin::Body::Welcome(pb::Welcome { session_id: session_str })),
            }))
            .await
            .is_err()
        {
            return Err(Status::aborted("plugin closed before welcome"));
        }

        // Forwarder #1: agent.action_rx → outbound stream as ActionItem.
        // Forwarder #2: agent.cancel_rx → outbound stream as Cancel ActionItem.
        // Forwarder #3: inbound stream (FromPlugin) → agent.status_tx
        //                 as the internal ActionStatus enum.
        //
        // All three terminate when their input source closes; closing
        // any forwarder closes the others via the shutdown_token below.
        let shutdown = tokio_util::sync::CancellationToken::new();
        spawn_action_forwarder(sink.action_rx, out_tx.clone(), shutdown.clone());
        spawn_cancel_forwarder(sink.cancel_rx, out_tx.clone(), shutdown.clone());
        spawn_status_forwarder(inbound, sink.status_tx, agent_id.clone(), shutdown);

        // The plugin sees a stream of ToPlugin messages. tonic owns the
        // receiver wrapper; we just hand it back wrapped in Pin<Box>.
        let outbound: Self::OpenStream = Box::pin(ReceiverStream::new(out_rx));
        Ok(Response::new(outbound))
    }
}

fn spawn_action_forwarder(
    mut action_rx: mpsc::Receiver<DispatchedAction>,
    out_tx: mpsc::Sender<Result<pb::ToPlugin, Status>>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                maybe_action = action_rx.recv() => {
                    let Some(action) = maybe_action else { shutdown.cancel(); return; };
                    let item = dispatched_to_proto(action);
                    let env = pb::ToPlugin { body: Some(pb::to_plugin::Body::Action(item)) };
                    if out_tx.send(Ok(env)).await.is_err() { shutdown.cancel(); return; }
                }
            }
        }
    });
}

fn spawn_cancel_forwarder(
    mut cancel_rx: mpsc::Receiver<Cookie>,
    out_tx: mpsc::Sender<Result<pb::ToPlugin, Status>>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                maybe_cookie = cancel_rx.recv() => {
                    let Some(cookie) = maybe_cookie else { shutdown.cancel(); return; };
                    let cancel_action = pb::ActionItem {
                        cookie: cookie.get(),
                        kind: pb::ActionKind::Cancel as i32,
                        ..pb::ActionItem::default()
                    };
                    let env = pb::ToPlugin { body: Some(pb::to_plugin::Body::Action(cancel_action)) };
                    if out_tx.send(Ok(env)).await.is_err() { shutdown.cancel(); return; }
                }
            }
        }
    });
}

fn spawn_status_forwarder(
    mut inbound: Streaming<pb::FromPlugin>,
    status_tx: mpsc::Sender<ActionStatus>,
    agent_id: AgentId,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                maybe_msg = inbound.next() => {
                    let msg = match maybe_msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(target: "hsmd.grpc", agent = %agent_id, error = %e, "stream error");
                            shutdown.cancel();
                            return;
                        }
                        None => {
                            debug!(target: "hsmd.grpc", agent = %agent_id, "plugin closed inbound");
                            shutdown.cancel();
                            return;
                        }
                    };
                    let internal = match msg.body {
                        Some(pb::from_plugin::Body::Status(s)) => proto_to_status(s),
                        Some(pb::from_plugin::Body::Hello(_)) => {
                            warn!(target: "hsmd.grpc", agent = %agent_id, "unexpected duplicate Hello");
                            continue;
                        }
                        None => {
                            warn!(target: "hsmd.grpc", agent = %agent_id, "FromPlugin with empty body");
                            continue;
                        }
                    };
                    if status_tx.send(internal).await.is_err() {
                        // Daemon's status drain task gone — bail.
                        error!(target: "hsmd.grpc", agent = %agent_id, "status_tx closed");
                        shutdown.cancel();
                        return;
                    }
                }
            }
        }
    });
}

fn dispatched_to_proto(d: DispatchedAction) -> pb::ActionItem {
    let kind = match d.action.kind {
        ActionKind::Archive => pb::ActionKind::Archive,
        ActionKind::Restore => pb::ActionKind::Restore,
        ActionKind::Remove => pb::ActionKind::Remove,
        ActionKind::Cancel => pb::ActionKind::Cancel,
    } as i32;
    pb::ActionItem {
        cookie: d.action.cookie.get(),
        archive_id: d.action.archive_id.get(),
        kind,
        primary_path: d.primary_path.to_string_lossy().into_owned(),
        write_path: d.write_path.map(|p| p.to_string_lossy().into_owned()).unwrap_or_default(),
        offset: d.action.extent.offset,
        length: d.action.extent.length,
        data: d.action.data.to_vec(),
        existing: d.existing.map(backend_obj_to_proto),
        fid_seq: d.action.fid.seq,
        fid_oid: d.action.fid.oid,
        fid_ver: d.action.fid.ver,
    }
}

fn proto_to_status(s: pb::ActionStatus) -> ActionStatus {
    let cookie = Cookie::new(s.cookie);
    if !s.completed {
        return ActionStatus::Progress {
            cookie,
            bytes_advanced: s.bytes_advanced,
        };
    }
    if s.errno == 0 {
        ActionStatus::Completed {
            cookie,
            total_bytes: s.bytes_advanced,
            result: s.result.map(proto_to_backend_obj),
        }
    } else {
        ActionStatus::Failed {
            cookie,
            errno: s.errno,
            reason: format!("plugin reported errno {}", s.errno),
        }
    }
}

fn backend_obj_to_proto(o: BackendObject) -> pb::BackendObject {
    pb::BackendObject {
        uuid: o.uuid,
        hash: o.hash.to_vec(),
        url: o.url,
    }
}

fn proto_to_backend_obj(o: pb::BackendObject) -> BackendObject {
    let mut hash = [0u8; 32];
    let n = o.hash.len().min(32);
    hash[..n].copy_from_slice(&o.hash[..n]);
    BackendObject {
        uuid: o.uuid,
        hash,
        url: o.url,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use hsm_core::{Action, AgentId as _AgentId, ArchiveId, Cookie, Extent, Fid};

    fn dispatched(cookie: u64, kind: ActionKind) -> DispatchedAction {
        DispatchedAction {
            action: Action {
                cookie: Cookie::new(cookie),
                fid: Fid::new(1, cookie as u32, 0),
                dfid: Fid::new(1, cookie as u32, 0),
                archive_id: ArchiveId::new(7),
                kind,
                extent: Extent::new(0, 4096),
                gid: 0,
                data: Bytes::from_static(b"hint"),
            },
            primary_path: "/mnt/lustre/x".into(),
            write_path: Some("/mnt/lustre/y".into()),
            existing: None,
        }
    }

    #[test]
    fn dispatched_round_trips_through_proto() {
        let d = dispatched(0xabc, ActionKind::Archive);
        let pb = super::dispatched_to_proto(d.clone());
        assert_eq!(pb.cookie, 0xabc);
        assert_eq!(pb.archive_id, 7);
        assert_eq!(pb.kind, pb::ActionKind::Archive as i32);
        assert_eq!(pb.primary_path, "/mnt/lustre/x");
        assert_eq!(pb.write_path, "/mnt/lustre/y");
        assert_eq!(pb.data, b"hint");
    }

    #[test]
    fn proto_status_progress_and_terminal_classify() {
        let progress = pb::ActionStatus {
            cookie: 1,
            bytes_advanced: 100,
            completed: false,
            errno: 0,
            result: None,
        };
        match super::proto_to_status(progress) {
            ActionStatus::Progress { cookie, bytes_advanced } => {
                assert_eq!(cookie, Cookie::new(1));
                assert_eq!(bytes_advanced, 100);
            }
            other => panic!("expected Progress, got {other:?}"),
        }

        let succeeded = pb::ActionStatus {
            cookie: 2,
            bytes_advanced: 4096,
            completed: true,
            errno: 0,
            result: Some(pb::BackendObject {
                uuid: "u".into(),
                hash: vec![0xab; 32],
                url: "noop://u".into(),
            }),
        };
        match super::proto_to_status(succeeded) {
            ActionStatus::Completed { cookie, total_bytes, result } => {
                assert_eq!(cookie, Cookie::new(2));
                assert_eq!(total_bytes, 4096);
                let obj = result.unwrap();
                assert_eq!(obj.uuid, "u");
                assert_eq!(obj.hash, [0xab; 32]);
            }
            other => panic!("expected Completed, got {other:?}"),
        }

        let failed = pb::ActionStatus {
            cookie: 3,
            bytes_advanced: 1024,
            completed: true,
            errno: 28, // ENOSPC
            result: None,
        };
        match super::proto_to_status(failed) {
            ActionStatus::Failed { cookie, errno, .. } => {
                assert_eq!(cookie, Cookie::new(3));
                assert_eq!(errno, 28);
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn backend_obj_round_trip_truncates_long_hash() {
        // BackendObject.hash is fixed [u8;32]; a longer wire hash gets
        // truncated rather than panicking.
        let pb_obj = pb::BackendObject {
            uuid: "k".into(),
            hash: vec![1; 64],
            url: "u".into(),
        };
        let internal = super::proto_to_backend_obj(pb_obj);
        assert_eq!(internal.hash, [1u8; 32]);
    }

    /// The agent_id type must round-trip cleanly so logs use the user's id.
    #[test]
    fn agent_id_construction_from_hello() {
        let id = _AgentId::new("noop-0");
        assert_eq!(id.as_str(), "noop-0");
    }
}
