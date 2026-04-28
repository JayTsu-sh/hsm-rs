//! [`HsmControlService`] — gRPC management service for `hsmctl`.
//!
//! Implements `hsm.v1.HsmControl` (Status / ListAgents / ListActions).
//! The service holds read-only handles to the store and agent registry;
//! it never mutates daemon state.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use hsm_core::{ActionKind, ArState};
use hsm_proto::v1 as pb;
use hsm_proto::v1::hsm_control_server::HsmControl;
use hsm_scheduler::AgentRegistry;
use hsm_store::ActionStore;
use tonic::{Request, Response, Status};

/// Hard cap on `list_actions` responses per RPC call.
/// Protects against unbounded reads on large stores.
/// Pagination will be introduced when needed; until then this is the safety net.
const LIST_ACTIONS_LIMIT: usize = 10_000;

/// gRPC management service.
///
/// Generic over `Store` so the daemon binary can use it with both
/// `MemStore` (tests) and `SqliteStore` (production) without boxing.
#[derive(Clone)]
pub struct HsmControlService<Store> {
    store: Arc<Store>,
    registry: AgentRegistry,
}

impl<Store: ActionStore + 'static> HsmControlService<Store> {
    /// Build from the daemon's owned store and registry.
    pub fn new(store: Arc<Store>, registry: AgentRegistry) -> Self {
        Self { store, registry }
    }
}

#[tonic::async_trait]
impl<Store: ActionStore + Send + Sync + 'static> HsmControl for HsmControlService<Store> {
    async fn status(
        &self,
        _request: Request<pb::CtlStatusRequest>,
    ) -> Result<Response<pb::CtlStatusResponse>, Status> {
        // status_counts() avoids materialising full records: SqliteStore uses
        // a single GROUP BY query; MemStore falls back to load_all().
        let (actions_waiting, actions_started, actions_total) = self
            .store
            .status_counts()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let agents = self
            .registry
            .snapshot()
            .into_iter()
            .map(agent_to_proto)
            .collect();

        Ok(Response::new(pb::CtlStatusResponse {
            actions_waiting,
            actions_started,
            actions_total,
            agents,
        }))
    }

    async fn list_agents(
        &self,
        _request: Request<pb::CtlListAgentsRequest>,
    ) -> Result<Response<pb::CtlListAgentsResponse>, Status> {
        let agents = self
            .registry
            .snapshot()
            .into_iter()
            .map(agent_to_proto)
            .collect();
        Ok(Response::new(pb::CtlListAgentsResponse { agents }))
    }

    async fn list_actions(
        &self,
        request: Request<pb::CtlListActionsRequest>,
    ) -> Result<Response<pb::CtlListActionsResponse>, Status> {
        let req = request.into_inner();
        let state_filter = req.state_filter.to_lowercase();
        let archive_filter = req.archive_id;

        let valid_states = ["", "waiting", "started", "succeed", "failed", "canceled"];
        if !valid_states.contains(&state_filter.as_str()) {
            return Err(Status::invalid_argument(format!(
                "unknown state_filter {state_filter:?}; valid: {}",
                valid_states[1..].join(", ")
            )));
        }

        let state_opt = if state_filter.is_empty() {
            None
        } else {
            Some(state_filter.as_str())
        };
        let archive_opt = if archive_filter == 0 {
            None
        } else {
            Some(archive_filter)
        };

        // query_actions pushes filters + limit to SQL on SqliteStore, avoiding
        // full-table materialisation on large deployments.
        let records = self
            .store
            .query_actions(state_opt, archive_opt, LIST_ACTIONS_LIMIT)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let actions = records
            .into_iter()
            .map(|r| {
                let (state_str, agent_id, since_ms) = encode_state(&r.state);
                pb::CtlActionRecord {
                    cookie: r.action.cookie.get(),
                    fid: format!(
                        "[{:#x}:{:#x}:{:#x}]",
                        r.action.fid.seq, r.action.fid.oid, r.action.fid.ver
                    ),
                    archive_id: r.action.archive_id.get(),
                    kind: encode_kind(r.action.kind).to_string(),
                    state: state_str.to_string(),
                    agent_id,
                    progress: r.progress.offset,
                    since_ms,
                }
            })
            .collect();

        Ok(Response::new(pb::CtlListActionsResponse { actions }))
    }
}

// ── helpers ────────────────────────────────────────────────────────────────

fn agent_to_proto(a: hsm_scheduler::AgentInfo) -> pb::CtlAgentInfo {
    let registered_at_secs = a
        .since
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    pb::CtlAgentInfo {
        agent_id: a.id.as_str().to_owned(),
        archive_ids: a.archives.iter().map(|id| id.get()).collect(),
        registered_at_secs,
    }
}

fn encode_kind(k: ActionKind) -> &'static str {
    match k {
        ActionKind::Archive => "archive",
        ActionKind::Restore => "restore",
        ActionKind::Remove => "remove",
        ActionKind::Cancel => "cancel",
    }
}

fn encode_state(s: &ArState) -> (&'static str, String, i64) {
    match s {
        ArState::Waiting => ("waiting", String::new(), 0),
        ArState::Started {
            agent,
            since_unix_ms,
        } => ("started", agent.as_str().to_owned(), *since_unix_ms as i64),
        ArState::Succeed { .. } => ("succeed", String::new(), 0),
        ArState::Failed { .. } => ("failed", String::new(), 0),
        ArState::Canceled => ("canceled", String::new(), 0),
    }
}
