//! Daemon orchestration — main loop wiring recv / store / scheduler /
//! dispatcher / status drain together.
//!
//! The daemon owns three long-running tasks:
//!
//! 1. **recv loop** — pulls action batches from the [`RecvSource`],
//!    persists them via [`ActionStore::insert`], hands them to the
//!    [`Scheduler::enqueue`].
//! 2. **dispatch tick** — every `tick_interval`, calls
//!    `Scheduler::pick_ready` and pushes assignments to the matching
//!    [`AgentConn::action_tx`], updating the store transition to
//!    `Started { agent }`.
//! 3. **status drain** — for each registered agent, drains
//!    [`AgentConn::status_rx`] forever; updates store progress on
//!    `Progress` events and store + (eventually) llapi-end on
//!    `Completed` / `Failed`.
//!
//! Shutdown: drop the [`DaemonHandle`]. Every spawned task uses a
//! [`CancellationToken`] cloned from the handle; dropping the handle
//! cancels all of them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hsm_core::{ActionRecord, AgentId, ArState, Cookie, Extent};
use hsm_scheduler::{AgentRegistry, Assignment, Scheduler};
use hsm_store::ActionStore;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::agent::{ActionStatus, AgentConn, DispatchedAction};
use crate::copytool_recv::RecvSource;
use crate::error::DaemonError;

/// Tunables for the daemon main loop.
#[derive(Clone, Debug)]
pub struct DaemonConfig {
    /// How often the dispatcher wakes up to call `Scheduler::pick_ready`.
    /// Tighter = lower latency on incoming actions; looser = lower CPU.
    pub tick_interval: Duration,
    /// Cap on assignments returned per tick. Keeps a flood of new
    /// actions from monopolizing the dispatch path.
    pub max_per_tick: usize,
    /// Lustre mountpoint root. Used to resolve FIDs to filesystem paths
    /// for the dispatched action's `primary_path` / `write_path`.
    /// In M2d.1 the test rig uses `/tmp/...` and skips real FID
    /// resolution; M2d.2 plugs in `lustre-llapi` helpers.
    pub mountpoint: PathBuf,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(50),
            max_per_tick: 32,
            mountpoint: PathBuf::from("/mnt/lustre"),
        }
    }
}

/// Handle returned by [`Daemon::start`]. Drop to request shutdown; await
/// `shutdown()` to join all spawned tasks.
///
/// Holds clones of the daemon's shared state so callers can:
/// - register additional agents *after* the daemon is running (typical
///   for the gRPC service which accepts plugin connections lazily);
/// - peek at the registry / agent table for diagnostics + tests;
/// - issue cross-agent control signals (cancel) without going through
///   the full scheduler tick.
pub struct DaemonHandle {
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    registry: AgentRegistry,
    agents: Arc<RwLock<HashMap<AgentId, AgentChannels>>>,
    register_tx: mpsc::Sender<AgentConn>,
}

impl DaemonHandle {
    /// Cancel all tasks and wait for them to exit.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        for t in self.tasks {
            let _ = t.await;
        }
    }

    /// Read-only view of the agent registry. Tests + the future
    /// `hsmctl status` use this to introspect connection health.
    pub fn registry(&self) -> &AgentRegistry {
        &self.registry
    }

    /// Register a new agent connection at runtime. Used by the gRPC
    /// service when a plugin opens its bidi stream. Returns
    /// `Err` if the daemon is already shut down (registration channel
    /// closed).
    pub async fn enroll_agent(&self, conn: AgentConn) -> Result<(), AgentConn> {
        self.register_tx.send(conn).await.map_err(|e| e.0)
    }

    /// Send a cancel signal for `cookie` to whichever agent currently
    /// owns it. Returns `false` if no agent claims this cookie (already
    /// finished or never dispatched).
    pub async fn cancel(&self, agent_id: &AgentId, cookie: Cookie) -> bool {
        let chans = self.agents.read().get(agent_id).cloned();
        match chans {
            Some(c) => c.cancel_tx.send(cookie).await.is_ok(),
            None => false,
        }
    }

    /// Build an [`AgentRegistrar`] closure that pushes new agent
    /// connections into this handle's enrollment queue. Captures *only*
    /// the mpsc sender — the registrar can be cheaply cloned and shared
    /// across the gRPC service / connection acceptors without contending
    /// for the rest of the handle's state.
    ///
    /// The returned closure is `Fn(AgentConn) -> Result<(), String>` so
    /// it satisfies [`AgentRegistrar`]'s callable shape. The push
    /// happens via `try_send` (non-blocking); if the daemon is shutting
    /// down or the queue is unexpectedly full, the closure returns
    /// `Err(reason)` and the gRPC service will abort the new
    /// connection cleanly.
    pub fn registrar(&self) -> crate::grpc::AgentRegistrar {
        let tx = self.register_tx.clone();
        Arc::new(move |conn| match tx.try_send(conn) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err("daemon shutting down".into())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                Err("daemon enroll queue full".into())
            }
        })
    }
}

/// The orchestrator. Owns the store / scheduler / agent-registry handles
/// and spawns the three loops on [`start`](Self::start).
pub struct Daemon<Sched, Store, Recv> {
    pub(crate) store: Arc<Store>,
    pub(crate) scheduler: Arc<Sched>,
    pub(crate) registry: AgentRegistry,
    pub(crate) recv: Recv,
    pub(crate) config: DaemonConfig,
    /// Map from AgentId → outbound channels (action_tx + cancel_tx).
    /// Status_rx for each agent is owned by the per-agent status drain
    /// task; once spawned it doesn't need to live in the daemon struct.
    pub(crate) agents: Arc<RwLock<HashMap<AgentId, AgentChannels>>>,
}

#[derive(Clone)]
pub(crate) struct AgentChannels {
    pub action_tx: mpsc::Sender<DispatchedAction>,
    pub cancel_tx: mpsc::Sender<Cookie>,
}

impl<Sched, Store, Recv> Daemon<Sched, Store, Recv>
where
    Sched: Scheduler + 'static,
    Store: ActionStore + 'static,
    Recv: RecvSource + 'static,
{
    /// Build a daemon. Doesn't start anything yet — call
    /// [`start`](Self::start) once you've registered initial agents.
    pub fn new(store: Arc<Store>, scheduler: Arc<Sched>, recv: Recv, config: DaemonConfig) -> Self {
        Self {
            store,
            scheduler,
            registry: AgentRegistry::new(),
            recv,
            config,
            agents: Arc::default(),
        }
    }

    /// Read-only access to the registry — useful for the test rig to
    /// observe which agents are connected.
    pub fn registry(&self) -> &AgentRegistry {
        &self.registry
    }

    /// Register an agent connection. Spawns the per-agent status drain
    /// task (see [`spawn_status_drain`]). The dispatcher will start
    /// matching actions against this agent's archive_ids on the next
    /// tick.
    pub fn register_agent(&self, conn: AgentConn) -> JoinHandle<()> {
        self.registry.register(conn.id.clone(), conn.archives.iter().copied());
        let chans = AgentChannels { action_tx: conn.action_tx, cancel_tx: conn.cancel_tx };
        self.agents.write().insert(conn.id.clone(), chans);
        spawn_status_drain(
            conn.id,
            conn.status_rx,
            self.store.clone(),
        )
    }

    /// Start the recv + dispatch loops. Returns a [`DaemonHandle`] that
    /// callers use to enroll agents at runtime, peek state, and shut
    /// down cleanly.
    pub fn start(mut self) -> DaemonHandle {
        let cancel = CancellationToken::new();
        let mut tasks = Vec::new();

        // Channel used by external plugin acceptors (gRPC, in-proc test
        // rigs) to push new agents into the daemon at runtime. The
        // dispatcher's enrollment task drains this and runs the same
        // register_agent logic the in-process tests use directly.
        let (register_tx, mut register_rx) = mpsc::channel::<AgentConn>(32);
        let enroll_cancel = cancel.clone();
        let enroll_registry = self.registry.clone();
        let enroll_agents = self.agents.clone();
        let enroll_store = self.store.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = enroll_cancel.cancelled() => {
                        debug!(target: "hsmd.enroll", "shutdown");
                        return;
                    }
                    Some(conn) = register_rx.recv() => {
                        enroll_registry.register(conn.id.clone(), conn.archives.iter().copied());
                        let chans = AgentChannels {
                            action_tx: conn.action_tx,
                            cancel_tx: conn.cancel_tx,
                        };
                        enroll_agents.write().insert(conn.id.clone(), chans);
                        let _drain = spawn_status_drain(conn.id, conn.status_rx, enroll_store.clone());
                        // _drain JoinHandle is intentionally not held —
                        // the per-agent status drain runs until the
                        // status_tx side closes (gRPC stream end / plugin
                        // exit), which is the right shutdown trigger.
                    }
                }
            }
        }));

        // ---- recv loop ---------------------------------------------------
        let recv_cancel = cancel.clone();
        let store_for_recv = self.store.clone();
        let sched_for_recv = self.scheduler.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = recv_cancel.cancelled() => {
                        debug!(target: "hsmd.recv", "shutdown");
                        return;
                    }
                    res = self.recv.next_batch() => {
                        match res {
                            Ok(batch) => {
                                for ra in batch {
                                    let action = ra.into_action();
                                    let rec = ActionRecord::new(action.clone(), SystemTime::now());
                                    if let Err(e) = store_for_recv.insert(rec).await {
                                        warn!(target: "hsmd.recv", error = %e, "store insert failed");
                                        continue;
                                    }
                                    sched_for_recv.enqueue(action);
                                }
                            }
                            Err(e) => {
                                error!(target: "hsmd.recv", error = %e, "recv source failed");
                                // Back off briefly so a hot loop doesn't burn CPU on
                                // a persistently broken source.
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
            }
        }));

        // ---- dispatch tick ----------------------------------------------
        let tick_cancel = cancel.clone();
        let scheduler = self.scheduler.clone();
        let registry = self.registry.clone();
        let agents = self.agents.clone();
        let store_for_tick = self.store.clone();
        let cfg = self.config.clone();
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(cfg.tick_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = tick_cancel.cancelled() => {
                        debug!(target: "hsmd.tick", "shutdown");
                        return;
                    }
                    _ = interval.tick() => {
                        let assignments = scheduler.pick_ready(&registry, cfg.max_per_tick);
                        if assignments.is_empty() { continue; }
                        for assignment in assignments {
                            dispatch_one(&store_for_tick, &agents, &cfg.mountpoint, assignment).await;
                        }
                    }
                }
            }
        }));

        DaemonHandle {
            cancel,
            tasks,
            registry: self.registry,
            agents: self.agents,
            register_tx,
        }
    }
}

async fn dispatch_one<Store: ActionStore>(
    store: &Arc<Store>,
    agents: &Arc<RwLock<HashMap<AgentId, AgentChannels>>>,
    mount: &PathBuf,
    Assignment { action, agent }: Assignment,
) {
    let cookie = action.cookie;
    let agent_chan = match agents.read().get(&agent).cloned() {
        Some(c) => c,
        None => {
            warn!(target: "hsmd.tick", %agent, %cookie, "agent missing at dispatch time; dropping");
            return;
        }
    };

    // Mark Started in the store BEFORE pushing to the agent so a crash
    // between the two leaves us with at most a stale Started (recoverable)
    // rather than a phantom dispatch the store can't see.
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    if let Err(e) = store
        .transition(
            cookie,
            ArState::Started { agent: agent.clone(), since_unix_ms: now_ms },
        )
        .await
    {
        warn!(target: "hsmd.tick", %cookie, error = %e, "transition to Started failed");
        return;
    }

    let dispatched = DispatchedAction {
        action: action.clone(),
        // M2d.1: stub path resolution. M2d.2 will plug in the
        // FID-to-path helper from lustre-llapi.
        primary_path: mount.join(format!("__fid__{}", action.fid)),
        write_path: None,
        existing: None, // Restore/Remove existing-object lookup lands in M2d.2 (xattr read).
    };

    if let Err(e) = agent_chan.action_tx.send(dispatched).await {
        error!(target: "hsmd.tick", %cookie, %agent, error = %e, "agent action_tx send failed");
        // Roll back to Waiting so a future tick can retry on a different agent.
        if let Err(e) = store.transition(cookie, ArState::Waiting).await {
            warn!(target: "hsmd.tick", %cookie, error = %e, "rollback to Waiting failed");
        }
    }
}

/// Spawn a long-running task draining one agent's `status_rx`.
fn spawn_status_drain<Store: ActionStore + 'static>(
    agent: AgentId,
    mut status_rx: mpsc::Receiver<ActionStatus>,
    store: Arc<Store>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!(target: "hsmd.status", %agent, "status drain task started");
        while let Some(status) = status_rx.recv().await {
            match status {
                ActionStatus::Progress { cookie, bytes_advanced } => {
                    // Convert cumulative bytes into a "progress extent"
                    // for the store. We don't know the original extent
                    // length here without a lookup; the daemon's view of
                    // progress is just (offset, length-so-far) for the
                    // purposes of monotonicity, so use bytes_advanced
                    // as the offset of the next-unwritten byte.
                    let e = Extent::new(bytes_advanced, 0);
                    if let Err(err) = store.update_progress(cookie, e).await {
                        warn!(target: "hsmd.status", %cookie, error = %err, "update_progress");
                    }
                }
                ActionStatus::Completed { cookie, total_bytes, result } => {
                    debug!(target: "hsmd.status", %cookie, total_bytes, has_obj = result.is_some(), "completed");
                    handle_terminal(&store, cookie, ArState::Succeed { rc: 0 }).await;
                    // M2d.2 will:
                    //   - Persist `result` into trusted.lhsm_uuid / hash / url xattrs.
                    //   - Call HsmActionHandle::end(cookie, extent, EndStatus::Ok).
                }
                ActionStatus::Failed { cookie, errno, reason } => {
                    warn!(target: "hsmd.status", %cookie, errno, %reason, "failed");
                    handle_terminal(&store, cookie, ArState::Failed { rc: errno }).await;
                }
            }
        }
        info!(target: "hsmd.status", %agent, "status drain task exiting (channel closed)");
    })
}

async fn handle_terminal<Store: ActionStore>(store: &Arc<Store>, cookie: Cookie, new: ArState) {
    if let Err(e) = store.transition(cookie, new).await {
        warn!(target: "hsmd.status", %cookie, error = %e, "transition to terminal failed");
    }
}

// DaemonError is re-exported from the crate root via lib.rs.
#[allow(unused_imports)]
use DaemonError as _;
