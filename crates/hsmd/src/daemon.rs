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

use dashmap::DashMap;
use hsm_core::{ActionKind, ActionRecord, AgentId, ArState, Cookie, Extent};
use hsm_scheduler::{AgentRegistry, Assignment, Scheduler};
use hsm_store::ActionStore;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::agent::{ActionStatus, AgentConn, DispatchedAction};
use crate::copytool_recv::{ActionCompletion, RecvSource};
use crate::error::DaemonError;
use crate::xattr_store::{self, XattrNamespace};

/// Linux `ENODATA` — returned when a Restore/Remove is issued for a
/// file with no `trusted.lhsm_*` xattrs (i.e. never archived).
const ENODATA: i32 = 61;

/// In-flight `(cookie → primary_path)` map used by the dispatch loop
/// to tell the status drain task where to write `trusted.lhsm_*`
/// Metadata tracked for each in-flight action: primary path (for xattr writes
/// on Archive) and the action kind (for building the correct extent on completion).
/// Entries are removed on any terminal status.
type PendingPaths = Arc<DashMap<Cookie, (PathBuf, ActionKind)>>;

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
    /// Which xattr namespace the daemon writes the per-file
    /// [`BackendObject`] into after a successful Archive (and reads
    /// from on Restore / Remove dispatch).
    ///
    /// - `Some(Trusted)` — production Lustre (`trusted.lhsm_*`,
    ///   requires `CAP_SYS_ADMIN`).
    /// - `Some(User)` — dev / CI without root (`user.lhsm_*`).
    /// - `None` — disable xattr persistence entirely. Restore /
    ///   Remove will fail with ENODATA (no metadata to look up).
    ///   Used by in-process tests that don't materialize the
    ///   stub `__fid__<fid>` files on disk.
    pub xattr_namespace: Option<crate::xattr_store::XattrNamespace>,

    /// When `true`, `dispatch_one` resolves FIDs using the Lustre
    /// virtual `.lustre/fid/<fid>` path (live mode). When `false`,
    /// it uses the `__fid__<fid>` stub convention used by mock tests.
    pub use_lustre_fid_path: bool,

    /// Optional sender for Lustre-side action completion.
    /// When `Some`, `handle_terminal` sends an [`ActionCompletion`] which
    /// causes the `LiveRecvSource` thread to call `llapi_hsm_action_end`.
    /// `None` for mock / in-process tests where no Lustre kernel is present.
    pub completion_tx: Option<crate::copytool_recv::CompletionTx>,

    /// How long to wait for an agent to reconnect and claim a `Started`
    /// action before re-queuing it as `Waiting` on daemon restart.
    /// Should be longer than the longest expected plugin restart time.
    /// Defaults to 10 minutes.
    pub recovery_grace_period: Duration,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(50),
            max_per_tick: 32,
            mountpoint: PathBuf::from("/mnt/lustre"),
            // Default OFF for the library-level config (used by
            // in-process tests that don't write real files). The
            // production binary's TOML config flips this to
            // `Some(Trusted)`.
            xattr_namespace: None,
            use_lustre_fid_path: false,
            completion_tx: None,
            recovery_grace_period: Duration::from_secs(600),
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
    /// Cancel tokens for grace-period watcher tasks from `recover()`.
    /// Drained here (not in `start()`) so watchers survive until shutdown.
    recovery_cancels: Arc<parking_lot::Mutex<Vec<CancellationToken>>>,
}

impl DaemonHandle {
    /// Cancel all tasks and wait for them to exit.
    pub async fn shutdown(self) {
        // Cancel grace-period watchers first so they don't fire after
        // the store pool is dropped.
        for token in self.recovery_cancels.lock().drain(..) {
            token.cancel();
        }
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
    /// Per-cookie primary_path stash so the per-agent status drain
    /// can find the file to write xattrs to on Archive completion.
    pub(crate) pending_paths: PendingPaths,
    /// Cancel tokens for grace-period watcher tasks spawned by `recover()`.
    /// `start()` drains these into the daemon's cancel scope so shutdown
    /// correctly aborts all outstanding grace watchers.
    pub(crate) recovery_cancels: Arc<parking_lot::Mutex<Vec<tokio_util::sync::CancellationToken>>>,
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
            pending_paths: Arc::new(DashMap::new()),
            recovery_cancels: Arc::default(),
        }
    }

    /// Read-only access to the registry — useful for the test rig to
    /// observe which agents are connected.
    pub fn registry(&self) -> &AgentRegistry {
        &self.registry
    }

    /// Build a [`crate::HsmControlService`] that exposes store + registry
    /// state via the `HsmControl` gRPC management interface.
    ///
    /// Call before [`start`](Self::start) so the service holds valid
    /// refs before the daemon loops begin. The returned service is passed
    /// directly to the tonic server.
    pub fn control_service(&self) -> crate::control::HsmControlService<Store> {
        crate::control::HsmControlService::new(self.store.clone(), self.registry.clone())
    }

    /// Replay persisted store state after a daemon restart.
    ///
    /// Must be called **before** [`start`](Self::start). For each record:
    ///
    /// - `Waiting` → re-enqueued to the scheduler immediately.
    /// - `Started` → a grace-period watcher task is spawned. If the
    ///   original agent reconnects and the record transitions away from
    ///   `Started` before the grace period expires, the watcher is a
    ///   no-op. Otherwise the record is rolled back to `Waiting` and
    ///   re-queued.
    /// - Terminal (`Succeed` / `Failed` / `Canceled`) → deleted from the
    ///   store (they are already done; no re-dispatch needed).
    ///
    /// When `SqliteStore` is in use this provides crash-safe in-flight
    /// recovery. With `MemStore` the store is always empty on startup so
    /// this is a no-op.
    pub async fn recover(&self) {
        let records = match self.store.load_all().await {
            Ok(r) => r,
            Err(e) => {
                error!(target: "hsmd.recovery", error = %e, "load_all failed; skipping recovery");
                return;
            }
        };

        if records.is_empty() {
            debug!(target: "hsmd.recovery", "store empty; nothing to recover");
            return;
        }

        let mut requeued = 0usize;
        let mut grace_started = 0usize;
        let mut cleaned = 0usize;

        for rec in records {
            let cookie = rec.action.cookie;
            match &rec.state {
                ArState::Waiting => {
                    self.scheduler.enqueue(rec.action.clone());
                    requeued += 1;
                }
                ArState::Started { since_unix_ms, .. } => {
                    let store = self.store.clone();
                    let scheduler = self.scheduler.clone();
                    let action = rec.action.clone();
                    let grace = self.config.recovery_grace_period;
                    let started_ms = *since_unix_ms;
                    // Clone the daemon-level cancel token so grace tasks are
                    // aborted on DaemonHandle::shutdown() and don't keep the
                    // store's connection pool alive after the daemon stops.
                    let cancel = tokio_util::sync::CancellationToken::new();
                    let cancel_child = cancel.child_token();
                    // Store the parent token so start() can cancel all grace tasks.
                    self.recovery_cancels.lock().push(cancel);
                    tokio::spawn(async move {
                        let elapsed = SystemTime::now()
                            .duration_since(UNIX_EPOCH + Duration::from_millis(started_ms))
                            .unwrap_or(Duration::ZERO);
                        let remaining = grace.saturating_sub(elapsed);
                        tokio::select! {
                            () = cancel_child.cancelled() => {
                                debug!(target: "hsmd.recovery", %cookie, "grace task cancelled (shutdown)");
                                return;
                            }
                            () = tokio::time::sleep(remaining) => {}
                        }
                        match store.get(cookie).await {
                            Ok(Some(r)) if matches!(r.state, ArState::Started { .. }) => {
                                warn!(
                                    target: "hsmd.recovery",
                                    %cookie, "grace period expired; rolling back to Waiting"
                                );
                                let _ = store.transition(cookie, ArState::Waiting).await;
                                scheduler.enqueue(action);
                            }
                            _ => {
                                debug!(
                                    target: "hsmd.recovery",
                                    %cookie, "grace: action already claimed or completed"
                                );
                            }
                        }
                    });
                    grace_started += 1;
                }
                _ => {
                    // Terminal state — clean up the store entry.
                    if let Err(e) = self.store.delete(cookie).await {
                        warn!(target: "hsmd.recovery", %cookie, error = %e, "delete terminal record failed");
                    }
                    cleaned += 1;
                }
            }
        }

        info!(
            target: "hsmd.recovery",
            requeued, grace_started, cleaned,
            "recovery complete"
        );
    }

    /// Register an agent connection. Spawns the per-agent status drain
    /// task (see [`spawn_status_drain`]). The dispatcher will start
    /// matching actions against this agent's archive_ids on the next
    /// tick.
    pub fn register_agent(&self, conn: AgentConn) -> JoinHandle<()> {
        self.registry
            .register(conn.id.clone(), conn.archives.iter().copied());
        let chans = AgentChannels {
            action_tx: conn.action_tx,
            cancel_tx: conn.cancel_tx,
        };
        self.agents.write().insert(conn.id.clone(), chans);
        spawn_status_drain(
            conn.id,
            conn.status_rx,
            self.store.clone(),
            self.pending_paths.clone(),
            self.config.xattr_namespace,
            self.config.completion_tx.clone(),
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
        let enroll_paths = self.pending_paths.clone();
        let enroll_xattr_ns = self.config.xattr_namespace;
        let enroll_completion_tx = self.config.completion_tx.clone();
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
                        let _drain = spawn_status_drain(
                            conn.id,
                            conn.status_rx,
                            enroll_store.clone(),
                            enroll_paths.clone(),
                            enroll_xattr_ns,
                            enroll_completion_tx.clone(),
                        );
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
        let pending_paths = self.pending_paths.clone();
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
                            dispatch_one(
                                &store_for_tick,
                                &agents,
                                &cfg.mountpoint,
                                cfg.xattr_namespace,
                                &pending_paths,
                                cfg.use_lustre_fid_path,
                                assignment,
                            ).await;
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
            recovery_cancels: self.recovery_cancels,
            register_tx,
        }
    }
}

async fn dispatch_one<Store: ActionStore>(
    store: &Arc<Store>,
    agents: &Arc<RwLock<HashMap<AgentId, AgentChannels>>>,
    mount: &PathBuf,
    xattr_ns: Option<XattrNamespace>,
    pending_paths: &PendingPaths,
    use_lustre_fid_path: bool,
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

    // Build the primary path for the action's file.
    // Live mode: use Lustre's virtual `.lustre/fid/<fid>` path (readable
    // via normal POSIX calls; writable for restore via the dfid mechanism).
    // Mock mode: fall back to the `__fid__<fid>` stub convention.
    let primary_path = if use_lustre_fid_path {
        let fid_str = format!("{}", action.fid);
        let bare = fid_str.trim_start_matches('[').trim_end_matches(']');
        mount.join(".lustre").join("fid").join(bare)
    } else {
        mount.join(format!("__fid__{}", action.fid))
    };

    // For Restore / Remove: read the per-file BackendObject from
    // xattrs *before* claiming Started, so a missing-xattr action
    // fails fast with ENODATA without flipping store state. Skipped
    // entirely when xattr persistence is disabled (in-process tests).
    let existing = match (action.kind, xattr_ns) {
        (ActionKind::Restore | ActionKind::Remove, Some(ns)) => {
            match xattr_store::read_obj(&primary_path, ns) {
                Ok(Some(obj)) => Some(obj),
                Ok(None) => {
                    warn!(
                        target: "hsmd.tick", %cookie, kind = ?action.kind,
                        path = %primary_path.display(),
                        "no lhsm_* xattrs (file never archived); failing with ENODATA"
                    );
                    handle_terminal(store, cookie, ArState::Failed { rc: ENODATA }).await;
                    return;
                }
                Err(e) => {
                    warn!(
                        target: "hsmd.tick", %cookie, kind = ?action.kind,
                        path = %primary_path.display(), error = %e,
                        "xattr read failed; failing with EIO"
                    );
                    handle_terminal(store, cookie, ArState::Failed { rc: 5 }).await;
                    return;
                }
            }
        }
        _ => None,
    };

    // Mark Started in the store BEFORE pushing to the agent so a crash
    // between the two leaves us with at most a stale Started (recoverable)
    // rather than a phantom dispatch the store can't see.
    let now_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    if let Err(e) = store
        .transition(
            cookie,
            ArState::Started {
                agent: agent.clone(),
                since_unix_ms: now_ms,
            },
        )
        .await
    {
        warn!(target: "hsmd.tick", %cookie, error = %e, "transition to Started failed");
        return;
    }

    // For Restore: determine the write_path the plugin uses to materialise
    // the file's bytes.
    //
    // Live Lustre: writing directly to the `.lustre/fid/<fid>` path of a
    // released file triggers Lustre's automatic restore, which deadlocks with
    // our own copytool restore. Instead we use a deterministic temp path;
    // the recv thread later copies these bytes to the Lustre action FD via
    // `llapi_hsm_action_get_fd` before calling `llapi_hsm_action_end`.
    //
    // Mock mode: keep using `primary_path` (the `__fid__<fid>` stub), which
    // is a normal file the plugin can write to directly.
    let write_path = matches!(action.kind, ActionKind::Restore).then(|| {
        if use_lustre_fid_path {
            crate::copytool_recv::restore_temp_path(cookie)
        } else {
            primary_path.clone()
        }
    });

    pending_paths.insert(cookie, (primary_path.clone(), action.kind));

    // For Archive actions in live Lustre mode, resolve the real path via
    // llapi_fid2path for the shadow namespace. Other action kinds don't need
    // this (Restore/Remove use the stored uuid from xattrs, not the path).
    // For Archive: needed to write the shadow namespace entry.
    // For Remove: needed to also remove the shadow entry when deleting the backend copy.
    let lustre_path =
        if matches!(action.kind, ActionKind::Archive | ActionKind::Remove) && use_lustre_fid_path {
            let fid_str = format!(
                "{:#x}:{:#x}:{:#x}",
                action.fid.seq, action.fid.oid, action.fid.ver
            );
            let mount_clone = mount.clone();
            tokio::task::spawn_blocking(move || {
                lustre_llapi::LiveCopytool::fid_to_path(&mount_clone, &fid_str)
            })
            .await
            .ok()
            .and_then(|r| r.ok())
        } else {
            None
        };

    let dispatched = DispatchedAction {
        action: action.clone(),
        primary_path,
        write_path,
        existing,
        lustre_path,
    };

    if let Err(e) = agent_chan.action_tx.send(dispatched).await {
        error!(target: "hsmd.tick", %cookie, %agent, error = %e, "agent action_tx send failed");
        // Roll back to Waiting so a future tick can retry on a different agent.
        if let Err(e) = store.transition(cookie, ArState::Waiting).await {
            warn!(target: "hsmd.tick", %cookie, error = %e, "rollback to Waiting failed");
        }
        // Drop the pending path so we don't leak; a future re-dispatch
        // will re-insert it.
        pending_paths.remove(&cookie);
    }
}

/// Spawn a long-running task draining one agent's `status_rx`. On a
/// successful Archive completion that came back with a `BackendObject`,
/// this task also persists the object into the file's xattrs so a
/// later Restore / Remove can find it.
fn spawn_status_drain<Store: ActionStore + 'static>(
    agent: AgentId,
    mut status_rx: mpsc::Receiver<ActionStatus>,
    store: Arc<Store>,
    pending_paths: PendingPaths,
    xattr_ns: Option<XattrNamespace>,
    completion_tx: Option<crate::copytool_recv::CompletionTx>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!(target: "hsmd.status", %agent, "status drain task started");
        while let Some(status) = status_rx.recv().await {
            match status {
                ActionStatus::Progress {
                    cookie,
                    bytes_advanced,
                } => {
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
                ActionStatus::Completed {
                    cookie,
                    total_bytes,
                    result,
                } => {
                    debug!(
                        target: "hsmd.status", %cookie, total_bytes,
                        has_obj = result.is_some(), "completed"
                    );
                    // Persist BackendObject to xattrs on Archive
                    // success. We do this BEFORE flipping the store to
                    // Succeed so a daemon crash between the two leaves
                    // either (a) a still-Started action that retries
                    // (and the plugin's archive is idempotent on the
                    // backend object name) or (b) a clean Succeed with
                    // xattrs in place.
                    if let (Some(obj), Some(ns)) = (result.as_ref(), xattr_ns) {
                        if let Some(path) = pending_paths.get(&cookie).map(|e| e.value().0.clone())
                        {
                            if let Err(e) = xattr_store::write_obj(&path, ns, obj) {
                                warn!(
                                    target: "hsmd.status", %cookie,
                                    path = %path.display(), error = %e,
                                    "xattr write failed; treating action as failed"
                                );
                                pending_paths.remove(&cookie);
                                handle_terminal(&store, cookie, ArState::Failed { rc: 5 }).await;
                                continue;
                            }
                        } else {
                            warn!(
                                target: "hsmd.status", %cookie,
                                "no pending_paths entry for completed Archive; skipping xattr write"
                            );
                        }
                    }
                    let action_kind = pending_paths.remove(&cookie).map(|(_, (_, k))| k);
                    handle_terminal(&store, cookie, ArState::Succeed { rc: 0 }).await;
                    // Notify the LiveRecvSource thread to call llapi_hsm_action_end(rc=0).
                    if let Some(ref tx) = completion_tx {
                        let kind = action_kind.unwrap_or(ActionKind::Archive);
                        let restore_temp_path = if matches!(kind, ActionKind::Restore) {
                            let p = crate::copytool_recv::restore_temp_path(cookie);
                            if p.exists() { Some(p) } else { None }
                        } else {
                            None
                        };
                        let _ = tx.0.try_send(ActionCompletion {
                            cookie,
                            status: lustre_llapi::EndStatus::Ok,
                            total_bytes,
                            kind,
                            restore_temp_path,
                        });
                    }
                }
                ActionStatus::Failed {
                    cookie,
                    errno,
                    reason,
                } => {
                    warn!(target: "hsmd.status", %cookie, errno, %reason, "failed");
                    let action_kind = pending_paths.remove(&cookie).map(|(_, (_, k))| k);
                    handle_terminal(&store, cookie, ArState::Failed { rc: errno }).await;
                    // Notify the LiveRecvSource thread to call llapi_hsm_action_end(rc=errno).
                    // Clean up any restore temp file on failure.
                    if let Some(ref tx) = completion_tx {
                        let kind = action_kind.unwrap_or(ActionKind::Archive);
                        let restore_temp_path = {
                            let p = crate::copytool_recv::restore_temp_path(cookie);
                            // Unconditional delete — ENOENT is harmless and avoids
                            // a TOCTOU race between exists() and remove_file().
                            let _ = tokio::fs::remove_file(&p).await;
                            None
                        };
                        let _ = tx.0.try_send(ActionCompletion {
                            cookie,
                            status: lustre_llapi::EndStatus::Failed(errno),
                            total_bytes: 0,
                            kind,
                            restore_temp_path,
                        });
                    }
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
