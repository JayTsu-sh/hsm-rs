//! The [`Scheduler`] trait + [`Assignment`] result type.

use hsm_core::{Action, AgentId, Cookie};

use crate::agent::AgentRegistry;

/// The output of a successful scheduling decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    /// Action chosen for dispatch.
    pub action: Action,
    /// Agent that should run it.
    pub agent: AgentId,
}

/// Pluggable scheduling strategy.
///
/// Implementations are stateful (the FIFO impl owns per-kind queues),
/// but **synchronous** — picking the next batch is fast in-memory work.
/// The daemon's main loop runs `pick_ready` at a fixed cadence (see
/// `docs/DESIGN.md` §6) so contention is bounded.
///
/// Composition: rather than a single monolithic strategy, the daemon
/// will eventually thread an `Action` through a chain (`HostAffinity →
/// BatchByHint → ConsistentHash → FifoPerKind`). Each strategy's
/// `enqueue` consults its own routing rules and either claims the
/// action or passes it to the next link. The trait stays uniform — the
/// chain is built from outside.
pub trait Scheduler: Send + Sync {
    /// Add an action to the schedulable pool. Implementations decide
    /// internal placement (per-kind queue, per-archive bucket, etc.).
    fn enqueue(&self, action: Action);

    /// Drain ready-to-dispatch actions, capped by `max`. The scheduler
    /// must only return assignments for which the chosen agent is in
    /// `agents` *and* serves the action's archive id.
    ///
    /// The daemon calls this every tick; returning an empty vec means
    /// "nothing ready (queues empty, no capable agents, or all
    /// matching agents are saturated downstream)".
    fn pick_ready(&self, agents: &AgentRegistry, max: usize) -> Vec<Assignment>;

    /// Notify the scheduler that `cookie` has finished. Default no-op;
    /// strategies that track inflight (e.g. batching slots) override.
    fn on_complete(&self, _cookie: Cookie, _rc: i32) {}

    /// Notify the scheduler that `agent` disconnected. Default no-op;
    /// strategies that pin actions to specific agents (host-affinity)
    /// must release those bindings here.
    fn on_disconnect(&self, _agent: &AgentId) {}

    /// Number of actions currently waiting (not yet dispatched).
    /// Mostly for metrics + tests.
    fn queue_depth(&self) -> usize;
}
