//! `FifoPerKind` — the default scheduling strategy.
//!
//! Maintains one queue per [`ActionKind`] (Archive / Restore / Remove /
//! Cancel) and round-robins agents within each archive_id. This matches
//! the kernel coordinator's own dispatch order and is sufficient for
//! single-tier deployments where every agent serves the same archive.
//!
//! Why per-kind, not one big queue: Restore is latency-sensitive (a user
//! is waiting), Archive / Remove are background. Keeping them in
//! separate queues lets later strategies (or just the daemon's tick
//! loop) prioritize Restore without disturbing Archive throughput. The
//! M4 priority strategy will plug in here.
//!
//! Cancel actions are handled at the same priority as Restore — the
//! kernel emits Cancel for in-flight work that needs aborting, and
//! agents need to see it promptly.

use std::collections::VecDeque;

use hsm_core::{Action, ActionKind, AgentId, ArchiveId, Cookie};
use parking_lot::Mutex;

use crate::agent::AgentRegistry;
use crate::scheduler::{Assignment, Scheduler};

/// FIFO-per-kind scheduling strategy.
///
/// Cheap to clone — internal state is `Arc<Mutex<…>>`. The daemon
/// keeps a single instance shared across the recv task and the
/// dispatcher tick.
#[derive(Clone, Default)]
pub struct FifoPerKind {
    inner: std::sync::Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    archive: VecDeque<Action>,
    restore: VecDeque<Action>,
    remove: VecDeque<Action>,
    cancel: VecDeque<Action>,
    /// Round-robin cursor per archive_id, so successive picks for the
    /// same archive don't always hit the first agent.
    rr: std::collections::HashMap<ArchiveId, usize>,
}

impl FifoPerKind {
    /// Construct an empty scheduler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pick a queue in priority order. Restore + Cancel before
    /// Archive + Remove.
    fn next_action(inner: &mut Inner) -> Option<Action> {
        // Round-robin between Cancel and Restore so neither starves the
        // other when both are busy. Coin-flip via the sum of queue
        // depths — whichever fits the user's request profile better
        // wins. Concretely: if cancel has more, drain cancel first.
        if inner.cancel.len() >= inner.restore.len() {
            if let Some(a) = inner.cancel.pop_front() {
                return Some(a);
            }
            if let Some(a) = inner.restore.pop_front() {
                return Some(a);
            }
        } else {
            if let Some(a) = inner.restore.pop_front() {
                return Some(a);
            }
            if let Some(a) = inner.cancel.pop_front() {
                return Some(a);
            }
        }
        // Background tier: Archive then Remove (Remove is fast metadata
        // work, Archive is long IO; favour Archive so we don't starve
        // it behind a flood of Removes).
        if let Some(a) = inner.archive.pop_front() {
            return Some(a);
        }
        inner.remove.pop_front()
    }

    /// Pick an agent for `archive` using a round-robin cursor.
    /// Returns `None` if no connected agent serves this archive.
    fn pick_agent(
        rr: &mut std::collections::HashMap<ArchiveId, usize>,
        agents: &AgentRegistry,
        archive: ArchiveId,
    ) -> Option<AgentId> {
        let candidates = agents.agents_serving(archive);
        if candidates.is_empty() {
            return None;
        }
        let cursor = rr.entry(archive).or_insert(0);
        let idx = *cursor % candidates.len();
        *cursor = (*cursor).wrapping_add(1);
        Some(candidates[idx].id.clone())
    }
}

impl Scheduler for FifoPerKind {
    fn enqueue(&self, action: Action) {
        let mut inner = self.inner.lock();
        let q = match action.kind {
            ActionKind::Archive => &mut inner.archive,
            ActionKind::Restore => &mut inner.restore,
            ActionKind::Remove => &mut inner.remove,
            ActionKind::Cancel => &mut inner.cancel,
        };
        q.push_back(action);
    }

    fn pick_ready(&self, agents: &AgentRegistry, max: usize) -> Vec<Assignment> {
        let mut out = Vec::with_capacity(max);
        if max == 0 || agents.is_empty() {
            return out;
        }
        let mut inner = self.inner.lock();
        // Track actions we couldn't assign so we can put them back at
        // the *front* — preserving FIFO order from the original
        // submission.
        let mut requeue: Vec<Action> = Vec::new();
        while out.len() < max {
            let Some(action) = Self::next_action(&mut inner) else {
                break;
            };
            match Self::pick_agent(&mut inner.rr, agents, action.archive_id) {
                Some(agent) => out.push(Assignment { action, agent }),
                None => requeue.push(action),
            }
        }
        // Restore unassigned actions to the head of their respective
        // queue, in reverse so the original order is preserved.
        for a in requeue.into_iter().rev() {
            let q = match a.kind {
                ActionKind::Archive => &mut inner.archive,
                ActionKind::Restore => &mut inner.restore,
                ActionKind::Remove => &mut inner.remove,
                ActionKind::Cancel => &mut inner.cancel,
            };
            q.push_front(a);
        }
        out
    }

    fn on_complete(&self, _cookie: Cookie, _rc: i32) {
        // FIFO has no per-cookie tracking; future strategies (batching,
        // affinity) override.
    }

    fn on_disconnect(&self, _agent: &AgentId) {
        // FIFO doesn't pin actions to specific agents at enqueue time —
        // re-dispatch happens naturally because nothing's bound. The
        // daemon's reassignment of in-flight actions for a dropped
        // agent goes through the action store + a fresh enqueue here.
    }

    fn queue_depth(&self) -> usize {
        let inner = self.inner.lock();
        inner.archive.len() + inner.restore.len() + inner.remove.len() + inner.cancel.len()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use hsm_core::{Cookie, Extent, Fid};

    use super::*;

    fn act(cookie: u64, kind: ActionKind, archive: u32) -> Action {
        Action {
            cookie: Cookie::new(cookie),
            fid: Fid::new(1, cookie as u32, 0),
            dfid: Fid::new(1, cookie as u32, 0),
            archive_id: ArchiveId::new(archive),
            kind,
            extent: Extent::WHOLE,
            gid: 0,
            data: Bytes::new(),
        }
    }

    #[test]
    fn empty_registry_returns_no_assignments() {
        let s = FifoPerKind::new();
        s.enqueue(act(1, ActionKind::Archive, 1));
        let assignments = s.pick_ready(&AgentRegistry::new(), 10);
        assert!(assignments.is_empty());
        // Action stays queued for next attempt.
        assert_eq!(s.queue_depth(), 1);
    }

    #[test]
    fn restore_drains_before_archive() {
        let s = FifoPerKind::new();
        let r = AgentRegistry::new();
        r.register(AgentId::new("m0"), [ArchiveId::new(1)]);

        // Two archives queued first, then a restore. Priority should
        // surface the restore before either archive.
        s.enqueue(act(10, ActionKind::Archive, 1));
        s.enqueue(act(11, ActionKind::Archive, 1));
        s.enqueue(act(20, ActionKind::Restore, 1));

        let picked = s.pick_ready(&r, 1);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].action.cookie, Cookie::new(20));
    }

    #[test]
    fn cancel_and_restore_share_priority() {
        let s = FifoPerKind::new();
        let r = AgentRegistry::new();
        r.register(AgentId::new("m0"), [ArchiveId::new(1)]);

        s.enqueue(act(20, ActionKind::Restore, 1));
        s.enqueue(act(30, ActionKind::Cancel, 1));
        // Both should drain before any background work runs; order
        // between them is balanced (cancel-heavy → cancel first).
        let picked = s.pick_ready(&r, 10);
        let kinds: Vec<_> = picked.iter().map(|a| a.action.kind).collect();
        assert!(kinds.contains(&ActionKind::Cancel));
        assert!(kinds.contains(&ActionKind::Restore));
        assert_eq!(picked.len(), 2);
    }

    #[test]
    fn picks_only_for_capable_agents_others_requeued() {
        let s = FifoPerKind::new();
        let r = AgentRegistry::new();
        // Only an agent for archive 2 is online.
        r.register(AgentId::new("m2"), [ArchiveId::new(2)]);

        // One action for archive 1 (no capable agent), one for archive 2.
        s.enqueue(act(1, ActionKind::Archive, 1));
        s.enqueue(act(2, ActionKind::Archive, 2));

        let picked = s.pick_ready(&r, 10);
        // Only the archive-2 action gets dispatched. The archive-1
        // action stays in the queue.
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].action.archive_id, ArchiveId::new(2));
        assert_eq!(picked[0].agent, AgentId::new("m2"));
        assert_eq!(s.queue_depth(), 1);
    }

    #[test]
    fn round_robins_agents_within_an_archive() {
        let s = FifoPerKind::new();
        let r = AgentRegistry::new();
        r.register(AgentId::new("m0"), [ArchiveId::new(1)]);
        r.register(AgentId::new("m1"), [ArchiveId::new(1)]);

        for c in 0..6 {
            s.enqueue(act(c, ActionKind::Archive, 1));
        }
        let picked = s.pick_ready(&r, 6);
        let agents: Vec<_> = picked
            .iter()
            .map(|a| a.agent.as_str().to_string())
            .collect();

        // We saw both agents (round-robin worked) and the population is
        // balanced (3 each in any order — agents_serving returns a Vec
        // whose order isn't guaranteed across calls but cursor advance
        // gives at least one of each).
        let m0_count = agents.iter().filter(|n| *n == "m0").count();
        let m1_count = agents.iter().filter(|n| *n == "m1").count();
        assert!(m0_count >= 1 && m1_count >= 1, "saw {agents:?}");
        assert_eq!(m0_count + m1_count, 6);
    }

    #[test]
    fn requeue_preserves_fifo_order() {
        let s = FifoPerKind::new();
        let r = AgentRegistry::new();
        // No agents → all picks fail and go back into queue.
        s.enqueue(act(1, ActionKind::Archive, 1));
        s.enqueue(act(2, ActionKind::Archive, 1));
        s.enqueue(act(3, ActionKind::Archive, 1));

        let picked = s.pick_ready(&r, 10);
        assert!(picked.is_empty());
        assert_eq!(s.queue_depth(), 3);

        // Now bring an agent online; FIFO order must be preserved.
        r.register(AgentId::new("m0"), [ArchiveId::new(1)]);
        let picked = s.pick_ready(&r, 10);
        let cookies: Vec<_> = picked.iter().map(|a| a.action.cookie.get()).collect();
        assert_eq!(cookies, vec![1, 2, 3]);
    }

    #[test]
    fn max_caps_returned_assignments() {
        let s = FifoPerKind::new();
        let r = AgentRegistry::new();
        r.register(AgentId::new("m0"), [ArchiveId::new(1)]);
        for c in 0..10 {
            s.enqueue(act(c, ActionKind::Archive, 1));
        }
        let picked = s.pick_ready(&r, 3);
        assert_eq!(picked.len(), 3);
        assert_eq!(s.queue_depth(), 7);
    }

    #[test]
    fn queue_depth_tracks_per_kind() {
        let s = FifoPerKind::new();
        s.enqueue(act(1, ActionKind::Archive, 1));
        s.enqueue(act(2, ActionKind::Restore, 1));
        s.enqueue(act(3, ActionKind::Remove, 1));
        s.enqueue(act(4, ActionKind::Cancel, 1));
        assert_eq!(s.queue_depth(), 4);
    }
}
