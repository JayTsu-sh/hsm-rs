//! Agent registry — who's online, what they can serve.
//!
//! "Agent" = one running data-mover (a `hsm-plugin-terrasync` process,
//! or a remote `hsmd-agent` node in coordinator mode). The daemon
//! registers and unregisters agents in response to plugin connect /
//! disconnect events; the scheduler matches actions to agents by
//! archive_id capability.
//!
//! `AgentRegistry` is `Send + Sync` and uses [`parking_lot::RwLock`] —
//! the daemon's main loop reads it on every action, plugin lifecycle
//! events write to it occasionally.

use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use hsm_core::{AgentId, ArchiveId};
use parking_lot::RwLock;

/// Capabilities + last-seen for one connected agent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentInfo {
    /// Stable identifier (typically `hostname:pid` or remote node id).
    pub id: AgentId,
    /// Archive ids this agent can serve.
    pub archives: HashSet<ArchiveId>,
    /// Wall-clock time of registration (for diagnostics).
    pub since: SystemTime,
}

impl AgentInfo {
    /// `true` if this agent advertised support for `archive`.
    pub fn serves(&self, archive: ArchiveId) -> bool {
        self.archives.contains(&archive)
    }
}

/// Registry of currently-connected agents.
///
/// Cheap to clone — internal state is `Arc<RwLock<…>>`. Multiple
/// schedulers / observers / metrics exporters can share one registry.
#[derive(Clone, Default)]
pub struct AgentRegistry {
    inner: std::sync::Arc<RwLock<HashMap<AgentId, AgentInfo>>>,
}

impl AgentRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add (or replace) an agent's record. Replacing is intentional:
    /// the same plugin reconnecting under the same id should drop its
    /// old capability set in favour of whatever it advertises now.
    pub fn register(&self, id: AgentId, archives: impl IntoIterator<Item = ArchiveId>) {
        let info = AgentInfo {
            id: id.clone(),
            archives: archives.into_iter().collect(),
            since: SystemTime::now(),
        };
        self.inner.write().insert(id, info);
    }

    /// Remove an agent. Returns the dropped record so the daemon can
    /// re-dispatch its in-flight actions.
    pub fn unregister(&self, id: &AgentId) -> Option<AgentInfo> {
        self.inner.write().remove(id)
    }

    /// Snapshot of all currently-connected agents.
    pub fn snapshot(&self) -> Vec<AgentInfo> {
        self.inner.read().values().cloned().collect()
    }

    /// All agents that can serve `archive`.
    pub fn agents_serving(&self, archive: ArchiveId) -> Vec<AgentInfo> {
        self.inner
            .read()
            .values()
            .filter(|a| a.serves(archive))
            .cloned()
            .collect()
    }

    /// Number of currently-connected agents.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// `true` if no agents are connected.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(items: &[u32]) -> HashSet<ArchiveId> {
        items.iter().copied().map(ArchiveId::new).collect()
    }

    #[test]
    fn agent_info_serves_only_advertised_archives() {
        let info = AgentInfo {
            id: AgentId::new("m0"),
            archives: ids(&[1, 2]),
            since: SystemTime::UNIX_EPOCH,
        };
        assert!(info.serves(ArchiveId::new(1)));
        assert!(info.serves(ArchiveId::new(2)));
        assert!(!info.serves(ArchiveId::new(3)));
    }

    #[test]
    fn register_and_filter_by_archive() {
        let r = AgentRegistry::new();
        r.register(AgentId::new("m0"), [ArchiveId::new(1), ArchiveId::new(2)]);
        r.register(AgentId::new("m1"), [ArchiveId::new(2), ArchiveId::new(3)]);
        assert_eq!(r.len(), 2);

        let arc1 = r.agents_serving(ArchiveId::new(1));
        assert_eq!(arc1.len(), 1);
        assert_eq!(arc1[0].id, AgentId::new("m0"));

        let arc2 = r.agents_serving(ArchiveId::new(2));
        assert_eq!(arc2.len(), 2);

        let arc4 = r.agents_serving(ArchiveId::new(4));
        assert!(arc4.is_empty());
    }

    #[test]
    fn re_register_replaces_capabilities() {
        let r = AgentRegistry::new();
        r.register(AgentId::new("m0"), [ArchiveId::new(1)]);
        // Same id, broader archive set — old set must be dropped.
        r.register(AgentId::new("m0"), [ArchiveId::new(1), ArchiveId::new(2)]);
        let snap = r.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].archives, ids(&[1, 2]));
    }

    #[test]
    fn unregister_returns_dropped_record() {
        let r = AgentRegistry::new();
        r.register(AgentId::new("m0"), [ArchiveId::new(1)]);
        let dropped = r.unregister(&AgentId::new("m0")).unwrap();
        assert_eq!(dropped.archives, ids(&[1]));
        assert!(r.unregister(&AgentId::new("m0")).is_none());
        assert!(r.is_empty());
    }
}
