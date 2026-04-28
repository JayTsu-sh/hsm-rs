//! In-memory [`ActionStore`] implementation.
//!
//! Backed by [`DashMap`] for sharded read-mostly access without a global
//! mutex. Suitable for:
//!
//! - Unit / integration tests of higher layers
//! - Single-node deployments where loss-on-restart is acceptable (rare;
//!   the daemon will re-pull from the kernel coordinator)
//!
//! The Sqlite-backed variant in M2b' has the same observable semantics —
//! the contract tests in `tests/contract.rs` exercise both.

use std::time::SystemTime;

use dashmap::DashMap;
use hsm_core::{ActionRecord, AgentId, ArState, Cookie, Extent};

use crate::error::{StoreError, StoreResult};
use crate::store::ActionStore;

/// In-memory action store.
///
/// `Clone` is intentional: handed Arc-style around the daemon. Each clone
/// shares the same underlying [`DashMap`].
#[derive(Clone, Default)]
pub struct MemStore {
    inner: std::sync::Arc<DashMap<Cookie, ActionRecord>>,
}

impl MemStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ActionStore for MemStore {
    async fn insert(&self, rec: ActionRecord) -> StoreResult<()> {
        let cookie = rec.action.cookie;
        // `entry().or_insert_with` would silently overwrite via
        // `or_insert_with`; we want explicit failure on duplicate.
        if self.inner.contains_key(&cookie) {
            return Err(StoreError::DuplicateCookie(cookie));
        }
        self.inner.insert(cookie, rec);
        Ok(())
    }

    async fn transition(&self, cookie: Cookie, new: ArState) -> StoreResult<()> {
        let mut entry = self
            .inner
            .get_mut(&cookie)
            .ok_or(StoreError::UnknownCookie(cookie))?;
        if entry.state.is_terminal() {
            return Err(StoreError::TerminalTransition {
                cookie,
                from: entry.state.clone(),
            });
        }
        entry.state = new;
        entry.updated_at = SystemTime::now();
        Ok(())
    }

    async fn update_progress(&self, cookie: Cookie, e: Extent) -> StoreResult<()> {
        let mut entry = self
            .inner
            .get_mut(&cookie)
            .ok_or(StoreError::UnknownCookie(cookie))?;
        if e.offset < entry.progress.offset {
            return Err(StoreError::ProgressRegression {
                cookie,
                have: entry.progress,
                got: e,
            });
        }
        entry.progress = e;
        entry.updated_at = SystemTime::now();
        Ok(())
    }

    async fn delete(&self, cookie: Cookie) -> StoreResult<()> {
        // Idempotent: missing key is fine.
        self.inner.remove(&cookie);
        Ok(())
    }

    async fn get(&self, cookie: Cookie) -> StoreResult<Option<ActionRecord>> {
        Ok(self.inner.get(&cookie).map(|e| e.value().clone()))
    }

    async fn load_all(&self) -> StoreResult<Vec<ActionRecord>> {
        Ok(self.inner.iter().map(|e| e.value().clone()).collect())
    }

    async fn list_by_agent(&self, agent: &AgentId) -> StoreResult<Vec<ActionRecord>> {
        Ok(self
            .inner
            .iter()
            .filter(|e| matches!(&e.value().state, ArState::Started { agent: a, .. } if a == agent))
            .map(|e| e.value().clone())
            .collect())
    }

    async fn len(&self) -> StoreResult<usize> {
        Ok(self.inner.len())
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use bytes::Bytes;
    use hsm_core::{Action, ActionKind, ArchiveId, Fid};

    use super::*;

    fn record(cookie: u64) -> ActionRecord {
        let action = Action {
            cookie: Cookie::new(cookie),
            fid: Fid::new(0x200000401, cookie as u32, 0),
            dfid: Fid::new(0x200000401, cookie as u32, 0),
            archive_id: ArchiveId::new(1),
            kind: ActionKind::Archive,
            extent: Extent::WHOLE,
            gid: 0,
            data: Bytes::new(),
        };
        ActionRecord::new(action, SystemTime::UNIX_EPOCH)
    }

    #[tokio::test]
    async fn insert_then_get_round_trip() {
        let s = MemStore::new();
        s.insert(record(1)).await.unwrap();
        let r = s.get(Cookie::new(1)).await.unwrap().unwrap();
        assert_eq!(r.action.cookie, Cookie::new(1));
        assert!(matches!(r.state, ArState::Waiting));
    }

    #[tokio::test]
    async fn duplicate_insert_errors() {
        let s = MemStore::new();
        s.insert(record(1)).await.unwrap();
        let err = s.insert(record(1)).await.unwrap_err();
        assert_eq!(err, StoreError::DuplicateCookie(Cookie::new(1)));
    }

    #[tokio::test]
    async fn transition_advances_state_and_timestamp() {
        let s = MemStore::new();
        s.insert(record(1)).await.unwrap();
        let agent = AgentId::new("mover-0");
        s.transition(
            Cookie::new(1),
            ArState::Started {
                agent: agent.clone(),
                since_unix_ms: 0,
            },
        )
        .await
        .unwrap();
        let r = s.get(Cookie::new(1)).await.unwrap().unwrap();
        assert!(matches!(r.state, ArState::Started { .. }));
        assert!(r.updated_at >= r.created_at);
    }

    #[tokio::test]
    async fn transition_from_terminal_is_rejected() {
        let s = MemStore::new();
        s.insert(record(1)).await.unwrap();
        s.transition(Cookie::new(1), ArState::Succeed { rc: 0 })
            .await
            .unwrap();
        let err = s
            .transition(Cookie::new(1), ArState::Waiting)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::TerminalTransition {
                cookie,
                from: ArState::Succeed { rc: 0 },
            } if cookie == Cookie::new(1)
        ));
    }

    #[tokio::test]
    async fn unknown_cookie_errors_on_transition_progress_get() {
        let s = MemStore::new();
        let bad = Cookie::new(999);
        assert_eq!(
            s.transition(bad, ArState::Waiting).await.unwrap_err(),
            StoreError::UnknownCookie(bad)
        );
        assert_eq!(
            s.update_progress(bad, Extent::new(0, 1)).await.unwrap_err(),
            StoreError::UnknownCookie(bad)
        );
        assert!(s.get(bad).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn progress_advances_and_rejects_regression() {
        let s = MemStore::new();
        s.insert(record(1)).await.unwrap();
        s.update_progress(Cookie::new(1), Extent::new(0, 4096))
            .await
            .unwrap();
        s.update_progress(Cookie::new(1), Extent::new(4096, 4096))
            .await
            .unwrap();

        let err = s
            .update_progress(Cookie::new(1), Extent::new(2048, 1024))
            .await
            .unwrap_err();
        match err {
            StoreError::ProgressRegression { cookie, have, got } => {
                assert_eq!(cookie, Cookie::new(1));
                assert_eq!(have.offset, 4096);
                assert_eq!(got.offset, 2048);
            }
            other => panic!("expected ProgressRegression, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let s = MemStore::new();
        s.delete(Cookie::new(404)).await.unwrap(); // nothing to remove, no error
        s.insert(record(1)).await.unwrap();
        s.delete(Cookie::new(1)).await.unwrap();
        s.delete(Cookie::new(1)).await.unwrap(); // second delete also no-op
        assert!(s.get(Cookie::new(1)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_by_agent_filters_started_only() {
        let s = MemStore::new();
        let m0 = AgentId::new("mover-0");
        let m1 = AgentId::new("mover-1");

        s.insert(record(1)).await.unwrap();
        s.insert(record(2)).await.unwrap();
        s.insert(record(3)).await.unwrap();

        s.transition(
            Cookie::new(1),
            ArState::Started {
                agent: m0.clone(),
                since_unix_ms: 0,
            },
        )
        .await
        .unwrap();
        s.transition(
            Cookie::new(2),
            ArState::Started {
                agent: m1.clone(),
                since_unix_ms: 0,
            },
        )
        .await
        .unwrap();
        // 3 stays Waiting → not attributed to any agent.

        let m0_rows = s.list_by_agent(&m0).await.unwrap();
        assert_eq!(m0_rows.len(), 1);
        assert_eq!(m0_rows[0].action.cookie, Cookie::new(1));

        let m1_rows = s.list_by_agent(&m1).await.unwrap();
        assert_eq!(m1_rows.len(), 1);
        assert_eq!(m1_rows[0].action.cookie, Cookie::new(2));

        // Recovery preview: load_all returns everything.
        let all = s.load_all().await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn len_tracks_inserts_and_deletes() {
        let s = MemStore::new();
        assert_eq!(s.len().await.unwrap(), 0);
        s.insert(record(1)).await.unwrap();
        s.insert(record(2)).await.unwrap();
        assert_eq!(s.len().await.unwrap(), 2);
        s.delete(Cookie::new(1)).await.unwrap();
        assert_eq!(s.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let a = MemStore::new();
        let b = a.clone();
        a.insert(record(1)).await.unwrap();
        assert!(b.get(Cookie::new(1)).await.unwrap().is_some());
    }
}
