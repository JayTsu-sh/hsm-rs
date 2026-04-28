//! [`AnyStore`] — type-erased store for the `hsmd` binary.
//!
//! `ActionStore` uses `impl Future` in return position, which makes the trait
//! not object-safe (no `dyn ActionStore`). `AnyStore` is an enum that
//! delegates to either [`MemStore`] or [`SqliteStore`] without dynamic
//! dispatch, letting the binary pick the backend at startup without a
//! type-parameter explosion.

use hsm_core::{ActionRecord, AgentId, ArState, Cookie, Extent};

use crate::{ActionStore, MemStore, SqliteStore, StoreResult};

/// Store backend selector for the `hsmd` binary.
pub enum AnyStore {
    /// In-memory store — fast, no persistence. Suitable for dev / CI.
    Mem(MemStore),
    /// SQLite-backed store — crash-safe. Required for production.
    Sqlite(SqliteStore),
}

impl ActionStore for AnyStore {
    async fn insert(&self, rec: ActionRecord) -> StoreResult<()> {
        match self {
            Self::Mem(s) => s.insert(rec).await,
            Self::Sqlite(s) => s.insert(rec).await,
        }
    }

    async fn transition(&self, cookie: Cookie, new: ArState) -> StoreResult<()> {
        match self {
            Self::Mem(s) => s.transition(cookie, new).await,
            Self::Sqlite(s) => s.transition(cookie, new).await,
        }
    }

    async fn update_progress(&self, cookie: Cookie, e: Extent) -> StoreResult<()> {
        match self {
            Self::Mem(s) => s.update_progress(cookie, e).await,
            Self::Sqlite(s) => s.update_progress(cookie, e).await,
        }
    }

    async fn delete(&self, cookie: Cookie) -> StoreResult<()> {
        match self {
            Self::Mem(s) => s.delete(cookie).await,
            Self::Sqlite(s) => s.delete(cookie).await,
        }
    }

    async fn get(&self, cookie: Cookie) -> StoreResult<Option<ActionRecord>> {
        match self {
            Self::Mem(s) => s.get(cookie).await,
            Self::Sqlite(s) => s.get(cookie).await,
        }
    }

    async fn load_all(&self) -> StoreResult<Vec<ActionRecord>> {
        match self {
            Self::Mem(s) => s.load_all().await,
            Self::Sqlite(s) => s.load_all().await,
        }
    }

    async fn list_by_agent(&self, agent: &AgentId) -> StoreResult<Vec<ActionRecord>> {
        match self {
            Self::Mem(s) => s.list_by_agent(agent).await,
            Self::Sqlite(s) => s.list_by_agent(agent).await,
        }
    }

    async fn len(&self) -> StoreResult<usize> {
        match self {
            Self::Mem(s) => s.len().await,
            Self::Sqlite(s) => s.len().await,
        }
    }

    async fn query_actions(
        &self,
        state_filter: Option<&str>,
        archive_id: Option<u32>,
        limit: usize,
    ) -> StoreResult<Vec<ActionRecord>> {
        match self {
            Self::Mem(s) => s.query_actions(state_filter, archive_id, limit).await,
            Self::Sqlite(s) => s.query_actions(state_filter, archive_id, limit).await,
        }
    }

    async fn status_counts(&self) -> StoreResult<(u64, u64, u64)> {
        match self {
            Self::Mem(s) => s.status_counts().await,
            // SqliteStore overrides status_counts with an efficient GROUP BY query.
            Self::Sqlite(s) => s.status_counts().await,
        }
    }
}
