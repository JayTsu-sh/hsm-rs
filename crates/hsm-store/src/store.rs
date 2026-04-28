//! The [`ActionStore`] trait.
//!
//! Async by construction (Rust 2024 native `async fn` in trait) so the
//! same trait fits both the in-memory impl (where awaits are no-ops) and
//! the future SqliteStore where awaits hit disk.

use hsm_core::{ActionRecord, AgentId, ArState, Cookie, Extent};

use crate::error::StoreResult;

/// Persistent state for in-flight HSM actions.
///
/// Implementations must enforce the invariants documented on the crate
/// root: cookie uniqueness on insert, monotonic progress, sticky terminal
/// states. Tests asserting cross-implementation behavior live in
/// `tests/contract.rs` and exercise every trait impl through the same
/// suite.
pub trait ActionStore: Send + Sync {
    /// Add a new record to the store.
    ///
    /// Returns [`StoreError::DuplicateCookie`] if a record with the same
    /// cookie already exists; the daemon should never see this barring a
    /// bug.
    ///
    /// [`StoreError::DuplicateCookie`]: crate::StoreError::DuplicateCookie
    fn insert(&self, rec: ActionRecord) -> impl Future<Output = StoreResult<()>> + Send;

    /// Transition `cookie`'s state to `new`.
    ///
    /// Rejects transitions out of terminal states with
    /// [`StoreError::TerminalTransition`]; rejects unknown cookies with
    /// [`StoreError::UnknownCookie`].
    ///
    /// [`StoreError::TerminalTransition`]: crate::StoreError::TerminalTransition
    /// [`StoreError::UnknownCookie`]: crate::StoreError::UnknownCookie
    fn transition(
        &self,
        cookie: Cookie,
        new: ArState,
    ) -> impl Future<Output = StoreResult<()>> + Send;

    /// Record progress on a streaming transfer. The new extent's `offset`
    /// must be `>=` the current one — see
    /// [`StoreError::ProgressRegression`].
    ///
    /// [`StoreError::ProgressRegression`]: crate::StoreError::ProgressRegression
    fn update_progress(
        &self,
        cookie: Cookie,
        e: Extent,
    ) -> impl Future<Output = StoreResult<()>> + Send;

    /// Remove a record from the store. No-op if the cookie isn't present;
    /// this matches the daemon's clean-up path where the same delete may
    /// race with a recovery sweep.
    fn delete(&self, cookie: Cookie) -> impl Future<Output = StoreResult<()>> + Send;

    /// Fetch a single record by cookie.
    fn get(&self, cookie: Cookie)
    -> impl Future<Output = StoreResult<Option<ActionRecord>>> + Send;

    /// Drain everything for crash recovery. Order is unspecified — the
    /// scheduler is responsible for re-queueing anything that's still
    /// `Waiting` or `Started` past a grace period.
    fn load_all(&self) -> impl Future<Output = StoreResult<Vec<ActionRecord>>> + Send;

    /// All records currently attributed to `agent` (i.e. in
    /// `ArState::Started { agent: …, .. }`). Used when a plugin
    /// disconnects to figure out what to reassign.
    fn list_by_agent(
        &self,
        agent: &AgentId,
    ) -> impl Future<Output = StoreResult<Vec<ActionRecord>>> + Send;

    /// Number of records currently held — handy for observability and
    /// for tests that don't want to drain the whole store via `load_all`.
    fn len(&self) -> impl Future<Output = StoreResult<usize>> + Send;

    /// `true` if the store holds no records.
    fn is_empty(&self) -> impl Future<Output = StoreResult<bool>> + Send {
        async move { Ok(self.len().await? == 0) }
    }

    /// Filtered action listing with an upper bound on result size.
    ///
    /// - `state_filter`: `None` = all; `Some("waiting"|"started"|…)` filters by state name.
    /// - `archive_id`: `None` = all archives.
    /// - `limit`: hard cap on the number of returned records.
    ///
    /// Default: `load_all()` + in-memory filter. Override with SQL WHERE + LIMIT.
    fn query_actions(
        &self,
        state_filter: Option<&str>,
        archive_id: Option<u32>,
        limit: usize,
    ) -> impl Future<Output = StoreResult<Vec<ActionRecord>>> + Send
    where
        Self: Sync,
    {
        let state_filter = state_filter.map(str::to_lowercase);
        async move {
            let all = self.load_all().await?;
            Ok(all
                .into_iter()
                .filter(|r| {
                    if let Some(aid) = archive_id {
                        if r.action.archive_id.get() != aid {
                            return false;
                        }
                    }
                    match state_filter.as_deref() {
                        Some("waiting") => matches!(r.state, ArState::Waiting),
                        Some("started") => matches!(r.state, ArState::Started { .. }),
                        Some("succeed") => matches!(r.state, ArState::Succeed { .. }),
                        Some("failed") => matches!(r.state, ArState::Failed { .. }),
                        Some("canceled") => matches!(r.state, ArState::Canceled),
                        _ => true,
                    }
                })
                .take(limit)
                .collect())
        }
    }

    /// Count actions by state without materialising full records.
    ///
    /// Returns `(waiting, started, total)`.
    ///
    /// The default implementation calls `load_all()` — persistent stores
    /// should override with a backend-native aggregate query.
    fn status_counts(&self) -> impl Future<Output = StoreResult<(u64, u64, u64)>> + Send
    where
        Self: Sync,
    {
        async move {
            let records = self.load_all().await?;
            let total = records.len() as u64;
            let (mut waiting, mut started) = (0u64, 0u64);
            for r in &records {
                match r.state {
                    ArState::Waiting => waiting += 1,
                    ArState::Started { .. } => started += 1,
                    _ => {}
                }
            }
            Ok((waiting, started, total))
        }
    }
}
