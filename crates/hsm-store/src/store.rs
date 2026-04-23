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
    fn transition(&self, cookie: Cookie, new: ArState) -> impl Future<Output = StoreResult<()>> + Send;

    /// Record progress on a streaming transfer. The new extent's `offset`
    /// must be `>=` the current one — see
    /// [`StoreError::ProgressRegression`].
    ///
    /// [`StoreError::ProgressRegression`]: crate::StoreError::ProgressRegression
    fn update_progress(&self, cookie: Cookie, e: Extent) -> impl Future<Output = StoreResult<()>> + Send;

    /// Remove a record from the store. No-op if the cookie isn't present;
    /// this matches the daemon's clean-up path where the same delete may
    /// race with a recovery sweep.
    fn delete(&self, cookie: Cookie) -> impl Future<Output = StoreResult<()>> + Send;

    /// Fetch a single record by cookie.
    fn get(&self, cookie: Cookie) -> impl Future<Output = StoreResult<Option<ActionRecord>>> + Send;

    /// Drain everything for crash recovery. Order is unspecified — the
    /// scheduler is responsible for re-queueing anything that's still
    /// `Waiting` or `Started` past a grace period.
    fn load_all(&self) -> impl Future<Output = StoreResult<Vec<ActionRecord>>> + Send;

    /// All records currently attributed to `agent` (i.e. in
    /// `ArState::Started { agent: …, .. }`). Used when a plugin
    /// disconnects to figure out what to reassign.
    fn list_by_agent(&self, agent: &AgentId) -> impl Future<Output = StoreResult<Vec<ActionRecord>>> + Send;

    /// Number of records currently held — handy for observability and
    /// for tests that don't want to drain the whole store via `load_all`.
    fn len(&self) -> impl Future<Output = StoreResult<usize>> + Send;
}
