//! Errors surfaced from store operations.

use hsm_core::{ArState, Cookie, Extent};
use thiserror::Error;

/// Result alias for store operations.
pub type StoreResult<T> = Result<T, StoreError>;

/// Failures the store may report to the caller.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum StoreError {
    /// Tried to `insert` a record whose cookie already exists.
    /// The kernel coordinator does not reuse in-flight cookies, so this
    /// is always a hsm-rs bug — surfaced rather than silently overwritten.
    #[error("cookie {0} already present in store")]
    DuplicateCookie(Cookie),

    /// Tried to operate on a cookie the store has never seen (or has
    /// already removed via [`crate::ActionStore::delete`]).
    #[error("cookie {0} not found in store")]
    UnknownCookie(Cookie),

    /// `update_progress` was called with an offset earlier than the
    /// current progress — a buggy mover going backwards.
    #[error("progress would regress for cookie {cookie}: have {have:?}, got {got:?}")]
    ProgressRegression {
        /// The cookie whose progress we tried to update.
        cookie: Cookie,
        /// The currently-stored extent.
        have: Extent,
        /// The extent the caller tried to write.
        got: Extent,
    },

    /// `transition` was called from a terminal state. Once Succeed /
    /// Failed / Canceled, the record is sticky.
    #[error("cookie {cookie} cannot transition out of terminal state {from:?}")]
    TerminalTransition {
        /// The cookie whose state we tried to mutate.
        cookie: Cookie,
        /// The terminal state currently held.
        from: ArState,
    },

    /// Persistence backend (sqlite, redis, …) failed. Mem store never
    /// returns this; SqliteStore wraps `sqlx::Error` here.
    #[error("backend error: {0}")]
    Backend(String),
}
