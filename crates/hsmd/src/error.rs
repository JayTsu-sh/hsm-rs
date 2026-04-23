//! Error type for the daemon.

use hsm_store::StoreError;
use lustre_llapi::HsmError;
use thiserror::Error;

/// Result alias for daemon operations.
pub type DaemonResult<T> = Result<T, DaemonError>;

/// Failures the daemon may report. Wraps the lower-level error types so
/// upstream loggers can attach context without unpicking the underlying
/// cause.
#[derive(Debug, Error)]
pub enum DaemonError {
    /// Persisted-state failure (insert / transition / progress).
    #[error("store error: {0}")]
    Store(#[from] StoreError),

    /// Lustre llapi failure (recv, begin, end, progress).
    #[error("hsm error: {0}")]
    Hsm(#[from] HsmError),

    /// Internal channel closed unexpectedly. Either an agent task
    /// exited / panicked, or shutdown raced with normal traffic.
    #[error("channel closed: {0}")]
    ChannelClosed(&'static str),

    /// No agent currently registered to serve `archive_id`.
    /// Surfaced from the dispatcher so callers can decide whether to
    /// retry, dead-letter, or fail the action.
    #[error("no agent registered for archive_id {0}")]
    NoCapableAgent(u32),
}
