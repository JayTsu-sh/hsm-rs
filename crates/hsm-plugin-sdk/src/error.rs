//! Error type plugins return from [`Mover`](crate::Mover) methods.

use thiserror::Error;

/// Result alias for plugin operations.
pub type MoverResult<T> = Result<T, MoverError>;

/// What can go wrong inside a [`Mover`](crate::Mover) implementation.
///
/// The SDK maps these to the gRPC `ActionStatus.errno` field (M2d) so the
/// daemon can surface a sensible value to `llapi_hsm_action_end`. The
/// classifications are deliberately coarse — exact errno is reported via
/// the `Backend { errno }` variant when callers know one.
#[derive(Debug, Error)]
pub enum MoverError {
    /// Action was cancelled by the daemon (HSMA_CANCEL or shutdown).
    /// Reported to the kernel as ECANCELED (125).
    #[error("transfer cancelled by caller")]
    Cancelled,

    /// Backend rejected the operation with a known POSIX errno.
    /// Examples: ENOENT (object missing on restore), EIO (network).
    #[error("backend errno {errno}: {reason}")]
    Backend {
        /// Positive POSIX errno value.
        errno: i32,
        /// Human-readable context for logs.
        reason: String,
    },

    /// Transient backend failure — daemon may retry the *same* action.
    /// Examples: rate-limited, throttled, connection reset.
    #[error("transient backend failure: {0}")]
    Transient(String),

    /// Integrity check failed (BLAKE3 hash mismatch on restore).
    /// Distinct from `Backend` because the daemon may want to surface
    /// a stronger alert (data corruption, not just transport hiccup).
    #[error("integrity check failed: {0}")]
    Integrity(String),

    /// Mover author chose to fail with a generic message. Tests use this
    /// to keep error setup short; production code should pick something
    /// more specific.
    #[error("{0}")]
    Other(String),
}

impl MoverError {
    /// POSIX errno value the daemon should pass to `llapi_hsm_action_end`.
    /// Maps each variant to a sensible Linux errno (positive integer).
    pub fn errno(&self) -> i32 {
        match self {
            // Linux ECANCELED.
            MoverError::Cancelled => 125,
            MoverError::Backend { errno, .. } => *errno,
            // EAGAIN — coordinator may retry.
            MoverError::Transient(_) => 11,
            // EIO — best generic match for "we got bytes, they were wrong".
            MoverError::Integrity(_) => 5,
            // EIO as catch-all so the kernel never sees rc=0 on a failure.
            MoverError::Other(_) => 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errnos_are_stable_and_distinct_per_class() {
        assert_eq!(MoverError::Cancelled.errno(), 125);
        assert_eq!(MoverError::Transient("x".into()).errno(), 11);
        assert_eq!(MoverError::Integrity("x".into()).errno(), 5);
        assert_eq!(MoverError::Other("x".into()).errno(), 5);
        assert_eq!(MoverError::Backend { errno: 28, reason: "ENOSPC".into() }.errno(), 28);
    }
}
