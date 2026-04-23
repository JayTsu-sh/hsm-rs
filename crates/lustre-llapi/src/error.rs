//! Error type for the safe HSM wrapper.

use std::path::PathBuf;

use lustre_hsm_uapi::conv::ConvError;
use thiserror::Error;

/// Result alias for the lustre-llapi crate.
pub type Result<T> = std::result::Result<T, HsmError>;

/// Failures the safe wrapper can surface.
///
/// All variants carry enough context to log + classify (retryable vs fatal)
/// without re-deriving from a bare errno.
#[derive(Debug, Error)]
pub enum HsmError {
    /// `llapi_hsm_copytool_register` failed (bad mount, no Lustre kernel
    /// module, no permission, etc.).
    #[error("copytool registration on {mount} failed: errno {errno}")]
    Register {
        /// The mount point we tried to register on.
        mount: PathBuf,
        /// Negative return code from llapi (already converted to positive errno).
        errno: i32,
    },

    /// `llapi_hsm_copytool_recv` failed.
    #[error("copytool recv failed: errno {0}")]
    Recv(i32),

    /// `llapi_hsm_action_begin` failed.
    #[error("hsm_action_begin failed: errno {0}")]
    Begin(i32),

    /// `llapi_hsm_action_end` failed.
    #[error("hsm_action_end failed: errno {0}")]
    End(i32),

    /// `llapi_hsm_action_progress` failed.
    #[error("hsm_action_progress failed: errno {0}")]
    Progress(i32),

    /// Walker encountered an `hsm_action_item` whose declared length is
    /// inconsistent with the surrounding `hsm_action_list`.
    #[error("malformed hsm_action_list: {reason}")]
    MalformedActionList {
        /// Human-readable explanation.
        reason: String,
    },

    /// Wire `hai_action` value did not map to any [`ActionKind`]
    /// we model.
    ///
    /// [`ActionKind`]: hsm_core::ActionKind
    #[error("unknown wire action kind {0}")]
    UnknownActionKind(u32),

    /// A method was called against an unknown / already-completed cookie.
    /// Most often hit in the mock; the live path returns this if begin/end
    /// is invoked for a cookie the kernel never issued.
    #[error("no in-flight action for cookie {0:#x}")]
    UnknownCookie(u64),

    /// Caller passed an empty archive list to register. Lustre accepts this
    /// (means "all archives") but we reject it here to force callers to be
    /// explicit — silently inheriting all archives is rarely what you want.
    #[error("register requires at least one archive id")]
    EmptyArchiveList,
}

impl From<ConvError> for HsmError {
    fn from(e: ConvError) -> Self {
        match e {
            ConvError::UnknownCopytoolAction(v) => HsmError::UnknownActionKind(v),
        }
    }
}
