//! HSM-relevant errno wrapper.
//!
//! Lustre HSM reports completion status via positive `errno` values
//! (see `hsm_progress::hp_errval`, `llapi_hsm_action_end(rc)`). This
//! wrapper keeps `i32` errnos from getting confused with arbitrary
//! integers when they pass through the action store / scheduler.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Wrapper around a positive `errno` reported through the HSM ABI.
///
/// Values are POSIX errno (`EIO`, `ENOENT`, …); we don't enumerate them
/// because the kernel can return any errno, and the platform's `libc`
/// crate is the only authoritative source. `0` means success.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HsmErrno(pub i32);

impl HsmErrno {
    /// Success.
    pub const OK: Self = Self(0);

    /// Wrap a raw `i32`.
    pub const fn new(v: i32) -> Self {
        Self(v)
    }

    /// `true` if this is the success sentinel (`0`).
    pub const fn is_ok(self) -> bool {
        self.0 == 0
    }

    /// Get the raw value (signed, for ABI compat — but coordinator
    /// expects positive values).
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for HsmErrno {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ok() {
            f.write_str("OK")
        } else {
            write!(f, "errno={}", self.0)
        }
    }
}

impl fmt::Debug for HsmErrno {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HsmErrno({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_sentinel() {
        assert!(HsmErrno::OK.is_ok());
        assert!(!HsmErrno::new(5).is_ok());
        assert_eq!(HsmErrno::OK.to_string(), "OK");
        assert_eq!(HsmErrno::new(5).to_string(), "errno=5");
    }
}
