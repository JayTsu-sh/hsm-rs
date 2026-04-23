//! HSM action — the unit of work flowing from the Lustre coordinator to a
//! copytool, plus the in-memory state machine the daemon mirrors locally.
//!
//! The wire-level shape (`hsm_action_item`, `hsm_action_list`) lives in
//! `lustre-hsm-uapi`. This module is the application-level model — what the
//! scheduler / store / dispatcher pass around.

use std::time::SystemTime;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::backend::ArchiveId;
use crate::fid::Fid;

/// HSM action kind.
///
/// Mirrors `enum hsm_copytool_action` (`HSMA_ARCHIVE / RESTORE / REMOVE /
/// CANCEL`) from `lustre/include/uapi/linux/lustre/lustre_user.h`. The numeric
/// values are *not* preserved here — conversion to/from the wire enum is
/// `lustre-llapi`'s job.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum ActionKind {
    /// File → archive backend.
    Archive,
    /// Archive backend → file.
    Restore,
    /// Delete the archive payload.
    Remove,
    /// Abort an in-flight action (matched by cookie).
    Cancel,
}

/// Cookie issued by the kernel coordinator to identify one in-flight action.
///
/// 64-bit, opaque to userspace. Newtype to keep it from being confused with
/// [`Fid`] components or generic byte counts.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cookie(pub u64);

impl Cookie {
    /// Wrap a raw `u64`.
    pub const fn new(v: u64) -> Self {
        Self(v)
    }
    /// Unwrap to raw `u64`.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Cookie {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

impl std::fmt::Debug for Cookie {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cookie({:#x})", self.0)
    }
}

/// Connected agent identifier — used by scheduler / store to attribute
/// in-flight actions to a particular plugin process or remote mover node.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(pub String);

impl AgentId {
    /// Wrap a string id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    /// Borrow the string id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Byte range an action covers.
///
/// `length == EXTENT_WHOLE` is the Lustre-internal convention for "the entire
/// file". Concrete byte counts otherwise.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct Extent {
    /// Start offset (bytes).
    pub offset: u64,
    /// Length (bytes), or [`Extent::WHOLE_LENGTH`].
    pub length: u64,
}

impl Extent {
    /// Sentinel `length` meaning "rest of the file" — matches the Lustre
    /// internal convention (`OBD_OBJECT_EOF`). Conversions to/from the wire
    /// enum live in `lustre-llapi`.
    pub const WHOLE_LENGTH: u64 = u64::MAX;

    /// Whole-file extent starting at byte 0.
    pub const WHOLE: Self = Self { offset: 0, length: Self::WHOLE_LENGTH };

    /// Concrete extent.
    pub const fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    /// `true` if this represents the entire file.
    pub const fn is_whole_file(&self) -> bool {
        self.offset == 0 && self.length == Self::WHOLE_LENGTH
    }
}

/// One unit of HSM work as scheduled by the daemon and executed by a mover.
///
/// `data` carries opaque per-action hints from the Lustre user (`hai_data`,
/// up to ~12 bytes by default). Schedulers may match on it (e.g. coordinatool
/// `archive_on_hosts tag=…`); movers should treat it as untrusted and never
/// splice it into filesystem paths or URLs without validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    /// Kernel-issued action cookie. Unique per in-flight action.
    pub cookie: Cookie,
    /// FID identifying the target file in the namespace.
    pub fid: Fid,
    /// Data FID — used to open for read/write under HSM-released layouts.
    /// Often equal to `fid` but not guaranteed.
    pub dfid: Fid,
    /// Which archive tier this action targets.
    pub archive_id: ArchiveId,
    /// What to do.
    pub kind: ActionKind,
    /// Byte range to operate on (whole file by default).
    pub extent: Extent,
    /// Group lock id for concurrent-access serialization (0 means none).
    pub gid: u64,
    /// Opaque per-action hint from the user (`hai_data`).
    pub data: Bytes,
}

/// Mirror of the kernel `enum agent_req_status` (ARS_*).
///
/// Stored in [`ActionRecord::state`]; the kernel coordinator owns the truth
/// (in its action llog) — we keep this locally so the scheduler/store can
/// reason about progress without round-tripping to the kernel.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArState {
    /// Queued, awaiting agent pickup.
    Waiting,
    /// Agent has called `llapi_hsm_action_begin`.
    Started {
        /// Which agent is running this action.
        agent: AgentId,
        /// When `Started` was entered (millis since UNIX epoch).
        since_unix_ms: u64,
    },
    /// `llapi_hsm_action_end` returned success.
    Succeed {
        /// Return code (typically 0).
        rc: i32,
    },
    /// `llapi_hsm_action_end` returned a non-zero error.
    Failed {
        /// Error code (positive errno).
        rc: i32,
    },
    /// Cancellation propagated to coordinator.
    Canceled,
}

impl ArState {
    /// `true` if no further state transitions are expected.
    pub const fn is_terminal(&self) -> bool {
        matches!(self, ArState::Succeed { .. } | ArState::Failed { .. } | ArState::Canceled)
    }
}

/// One row of the persisted action store.
///
/// Combines the static [`Action`] description with the dynamic state machine.
/// `progress` is the *furthest extent processed so far* and must advance
/// monotonically — buggy movers reporting backwards progress are rejected
/// at the store layer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionRecord {
    /// The action being tracked.
    pub action: Action,
    /// Current state.
    pub state: ArState,
    /// Furthest extent processed (offset + length already moved).
    pub progress: Extent,
    /// When the record was first persisted.
    pub created_at: SystemTime,
    /// When the state was last updated.
    pub updated_at: SystemTime,
}

impl ActionRecord {
    /// Build a fresh record in `Waiting` state, progress at offset 0.
    pub fn new(action: Action, now: SystemTime) -> Self {
        Self {
            action,
            state: ArState::Waiting,
            progress: Extent::new(0, 0),
            created_at: now,
            updated_at: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_action(kind: ActionKind) -> Action {
        Action {
            cookie: Cookie::new(0xdead_beef),
            fid: Fid::new(0x200000401, 0x12, 0),
            dfid: Fid::new(0x200000401, 0x12, 0),
            archive_id: ArchiveId::new(1),
            kind,
            extent: Extent::WHOLE,
            gid: 0,
            data: Bytes::from_static(b"tag=n0"),
        }
    }

    #[test]
    fn cookie_display_is_hex() {
        assert_eq!(Cookie::new(0xabc).to_string(), "0xabc");
    }

    #[test]
    fn extent_whole_round_trip() {
        let e = Extent::WHOLE;
        assert!(e.is_whole_file());
        assert_eq!(e.length, Extent::WHOLE_LENGTH);
    }

    #[test]
    fn extent_specific_is_not_whole() {
        let e = Extent::new(0, 4096);
        assert!(!e.is_whole_file());
    }

    #[test]
    fn ar_state_terminal_classification() {
        assert!(!ArState::Waiting.is_terminal());
        assert!(!ArState::Started {
            agent: AgentId::new("m0"),
            since_unix_ms: 0,
        }
        .is_terminal());
        assert!(ArState::Succeed { rc: 0 }.is_terminal());
        assert!(ArState::Failed { rc: 5 }.is_terminal());
        assert!(ArState::Canceled.is_terminal());
    }

    #[test]
    fn record_starts_waiting_with_zero_progress() {
        let now = SystemTime::UNIX_EPOCH;
        let r = ActionRecord::new(sample_action(ActionKind::Archive), now);
        assert!(matches!(r.state, ArState::Waiting));
        assert_eq!(r.progress, Extent::new(0, 0));
        assert_eq!(r.created_at, r.updated_at);
    }

    #[test]
    fn agent_id_string_helpers() {
        let a = AgentId::new("mover-0");
        assert_eq!(a.as_str(), "mover-0");
        assert_eq!(a.to_string(), "mover-0");
    }
}
