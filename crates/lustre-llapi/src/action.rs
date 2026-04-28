//! `ReceivedAction` (a parsed `hsm_action_item`) and the `ActionHandle` RAII
//! wrapper that ties begin/end together at compile time.

use bytes::Bytes;
use hsm_core::{Action, ActionKind, ArchiveId, Cookie, Extent, Fid};

use crate::error::Result;

/// One `hsm_action_item` after we've copied its fields out of the kernel
/// buffer into safe Rust types.
///
/// The wire-level struct is variable-length (trailing `hai_data[]`) and
/// alignment-sensitive (parent is `__attribute__((packed))`); rather than
/// have `recv` hand back a zero-copy view into the kernel buffer (which
/// would force every consumer to think about lifetimes and unaligned
/// reads), we eagerly copy into this owned struct.
///
/// `data` is `bytes::Bytes` so cloning is cheap (Arc-backed) — the
/// scheduler may hand the same action to multiple inspection points.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivedAction {
    /// Cookie issued by the kernel coordinator.
    pub cookie: Cookie,
    /// FID of the file the action targets.
    pub fid: Fid,
    /// Data FID for read/write under HSM-released layouts.
    pub dfid: Fid,
    /// Archive id (resolved at the action-list level, copied here for
    /// convenience so consumers don't need to thread the parent list).
    pub archive_id: ArchiveId,
    /// What the kernel wants done.
    pub kind: ActionKind,
    /// Byte range to operate on.
    pub extent: Extent,
    /// Group-lock id (0 means none).
    pub gid: u64,
    /// Opaque per-action data (`hai_data`). Untrusted — never splice into
    /// filesystem paths or URLs without validation.
    pub data: Bytes,
    /// Filesystem name from the parent `hsm_action_list` (e.g. `"lustre"`).
    pub fsname: String,
}

impl ReceivedAction {
    /// Build the application-level [`Action`] (`hsm_core`) from this
    /// wire-level record.
    ///
    /// Equivalent to copying the public fields one-to-one — the two types
    /// exist as separate names so callers signal intent (`ReceivedAction`
    /// = "just off the wire", `Action` = "scheduling unit").
    pub fn into_action(self) -> Action {
        Action {
            cookie: self.cookie,
            fid: self.fid,
            dfid: self.dfid,
            archive_id: self.archive_id,
            kind: self.kind,
            extent: self.extent,
            gid: self.gid,
            data: self.data,
        }
    }
}

/// Outcome reported back to the kernel via `llapi_hsm_action_end`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EndStatus {
    /// Action completed successfully.
    Ok,
    /// Action failed with a positive errno.
    Failed(i32),
    /// Action was cancelled (typically as the result of an `HSMA_CANCEL`
    /// landing on a different cookie). Reported to the kernel as
    /// `errno = ECANCELED` (125 on Linux x86_64).
    Cancelled,
}

impl EndStatus {
    /// Errno value to pass through to `llapi_hsm_action_end(rc=)`.
    /// 0 on success; positive errno on failure / cancel.
    pub const fn errno(self) -> i32 {
        match self {
            EndStatus::Ok => 0,
            EndStatus::Failed(rc) => rc,
            // ECANCELED = 125 on Linux. We hard-code instead of pulling
            // libc just for this constant — locking in the value here also
            // makes the mock deterministic across platforms.
            EndStatus::Cancelled => 125,
        }
    }
}

/// RAII handle for an in-flight HSM action.
///
/// Created by `Copytool::begin`; must be ended via [`ActionHandle::end`]
/// (consumes self → end runs exactly once, statically). If dropped without
/// `end`, the underlying copytool's `abandon` hook is invoked so the
/// action does not leak in either the kernel's or the mock's bookkeeping.
///
/// Holds an exclusive borrow of the parent `&mut C`; this matches the
/// reality that progress/end calls mutate per-cookie state in the
/// copytool, and Rust's borrow checker happily prevents multiple
/// concurrent handles for the same copytool instance. Concurrency across
/// multiple actions is achieved at the *daemon* layer by sharding by
/// `archive_id` to separate copytool instances or by serializing
/// progress/end through a single owner task — see
/// `docs/DESIGN.md` §6.
pub struct ActionHandle<'c, C: HasCookieLifecycle> {
    parent: &'c mut C,
    cookie: Cookie,
    /// Set true after `end` consumes self successfully — Drop checks it.
    finished: bool,
}

/// Capability the parent copytool needs to provide for `ActionHandle` to
/// drive it. Implemented by both [`MockCopytool`](crate::mock::MockCopytool)
/// and (Linux only) [`LiveCopytool`](crate::live::LiveCopytool).
pub trait HasCookieLifecycle {
    /// Send a non-terminal progress update to the coordinator.
    fn cookie_progress(&mut self, cookie: Cookie, e: Extent) -> Result<()>;
    /// Send the terminal `end` event.
    fn cookie_end(&mut self, cookie: Cookie, e: Extent, status: EndStatus) -> Result<()>;
    /// Drop-time cleanup hook for an action that was begun but never
    /// ended (panic / `?` early-return). Implementations should free
    /// per-cookie state and may best-effort report cancellation upstream.
    fn cookie_abandon(&mut self, cookie: Cookie);
}

impl<'c, C: HasCookieLifecycle> ActionHandle<'c, C> {
    /// Construct from a parent + cookie. **Internal**: callers obtain a
    /// handle via the parent's `begin`, never by calling this directly.
    #[doc(hidden)]
    pub fn new(parent: &'c mut C, cookie: Cookie) -> Self {
        Self {
            parent,
            cookie,
            finished: false,
        }
    }

    /// Cookie this handle tracks.
    pub fn cookie(&self) -> Cookie {
        self.cookie
    }

    /// Send a progress update.
    pub fn progress(&mut self, e: Extent) -> Result<()> {
        self.parent.cookie_progress(self.cookie, e)
    }

    /// Terminate this action with the given status. Consumes the handle so
    /// `end` cannot be called twice and Drop won't issue a redundant
    /// `abandon`.
    pub fn end(mut self, e: Extent, status: EndStatus) -> Result<()> {
        let res = self.parent.cookie_end(self.cookie, e, status);
        self.finished = true;
        res
    }
}

impl<C: HasCookieLifecycle> Drop for ActionHandle<'_, C> {
    fn drop(&mut self) {
        if !self.finished {
            self.parent.cookie_abandon(self.cookie);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_status_errnos() {
        assert_eq!(EndStatus::Ok.errno(), 0);
        assert_eq!(EndStatus::Failed(5).errno(), 5);
        assert_eq!(EndStatus::Cancelled.errno(), 125);
    }

    #[test]
    fn received_action_into_action_preserves_fields() {
        let r = ReceivedAction {
            cookie: Cookie::new(0xdead),
            fid: Fid::new(0x200000401, 0x12, 0),
            dfid: Fid::new(0x200000401, 0x12, 0),
            archive_id: ArchiveId::new(7),
            kind: ActionKind::Restore,
            extent: Extent::WHOLE,
            gid: 0,
            data: Bytes::from_static(b"tag=n0"),
            fsname: "lustre".to_string(),
        };
        let a = r.clone().into_action();
        assert_eq!(a.cookie, r.cookie);
        assert_eq!(a.fid, r.fid);
        assert_eq!(a.dfid, r.dfid);
        assert_eq!(a.archive_id, r.archive_id);
        assert_eq!(a.kind, r.kind);
        assert_eq!(a.extent, r.extent);
        assert_eq!(a.gid, r.gid);
        assert_eq!(a.data, r.data);
    }
}
