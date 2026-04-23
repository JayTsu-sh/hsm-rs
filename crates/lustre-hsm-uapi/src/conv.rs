//! Conversions between `lustre-sys` ABI types and `hsm-core` domain types.
//!
//! These conversions are total / lossless for the supported subset and
//! return a `Result` for the cases where the wire value is outside the
//! enum we model (e.g. a future `HSMA_…` value the daemon doesn't know).

use hsm_core::{ActionKind, Cookie, Extent, Fid};
use lustre_sys as sys;
use thiserror::Error;

use crate::consts::{HSMA_ARCHIVE, HSMA_CANCEL, HSMA_REMOVE, HSMA_RESTORE};

/// Conversion failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConvError {
    /// Wire `hai_action` value is not one we model.
    ///
    /// `HSMA_NONE` (10) is treated as an error here on the path from wire
    /// to domain because the daemon should not act on it — the kernel uses
    /// it as a noop / placeholder.
    #[error("unknown hsm_copytool_action value: {0}")]
    UnknownCopytoolAction(u32),
}

// ── lu_fid ↔ Fid ─────────────────────────────────────────────────────────

/// Convert a wire `lu_fid` to the domain [`Fid`].
///
/// Takes by value rather than by reference: bindgen marks `lu_fid` as
/// `#[repr(C, packed)]` (it lives inside packed `hsm_action_item`), and
/// taking `&sys::lu_fid` to read its fields would create unaligned references
/// (UB). `lu_fid` is `Copy` so this is cheap.
pub fn fid_from_sys(f: sys::lu_fid) -> Fid {
    Fid::new(f.f_seq, f.f_oid, f.f_ver)
}

/// Convert a domain [`Fid`] to a wire `lu_fid`.
pub fn fid_to_sys(f: Fid) -> sys::lu_fid {
    sys::lu_fid {
        f_seq: f.seq,
        f_oid: f.oid,
        f_ver: f.ver,
    }
}

// ── HSMA_* ↔ ActionKind ──────────────────────────────────────────────────

/// Convert the wire `hai_action` (a `u32` matching `enum hsm_copytool_action`)
/// to a domain [`ActionKind`].
pub fn action_kind_from_wire(v: u32) -> Result<ActionKind, ConvError> {
    match v {
        x if x == HSMA_ARCHIVE => Ok(ActionKind::Archive),
        x if x == HSMA_RESTORE => Ok(ActionKind::Restore),
        x if x == HSMA_REMOVE => Ok(ActionKind::Remove),
        x if x == HSMA_CANCEL => Ok(ActionKind::Cancel),
        other => Err(ConvError::UnknownCopytoolAction(other)),
    }
}

/// Convert a domain [`ActionKind`] back to the wire `HSMA_*` value.
pub fn action_kind_to_wire(k: ActionKind) -> u32 {
    match k {
        ActionKind::Archive => HSMA_ARCHIVE,
        ActionKind::Restore => HSMA_RESTORE,
        ActionKind::Remove => HSMA_REMOVE,
        ActionKind::Cancel => HSMA_CANCEL,
    }
}

// ── hsm_extent ↔ Extent ──────────────────────────────────────────────────

/// Convert a wire `hsm_extent` (offset + length) to a domain [`Extent`].
///
/// By value to avoid potential unaligned-reference UB if used on a copy
/// embedded in a packed parent struct (see [`fid_from_sys`]).
pub fn extent_from_sys(e: sys::hsm_extent) -> Extent {
    Extent::new(e.offset, e.length)
}

/// Convert a domain [`Extent`] back to wire form.
pub fn extent_to_sys(e: Extent) -> sys::hsm_extent {
    sys::hsm_extent {
        offset: e.offset,
        length: e.length,
    }
}

// ── hai_cookie ↔ Cookie ──────────────────────────────────────────────────

/// Wrap a raw wire cookie.
pub fn cookie_from_wire(v: u64) -> Cookie {
    Cookie::new(v)
}

/// Unwrap a domain [`Cookie`] back to wire `u64`.
pub fn cookie_to_wire(c: Cookie) -> u64 {
    c.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fid_round_trip() {
        let f = Fid::new(0x200000401, 0x12, 0);
        let s = fid_to_sys(f);
        // bindgen marks lu_fid as packed (it lives inside packed structs);
        // copy to locals before asserting to avoid unaligned-ref UB.
        let (seq, oid, ver) = (s.f_seq, s.f_oid, s.f_ver);
        assert_eq!(seq, 0x200000401);
        assert_eq!(oid, 0x12);
        assert_eq!(ver, 0);
        assert_eq!(fid_from_sys(s), f);
    }

    #[test]
    fn extent_round_trip() {
        let e = Extent::new(1024, 4096);
        let s = extent_to_sys(e);
        assert_eq!(extent_from_sys(s), e);
    }

    #[test]
    fn cookie_round_trip() {
        let c = Cookie::new(0xdead_beef_cafe);
        assert_eq!(cookie_to_wire(c), 0xdead_beef_cafe);
        assert_eq!(cookie_from_wire(0xdead_beef_cafe), c);
    }

    #[test]
    fn action_kind_known_values() {
        assert_eq!(action_kind_from_wire(HSMA_ARCHIVE), Ok(ActionKind::Archive));
        assert_eq!(action_kind_from_wire(HSMA_RESTORE), Ok(ActionKind::Restore));
        assert_eq!(action_kind_from_wire(HSMA_REMOVE), Ok(ActionKind::Remove));
        assert_eq!(action_kind_from_wire(HSMA_CANCEL), Ok(ActionKind::Cancel));
    }

    #[test]
    fn action_kind_to_wire_round_trip() {
        for k in [ActionKind::Archive, ActionKind::Restore, ActionKind::Remove, ActionKind::Cancel] {
            assert_eq!(action_kind_from_wire(action_kind_to_wire(k)), Ok(k));
        }
    }

    #[test]
    fn unknown_action_kind_returns_err() {
        // HSMA_NONE (10) is intentionally rejected — daemon shouldn't act on it.
        let err = action_kind_from_wire(10).unwrap_err();
        assert_eq!(err, ConvError::UnknownCopytoolAction(10));

        // Random unknown value.
        let err = action_kind_from_wire(99).unwrap_err();
        assert_eq!(err, ConvError::UnknownCopytoolAction(99));
    }
}
