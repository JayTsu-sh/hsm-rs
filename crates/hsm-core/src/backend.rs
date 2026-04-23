//! Backend identification and per-object metadata persisted in xattrs.
//!
//! `ArchiveId` matches the on-wire `__u32` archive number Lustre uses to
//! address a particular HSM tier; `BackendObject` is the trio
//! `(uuid, hash, url)` we round-trip through Lustre xattrs (see
//! [`crate::xattr`]) so a restored / removed file can find its archive
//! payload across copytool restarts.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Lustre HSM archive identifier.
///
/// Newtype around `u32` to keep it from being mixed with `Cookie`, `Fid` ints,
/// or generic counters. Display prints just the number for log readability.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArchiveId(pub u32);

impl ArchiveId {
    /// Construct from a raw `u32` archive id.
    pub const fn new(v: u32) -> Self {
        Self(v)
    }

    /// Get the raw `u32`.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ArchiveId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ArchiveId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArchiveId({})", self.0)
    }
}

/// Metadata identifying an archived object on a backend.
///
/// Persisted as three xattrs (`trusted.lhsm_uuid`, `trusted.lhsm_hash`,
/// `trusted.lhsm_url` — see [`crate::xattr`]) on the Lustre file once an
/// `ARCHIVE` action completes; consumed on `RESTORE` and `REMOVE`.
///
/// The `hash` is BLAKE3 (32 bytes) — terrasync-rs computes this natively and
/// hsm-plugin-terrasync writes it into the xattr.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendObject {
    /// Backend-specific key — for terrasync this is the relative path
    /// under the storage root (e.g. `"00ab/0x200000401:0x12:0x0"`).
    pub uuid: String,
    /// BLAKE3 hash of the object contents. Used to verify integrity on
    /// restore and to detect bit-rot in periodic scrubs.
    pub hash: [u8; 32],
    /// Full URL of the object, including backend scheme. Cross-cluster
    /// migration / disaster recovery uses this to relocate.
    pub url: String,
}

impl BackendObject {
    /// Hash formatted as 64 lowercase hex chars (BLAKE3 standard).
    pub fn hash_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.hash {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_id_display_and_debug() {
        let a = ArchiveId::new(7);
        assert_eq!(a.to_string(), "7");
        assert_eq!(format!("{a:?}"), "ArchiveId(7)");
    }

    #[test]
    fn archive_id_orders_numerically() {
        let mut v = vec![ArchiveId::new(3), ArchiveId::new(1), ArchiveId::new(2)];
        v.sort();
        assert_eq!(v.iter().map(|a| a.get()).collect::<Vec<_>>(), [1, 2, 3]);
    }

    #[test]
    fn backend_object_hash_hex_round_trip() {
        let mut h = [0u8; 32];
        for (i, b) in h.iter_mut().enumerate() {
            *b = i as u8;
        }
        let obj = BackendObject {
            uuid: "k/abc".into(),
            hash: h,
            url: "s3://bucket/k/abc".into(),
        };
        let hex = obj.hash_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(&hex[..6], "000102");
        // round-trip via the hex crate (workspace dev-dep).
        let decoded = hex::decode(&hex).unwrap();
        assert_eq!(decoded, h);
    }
}
