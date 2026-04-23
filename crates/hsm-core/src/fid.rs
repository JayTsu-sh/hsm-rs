//! Lustre File Identifier (FID).
//!
//! Mirrors the wire-level `struct lu_fid { __u64 f_seq; __u32 f_oid; __u32 f_ver; }`
//! defined in `lustre/include/uapi/linux/lustre/lustre_idl.h`. This is the
//! application-level Rust model — the `#[repr(C)]` companion lives in
//! `lustre-hsm-uapi`.
//!
//! FIDs are formatted in Lustre tools as `[seq:oid:ver]` (square brackets,
//! lowercase hex, no `0x`). We adopt the same canonical form for `Display`
//! and `FromStr` so that Lustre operators see familiar text.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Lustre File Identifier.
///
/// Equality is structural; `Hash` derives so FIDs can key maps.
/// `Copy` because the type is 16 bytes — small enough to pass by value.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Fid {
    /// Sequence — assigned by FID server, scopes the (oid, ver) tuple.
    pub seq: u64,
    /// Object identifier within the sequence.
    pub oid: u32,
    /// Version — usually 0; non-zero for some special files.
    pub ver: u32,
}

impl Fid {
    /// The all-zero FID. Lustre uses this as a sentinel for "no FID".
    pub const ZERO: Self = Self { seq: 0, oid: 0, ver: 0 };

    /// Construct a FID from its three components.
    pub const fn new(seq: u64, oid: u32, ver: u32) -> Self {
        Self { seq, oid, ver }
    }

    /// `true` if this FID is the all-zero sentinel.
    pub const fn is_zero(&self) -> bool {
        self.seq == 0 && self.oid == 0 && self.ver == 0
    }
}

impl fmt::Display for Fid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Lustre canonical form: [seq:oid:ver] in lowercase hex.
        write!(f, "[{:#x}:{:#x}:{:#x}]", self.seq, self.oid, self.ver)
    }
}

impl fmt::Debug for Fid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Reuse Display so debug output stays readable in logs.
        fmt::Display::fmt(self, f)
    }
}

/// Failure parsing a string into a [`Fid`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FidParseError {
    /// The input did not match `[seq:oid:ver]`.
    #[error("FID must be formatted as [seq:oid:ver], got {0:?}")]
    BadShape(String),
    /// One of the three components could not be parsed as a hex integer.
    #[error("FID component {component} is not valid hex: {value:?}")]
    BadHex {
        /// Which of seq/oid/ver failed.
        component: &'static str,
        /// The offending substring.
        value: String,
    },
}

impl FromStr for Fid {
    type Err = FidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Strip optional [ ... ] brackets.
        let body = s.strip_prefix('[').and_then(|s| s.strip_suffix(']')).unwrap_or(s);
        let mut parts = body.split(':');
        let seq_s = parts.next().ok_or_else(|| FidParseError::BadShape(s.to_string()))?;
        let oid_s = parts.next().ok_or_else(|| FidParseError::BadShape(s.to_string()))?;
        let ver_s = parts.next().ok_or_else(|| FidParseError::BadShape(s.to_string()))?;
        if parts.next().is_some() {
            return Err(FidParseError::BadShape(s.to_string()));
        }

        Ok(Self {
            seq: parse_hex_u64(seq_s, "seq")?,
            oid: parse_hex_u32(oid_s, "oid")?,
            ver: parse_hex_u32(ver_s, "ver")?,
        })
    }
}

fn parse_hex_u64(s: &str, component: &'static str) -> Result<u64, FidParseError> {
    let trimmed = s.trim_start_matches("0x").trim_start_matches("0X");
    u64::from_str_radix(trimmed, 16).map_err(|_| FidParseError::BadHex {
        component,
        value: s.to_string(),
    })
}

fn parse_hex_u32(s: &str, component: &'static str) -> Result<u32, FidParseError> {
    let trimmed = s.trim_start_matches("0x").trim_start_matches("0X");
    u32::from_str_radix(trimmed, 16).map_err(|_| FidParseError::BadHex {
        component,
        value: s.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_matches_lustre_canonical_form() {
        let f = Fid::new(0x200000401, 0x12, 0);
        assert_eq!(f.to_string(), "[0x200000401:0x12:0x0]");
    }

    #[test]
    fn round_trip_from_str_and_display() {
        let cases = [
            Fid::ZERO,
            Fid::new(0x200000400, 1, 0),
            Fid::new(u64::MAX, u32::MAX, u32::MAX),
        ];
        for f in cases {
            let s = f.to_string();
            let parsed: Fid = s.parse().expect("parse own Display");
            assert_eq!(parsed, f);
        }
    }

    #[test]
    fn parses_form_without_brackets() {
        let f: Fid = "0x200000401:0x12:0x0".parse().unwrap();
        assert_eq!(f, Fid::new(0x200000401, 0x12, 0));
    }

    #[test]
    fn parses_without_0x_prefix() {
        let f: Fid = "[200000401:12:0]".parse().unwrap();
        assert_eq!(f, Fid::new(0x200000401, 0x12, 0));
    }

    #[test]
    fn rejects_bad_shape() {
        for bad in ["[0x1:0x2]", "[0x1:0x2:0x3:0x4]", "garbage", ""] {
            assert!(matches!(bad.parse::<Fid>(), Err(FidParseError::BadShape(_))));
        }
    }

    #[test]
    fn rejects_non_hex() {
        let res: Result<Fid, _> = "[0xZZ:0x12:0x0]".parse();
        assert!(matches!(res, Err(FidParseError::BadHex { component: "seq", .. })));
    }

    #[test]
    fn zero_constant_round_trip() {
        assert!(Fid::ZERO.is_zero());
        assert!(!Fid::new(1, 0, 0).is_zero());
    }
}
