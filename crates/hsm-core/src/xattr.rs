//! xattr name constants for the per-file HSM metadata we persist on Lustre.
//!
//! Names are deliberately byte-compatible with the Go-based **lemur**
//! copytool so that an `hsm-rs` daemon and a `lemur` daemon could in
//! principle restore each other's archived files. Cross-implementation
//! compatibility tests are out of scope for now but keeping the names
//! aligned costs nothing.

/// Backend object key (terrasync `relative_path`, lemur "uuid").
pub const XATTR_UUID: &str = "trusted.lhsm_uuid";

/// BLAKE3 hex digest of the archived bytes.
pub const XATTR_HASH: &str = "trusted.lhsm_hash";

/// Full URL of the archived object (e.g. `s3+https://host/bucket/key`).
pub const XATTR_URL: &str = "trusted.lhsm_url";

/// All xattrs an `hsm-rs` daemon writes / reads.
pub const ALL: &[&str] = &[XATTR_UUID, XATTR_HASH, XATTR_URL];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_under_trusted_namespace() {
        // Lustre xattrs in the `trusted.*` namespace require CAP_SYS_ADMIN to
        // write — exactly what we want for HSM metadata (only the daemon
        // should set them, never end-user processes).
        for name in ALL {
            assert!(name.starts_with("trusted."), "{name}");
        }
    }

    #[test]
    fn names_are_unique() {
        let mut seen: Vec<&&str> = Vec::new();
        for n in ALL {
            assert!(!seen.contains(&n), "duplicate xattr name: {n}");
            seen.push(n);
        }
    }
}
