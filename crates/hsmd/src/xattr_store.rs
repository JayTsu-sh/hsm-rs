//! Persist [`BackendObject`] on a Lustre file via extended attributes.
//!
//! Three xattrs per file (names from [`hsm_core::xattr`]):
//!
//! | xattr (`trusted.` prefix)        | value                                |
//! |----------------------------------|--------------------------------------|
//! | `lhsm_uuid`                      | `obj.uuid` as UTF-8                  |
//! | `lhsm_hash`                      | BLAKE3 hash, 64-char lowercase hex   |
//! | `lhsm_url`                       | `obj.url` as UTF-8                   |
//!
//! Production Lustre uses `trusted.*` (CAP_SYS_ADMIN required). For
//! tests / dev environments without root, [`XattrNamespace::User`]
//! flips the prefix to `user.*` so the names land on common ext4/xfs.
//!
//! On a fresh file (Restore for an unarchived inode): [`read_obj`]
//! returns `Ok(None)` so the daemon can surface a precise ENODATA
//! to the kernel instead of "I/O error". Other failures (permission,
//! truncated payload) propagate as [`XattrError`].
//!
//! Round-trip is exact: `write_obj` then `read_obj` returns a
//! [`BackendObject`] with byte-identical fields.

use std::path::Path;

use hsm_core::BackendObject;
use thiserror::Error;

/// Which xattr namespace to use. Names are otherwise byte-identical.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub enum XattrNamespace {
    /// Lustre / production: `trusted.lhsm_*`. Requires CAP_SYS_ADMIN.
    #[default]
    Trusted,
    /// Dev / CI without root: `user.lhsm_*`. Works on ext4 / xfs / tmpfs.
    User,
}

impl XattrNamespace {
    fn name(self, suffix: &str) -> String {
        let prefix = match self {
            XattrNamespace::Trusted => "trusted",
            XattrNamespace::User => "user",
        };
        format!("{prefix}.{suffix}")
    }

    fn uuid_name(self) -> String {
        self.name("lhsm_uuid")
    }
    fn hash_name(self) -> String {
        self.name("lhsm_hash")
    }
    fn url_name(self) -> String {
        self.name("lhsm_url")
    }
}

/// Errors round-tripping a [`BackendObject`] through xattrs.
#[derive(Debug, Error)]
pub enum XattrError {
    /// Underlying syscall failure (EACCES, ENOSPC, EOPNOTSUPP, …).
    #[error("xattr {op} on {path}: {source}")]
    Io {
        /// Operation: `"set"`, `"get"`, `"remove"`.
        op: &'static str,
        /// File the syscall targeted.
        path: String,
        /// Underlying error.
        #[source]
        source: std::io::Error,
    },

    /// Hash xattr was present but did not decode to a 32-byte value.
    #[error("hash xattr on {path} is not 32 bytes of hex: {reason}")]
    BadHash {
        /// File the bad value came from.
        path: String,
        /// Why it failed.
        reason: String,
    },

    /// One of `uuid` / `url` xattrs contained invalid UTF-8.
    #[error("xattr {name} on {path} is not valid UTF-8")]
    BadUtf8 {
        /// xattr name with the bad bytes.
        name: String,
        /// File the bad value came from.
        path: String,
    },

    /// Some xattrs were present but not all three. Refuses to return a
    /// half-built [`BackendObject`].
    #[error("partial xattr set on {path}: {present} present, missing {missing:?}")]
    Partial {
        /// File with the partial set.
        path: String,
        /// Count of xattrs that were found.
        present: usize,
        /// Names of the xattrs that were missing.
        missing: Vec<String>,
    },
}

/// Result alias.
pub type XattrResult<T> = Result<T, XattrError>;

/// Persist `obj` to `path`'s xattrs in `ns`. Existing values are
/// overwritten.
pub fn write_obj(path: &Path, ns: XattrNamespace, obj: &BackendObject) -> XattrResult<()> {
    set(path, ns.uuid_name(), obj.uuid.as_bytes())?;
    set(path, ns.hash_name(), obj.hash_hex().as_bytes())?;
    set(path, ns.url_name(), obj.url.as_bytes())?;
    Ok(())
}

/// Read a previously-persisted [`BackendObject`] from `path`'s xattrs.
///
/// - `Ok(Some(_))`: all three xattrs present + decoded cleanly.
/// - `Ok(None)`: none of them present (file was never archived).
/// - `Err(_)`: I/O failure, partial set, or malformed value.
pub fn read_obj(path: &Path, ns: XattrNamespace) -> XattrResult<Option<BackendObject>> {
    let uuid_name = ns.uuid_name();
    let hash_name = ns.hash_name();
    let url_name = ns.url_name();

    let uuid_v = get_optional(path, &uuid_name)?;
    let hash_v = get_optional(path, &hash_name)?;
    let url_v = get_optional(path, &url_name)?;

    match (uuid_v, hash_v, url_v) {
        (None, None, None) => Ok(None),
        (Some(uuid), Some(hash), Some(url)) => {
            let uuid = String::from_utf8(uuid).map_err(|_| XattrError::BadUtf8 {
                name: uuid_name,
                path: path.display().to_string(),
            })?;
            let url = String::from_utf8(url).map_err(|_| XattrError::BadUtf8 {
                name: url_name,
                path: path.display().to_string(),
            })?;
            let hash = decode_hash(path, &hash)?;
            Ok(Some(BackendObject { uuid, hash, url }))
        }
        (uuid, hash, url) => {
            let mut missing = Vec::new();
            if uuid.is_none() {
                missing.push(uuid_name);
            }
            if hash.is_none() {
                missing.push(hash_name);
            }
            if url.is_none() {
                missing.push(url_name);
            }
            let present = 3 - missing.len();
            Err(XattrError::Partial {
                path: path.display().to_string(),
                present,
                missing,
            })
        }
    }
}

/// Remove all `lhsm_*` xattrs in `ns` from `path`. Missing xattrs are
/// not an error (post-Remove this should be a no-op).
pub fn clear_obj(path: &Path, ns: XattrNamespace) -> XattrResult<()> {
    for name in [ns.uuid_name(), ns.hash_name(), ns.url_name()] {
        remove_if_present(path, &name)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// low-level wrappers around the `xattr` crate
// ---------------------------------------------------------------------------

fn set(path: &Path, name: String, value: &[u8]) -> XattrResult<()> {
    xattr::set(path, &name, value).map_err(|e| XattrError::Io {
        op: "set",
        path: path.display().to_string(),
        source: e,
    })
}

fn get_optional(path: &Path, name: &str) -> XattrResult<Option<Vec<u8>>> {
    match xattr::get(path, name) {
        Ok(v) => Ok(v),
        Err(e) if e.raw_os_error() == Some(libc_enodata()) => Ok(None),
        Err(e) => Err(XattrError::Io {
            op: "get",
            path: path.display().to_string(),
            source: e,
        }),
    }
}

fn remove_if_present(path: &Path, name: &str) -> XattrResult<()> {
    match xattr::remove(path, name) {
        Ok(()) => Ok(()),
        Err(e) if e.raw_os_error() == Some(libc_enodata()) => Ok(()),
        Err(e) => Err(XattrError::Io {
            op: "remove",
            path: path.display().to_string(),
            source: e,
        }),
    }
}

fn decode_hash(path: &Path, raw: &[u8]) -> XattrResult<[u8; 32]> {
    let s = std::str::from_utf8(raw).map_err(|_| XattrError::BadHash {
        path: path.display().to_string(),
        reason: "not UTF-8".into(),
    })?;
    if s.len() != 64 {
        return Err(XattrError::BadHash {
            path: path.display().to_string(),
            reason: format!("len {} != 64", s.len()),
        });
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let byte_str = &s[i * 2..i * 2 + 2];
        out[i] = u8::from_str_radix(byte_str, 16).map_err(|e| XattrError::BadHash {
            path: path.display().to_string(),
            reason: format!("byte {i}: {e}"),
        })?;
    }
    Ok(out)
}

/// Linux `ENODATA` value (used both for missing xattrs and EOPNOTSUPP-on-some-FS).
const fn libc_enodata() -> i32 {
    61
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj() -> BackendObject {
        BackendObject {
            uuid: "0x200000401:0x12:0x0".into(),
            hash: [
                0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
                0x42, 0x42, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                0x0d, 0x0e, 0x0f, 0x10,
            ],
            url: "file:///srv/hsm/1/0x200000401:0x12:0x0".into(),
        }
    }

    fn make_file() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.bin");
        std::fs::write(&path, b"hello").unwrap();
        (dir, path)
    }

    #[test]
    fn round_trip_exact() {
        let (_dir, path) = make_file();
        write_obj(&path, XattrNamespace::User, &obj()).unwrap();
        let read = read_obj(&path, XattrNamespace::User).unwrap().unwrap();
        assert_eq!(read, obj());
    }

    #[test]
    fn read_no_xattrs_returns_none() {
        let (_dir, path) = make_file();
        assert_eq!(read_obj(&path, XattrNamespace::User).unwrap(), None);
    }

    #[test]
    fn read_partial_set_returns_partial_error() {
        let (_dir, path) = make_file();
        xattr::set(&path, "user.lhsm_uuid", b"abc").unwrap();
        let err = read_obj(&path, XattrNamespace::User).unwrap_err();
        match err {
            XattrError::Partial { present, missing, .. } => {
                assert_eq!(present, 1);
                assert_eq!(missing.len(), 2);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn read_bad_hash_length_returns_error() {
        let (_dir, path) = make_file();
        xattr::set(&path, "user.lhsm_uuid", b"u").unwrap();
        xattr::set(&path, "user.lhsm_url", b"u://x").unwrap();
        xattr::set(&path, "user.lhsm_hash", b"deadbeef").unwrap();
        let err = read_obj(&path, XattrNamespace::User).unwrap_err();
        assert!(matches!(err, XattrError::BadHash { .. }));
    }

    #[test]
    fn clear_removes_all_three_and_is_idempotent() {
        let (_dir, path) = make_file();
        write_obj(&path, XattrNamespace::User, &obj()).unwrap();
        clear_obj(&path, XattrNamespace::User).unwrap();
        assert_eq!(read_obj(&path, XattrNamespace::User).unwrap(), None);
        // Idempotent.
        clear_obj(&path, XattrNamespace::User).unwrap();
    }

    #[test]
    fn ns_namespacing_is_isolated() {
        let (_dir, path) = make_file();
        write_obj(&path, XattrNamespace::User, &obj()).unwrap();
        // Trusted side should not see User-namespaced attrs.
        // (And vice-versa — but writing trusted.* requires root, so we
        // skip that direction in this test.)
        assert_eq!(read_obj(&path, XattrNamespace::Trusted).unwrap(), None);
    }

    #[test]
    fn ns_name_helpers() {
        assert_eq!(XattrNamespace::Trusted.uuid_name(), "trusted.lhsm_uuid");
        assert_eq!(XattrNamespace::User.url_name(), "user.lhsm_url");
        assert_eq!(XattrNamespace::default(), XattrNamespace::Trusted);
    }
}
