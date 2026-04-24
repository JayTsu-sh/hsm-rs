//! Helpers for parsing + rendering the `file://<archive_root>/<archive_id>/<fid>`
//! object URL the plugin writes into [`BackendObject`].
//!
//! `BackendUrl` is the parsed form of a backend URL; `ArchiveLayout`
//! is the inverse — given an archive root, it builds URLs and on-disk
//! paths for new objects.

use std::path::{Path, PathBuf};

use hsm_core::{ArchiveId, Fid};
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use thiserror::Error;
use url::Url;

/// Percent-encoding set: encode everything except path-friendly ASCII.
/// We deliberately keep `:` (FID separators) raw — they're URL-safe in
/// path segments and let the backend file paths read naturally.
const PATH_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b':')
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Errors decoding a `file://…` URL into [`BackendUrl`].
#[derive(Debug, Error)]
pub enum BackendUrlError {
    /// Top-level URL parse failure.
    #[error("parse url {url}: {source}")]
    Parse {
        /// Offending URL.
        url: String,
        /// Underlying parse error.
        #[source]
        source: url::ParseError,
    },

    /// Scheme is something other than `file`.
    #[error("unsupported scheme {scheme:?} (M2e only supports file://)")]
    UnsupportedScheme {
        /// Scheme we found.
        scheme: String,
    },

    /// `file://` URL has no usable path component.
    #[error("file:// URL has empty path: {url}")]
    EmptyPath {
        /// Offending URL.
        url: String,
    },

    /// Percent-decoded path contained invalid UTF-8.
    #[error("file:// URL path is not valid UTF-8")]
    InvalidUtf8,
}

/// Parsed backend URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendUrl {
    /// Filesystem path portion of the URL (e.g.
    /// `/var/hsm-archive/1/0x200000401:0x12:0x0`).
    pub path: PathBuf,
}

impl BackendUrl {
    /// Parse a `file://…` URL. Other schemes are rejected (M3 will lift
    /// this restriction).
    pub fn parse(url: &str) -> Result<Self, BackendUrlError> {
        let parsed = Url::parse(url).map_err(|e| BackendUrlError::Parse {
            url: url.into(),
            source: e,
        })?;
        if parsed.scheme() != "file" {
            return Err(BackendUrlError::UnsupportedScheme {
                scheme: parsed.scheme().into(),
            });
        }
        let raw_path = parsed.path();
        if raw_path.is_empty() {
            return Err(BackendUrlError::EmptyPath { url: url.into() });
        }
        let decoded = percent_decode_str(raw_path)
            .decode_utf8()
            .map_err(|_| BackendUrlError::InvalidUtf8)?;
        Ok(Self {
            path: PathBuf::from(decoded.into_owned()),
        })
    }

    /// Render this URL back to its canonical `file://<path>` form.
    pub fn render(&self) -> String {
        let s = self.path.to_string_lossy();
        format!("file://{}", utf8_percent_encode(&s, PATH_ENCODE))
    }
}

/// Knows how to map an `(archive_id, fid)` pair to a (path, URL) pair
/// inside an archive root, and back.
#[derive(Clone, Debug)]
pub struct ArchiveLayout {
    /// Filesystem root the plugin writes into. Must be absolute.
    pub root: PathBuf,
}

impl ArchiveLayout {
    /// New layout rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Relative path under `root` for `(archive_id, fid)`. Always
    /// `<archive_id>/<fid>` where `<fid>` is the bracket-less canonical
    /// form `0xseq:0xoid:0xver`. The directory is created on demand by
    /// the storage backend.
    pub fn relative_path(archive_id: ArchiveId, fid: Fid) -> PathBuf {
        let mut p = PathBuf::with_capacity(48);
        p.push(archive_id.get().to_string());
        p.push(Self::uuid_for(fid));
        p
    }

    /// Absolute path of the backend object for `(archive_id, fid)`.
    pub fn full_path(&self, archive_id: ArchiveId, fid: Fid) -> PathBuf {
        self.root.join(Self::relative_path(archive_id, fid))
    }

    /// Backend URL of `(archive_id, fid)`.
    pub fn url(&self, archive_id: ArchiveId, fid: Fid) -> BackendUrl {
        BackendUrl {
            path: self.full_path(archive_id, fid),
        }
    }

    /// UUID we put in [`BackendObject`]: the FID rendered as
    /// `0xseq:0xoid:0xver` (bracket-less, path-safe). Stable across
    /// re-archives of the same file (which is exactly what HSM wants
    /// — Restore uses the same FID).
    pub fn uuid_for(fid: Fid) -> String {
        format!("{:#x}:{:#x}:{:#x}", fid.seq, fid.oid, fid.ver)
    }

    /// Strip `self.root` off `path` and return the leftover relative
    /// path. Returns `None` if `path` doesn't live under the root.
    pub fn relative_under(&self, path: &Path) -> Option<PathBuf> {
        path.strip_prefix(&self.root).ok().map(PathBuf::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_round_trips_through_parse_render() {
        let url = "file:///var/hsm-archive/1/0x200000401:0x12:0x0";
        let parsed = BackendUrl::parse(url).unwrap();
        assert_eq!(
            parsed.path,
            PathBuf::from("/var/hsm-archive/1/0x200000401:0x12:0x0")
        );
        assert_eq!(parsed.render(), url);
    }

    #[test]
    fn url_with_spaces_round_trips_via_percent_encoding() {
        let url = "file:///srv/has%20space/1/0x1:0x2:0x0";
        let parsed = BackendUrl::parse(url).unwrap();
        assert_eq!(parsed.path, PathBuf::from("/srv/has space/1/0x1:0x2:0x0"));
        assert_eq!(parsed.render(), url);
    }

    #[test]
    fn rejects_non_file_scheme() {
        let err = BackendUrl::parse("s3://bucket/key").unwrap_err();
        assert!(matches!(err, BackendUrlError::UnsupportedScheme { .. }));
    }

    #[test]
    fn rejects_garbage() {
        assert!(BackendUrl::parse("not-a-url").is_err());
    }

    #[test]
    fn layout_builds_path_and_url() {
        let layout = ArchiveLayout::new("/var/hsm-archive");
        let fid = Fid::new(0x200000401, 0x12, 0x0);
        let aid = ArchiveId::new(1);
        assert_eq!(
            layout.full_path(aid, fid),
            PathBuf::from("/var/hsm-archive/1/0x200000401:0x12:0x0")
        );
        let url = layout.url(aid, fid);
        assert_eq!(
            url.render(),
            "file:///var/hsm-archive/1/0x200000401:0x12:0x0"
        );
    }

    #[test]
    fn layout_relative_under_strips_root() {
        let layout = ArchiveLayout::new("/var/hsm-archive");
        let p = PathBuf::from("/var/hsm-archive/1/0x1:0x2:0x0");
        assert_eq!(
            layout.relative_under(&p),
            Some(PathBuf::from("1/0x1:0x2:0x0"))
        );
        assert!(layout.relative_under(Path::new("/elsewhere/x")).is_none());
    }

    #[test]
    fn uuid_for_is_fid_to_string() {
        let fid = Fid::new(0x200000401, 0x12, 0x0);
        assert_eq!(ArchiveLayout::uuid_for(fid), "0x200000401:0x12:0x0");
    }
}
