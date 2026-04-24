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

/// Errors decoding a backend URL into [`BackendUrl`].
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

    /// Scheme is one we don't know how to dispatch.
    #[error("unknown scheme {scheme:?} (known: file, s3, nfs, cifs)")]
    UnknownScheme {
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
    #[error("backend URL path is not valid UTF-8")]
    InvalidUtf8,
}

/// Backend URL scheme. The terrasync mover dispatches on this when
/// constructing per-action `(uuid, hash, url)` triples and on
/// Restore / Remove when deciding which `StorageEnum` to read from.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BackendScheme {
    /// `file://<path>` — local POSIX or Lustre-mounted filesystem.
    File,
    /// `s3://<bucket>/<prefix>` — AWS S3 or compatible.
    S3,
    /// `nfs://<server>/<export>` — NFSv3 / NFSv4 export.
    Nfs,
    /// `cifs://<host>/<share>` — SMB / CIFS share.
    Cifs,
}

impl BackendScheme {
    /// Lowercase scheme string as it appears in the URL.
    pub fn as_str(self) -> &'static str {
        match self {
            BackendScheme::File => "file",
            BackendScheme::S3 => "s3",
            BackendScheme::Nfs => "nfs",
            BackendScheme::Cifs => "cifs",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "file" => Some(Self::File),
            "s3" => Some(Self::S3),
            "nfs" => Some(Self::Nfs),
            "cifs" => Some(Self::Cifs),
            _ => None,
        }
    }
}

/// Parsed backend URL. Carries both the recognised scheme and the
/// scheme-specific payload (currently just the path component, since
/// host/port/credentials live alongside the URL in the plugin's TOML
/// config and are not derived from the URL itself).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendUrl {
    /// Recognised scheme.
    pub scheme: BackendScheme,
    /// For `file://` this is the absolute filesystem path. For
    /// `s3://bucket/prefix` it is `/bucket/prefix` (preserved as the
    /// URL-style path; bucket vs prefix split is the caller's job).
    /// For `nfs://server/export` it is `/export`. For
    /// `cifs://host/share` it is `/share`.
    pub path: PathBuf,
    /// `host` portion of the URL, when one is present (s3 / nfs /
    /// cifs). `None` for `file://`.
    pub host: Option<String>,
}

impl BackendUrl {
    /// Parse any of the supported schemes. Returns
    /// [`BackendUrlError::UnknownScheme`] for unrecognised schemes.
    pub fn parse(url: &str) -> Result<Self, BackendUrlError> {
        let parsed = Url::parse(url).map_err(|e| BackendUrlError::Parse {
            url: url.into(),
            source: e,
        })?;
        let scheme = BackendScheme::parse(parsed.scheme()).ok_or_else(|| {
            BackendUrlError::UnknownScheme {
                scheme: parsed.scheme().into(),
            }
        })?;
        let raw_path = parsed.path();
        if scheme == BackendScheme::File && raw_path.is_empty() {
            return Err(BackendUrlError::EmptyPath { url: url.into() });
        }
        let decoded = percent_decode_str(raw_path)
            .decode_utf8()
            .map_err(|_| BackendUrlError::InvalidUtf8)?;
        Ok(Self {
            scheme,
            path: PathBuf::from(decoded.into_owned()),
            host: parsed.host_str().map(|h| h.to_string()),
        })
    }

    /// Render this URL back to a canonical form.
    pub fn render(&self) -> String {
        let path = self.path.to_string_lossy();
        let encoded = utf8_percent_encode(&path, PATH_ENCODE);
        match (self.scheme, self.host.as_deref()) {
            (BackendScheme::File, _) => format!("file://{encoded}"),
            (scheme, Some(host)) => format!("{}://{host}{encoded}", scheme.as_str()),
            // Non-file scheme without a host is unusual — render with
            // an empty authority so round-trips still work.
            (scheme, None) => format!("{}://{encoded}", scheme.as_str()),
        }
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

    /// Backend URL of `(archive_id, fid)`. Layout currently always
    /// emits `file://` URLs since M3a only ships the LocalStorage
    /// destination; M3b will widen this to the layout's configured
    /// scheme.
    pub fn url(&self, archive_id: ArchiveId, fid: Fid) -> BackendUrl {
        BackendUrl {
            scheme: BackendScheme::File,
            path: self.full_path(archive_id, fid),
            host: None,
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
        assert_eq!(parsed.scheme, BackendScheme::File);
        assert_eq!(
            parsed.path,
            PathBuf::from("/var/hsm-archive/1/0x200000401:0x12:0x0")
        );
        assert_eq!(parsed.host, None);
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
    fn s3_url_parses_with_host_and_path() {
        let url = "s3://my-bucket/prefix/1/0x1:0x2:0x0";
        let parsed = BackendUrl::parse(url).unwrap();
        assert_eq!(parsed.scheme, BackendScheme::S3);
        assert_eq!(parsed.host.as_deref(), Some("my-bucket"));
        assert_eq!(parsed.path, PathBuf::from("/prefix/1/0x1:0x2:0x0"));
        assert_eq!(parsed.render(), url);
    }

    #[test]
    fn nfs_url_parses_with_host_and_export() {
        let url = "nfs://nfs.example.com/exports/hsm";
        let parsed = BackendUrl::parse(url).unwrap();
        assert_eq!(parsed.scheme, BackendScheme::Nfs);
        assert_eq!(parsed.host.as_deref(), Some("nfs.example.com"));
        assert_eq!(parsed.path, PathBuf::from("/exports/hsm"));
        assert_eq!(parsed.render(), url);
    }

    #[test]
    fn cifs_url_parses_with_host_and_share() {
        let url = "cifs://samba.example.com/hsm-share";
        let parsed = BackendUrl::parse(url).unwrap();
        assert_eq!(parsed.scheme, BackendScheme::Cifs);
        assert_eq!(parsed.host.as_deref(), Some("samba.example.com"));
        assert_eq!(parsed.path, PathBuf::from("/hsm-share"));
        assert_eq!(parsed.render(), url);
    }

    #[test]
    fn rejects_unknown_scheme() {
        let err = BackendUrl::parse("ftp://server/path").unwrap_err();
        assert!(matches!(err, BackendUrlError::UnknownScheme { .. }));
    }

    #[test]
    fn rejects_garbage() {
        assert!(BackendUrl::parse("not-a-url").is_err());
    }

    #[test]
    fn scheme_str_round_trip() {
        for s in [
            BackendScheme::File,
            BackendScheme::S3,
            BackendScheme::Nfs,
            BackendScheme::Cifs,
        ] {
            assert_eq!(BackendScheme::parse(s.as_str()), Some(s));
        }
        assert_eq!(BackendScheme::parse("ftp"), None);
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
