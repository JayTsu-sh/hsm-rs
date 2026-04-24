//! [`TerrasyncMover`] — implements [`Mover`] using terrasync-rs's
//! `storage_v2` `LocalStorage` for the file:// path.
//!
//! Per-action flow (see crate docs for the wider picture):
//!
//! ```text
//!   archive(ctx)
//!     ├─ src_storage = LocalStorage("/")            // FS root
//!     ├─ src_entry = src_storage.get_metadata(primary_relative_path)
//!     ├─ check_cancel
//!     ├─ bytes = StorageEnum::read_file_from(src, &src_entry, size)
//!     ├─ check_cancel; advance(size)
//!     ├─ hash = blake3(bytes)
//!     ├─ dst_entry = synthetic NASEntry { relative_path = layout.relative_path() }
//!     ├─ StorageEnum::write_file_from_bytes(dst, &dst_entry, bytes)
//!     └─ Ok(BackendObject { uuid: layout.uuid_for(fid), hash, url: layout.url(...).render() })
//! ```
//!
//! Restore is the mirror; Remove just calls `dst.delete_file(&entry)`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use hsm_core::BackendObject;
use hsm_plugin_sdk::{ActionCtx, Mover, MoverError, MoverResult};
use storage_v2::{EntryEnum, LocalStorage, NASEntry, StorageEnum};
use tracing::{debug, info, warn};

use crate::config::ArchiveLayout;

/// Mover backed by terrasync `LocalStorage` for source + destination.
///
/// `src` reads from the Lustre mountpoint (or its parent) so that
/// absolute paths resolve correctly via `LocalStorage::root_path`.
/// `dst` writes under `layout.root` — the per-action subpath comes
/// from `layout.relative_path()`.
#[derive(Clone)]
pub struct TerrasyncMover {
    /// Source storage rooted at `/` so any absolute primary path
    /// resolves as a relative path from `/`.
    src: Arc<StorageEnum>,
    /// Destination storage rooted at the archive root.
    dst: Arc<StorageEnum>,
    /// Layout helper for `(archive_id, fid)` → (path, url) conversion.
    layout: ArchiveLayout,
}

impl TerrasyncMover {
    /// New mover with `archive_root` as the destination LocalStorage
    /// root. The source LocalStorage is rooted at `/`.
    pub fn new(archive_root: impl Into<PathBuf>) -> Self {
        let layout = ArchiveLayout::new(archive_root);
        let src = Arc::new(StorageEnum::Local(LocalStorage::new("/", None)));
        let dst = Arc::new(StorageEnum::Local(LocalStorage::new(layout.root.clone(), None)));
        Self { src, dst, layout }
    }

    /// Returns the archive layout helper (useful for tests inspecting
    /// where the mover wrote its objects).
    pub fn layout(&self) -> &ArchiveLayout {
        &self.layout
    }
}

impl Mover for TerrasyncMover {
    async fn archive(&self, ctx: ActionCtx) -> MoverResult<BackendObject> {
        ctx.check_cancel()?;

        let primary = absolute(&ctx.primary_path)?;
        let src_relative = strip_root(&primary)?;
        let src_entry = match self.src.get_metadata(&src_relative).await {
            Ok(EntryEnum::NAS(e)) if !e.is_dir => EntryEnum::NAS(e),
            Ok(EntryEnum::NAS(_)) => {
                return Err(MoverError::Other(format!(
                    "primary {} is a directory, not a file",
                    primary.display()
                )));
            }
            Ok(other) => {
                return Err(MoverError::Other(format!(
                    "unexpected entry type for {}: {other:?}",
                    primary.display()
                )));
            }
            Err(e) => return Err(map_storage_err(e, &primary)),
        };
        let size = src_entry.get_size();

        // M2e limitation: read whole file into memory. M3.5 streaming
        // PR will let us interleave read + write + cancel.
        ctx.check_cancel()?;
        let bytes = StorageEnum::read_file_from(&self.src, &src_entry, size)
            .await
            .map_err(|e| map_storage_err(e, &primary))?;
        ctx.check_cancel()?;
        ctx.progress.advance(bytes.len() as u64);

        let hash = blake3_32(&bytes);

        let dst_relative = ArchiveLayout::relative_path(ctx.archive_id, ctx.fid);
        let dst_entry = synth_dest_entry(dst_relative, bytes.len() as u64);

        StorageEnum::write_file_from_bytes(&self.dst, &dst_entry, bytes)
            .await
            .map_err(|e| map_storage_err(e, &self.layout.full_path(ctx.archive_id, ctx.fid)))?;

        ctx.progress.flush().await;

        let url = self.layout.url(ctx.archive_id, ctx.fid);
        info!(
            target: "hsm.plugin.terrasync",
            cookie = %ctx.cookie, fid = %ctx.fid, archive_id = ctx.archive_id.get(),
            bytes = size, "archived"
        );
        Ok(BackendObject {
            uuid: ArchiveLayout::uuid_for(ctx.fid),
            hash,
            url: url.render(),
        })
    }

    async fn restore(&self, ctx: ActionCtx, obj: BackendObject) -> MoverResult<()> {
        ctx.check_cancel()?;

        let backend_relative = self
            .layout
            .relative_under(&self.layout.full_path(ctx.archive_id, ctx.fid))
            .ok_or_else(|| {
                MoverError::Other("backend object path escaped archive root".into())
            })?;
        debug!(
            target: "hsm.plugin.terrasync",
            cookie = %ctx.cookie,
            backend_relative = ?backend_relative,
            "restore: locating backend object"
        );

        let src_meta = match self.dst.get_metadata(&backend_relative).await {
            Ok(EntryEnum::NAS(e)) => EntryEnum::NAS(e),
            Ok(other) => {
                return Err(MoverError::Other(format!(
                    "backend object had unexpected entry type: {other:?}"
                )));
            }
            Err(e) => return Err(map_storage_err(e, &backend_relative)),
        };
        let size = src_meta.get_size();

        ctx.check_cancel()?;
        let bytes = StorageEnum::read_file_from(&self.dst, &src_meta, size)
            .await
            .map_err(|e| map_storage_err(e, &backend_relative))?;
        ctx.check_cancel()?;
        ctx.progress.advance(bytes.len() as u64);

        let actual = blake3_32(&bytes);
        if actual != obj.hash {
            warn!(
                target: "hsm.plugin.terrasync",
                cookie = %ctx.cookie, fid = %ctx.fid,
                "restore: BLAKE3 mismatch — corrupted backend object"
            );
            return Err(MoverError::Integrity(format!(
                "blake3 mismatch on restore for {}: backend={} actual={}",
                ctx.fid,
                hex32(&obj.hash),
                hex32(&actual)
            )));
        }

        let write_path = ctx
            .write_path
            .as_ref()
            .ok_or_else(|| MoverError::Other("restore missing write_path".into()))?;
        let write_path_abs = absolute(write_path)?;
        let write_relative = strip_root(&write_path_abs)?;
        let write_entry = synth_dest_entry(write_relative, bytes.len() as u64);

        StorageEnum::write_file_from_bytes(&self.src, &write_entry, bytes)
            .await
            .map_err(|e| map_storage_err(e, &write_path_abs))?;

        ctx.progress.flush().await;
        info!(
            target: "hsm.plugin.terrasync",
            cookie = %ctx.cookie, fid = %ctx.fid, bytes = size, "restored"
        );
        Ok(())
    }

    async fn remove(&self, ctx: ActionCtx, _obj: BackendObject) -> MoverResult<()> {
        ctx.check_cancel()?;
        let backend_relative = ArchiveLayout::relative_path(ctx.archive_id, ctx.fid);
        let entry = synth_dest_entry(backend_relative.clone(), 0);
        match self.dst.delete_file(&entry).await {
            Ok(()) => {
                info!(
                    target: "hsm.plugin.terrasync",
                    cookie = %ctx.cookie, fid = %ctx.fid, "removed"
                );
                Ok(())
            }
            Err(e) => {
                // Idempotency: missing is OK.
                if is_not_found(&e) {
                    info!(
                        target: "hsm.plugin.terrasync",
                        cookie = %ctx.cookie, fid = %ctx.fid,
                        "remove: backend object already gone (treating as success)"
                    );
                    return Ok(());
                }
                Err(map_storage_err(e, &backend_relative))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn absolute(p: &Path) -> MoverResult<PathBuf> {
    if p.is_absolute() {
        Ok(p.to_path_buf())
    } else {
        Err(MoverError::Other(format!(
            "expected absolute path, got {}",
            p.display()
        )))
    }
}

/// Strip leading `/` so the absolute path becomes a `LocalStorage("/")`-relative path.
fn strip_root(p: &Path) -> MoverResult<PathBuf> {
    p.strip_prefix("/")
        .map(PathBuf::from)
        .map_err(|_| MoverError::Other(format!("path {} not under /", p.display())))
}

fn blake3_32(b: &Bytes) -> [u8; 32] {
    *blake3::hash(b).as_bytes()
}

fn hex32(b: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for byte in b {
        use std::fmt::Write;
        let _ = write!(s, "{byte:02x}");
    }
    s
}

/// NASEntry that's good enough for `read_file_from` /
/// `write_file_from_bytes` — they only inspect `relative_path`,
/// `uid`, `gid`, `mode`. Other fields are filled with `Default`-ish
/// zeros so we don't accidentally claim e.g. mtimes we don't have.
fn synth_dest_entry(relative_path: PathBuf, size: u64) -> EntryEnum {
    let name = relative_path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let extension = relative_path
        .extension()
        .map(|s| s.to_string_lossy().into_owned());
    let now_ns = UNIX_EPOCH.elapsed().map(|d| d.as_nanos() as i64).unwrap_or(0);
    EntryEnum::NAS(NASEntry {
        name,
        relative_path,
        extension,
        is_dir: false,
        size,
        atime: now_ns,
        ctime: now_ns,
        mtime: now_ns,
        mode: 0o644,
        is_symlink: false,
        hard_links: Some(1),
        uid: None,
        gid: None,
        ino: None,
        file_handle: None,
        acl: None,
        owner: None,
        owner_group: None,
        xattrs: None,
    })
}

fn map_storage_err(e: storage_v2::error::StorageError, ctx_path: &Path) -> MoverError {
    use storage_v2::error::StorageError;
    match e {
        StorageError::IoError(io) if io.kind() == std::io::ErrorKind::NotFound => {
            MoverError::Backend {
                errno: 2, // ENOENT
                reason: format!("{} missing", ctx_path.display()),
            }
        }
        StorageError::IoError(io) if io.kind() == std::io::ErrorKind::PermissionDenied => {
            MoverError::Backend {
                errno: 13, // EACCES
                reason: format!("{}: {io}", ctx_path.display()),
            }
        }
        StorageError::IoError(io) => MoverError::Backend {
            errno: io.raw_os_error().unwrap_or(5),
            reason: format!("{}: {io}", ctx_path.display()),
        },
        other => MoverError::Other(format!("storage_v2: {other}")),
    }
}

fn is_not_found(e: &storage_v2::error::StorageError) -> bool {
    use storage_v2::error::StorageError;
    matches!(e, StorageError::IoError(io) if io.kind() == std::io::ErrorKind::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use hsm_core::{ActionKind, ArchiveId, Cookie, Extent, Fid};
    use hsm_plugin_sdk::{ActionCtxBuilder, ProgressConfig, ProgressReporter};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn ctx_for(
        cookie: u64,
        fid: Fid,
        archive_id: u32,
        kind: ActionKind,
        extent: Extent,
        primary: PathBuf,
        write: Option<PathBuf>,
        existing: Option<BackendObject>,
    ) -> ActionCtx {
        let (progress, _rx) = ProgressReporter::new(
            Cookie::new(cookie),
            extent,
            ProgressConfig::defaults(),
        );
        let mut b = ActionCtxBuilder::default()
            .cookie(Cookie::new(cookie))
            .fid(fid)
            .archive_id(ArchiveId::new(archive_id))
            .kind(kind)
            .extent(extent)
            .primary_path(primary)
            .hint(Bytes::new())
            .progress(progress)
            .cancel(CancellationToken::new());
        if let Some(p) = write {
            b = b.write_path(p);
        }
        if let Some(o) = existing {
            b = b.existing(o);
        }
        b.build()
    }

    #[tokio::test]
    async fn archive_then_restore_round_trip() {
        let work = tempfile::tempdir().unwrap();
        let archive_root = work.path().join("backend");
        let primary = work.path().join("data.bin");
        let restored = work.path().join("restored.bin");
        let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&primary, &payload).await.unwrap();

        let mover = TerrasyncMover::new(&archive_root);
        let fid = Fid::new(0x200000401, 0x12, 0x0);
        let aid = 1;

        // archive
        let ctx_a = ctx_for(
            100,
            fid,
            aid,
            ActionKind::Archive,
            Extent::WHOLE,
            primary.clone(),
            None,
            None,
        );
        let obj = mover.archive(ctx_a).await.expect("archive ok");
        assert_eq!(obj.uuid, "0x200000401:0x12:0x0");
        assert!(obj.url.starts_with("file://"));

        let backend_path = mover
            .layout()
            .full_path(ArchiveId::new(aid), fid);
        assert_eq!(tokio::fs::read(&backend_path).await.unwrap(), payload);

        // restore
        let ctx_r = ctx_for(
            101,
            fid,
            aid,
            ActionKind::Restore,
            Extent::WHOLE,
            primary.clone(),
            Some(restored.clone()),
            Some(obj.clone()),
        );
        mover.restore(ctx_r, obj.clone()).await.expect("restore ok");
        assert_eq!(tokio::fs::read(&restored).await.unwrap(), payload);

        // remove
        let ctx_d = ctx_for(
            102,
            fid,
            aid,
            ActionKind::Remove,
            Extent::WHOLE,
            PathBuf::new(),
            None,
            Some(obj.clone()),
        );
        mover.remove(ctx_d, obj.clone()).await.expect("remove ok");
        assert!(tokio::fs::try_exists(&backend_path).await.unwrap() == false);

        // remove again is idempotent
        let ctx_d2 = ctx_for(
            103,
            fid,
            aid,
            ActionKind::Remove,
            Extent::WHOLE,
            PathBuf::new(),
            None,
            Some(obj.clone()),
        );
        mover.remove(ctx_d2, obj).await.expect("remove idempotent");
    }

    #[tokio::test]
    async fn restore_with_corrupted_backend_returns_integrity_error() {
        let work = tempfile::tempdir().unwrap();
        let archive_root = work.path().join("backend");
        let primary = work.path().join("data.bin");
        let restored = work.path().join("restored.bin");
        tokio::fs::write(&primary, b"hello world").await.unwrap();

        let mover = TerrasyncMover::new(&archive_root);
        let fid = Fid::new(2, 17, 0);

        let obj = mover
            .archive(ctx_for(
                10,
                fid,
                1,
                ActionKind::Archive,
                Extent::WHOLE,
                primary.clone(),
                None,
                None,
            ))
            .await
            .unwrap();

        // Tamper the backend object on disk.
        let backend_path = mover.layout().full_path(ArchiveId::new(1), fid);
        tokio::fs::write(&backend_path, b"GOTCHA WORLD").await.unwrap();

        let err = mover
            .restore(
                ctx_for(
                    11,
                    fid,
                    1,
                    ActionKind::Restore,
                    Extent::WHOLE,
                    primary,
                    Some(restored.clone()),
                    Some(obj.clone()),
                ),
                obj,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, MoverError::Integrity(_)), "got {err:?}");
        // Restored file must NOT have been written when integrity fails.
        assert!(!tokio::fs::try_exists(&restored).await.unwrap());
    }

    #[tokio::test]
    async fn cancel_before_read_returns_cancelled() {
        let work = tempfile::tempdir().unwrap();
        let primary = work.path().join("data.bin");
        tokio::fs::write(&primary, vec![0u8; 1024]).await.unwrap();

        let mover = TerrasyncMover::new(work.path().join("backend"));
        let fid = Fid::new(2, 17, 0);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (progress, _rx) = ProgressReporter::new(
            Cookie::new(20),
            Extent::WHOLE,
            ProgressConfig::defaults(),
        );
        let ctx = ActionCtxBuilder::default()
            .cookie(Cookie::new(20))
            .fid(fid)
            .archive_id(ArchiveId::new(1))
            .kind(ActionKind::Archive)
            .extent(Extent::WHOLE)
            .primary_path(primary)
            .hint(Bytes::new())
            .progress(progress)
            .cancel(cancel)
            .build();

        let err = mover.archive(ctx).await.unwrap_err();
        assert!(matches!(err, MoverError::Cancelled));
    }

    #[tokio::test]
    async fn archive_rejects_relative_primary_path() {
        let mover = TerrasyncMover::new("/tmp/backend-rel-test");
        let ctx = ctx_for(
            30,
            Fid::new(2, 17, 0),
            1,
            ActionKind::Archive,
            Extent::WHOLE,
            PathBuf::from("relative.bin"),
            None,
            None,
        );
        let err = mover.archive(ctx).await.unwrap_err();
        assert!(matches!(err, MoverError::Other(_)));
    }

    #[tokio::test]
    async fn archive_emits_progress_advance() {
        let work = tempfile::tempdir().unwrap();
        let primary = work.path().join("data.bin");
        let payload = vec![7u8; 8192];
        tokio::fs::write(&primary, &payload).await.unwrap();

        let mover = TerrasyncMover::new(work.path().join("backend"));
        let fid = Fid::new(2, 17, 0);
        let (progress, mut rx) = ProgressReporter::new(
            Cookie::new(40),
            Extent::WHOLE,
            ProgressConfig::defaults(),
        );
        let ctx = ActionCtxBuilder::default()
            .cookie(Cookie::new(40))
            .fid(fid)
            .archive_id(ArchiveId::new(1))
            .kind(ActionKind::Archive)
            .extent(Extent::WHOLE)
            .primary_path(primary)
            .hint(Bytes::new())
            .progress(progress)
            .cancel(CancellationToken::new())
            .build();
        let _ = mover.archive(ctx).await.unwrap();

        // Drain progress events; the final flush must surface ≥ payload bytes.
        let mut total = 0u64;
        // Give the flush a beat to land.
        tokio::time::sleep(Duration::from_millis(50)).await;
        while let Ok(ev) = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            match ev {
                Some(e) => total = total.max(e.bytes_advanced),
                None => break,
            }
        }
        assert_eq!(total, payload.len() as u64);
    }
}
