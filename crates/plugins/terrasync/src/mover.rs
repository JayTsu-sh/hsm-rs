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
use data_mover::{EntryEnum, LocalStorage, NASEntry, StorageEnum};
use hsm_core::BackendObject;
use hsm_plugin_sdk::{ActionCtx, Mover, MoverError, MoverResult};
use tracing::{debug, info, warn};

use crate::config::ArchiveLayout;

/// Mover backed by terrasync `StorageEnum` for source + destination.
///
/// `src` always reads from the local POSIX namespace via
/// `LocalStorage("/")` (the Lustre mountpoint is just a sub-path).
/// `dst` is supplied by the caller — file:// in M3a, S3 / NFS / CIFS
/// pluggable in M3b without touching mover internals.
///
/// ## QoS / rate limiting
///
/// Set [`bandwidth_bps`](Self::with_bandwidth) to cap throughput. The
/// progress loop splits each transfer into 4 MiB virtual chunks and
/// sleeps between chunks so the effective rate never exceeds the limit.
/// This also gives per-chunk cancel points (vs. the previous whole-file
/// cancel gap).
#[derive(Clone)]
pub struct TerrasyncMover {
    /// Source storage rooted at `/`.
    src: Arc<StorageEnum>,
    /// Destination storage.
    dst: Arc<StorageEnum>,
    /// Layout helper for `(archive_id, fid)` → (path, url) conversion.
    layout: ArchiveLayout,
    /// Optional bandwidth cap in bytes/second. `None` = unlimited.
    bandwidth_bps: Option<u64>,
}

impl TerrasyncMover {
    /// New mover with an explicit destination [`StorageEnum`] and
    /// archive layout. The source is always `LocalStorage("/")`.
    pub fn with_dst(dst: Arc<StorageEnum>, layout: ArchiveLayout) -> Self {
        let src = Arc::new(StorageEnum::Local(LocalStorage::new("/", None)));
        Self { src, dst, layout, bandwidth_bps: None }
    }

    /// Convenience for the file:// path: builds a `LocalStorage` at
    /// `archive_root` and an [`ArchiveLayout`] rooted at the same.
    pub fn new(archive_root: impl Into<PathBuf>) -> Self {
        let layout = ArchiveLayout::new(archive_root);
        let dst = Arc::new(StorageEnum::Local(LocalStorage::new(
            layout.root.clone(),
            None,
        )));
        Self::with_dst(dst, layout)
    }

    /// Cap throughput at `bps` bytes per second. Pass `0` to disable.
    pub fn with_bandwidth(mut self, bps: u64) -> Self {
        self.bandwidth_bps = if bps == 0 { None } else { Some(bps) };
        self
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

        // M2e limitation: read whole file into memory. terrasync's
        // copy_file_with_cancel is merged but requires src/dst to
        // share the same relative_path — HSM rewrites it to
        // <archive_id>/<fid>, so we go through Bytes for now.
        ctx.check_cancel()?;
        let bytes = StorageEnum::read_file_from(&self.src, &src_entry, size)
            .await
            .map_err(|e| map_storage_err(e, &primary))?;
        ctx.check_cancel()?;

        let hash = blake3_32(&bytes);

        let dst_relative = ArchiveLayout::relative_path(ctx.archive_id, ctx.fid);
        let dst_entry = synth_dest_entry(dst_relative, bytes.len() as u64);

        StorageEnum::write_file_from_bytes(&self.dst, &dst_entry, bytes.clone())
            .await
            .map_err(|e| map_storage_err(e, &self.layout.full_path(ctx.archive_id, ctx.fid)))?;

        // Paced progress: 4 MiB chunks with cancel points + rate limiting.
        paced_advance(&ctx, bytes.len() as u64, self.bandwidth_bps).await?;
        ctx.progress.flush().await;

        // Shadow namespace: create an entry that mirrors the Lustre path.
        // Compatible with `lhsmtool_posix` shadow layout so the archive can
        // be browsed by original path (not just by FID).
        if let Some(ref lustre_path) = ctx.lustre_path {
            self.write_shadow(lustre_path, ctx.archive_id, ctx.fid)
                .await;
        }

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
            .ok_or_else(|| MoverError::Other("backend object path escaped archive root".into()))?;
        debug!(
            target: "hsm.plugin.terrasync",
            cookie = %ctx.cookie,
            backend_relative = ?backend_relative,
            "restore: locating backend object"
        );

        // Accept NAS (file://, NFS) and S3 entries — both implement get_size()
        // and are usable with StorageEnum::read_file_from().
        let src_meta = match self.dst.get_metadata(&backend_relative).await {
            Ok(e) if !e.get_is_dir() => e,
            Ok(_) => {
                return Err(MoverError::Other(format!(
                    "backend object {} is a directory, not a file",
                    backend_relative.display()
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

        let restored_len = bytes.len() as u64;
        StorageEnum::write_file_from_bytes(&self.src, &write_entry, bytes)
            .await
            .map_err(|e| map_storage_err(e, &write_path_abs))?;

        paced_advance(&ctx, restored_len, self.bandwidth_bps).await?;
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
            }
            Err(e) => {
                // Idempotency: missing is OK.
                if is_not_found(&e) {
                    info!(
                        target: "hsm.plugin.terrasync",
                        cookie = %ctx.cookie, fid = %ctx.fid,
                        "remove: backend object already gone (treating as success)"
                    );
                } else {
                    return Err(map_storage_err(e, &backend_relative));
                }
            }
        }
        // Also remove the shadow entry so the archive root stays consistent.
        if let Some(ref lustre_path) = ctx.lustre_path {
            self.remove_shadow(lustre_path).await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shadow namespace helpers (private)
// ---------------------------------------------------------------------------

impl TerrasyncMover {
    /// Write a shadow entry to the destination storage.
    ///
    /// **file:// backend**: creates a POSIX symlink under
    /// `<archive_root>/shadow/<lustre_path>` pointing to the data object.
    /// Compatible with `lhsmtool_posix` shadow layout.
    ///
    /// **Other backends (S3, NFS, CIFS)**: writes a small pointer object
    /// at `shadow/<lustre_path>` whose content is the FID string.
    async fn write_shadow(
        &self,
        lustre_path: &Path,
        archive_id: hsm_core::ArchiveId,
        fid: hsm_core::Fid,
    ) {
        match &*self.dst {
            StorageEnum::Local(_) => {
                // file:// mode: create a symlink exactly like lhsmtool_posix.
                let shadow = self.layout.shadow_full_path(lustre_path);
                if let Some(parent) = shadow.parent() {
                    if let Err(e) = tokio::fs::create_dir_all(parent).await {
                        warn!(
                            target: "hsm.plugin.terrasync",
                            path = %parent.display(), error = %e,
                            "shadow: failed to create directory"
                        );
                        return;
                    }
                }
                let target = ArchiveLayout::shadow_symlink_target(lustre_path, archive_id, fid);
                // Remove stale entry first (idempotent).
                let _ = tokio::fs::remove_file(&shadow).await;
                if let Err(e) = tokio::fs::symlink(&target, &shadow).await {
                    warn!(
                        target: "hsm.plugin.terrasync",
                        shadow = %shadow.display(), target = %target.display(), error = %e,
                        "shadow: symlink failed"
                    );
                } else {
                    debug!(
                        target: "hsm.plugin.terrasync",
                        shadow = %shadow.display(), target = %target.display(),
                        "shadow: symlink created"
                    );
                }
            }
            _ => {
                // S3/NFS/CIFS: write a tiny pointer object at shadow/<path>.
                let shadow_key = ArchiveLayout::shadow_relative(lustre_path);
                let content = Bytes::from(ArchiveLayout::uuid_for(fid));
                let entry = synth_dest_entry(shadow_key, content.len() as u64);
                if let Err(e) = StorageEnum::write_file_from_bytes(&self.dst, &entry, content).await
                {
                    warn!(
                        target: "hsm.plugin.terrasync",
                        lustre_path = %lustre_path.display(), error = %e,
                        "shadow: write pointer object failed"
                    );
                }
            }
        }
    }

    /// Remove the shadow entry for `lustre_path`, best-effort.
    async fn remove_shadow(&self, lustre_path: &Path) {
        match &*self.dst {
            StorageEnum::Local(_) => {
                let shadow = self.layout.shadow_full_path(lustre_path);
                let _ = tokio::fs::remove_file(&shadow).await;
            }
            _ => {
                let shadow_key = ArchiveLayout::shadow_relative(lustre_path);
                let entry = synth_dest_entry(shadow_key, 0);
                let _ = self.dst.delete_file(&entry).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Emit progress in 4 MiB virtual chunks with optional bandwidth pacing.
///
/// Even though the underlying I/O already completed (data-mover reads the
/// whole file at once), splitting progress here gives:
/// - Per-chunk cancel points every 4 MiB.
/// - Smooth progress reporting instead of a single jump at the end.
/// - Real throughput limiting: each chunk sleeps until `bytes / bps`
///   wall-clock time has elapsed since the transfer started, so the
///   action never completes faster than the configured rate.
async fn paced_advance(
    ctx: &hsm_plugin_sdk::ActionCtx,
    total: u64,
    bandwidth_bps: Option<u64>,
) -> MoverResult<()> {
    const CHUNK: u64 = 4 * 1024 * 1024; // 4 MiB
    let start = std::time::Instant::now();
    let mut sent = 0u64;
    while sent < total {
        ctx.check_cancel()?;
        let chunk = (total - sent).min(CHUNK);
        if let Some(bps) = bandwidth_bps {
            // Calculate when `sent + chunk` bytes should have been
            // transferred at the rate limit, then sleep the deficit.
            let target_ns = (sent + chunk) as u128 * 1_000_000_000 / bps as u128;
            let elapsed_ns = start.elapsed().as_nanos();
            if target_ns > elapsed_ns {
                let sleep = std::time::Duration::from_nanos(
                    (target_ns - elapsed_ns) as u64,
                );
                tokio::time::sleep(sleep).await;
            }
        }
        sent += chunk;
        ctx.progress.advance(chunk);
    }
    Ok(())
}

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
    let now_ns = UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
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

fn map_storage_err(e: data_mover::error::StorageError, ctx_path: &Path) -> MoverError {
    use data_mover::error::StorageError;
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

fn is_not_found(e: &data_mover::error::StorageError) -> bool {
    use data_mover::error::StorageError;
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
        let (progress, _rx) =
            ProgressReporter::new(Cookie::new(cookie), extent, ProgressConfig::defaults());
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

        let backend_path = mover.layout().full_path(ArchiveId::new(aid), fid);
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
        tokio::fs::write(&backend_path, b"GOTCHA WORLD")
            .await
            .unwrap();

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
        let (progress, _rx) =
            ProgressReporter::new(Cookie::new(20), Extent::WHOLE, ProgressConfig::defaults());
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
        let (progress, mut rx) =
            ProgressReporter::new(Cookie::new(40), Extent::WHOLE, ProgressConfig::defaults());
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

    #[tokio::test]
    async fn bandwidth_limit_slows_archive() {
        let work = tempfile::tempdir().unwrap();
        let primary = work.path().join("data.bin");
        // 2 MiB payload — small enough for a fast test, large enough to
        // trigger at least one sleep at the rate limit.
        let payload = vec![0xabu8; 2 * 1024 * 1024];
        tokio::fs::write(&primary, &payload).await.unwrap();

        // Cap at 8 MiB/s → 2 MiB should take ≥ 250 ms.
        let bps: u64 = 8 * 1024 * 1024;
        let mover = TerrasyncMover::new(work.path().join("backend"))
            .with_bandwidth(bps);

        let fid = Fid::new(2, 99, 0);
        let (progress, _rx) =
            ProgressReporter::new(Cookie::new(50), Extent::WHOLE, ProgressConfig::defaults());
        let ctx = ActionCtxBuilder::default()
            .cookie(Cookie::new(50))
            .fid(fid)
            .archive_id(ArchiveId::new(1))
            .kind(ActionKind::Archive)
            .extent(Extent::WHOLE)
            .primary_path(primary)
            .hint(Bytes::new())
            .progress(progress)
            .cancel(CancellationToken::new())
            .build();

        let start = std::time::Instant::now();
        mover.archive(ctx).await.expect("archive ok");
        let elapsed = start.elapsed();

        let expected_min = std::time::Duration::from_millis(
            (payload.len() as u64 * 1000 / bps) as u64,
        );
        assert!(
            elapsed >= expected_min,
            "expected archive to take ≥ {expected_min:?} at {bps} bps, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn unlimited_archive_is_fast() {
        let work = tempfile::tempdir().unwrap();
        let primary = work.path().join("data.bin");
        let payload = vec![0u8; 2 * 1024 * 1024];
        tokio::fs::write(&primary, &payload).await.unwrap();

        // No bandwidth cap — should complete well under 250 ms on any
        // reasonable CI machine.
        let mover = TerrasyncMover::new(work.path().join("backend"));
        let fid = Fid::new(2, 100, 0);
        let (progress, _rx) =
            ProgressReporter::new(Cookie::new(51), Extent::WHOLE, ProgressConfig::defaults());
        let ctx = ActionCtxBuilder::default()
            .cookie(Cookie::new(51))
            .fid(fid)
            .archive_id(ArchiveId::new(1))
            .kind(ActionKind::Archive)
            .extent(Extent::WHOLE)
            .primary_path(primary)
            .hint(Bytes::new())
            .progress(progress)
            .cancel(CancellationToken::new())
            .build();

        let start = std::time::Instant::now();
        mover.archive(ctx).await.expect("archive ok");
        // Without a limit this should finish in << 250 ms.
        assert!(
            start.elapsed() < std::time::Duration::from_millis(250),
            "unlimited archive took unexpectedly long: {:?}",
            start.elapsed()
        );
    }
}
