//! [`ActionCtx`] — everything a [`Mover`](crate::Mover) call receives
//! about the action it's processing.
//!
//! Holds:
//! - **Identity**: cookie / fid / archive — for logging, hashing,
//!   round-trip back to the daemon.
//! - **Geometry**: extent + filesystem paths the mover should use.
//!   Paths are pre-resolved by the daemon (e.g. `primary_path` is the
//!   Lustre file in absolute form, `write_path` is the dfid-opened
//!   path for restore).
//! - **Policy hint**: opaque `hai_data`. *Untrusted* — never splice
//!   into URLs or filesystem paths.
//! - **Hooks**: cancel token + progress reporter, plus an optional
//!   pre-existing [`BackendObject`] for restore / remove.

use std::path::PathBuf;

use bytes::Bytes;
use hsm_core::{ActionKind, ArchiveId, BackendObject, Cookie, Extent, Fid};
use tokio_util::sync::CancellationToken;

use crate::progress::ProgressReporter;

/// Per-action context handed to one [`Mover`](crate::Mover) invocation.
///
/// Held by value — fields are owned, no implicit borrows on the daemon.
/// Mover impls are free to destructure and pass parts around.
pub struct ActionCtx {
    /// Cookie issued by the kernel coordinator.
    pub cookie: Cookie,
    /// FID of the file the action targets.
    pub fid: Fid,
    /// Archive id this action targets.
    pub archive_id: ArchiveId,
    /// What the kernel asked us to do.
    pub kind: ActionKind,
    /// Byte range to operate on.
    pub extent: Extent,

    /// Filesystem path the mover should *read from*.
    /// - On `Archive`: the user's Lustre file.
    /// - On `Remove`: empty / unused.
    pub primary_path: PathBuf,

    /// Filesystem path the mover should *write to* on `Restore`.
    /// `None` for `Archive` / `Remove`.
    pub write_path: Option<PathBuf>,

    /// On `Restore` / `Remove`: the previously-archived object's
    /// metadata (read by the daemon from `trusted.lhsm_*` xattrs).
    /// `None` on `Archive` (mover decides + reports a fresh one).
    pub existing: Option<BackendObject>,

    /// Real Lustre path of the file relative to the Lustre mount root
    /// (e.g. `"test/dir/file.txt"`). Populated for Archive actions when
    /// the daemon successfully resolves the FID via `llapi_fid2path`.
    /// `None` in mock mode, for non-Archive kinds, or on resolution error.
    ///
    /// Used by movers to write the shadow namespace entry so the archive
    /// backend mirrors the Lustre directory structure (compatible with
    /// `lhsmtool_posix` shadow layout).
    pub lustre_path: Option<PathBuf>,

    /// Opaque per-action hint (`hai_data` from the user). Movers that
    /// route by this should validate against an allow-list — content is
    /// *attacker-controllable* in principle.
    pub hint: Bytes,

    /// Throttled progress reporter. Mover calls
    /// [`advance`](ProgressReporter::advance) on each transferred chunk
    /// and [`flush`](ProgressReporter::flush) before returning success.
    pub progress: ProgressReporter,

    /// Tripped by the daemon on shutdown or an `HSMA_CANCEL` for this
    /// cookie. Movers must check at chunk boundaries; the SDK doesn't
    /// inject the check automatically (the `Mover` impl knows what
    /// "chunk boundary" means for its backend).
    pub cancel: CancellationToken,
}

impl ActionCtx {
    /// `Ok(())` if no cancellation has been requested; otherwise
    /// `Err(MoverError::Cancelled)` so the mover can `?`-bubble out
    /// of its loop without hand-writing the match.
    pub fn check_cancel(&self) -> crate::MoverResult<()> {
        if self.cancel.is_cancelled() {
            Err(crate::MoverError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Builder shorthand used by tests + the SDK's `run()` machinery
    /// (M2d). Production code path lives in the daemon's dispatcher.
    pub fn builder() -> ActionCtxBuilder {
        ActionCtxBuilder::default()
    }
}

/// Helper for assembling an [`ActionCtx`] in tests / the dispatcher.
///
/// The default state is "Archive of cookie 0 on a zero FID, no paths" —
/// callers must set the fields they actually need; required ones are
/// caught at `build()` time.
#[derive(Default)]
pub struct ActionCtxBuilder {
    cookie: Option<Cookie>,
    fid: Option<Fid>,
    archive_id: Option<ArchiveId>,
    kind: Option<ActionKind>,
    extent: Option<Extent>,
    primary_path: Option<PathBuf>,
    write_path: Option<PathBuf>,
    existing: Option<BackendObject>,
    lustre_path: Option<PathBuf>,
    hint: Option<Bytes>,
    progress: Option<ProgressReporter>,
    cancel: Option<CancellationToken>,
}

impl ActionCtxBuilder {
    /// Mandatory: cookie.
    pub fn cookie(mut self, c: Cookie) -> Self {
        self.cookie = Some(c);
        self
    }
    /// Mandatory: fid.
    pub fn fid(mut self, f: Fid) -> Self {
        self.fid = Some(f);
        self
    }
    /// Mandatory: archive id.
    pub fn archive_id(mut self, a: ArchiveId) -> Self {
        self.archive_id = Some(a);
        self
    }
    /// Mandatory: action kind.
    pub fn kind(mut self, k: ActionKind) -> Self {
        self.kind = Some(k);
        self
    }
    /// Defaults to `Extent::WHOLE`.
    pub fn extent(mut self, e: Extent) -> Self {
        self.extent = Some(e);
        self
    }
    /// Defaults to empty `PathBuf` (movers using paths must set it).
    pub fn primary_path(mut self, p: impl Into<PathBuf>) -> Self {
        self.primary_path = Some(p.into());
        self
    }
    /// Optional restore destination.
    pub fn write_path(mut self, p: impl Into<PathBuf>) -> Self {
        self.write_path = Some(p.into());
        self
    }
    /// Optional pre-existing backend object (restore / remove).
    pub fn existing(mut self, o: BackendObject) -> Self {
        self.existing = Some(o);
        self
    }
    /// Real Lustre path relative to mount root (for shadow namespace).
    pub fn lustre_path(mut self, p: impl Into<PathBuf>) -> Self {
        self.lustre_path = Some(p.into());
        self
    }
    /// Opaque hint bytes; defaults to empty.
    pub fn hint(mut self, h: impl Into<Bytes>) -> Self {
        self.hint = Some(h.into());
        self
    }
    /// Mandatory: progress reporter (the receiver is the daemon's
    /// concern; the builder only sees the sender side).
    pub fn progress(mut self, p: ProgressReporter) -> Self {
        self.progress = Some(p);
        self
    }
    /// Defaults to a fresh, never-cancelled token.
    pub fn cancel(mut self, t: CancellationToken) -> Self {
        self.cancel = Some(t);
        self
    }

    /// Build the context, panicking on missing required fields.
    /// Tests / the dispatcher are the only callers; missing required
    /// fields is a programmer bug, not a runtime condition.
    pub fn build(self) -> ActionCtx {
        ActionCtx {
            cookie: self.cookie.expect("cookie required"),
            fid: self.fid.expect("fid required"),
            archive_id: self.archive_id.expect("archive_id required"),
            kind: self.kind.expect("kind required"),
            extent: self.extent.unwrap_or(Extent::WHOLE),
            primary_path: self.primary_path.unwrap_or_default(),
            write_path: self.write_path,
            existing: self.existing,
            lustre_path: self.lustre_path,
            hint: self.hint.unwrap_or_default(),
            progress: self.progress.expect("progress reporter required"),
            cancel: self.cancel.unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::ProgressReporter;

    #[test]
    fn check_cancel_signals_when_token_tripped() {
        let token = CancellationToken::new();
        let (p, _rx) = ProgressReporter::with_defaults(Cookie::new(1), Extent::WHOLE);
        let ctx = ActionCtx::builder()
            .cookie(Cookie::new(1))
            .fid(Fid::ZERO)
            .archive_id(ArchiveId::new(1))
            .kind(ActionKind::Archive)
            .progress(p)
            .cancel(token.clone())
            .build();
        assert!(ctx.check_cancel().is_ok());
        token.cancel();
        assert!(ctx.check_cancel().is_err());
    }

    #[test]
    fn builder_defaults_are_safe() {
        let (p, _rx) = ProgressReporter::with_defaults(Cookie::new(1), Extent::WHOLE);
        let ctx = ActionCtx::builder()
            .cookie(Cookie::new(7))
            .fid(Fid::new(1, 2, 0))
            .archive_id(ArchiveId::new(3))
            .kind(ActionKind::Restore)
            .progress(p)
            .build();
        assert_eq!(ctx.extent, Extent::WHOLE);
        assert!(ctx.primary_path.as_os_str().is_empty());
        assert!(ctx.write_path.is_none());
        assert!(ctx.existing.is_none());
        assert!(ctx.hint.is_empty());
        assert!(!ctx.cancel.is_cancelled());
    }

    #[test]
    #[should_panic(expected = "cookie required")]
    fn builder_panics_on_missing_required_field() {
        let (p, _rx) = ProgressReporter::with_defaults(Cookie::new(1), Extent::WHOLE);
        let _ = ActionCtx::builder().progress(p).build();
    }
}
