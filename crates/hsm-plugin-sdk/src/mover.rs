//! The [`Mover`] trait — the contract a plugin implements.
//!
//! Three async methods, one per `ActionKind` the daemon dispatches.
//! `Cancel` is handled implicitly: the daemon trips the
//! [`ActionCtx::cancel`](crate::ActionCtx) token, the mover sees it on
//! its next `check_cancel()` and `?`-bubbles `MoverError::Cancelled`.
//!
//! Async-fn-in-trait, Rust 2024 native. No `async-trait` macro needed.

use hsm_core::BackendObject;

use crate::ctx::ActionCtx;
use crate::error::MoverResult;

/// Backend implementation interface.
///
/// Required:
/// - `Send + Sync` so the SDK can hand it to spawned tasks.
/// - `'static` so the SDK can carry it for the process lifetime.
///
/// Plugins typically embed configuration (S3 client, terrasync
/// `StorageEnum`, BLAKE3 hasher state, …) as struct fields and
/// instantiate once in `main` before passing the mover to `run()`.
pub trait Mover: Send + Sync + 'static {
    /// Move bytes from the Lustre file (at `ctx.primary_path`) into
    /// the backend. Returns the metadata to persist as `trusted.lhsm_*`
    /// xattrs so future Restore / Remove can locate the object.
    ///
    /// Mover must:
    /// 1. Honour `ctx.extent` (offset + length).
    /// 2. Call `ctx.progress.advance(n)` after each chunk so the
    ///    daemon can surface progress to `llapi_hsm_action_progress`.
    /// 3. Call `ctx.check_cancel()?` at chunk boundaries.
    /// 4. Call `ctx.progress.flush().await` before returning success
    ///    so the final cumulative count lands.
    fn archive(&self, ctx: ActionCtx) -> impl Future<Output = MoverResult<BackendObject>> + Send;

    /// Pull bytes from `obj` back into the Lustre file (at
    /// `ctx.write_path`). Returns `Ok(())` on success — the daemon
    /// handles updating the file's HSM state.
    ///
    /// Mover should verify the BLAKE3 hash matches `obj.hash` after
    /// the transfer; on mismatch return `MoverError::Integrity`.
    fn restore(
        &self,
        ctx: ActionCtx,
        obj: BackendObject,
    ) -> impl Future<Output = MoverResult<()>> + Send;

    /// Delete `obj` from the backend.
    ///
    /// Should be idempotent: a missing object is **not** an error
    /// (return `Ok(())`). Backends that can't distinguish "missing"
    /// from "I/O failure" should still log the difference.
    fn remove(
        &self,
        ctx: ActionCtx,
        obj: BackendObject,
    ) -> impl Future<Output = MoverResult<()>> + Send;
}
