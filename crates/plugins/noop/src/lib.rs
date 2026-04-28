//! Reference no-op Mover.
//!
//! Doesn't actually move data: archive pretends to transfer
//! `ctx.extent.length` bytes (capped at a configured max for
//! `Extent::WHOLE`), restore pretends to pull them back, remove
//! pretends to delete. Useful for:
//!
//! - Validating the [`Mover`] trait shape end-to-end without spinning
//!   up a real backend.
//! - Dispatcher / store / scheduler integration tests in M2d that
//!   need *some* mover to succeed.
//! - Latency baseline: NoopMover with `chunk_delay = 0` measures the
//!   SDK overhead alone; with a deliberate delay it simulates a slow
//!   backend.
//!
//! Records every invocation in an internal log so tests can assert on
//! "what the daemon dispatched" without inspecting RPC traffic.

#![warn(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use hsm_core::BackendObject;
use hsm_plugin_sdk::{ActionCtx, ActionKind, Mover, MoverError, MoverResult};
use parking_lot::Mutex;

/// Per-action invocation record kept by [`NoopMover`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvocationRecord {
    /// Which method was called (`Archive`, `Restore`, `Remove`).
    pub kind: ActionKind,
    /// Cookie of the action.
    pub cookie: hsm_core::Cookie,
    /// Bytes the noop "transferred" (zero for Remove).
    pub bytes_transferred: u64,
    /// `Some(obj)` for Restore / Remove; `None` for Archive (the
    /// mover *creates* the object on archive).
    pub existing: Option<BackendObject>,
    /// Whether the mover bailed out due to cancellation.
    pub cancelled: bool,
}

/// Minimal in-memory mover for tests + SDK demos.
pub struct NoopMover {
    /// Bytes per simulated chunk during archive/restore.
    chunk_size: u64,
    /// Cap on bytes "transferred" when `extent.length == WHOLE_LENGTH`
    /// (the kernel's "whole file" sentinel). Without a cap we'd run
    /// for ~`u64::MAX` chunks.
    whole_file_bytes: u64,
    /// Optional sleep between chunks — simulates a slow backend.
    chunk_delay: Duration,
    /// Append-only log of completed (or cancelled) invocations.
    log: Arc<Mutex<Vec<InvocationRecord>>>,
}

impl Default for NoopMover {
    fn default() -> Self {
        Self {
            chunk_size: 1024 * 1024,
            whole_file_bytes: 16 * 1024 * 1024,
            chunk_delay: Duration::ZERO,
            log: Arc::default(),
        }
    }
}

impl NoopMover {
    /// New mover with custom chunking + delay knobs.
    pub fn new(chunk_size: u64, whole_file_bytes: u64, chunk_delay: Duration) -> Self {
        Self {
            chunk_size,
            whole_file_bytes,
            chunk_delay,
            log: Arc::default(),
        }
    }

    /// Snapshot of every action processed (in order).
    pub fn invocations(&self) -> Vec<InvocationRecord> {
        self.log.lock().clone()
    }

    fn record(&self, rec: InvocationRecord) {
        self.log.lock().push(rec);
    }

    /// Resolve "how many bytes does this action represent?" — capped
    /// for `Extent::WHOLE`.
    fn effective_bytes(&self, ctx: &ActionCtx) -> u64 {
        if ctx.extent.is_whole_file() {
            self.whole_file_bytes
        } else {
            ctx.extent.length
        }
    }

    /// Common chunk loop used by archive + restore. Returns the number
    /// of bytes transferred before a cancel (or all bytes on success).
    async fn chunk_loop(&self, ctx: &ActionCtx, total: u64) -> MoverResult<u64> {
        let mut transferred = 0u64;
        while transferred < total {
            ctx.check_cancel()?;
            let take = self.chunk_size.min(total - transferred);
            if !self.chunk_delay.is_zero() {
                // Race the cancel token against the sleep so a long
                // delay can be aborted promptly.
                tokio::select! {
                    () = tokio::time::sleep(self.chunk_delay) => {}
                    () = ctx.cancel.cancelled() => {
                        return Err(MoverError::Cancelled);
                    }
                }
            }
            transferred += take;
            ctx.progress.advance(take);
        }
        Ok(transferred)
    }
}

impl Mover for NoopMover {
    async fn archive(&self, ctx: ActionCtx) -> MoverResult<BackendObject> {
        let total = self.effective_bytes(&ctx);
        let cookie = ctx.cookie;
        match self.chunk_loop(&ctx, total).await {
            Ok(transferred) => {
                ctx.progress.flush().await;
                let uuid = format!("noop/{cookie}");
                let obj = BackendObject {
                    uuid: uuid.clone(),
                    // Deterministic stub hash (cookie repeated 4×). Lets tests
                    // assert without computing real BLAKE3.
                    hash: stub_hash(cookie.get()),
                    url: format!("noop://{uuid}"),
                };
                self.record(InvocationRecord {
                    kind: ActionKind::Archive,
                    cookie,
                    bytes_transferred: transferred,
                    existing: None,
                    cancelled: false,
                });
                Ok(obj)
            }
            Err(MoverError::Cancelled) => {
                self.record(InvocationRecord {
                    kind: ActionKind::Archive,
                    cookie,
                    bytes_transferred: 0,
                    existing: None,
                    cancelled: true,
                });
                Err(MoverError::Cancelled)
            }
            Err(other) => Err(other),
        }
    }

    async fn restore(&self, ctx: ActionCtx, obj: BackendObject) -> MoverResult<()> {
        let total = self.effective_bytes(&ctx);
        let cookie = ctx.cookie;
        match self.chunk_loop(&ctx, total).await {
            Ok(transferred) => {
                ctx.progress.flush().await;
                self.record(InvocationRecord {
                    kind: ActionKind::Restore,
                    cookie,
                    bytes_transferred: transferred,
                    existing: Some(obj),
                    cancelled: false,
                });
                Ok(())
            }
            Err(MoverError::Cancelled) => {
                self.record(InvocationRecord {
                    kind: ActionKind::Restore,
                    cookie,
                    bytes_transferred: 0,
                    existing: Some(obj),
                    cancelled: true,
                });
                Err(MoverError::Cancelled)
            }
            Err(other) => Err(other),
        }
    }

    async fn remove(&self, ctx: ActionCtx, obj: BackendObject) -> MoverResult<()> {
        // No bytes to move; just log and succeed (idempotent).
        self.record(InvocationRecord {
            kind: ActionKind::Remove,
            cookie: ctx.cookie,
            bytes_transferred: 0,
            existing: Some(obj),
            cancelled: false,
        });
        Ok(())
    }
}

/// 32-byte stub hash made by repeating the cookie's 8 LE bytes 4 times.
/// Deterministic and easy to recognize in test assertions.
fn stub_hash(cookie: u64) -> [u8; 32] {
    let mut h = [0u8; 32];
    let bytes = cookie.to_le_bytes();
    for chunk in h.chunks_exact_mut(8) {
        chunk.copy_from_slice(&bytes);
    }
    h
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hsm_plugin_sdk::{
        ActionCtx, ArchiveId, BackendObject, CancellationToken, Cookie, Extent, Fid,
        ProgressReporter,
    };

    use super::*;

    fn ctx(
        cookie: u64,
        kind: ActionKind,
        token: CancellationToken,
    ) -> (
        ActionCtx,
        tokio::sync::mpsc::Receiver<hsm_plugin_sdk::ProgressEvent>,
    ) {
        let (p, rx) = ProgressReporter::with_defaults(Cookie::new(cookie), Extent::WHOLE);
        let ctx = ActionCtx::builder()
            .cookie(Cookie::new(cookie))
            .fid(Fid::new(1, cookie as u32, 0))
            .archive_id(ArchiveId::new(1))
            .kind(kind)
            .progress(p)
            .cancel(token)
            .build();
        (ctx, rx)
    }

    #[tokio::test]
    async fn archive_returns_a_well_formed_backend_object() {
        let m = NoopMover::default();
        let (c, _rx) = ctx(0xabc, ActionKind::Archive, CancellationToken::new());
        let obj = m.archive(c).await.unwrap();
        assert_eq!(obj.uuid, "noop/0xabc");
        assert_eq!(obj.url, "noop://noop/0xabc");
        // First 8 bytes of stub hash = LE encoding of cookie.
        assert_eq!(&obj.hash[..8], &(0xabc_u64.to_le_bytes()));
    }

    #[tokio::test]
    async fn archive_records_invocation() {
        let m = NoopMover::default();
        let (c, _rx) = ctx(1, ActionKind::Archive, CancellationToken::new());
        m.archive(c).await.unwrap();
        let log = m.invocations();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].kind, ActionKind::Archive);
        assert_eq!(log[0].bytes_transferred, 16 * 1024 * 1024);
        assert!(!log[0].cancelled);
    }

    #[tokio::test]
    async fn cancellation_aborts_mid_transfer() {
        // 16 MiB whole-file at 1 MiB chunks, 50 ms delay each → ~800 ms.
        // Cancel after 60 ms → expect bail-out before completion.
        let m = NoopMover::new(1024 * 1024, 16 * 1024 * 1024, Duration::from_millis(50));
        let token = CancellationToken::new();
        let (c, _rx) = ctx(2, ActionKind::Archive, token.clone());
        let task = tokio::spawn(async move { m.archive(c).await });
        tokio::time::sleep(Duration::from_millis(60)).await;
        token.cancel();
        let res = task.await.unwrap();
        assert!(matches!(res, Err(MoverError::Cancelled)));
    }

    #[tokio::test]
    async fn restore_records_existing_object() {
        let m = NoopMover::default();
        let (c, _rx) = ctx(3, ActionKind::Restore, CancellationToken::new());
        let obj = BackendObject {
            uuid: "k/abc".into(),
            hash: [0u8; 32],
            url: "noop://k/abc".into(),
        };
        m.restore(c, obj.clone()).await.unwrap();
        let log = m.invocations();
        assert_eq!(log[0].kind, ActionKind::Restore);
        assert_eq!(log[0].existing.as_ref().unwrap().uuid, "k/abc");
    }

    #[tokio::test]
    async fn remove_is_immediate_success() {
        let m = NoopMover::default();
        let (c, _rx) = ctx(4, ActionKind::Remove, CancellationToken::new());
        let obj = BackendObject {
            uuid: "k/xyz".into(),
            hash: [0u8; 32],
            url: "noop://k/xyz".into(),
        };
        m.remove(c, obj).await.unwrap();
        let log = m.invocations();
        assert_eq!(log[0].kind, ActionKind::Remove);
        assert_eq!(log[0].bytes_transferred, 0);
    }

    #[tokio::test]
    async fn progress_events_actually_flow_through_the_reporter() {
        // 16 MiB at 1 MiB chunks with low bytes_threshold → many events.
        // We don't care about the exact count (depends on threshold
        // semantics); just that the channel gets at least the final
        // flush event.
        let m = NoopMover::default();
        let token = CancellationToken::new();
        let (c, mut rx) = ctx(5, ActionKind::Archive, token);
        m.archive(c).await.unwrap();

        // Drain all queued events.
        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }
        assert!(
            !events.is_empty(),
            "expected at least the final flush event"
        );
        // Last event must be the cumulative total.
        assert_eq!(events.last().unwrap().bytes_advanced, 16 * 1024 * 1024);
    }
}
