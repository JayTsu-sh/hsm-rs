//! Throttled progress reporter handed to [`Mover`](crate::Mover) calls.
//!
//! The dispatcher's chunk loop (see `docs/DESIGN.md` §10.3) wants to call
//! `progress.advance(n)` on every transferred chunk. If we forwarded each
//! advance straight to `llapi_hsm_action_progress`, a 4 MiB chunk on a
//! 100 GiB file would mean ~25 600 kernel RPCs — pure waste. The reporter
//! debounces: emit a [`ProgressEvent`] only when **either** of two
//! thresholds trips:
//!
//! - bytes accumulated since last emit ≥ `bytes_threshold`, **or**
//! - wall-clock since last emit ≥ `time_threshold`
//!
//! The bytes threshold is the hot-path gate (most common trigger on
//! big files); the time threshold is the slow-path safety net (so
//! restores on a 100 KiB file still surface a single update).
//!
//! Events flow through a bounded `tokio::mpsc::Sender`. The downstream
//! consumer (the SDK's gRPC `StatusStream` task in M2d) reads them and
//! relays to the daemon.

use std::time::{Duration, Instant};

use hsm_core::{Cookie, Extent};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::trace;

/// One throttled progress sample emitted by [`ProgressReporter`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgressEvent {
    /// Cookie of the in-flight action.
    pub cookie: Cookie,
    /// Furthest byte fully transferred (cumulative, not delta).
    pub bytes_advanced: u64,
    /// Original action extent — lets the daemon compute pct without
    /// re-looking it up.
    pub extent: Extent,
}

/// Tunables for [`ProgressReporter`].
#[derive(Clone, Copy, Debug)]
pub struct ProgressConfig {
    /// Bytes accumulated before forcing an emit. Hot-path gate.
    pub bytes_threshold: u64,
    /// Wall-clock between emits regardless of bytes. Slow-path safety
    /// net for tiny transfers.
    pub time_threshold: Duration,
}

impl ProgressConfig {
    /// Defaults from `docs/DESIGN.md` §10: 64 MiB or 5 s, whichever
    /// hits first. Tested at length below; tweak per backend if needed.
    pub const fn defaults() -> Self {
        Self {
            bytes_threshold: 64 * 1024 * 1024,
            time_threshold: Duration::from_secs(5),
        }
    }
}

impl Default for ProgressConfig {
    fn default() -> Self {
        Self::defaults()
    }
}

struct State {
    /// Bytes accumulated since the last emit.
    pending: u64,
    /// Cumulative bytes advanced over the lifetime of this reporter.
    total: u64,
    /// When the last emit happened (or reporter creation).
    last_emit: Instant,
}

/// Sender-side handle for chunk loops.
///
/// `Clone` cheaply (`Arc` internally) so multiple branches of an unrolled
/// chunk loop can share it.
pub struct ProgressReporter {
    cookie: Cookie,
    extent: Extent,
    config: ProgressConfig,
    tx: mpsc::Sender<ProgressEvent>,
    state: std::sync::Arc<Mutex<State>>,
}

impl Clone for ProgressReporter {
    fn clone(&self) -> Self {
        Self {
            cookie: self.cookie,
            extent: self.extent,
            config: self.config,
            tx: self.tx.clone(),
            state: self.state.clone(),
        }
    }
}

impl ProgressReporter {
    /// Pair a new reporter with an `mpsc::Receiver` the consumer drains.
    /// The channel capacity is small (16) — we don't want backlogs of
    /// progress events; if the consumer falls behind, dropping samples
    /// is correct (next emit carries the latest cumulative count anyway).
    pub fn new(
        cookie: Cookie,
        extent: Extent,
        config: ProgressConfig,
    ) -> (Self, mpsc::Receiver<ProgressEvent>) {
        let (tx, rx) = mpsc::channel(16);
        let reporter = Self {
            cookie,
            extent,
            config,
            tx,
            state: std::sync::Arc::new(Mutex::new(State {
                pending: 0,
                total: 0,
                last_emit: Instant::now(),
            })),
        };
        (reporter, rx)
    }

    /// Convenience: defaults from [`ProgressConfig::defaults`].
    pub fn with_defaults(cookie: Cookie, extent: Extent) -> (Self, mpsc::Receiver<ProgressEvent>) {
        Self::new(cookie, extent, ProgressConfig::defaults())
    }

    /// Cookie this reporter is bound to.
    pub fn cookie(&self) -> Cookie {
        self.cookie
    }

    /// Cumulative bytes advanced so far (across all `advance()` calls).
    /// Useful for end-of-action telemetry without flushing.
    pub fn total_advanced(&self) -> u64 {
        self.state.lock().total
    }

    /// Note that `bytes` more bytes have been transferred. May or may
    /// not actually emit an event, depending on the dual thresholds.
    /// Returns `Ok(())` even on consumer-dropped (we just stop emitting
    /// further; the mover keeps making progress).
    pub fn advance(&self, bytes: u64) {
        let event = {
            let mut s = self.state.lock();
            s.pending += bytes;
            s.total += bytes;
            let elapsed = s.last_emit.elapsed();
            let bytes_hit = s.pending >= self.config.bytes_threshold;
            let time_hit = elapsed >= self.config.time_threshold;
            if bytes_hit || time_hit {
                s.pending = 0;
                s.last_emit = Instant::now();
                Some(ProgressEvent {
                    cookie: self.cookie,
                    bytes_advanced: s.total,
                    extent: self.extent,
                })
            } else {
                None
            }
        };
        if let Some(ev) = event {
            // try_send: drop on backlog rather than block the chunk loop.
            // The next event carries the latest cumulative total, so a
            // dropped sample doesn't lose information — only delays the
            // next observation.
            match self.tx.try_send(ev) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    trace!(target: "hsm.sdk.progress", cookie = %self.cookie, "progress channel full, dropping sample");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    trace!(target: "hsm.sdk.progress", cookie = %self.cookie, "progress consumer dropped, ignoring");
                }
            }
        }
    }

    /// Force-emit a final progress event regardless of thresholds.
    /// Movers should call this just before returning success so the
    /// daemon's last view matches the real bytes transferred.
    pub async fn flush(&self) {
        let event = {
            let mut s = self.state.lock();
            s.pending = 0;
            s.last_emit = Instant::now();
            ProgressEvent {
                cookie: self.cookie,
                bytes_advanced: s.total,
                extent: self.extent,
            }
        };
        // Use blocking send for flush so the final cumulative count
        // really lands. Drop is still tolerated.
        let _ = self.tx.send(event).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> (Cookie, Extent) {
        (Cookie::new(0xabc), Extent::new(0, 1024 * 1024 * 1024))
    }

    #[tokio::test]
    async fn emits_when_bytes_threshold_hit() {
        let (c, e) = ctx();
        let cfg = ProgressConfig {
            bytes_threshold: 1024,
            time_threshold: Duration::from_secs(10),
        };
        let (r, mut rx) = ProgressReporter::new(c, e, cfg);
        // Below threshold: no emit yet.
        r.advance(512);
        assert!(rx.try_recv().is_err());
        // At threshold: one emit.
        r.advance(512);
        let ev = rx.try_recv().unwrap();
        assert_eq!(ev.cookie, c);
        assert_eq!(ev.bytes_advanced, 1024);
    }

    #[tokio::test]
    async fn emits_when_time_threshold_hit_even_with_few_bytes() {
        let (c, e) = ctx();
        let cfg = ProgressConfig {
            bytes_threshold: u64::MAX, // never trips on bytes alone
            time_threshold: Duration::from_millis(20),
        };
        let (r, mut rx) = ProgressReporter::new(c, e, cfg);
        r.advance(8); // first event immediately because last_emit was Instant::now() at construction time + 0 elapsed
        // actually — last_emit was just-now, so 0 elapsed → no emit.
        assert!(rx.try_recv().is_err());
        tokio::time::sleep(Duration::from_millis(40)).await;
        r.advance(8);
        let ev = rx.try_recv().unwrap();
        assert_eq!(ev.bytes_advanced, 16);
    }

    #[tokio::test]
    async fn cumulative_total_keeps_growing_across_events() {
        let (c, e) = ctx();
        let cfg = ProgressConfig {
            bytes_threshold: 100,
            time_threshold: Duration::from_secs(10),
        };
        let (r, mut rx) = ProgressReporter::new(c, e, cfg);
        r.advance(150); // emit, pending reset to 0
        r.advance(150); // emit, total=300
        let ev1 = rx.try_recv().unwrap();
        let ev2 = rx.try_recv().unwrap();
        assert_eq!(ev1.bytes_advanced, 150);
        assert_eq!(ev2.bytes_advanced, 300);
        assert_eq!(r.total_advanced(), 300);
    }

    #[tokio::test]
    async fn flush_emits_final_event_unconditionally() {
        let (c, e) = ctx();
        let cfg = ProgressConfig {
            bytes_threshold: u64::MAX,
            time_threshold: Duration::from_secs(10),
        };
        let (r, mut rx) = ProgressReporter::new(c, e, cfg);
        r.advance(7);
        assert!(rx.try_recv().is_err());
        r.flush().await;
        let ev = rx.try_recv().unwrap();
        assert_eq!(ev.bytes_advanced, 7);
    }

    #[tokio::test]
    async fn dropped_consumer_is_tolerated() {
        let (c, e) = ctx();
        let cfg = ProgressConfig {
            bytes_threshold: 1,
            time_threshold: Duration::from_secs(10),
        };
        let (r, rx) = ProgressReporter::new(c, e, cfg);
        drop(rx);
        // No panic when channel is closed.
        r.advance(1);
        r.advance(2);
        r.flush().await;
        assert_eq!(r.total_advanced(), 3);
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let (c, e) = ctx();
        let cfg = ProgressConfig {
            bytes_threshold: 100,
            time_threshold: Duration::from_secs(10),
        };
        let (r, mut rx) = ProgressReporter::new(c, e, cfg);
        let r2 = r.clone();
        r.advance(60);
        r2.advance(60);
        let ev = rx.try_recv().unwrap();
        assert_eq!(ev.bytes_advanced, 120);
    }
}
