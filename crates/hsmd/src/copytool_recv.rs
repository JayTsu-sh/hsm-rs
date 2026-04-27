//! Adapter from the synchronous `Copytool::recv` API to an async batch
//! stream the daemon's main loop can consume.
//!
//! Two reasons for the adapter:
//!
//! 1. `LiveCopytool::recv` blocks on the kernel pipe; we'll wrap it in
//!    `tokio::io::unix::AsyncFd` (M2d.2). The trait abstraction lets the
//!    daemon stay agnostic.
//! 2. `MockCopytool::recv` is a poll on an in-memory `VecDeque`; tests
//!    drive it by `enqueue`-ing actions and the daemon polls via the
//!    same trait.

use std::sync::Arc;
use std::time::Duration;

use hsm_core::{Cookie, Extent};
use lustre_llapi::{EndStatus, HasCookieLifecycle, MockCopytool, ReceivedAction};
use parking_lot::Mutex;

use crate::error::DaemonResult;

/// Completion signal sent from the daemon back to the `LiveRecvSource` thread.
/// Carries the cookie, outcome, and total bytes for the `llapi_hsm_action_end` call.
#[derive(Debug)]
pub struct ActionCompletion {
    pub cookie: Cookie,
    pub status: EndStatus,
    pub total_bytes: u64,
}

/// Cloneable sender end of the completion channel. The daemon holds one of
/// these for each `LiveRecvSource` and sends a signal after each action completes.
#[derive(Clone, Debug)]
pub struct CompletionTx(pub std::sync::mpsc::SyncSender<ActionCompletion>);

/// Async source of action batches.
///
/// Implementations resolve their futures only when there's something
/// new to deliver (or shutdown has been requested). A returned empty
/// vec is treated as a heartbeat — the daemon's main loop continues.
pub trait RecvSource: Send {
    /// Wait for the next batch of actions.
    fn next_batch(&mut self) -> impl Future<Output = DaemonResult<Vec<ReceivedAction>>> + Send;
}

/// Polling adapter around [`MockCopytool`] for the in-process test path.
///
/// Real production use (LiveCopytool + AsyncFd) lands in M2d.2.
pub struct MockRecvSource {
    ct: Arc<Mutex<MockCopytool>>,
    poll_interval: Duration,
}

impl MockRecvSource {
    /// Wrap the given mock with a polling interval.
    ///
    /// Tests use a tight interval (1–10 ms) for fast turnaround;
    /// real use would pick something more conservative.
    pub fn new(ct: Arc<Mutex<MockCopytool>>, poll_interval: Duration) -> Self {
        Self { ct, poll_interval }
    }
}

impl RecvSource for MockRecvSource {
    async fn next_batch(&mut self) -> DaemonResult<Vec<ReceivedAction>> {
        loop {
            let batch = self.ct.lock().recv()?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Live (M2d.2c)
// ---------------------------------------------------------------------------

/// Async recv source backed by a real [`lustre_llapi::LiveCopytool`].
///
/// The Lustre KUC fd is a `SOCK_SEQPACKET` netlink socket that requires
/// blocking I/O. We run `LiveCopytool::recv()` on a dedicated OS thread
/// (via `std::thread::spawn`) and forward batches through an mpsc channel
/// to the async side. This avoids the `O_NONBLOCK` / `AsyncFd` complexity
/// that the raw netlink fd doesn't support cleanly.
///
/// ## Action lifecycle
///
/// For each received action the recv thread calls `ct.begin_manual(action)`
/// before forwarding it to the daemon. When the daemon finishes processing
/// the action, it sends an [`ActionCompletion`] via [`CompletionTx`]. The
/// recv thread drains pending completions at the top of every iteration and
/// calls `ct.cookie_end()` for each — triggering `llapi_hsm_action_end`.
pub struct LiveRecvSource {
    rx: tokio::sync::mpsc::Receiver<crate::error::DaemonResult<Vec<ReceivedAction>>>,
    /// Sender end handed to the daemon so it can signal action completion.
    pub completion_tx: CompletionTx,
}

impl LiveRecvSource {
    /// Register as a copytool on `mount` for the given `archives`, then
    /// spawn a background thread that blocks on `recv()` and forwards
    /// action batches through the returned source.
    pub fn new(mount: &std::path::Path, archives: &[hsm_core::ArchiveId]) -> crate::error::DaemonResult<Self> {
        let ct = lustre_llapi::LiveCopytool::register(mount, archives)?;
        // Channel size 4: small back-pressure so the recv thread doesn't
        // race too far ahead of the daemon's store insert.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        // Unbounded-ish sync channel: completions arrive from the async
        // daemon; the recv thread drains them before each recv() call.
        // Bound = 256 to avoid unbounded growth if the recv thread stalls.
        let (completion_tx_inner, completion_rx) = std::sync::mpsc::sync_channel::<ActionCompletion>(256);
        let completion_tx = CompletionTx(completion_tx_inner);
        std::thread::Builder::new()
            .name("hsmd-kuc-recv".into())
            .spawn(move || {
                let mut ct = ct;
                loop {
                    // Drain completions first (non-blocking). This calls
                    // llapi_hsm_action_end for actions the daemon has finished.
                    while let Ok(compl) = completion_rx.try_recv() {
                        let extent = Extent::new(compl.total_bytes, 0);
                        if let Err(e) = ct.cookie_end(compl.cookie, extent, compl.status) {
                            tracing::warn!(
                                target: "hsmd.kuc",
                                cookie = %compl.cookie,
                                error = %e,
                                "llapi_hsm_action_end failed"
                            );
                        }
                    }

                    // Poll the KUC fd with a short timeout so we can process
                    // completions without blocking indefinitely on recv().
                    // The Lustre coordinator expects action_end before it
                    // dispatches the next batch, so we must not block here
                    // when a completion is waiting in completion_rx.
                    let fd = ct.raw_fd();
                    let has_data = {
                        let mut pollfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
                        // 100ms timeout: responsive to completions without busy-waiting.
                        let rc = unsafe { libc::poll(&mut pollfd, 1, 100) };
                        rc > 0 && (pollfd.revents & libc::POLLIN) != 0
                    };

                    if !has_data {
                        // No actions ready yet — loop back to drain completions.
                        continue;
                    }

                    // Actions are available; recv them.
                    let result: crate::error::DaemonResult<Vec<ReceivedAction>> =
                        ct.recv().map_err(Into::into);
                    let is_err = result.is_err();

                    // Call begin_manual for each received action so the
                    // per-cookie handle is registered in LiveCopytool::handles.
                    if let Ok(ref actions) = result {
                        for action in actions {
                            if let Err(e) = ct.begin_manual(action) {
                                tracing::warn!(
                                    target: "hsmd.kuc",
                                    cookie = %action.cookie,
                                    error = %e,
                                    "llapi_hsm_action_begin failed"
                                );
                            }
                        }
                    }

                    // Forward to the async daemon.
                    if tx.blocking_send(result).is_err() {
                        // Receiver dropped (daemon shutting down).
                        break;
                    }
                    if is_err {
                        break;
                    }
                }
                // Dropping `ct` here abandons any in-flight actions and
                // unregisters the copytool from Lustre.
            })
            .map_err(|e| crate::error::DaemonError::Io(e))?;
        Ok(Self { rx, completion_tx })
    }
}

impl RecvSource for LiveRecvSource {
    async fn next_batch(&mut self) -> crate::error::DaemonResult<Vec<ReceivedAction>> {
        loop {
            match self.rx.recv().await {
                Some(Ok(batch)) if !batch.is_empty() => return Ok(batch),
                Some(Ok(_)) => {} // empty list = coordinator heartbeat
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(crate::error::DaemonError::ChannelClosed("kuc recv thread exited"))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Enum dispatch (used by the binary to avoid boxing)
// ---------------------------------------------------------------------------

/// Untagged enum so the binary can hold either source without boxing.
/// `RecvSource` uses RPITIT (not object-safe), so boxing requires
/// refactoring the trait; the enum is cheaper.
pub enum AnyRecvSource {
    /// In-memory mock driven by [`MockCopytool::enqueue`].
    Mock(MockRecvSource),
    /// Live Lustre KUC source via [`LiveRecvSource`].
    Live(LiveRecvSource),
}

impl AnyRecvSource {
    /// Return the [`CompletionTx`] for live sources; `None` for mock sources
    /// (mock completions are no-ops at the Lustre layer).
    pub fn completion_tx(&self) -> Option<CompletionTx> {
        match self {
            Self::Live(l) => Some(l.completion_tx.clone()),
            Self::Mock(_) => None,
        }
    }
}

impl RecvSource for AnyRecvSource {
    async fn next_batch(&mut self) -> crate::error::DaemonResult<Vec<ReceivedAction>> {
        match self {
            Self::Mock(m) => m.next_batch().await,
            Self::Live(l) => l.next_batch().await,
        }
    }
}
