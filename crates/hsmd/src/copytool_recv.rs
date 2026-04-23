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

use lustre_llapi::{MockCopytool, ReceivedAction};
use parking_lot::Mutex;

use crate::error::DaemonResult;

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
