//! SDK for writing hsm-rs data-mover plugins.
//!
//! Plugin authors implement [`Mover`] for their backend (terrasync,
//! POSIX, custom) and the SDK takes care of:
//!
//! - The connection to `hsmd` (UDS gRPC, wired in M2d)
//! - The action stream + per-action task spawning
//! - Throttled progress reporting (so a chunk-loop calling `advance()`
//!   on every 4 MiB block doesn't translate into a 4 MiB-rate of
//!   `llapi_hsm_action_progress` calls into the kernel)
//! - Cancellation propagation (from the daemon → mover via
//!   [`tokio_util::sync::CancellationToken`])
//!
//! M2c ships the **trait + helpers** so that plugins can be written and
//! unit-tested in isolation. The `run()` entry point that wires gRPC
//! lands in M2d once `hsmd` exists to talk to.
//!
//! See `docs/DESIGN.md` §10 (terrasync mover sketch) for what a real
//! implementation looks like.

#![warn(missing_docs)]

pub mod ctx;
pub mod error;
pub mod mover;
pub mod progress;

pub use ctx::{ActionCtx, ActionCtxBuilder};
pub use error::{MoverError, MoverResult};
pub use mover::Mover;
pub use progress::{ProgressConfig, ProgressEvent, ProgressReporter};

// Re-export types plugins frequently need from hsm-core so they only
// need to add `hsm-plugin-sdk` to their Cargo.toml.
pub use hsm_core::{ActionKind, ArchiveId, BackendObject, Cookie, Extent, Fid};

// And the cancellation token type — same here.
pub use tokio_util::sync::CancellationToken;
