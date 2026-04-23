//! hsm-rs daemon orchestration.
//!
//! Wires the M1–M2c crates together:
//!
//! ```text
//!   RecvSource (MockCopytool / LiveCopytool wrapper)
//!         │ recv batches
//!         ▼
//!   ActionStore.insert        ──┐
//!         │                     │
//!         ▼                     │
//!   Scheduler.enqueue           │  per-action
//!         │                     │  state
//!         ▼ (tick)              │
//!   Scheduler.pick_ready ───────┤
//!         │ Assignment           │
//!         ▼                     │
//!   AgentConn.dispatch ─────────┘
//!         │
//!         ▼ Mover runs in agent process / task
//!   AgentConn.status_rx
//!         │ ActionStatus
//!         ▼
//!   ActionStore.transition + Copytool.end
//! ```
//!
//! M2d.1 keeps everything in-process (`InProcessAgent` runs the Mover on
//! a tokio task). M2d.2 will swap that for `GrpcAgent` over UDS without
//! changing the daemon's main loop.

#![warn(missing_docs)]

pub mod agent;
pub mod copytool_recv;
pub mod daemon;
pub mod error;
pub mod inproc;

pub use agent::{AgentConn, AgentSink, DispatchedAction};
pub use copytool_recv::{MockRecvSource, RecvSource};
pub use daemon::{Daemon, DaemonConfig, DaemonHandle};
pub use error::{DaemonError, DaemonResult};
pub use inproc::InProcessAgent;
