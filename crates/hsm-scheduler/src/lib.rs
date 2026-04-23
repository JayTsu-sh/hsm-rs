//! Action scheduling for hsm-rs.
//!
//! This crate is **not** a generic job scheduler (cron-like) — it's a
//! task router. The HSM problem is:
//!
//! 1. Actions arrive from the kernel coordinator (push, not poll).
//! 2. Every action targets a specific `archive_id`.
//! 3. Each agent (data-mover plugin) advertises which `archive_id`s it
//!    can serve.
//! 4. Routing rules layer on top: by default FIFO per kind, but a user
//!    can add host-affinity (`tag=n0 → [moverA, moverB]`) or other
//!    strategies (M4: consistent hashing, batching by hint).
//! 5. Agents may disconnect; their in-flight actions need to be
//!    re-dispatched after a grace period (handled by the daemon, not
//!    this crate — the scheduler just exposes hooks).
//!
//! See `docs/DESIGN.md` §7 for the full strategy chain and §8 for how
//! this composes with the action store.

#![warn(missing_docs)]

pub mod agent;
pub mod fifo;
pub mod scheduler;

pub use agent::{AgentInfo, AgentRegistry};
pub use fifo::FifoPerKind;
pub use scheduler::{Assignment, Scheduler};
