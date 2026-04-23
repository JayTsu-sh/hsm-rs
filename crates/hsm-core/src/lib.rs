//! Domain types for hsm-rs.
//!
//! This crate has **no Lustre dependency** — it can be built and tested on any
//! platform. The kernel-facing FFI lives in `lustre-sys` / `lustre-hsm-uapi`;
//! this crate consumes those via conversion functions in `lustre-llapi`.
//!
//! All types here mirror the *semantics* of the Lustre HSM ABI but are
//! deliberately not `#[repr(C)]` — they are the application-level model,
//! free to evolve independently of the wire format.

#![warn(missing_docs)]

pub mod action;
pub mod backend;
pub mod errno;
pub mod fid;
pub mod xattr;

pub use action::{Action, ActionKind, AgentId, ArState, ActionRecord, Cookie, Extent};
pub use backend::{ArchiveId, BackendObject};
pub use errno::HsmErrno;
pub use fid::Fid;
