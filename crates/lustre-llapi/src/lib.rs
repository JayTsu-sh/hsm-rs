//! Safe wrapper over the liblustreapi HSM surface.
//!
//! Two implementations live here:
//!
//! - [`LiveCopytool`](live::LiveCopytool) — Linux-only, FFI to liblustreapi.
//!   Used by the daemon in production.
//! - [`MockCopytool`](mock::MockCopytool) — pure Rust, no Lustre. Used by
//!   the daemon's unit tests and by anyone exercising the higher-level
//!   scheduling / store / plugin code without standing up a cluster.
//!
//! Both expose the same method shapes (`recv` / `begin` / `progress` /
//! `end`) so higher layers can carry an `enum Copytool { Live(_), Mock(_) }`
//! and dispatch at call sites. We deliberately do **not** define a unifying
//! trait yet — the daemon is single-binary, an enum is fine, and a trait
//! would force HRTBs / lifetimes onto every consumer.
//!
//! The [`ActionHandle`](action::ActionHandle) RAII wrapper sits on top of
//! the lower-level `begin/end` calls so callers get compile-time guarantees
//! that every begun action is ended exactly once.

#![warn(missing_docs)]

pub mod action;
pub mod error;
pub mod mock;

#[cfg(target_os = "linux")]
pub mod live;

pub use action::{ActionHandle, EndStatus, ReceivedAction};
pub use error::{HsmError, Result};
pub use mock::MockCopytool;

#[cfg(target_os = "linux")]
pub use live::LiveCopytool;
