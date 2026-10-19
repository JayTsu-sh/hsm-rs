//! Portable HSM identities and protected storage, backend and executor Interfaces.
//!
//! The external engine coordinates Archive / Release / Recall. Storage owns
//! authoritative metadata and atomic data effects. No filesystem-specific ABI
//! or kernel coordinator types are part of this crate.

#![warn(missing_docs)]

pub mod managed;
pub use managed::*;
