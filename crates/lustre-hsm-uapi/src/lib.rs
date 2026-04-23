//! Safe(-ish) interface over the Lustre HSM ABI exposed by `lustre-sys`.
//!
//! This crate is the boundary between raw bindgen output (`lustre-sys`) and
//! the rest of hsm-rs:
//!
//! - **Re-exports** the small set of ABI constants the daemon actually
//!   reads / writes (HSMA_*, HUA_*, HPS_*, HS_*) so callers don't have to
//!   thread the bindgen `EnumVariation::ModuleConsts` namespacing.
//! - **Conversions** between the wire types in `lustre-sys` and the
//!   application-level types in `hsm-core` (`Fid`, `ActionKind`,
//!   `Cookie`, `Extent`).
//!
//! On non-Linux targets the constants are absent (Lustre is Linux-only)
//! but the conversion module compiles so docs build everywhere.

#![warn(missing_docs)]

#[cfg(target_os = "linux")]
pub mod consts;

#[cfg(target_os = "linux")]
pub mod conv;

#[cfg(target_os = "linux")]
pub use consts::*;
