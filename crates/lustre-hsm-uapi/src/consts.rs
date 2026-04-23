//! Friendly re-exports of HSM ABI constants from `lustre-sys`.
//!
//! `lustre-sys` uses bindgen's `EnumVariation::ModuleConsts` so each enum's
//! members live under their own module path
//! (e.g. `lustre_sys::hsm_copytool_action::HSMA_ARCHIVE`). This module
//! flattens that into a single namespace and pins each value with a
//! `const` cast to a stable Rust integer type, decoupling the rest of
//! hsm-rs from bindgen's choices.

#![allow(missing_docs)] // ABI mirror — values documented in lustre_user.h

use lustre_sys as sys;

// ---- enum hsm_user_action -----------------------------------------------
pub const HUA_NONE: u32 = sys::hsm_user_action::HUA_NONE;
pub const HUA_ARCHIVE: u32 = sys::hsm_user_action::HUA_ARCHIVE;
pub const HUA_RESTORE: u32 = sys::hsm_user_action::HUA_RESTORE;
pub const HUA_RELEASE: u32 = sys::hsm_user_action::HUA_RELEASE;
pub const HUA_REMOVE: u32 = sys::hsm_user_action::HUA_REMOVE;
pub const HUA_CANCEL: u32 = sys::hsm_user_action::HUA_CANCEL;

// ---- enum hsm_copytool_action -------------------------------------------
pub const HSMA_NONE: u32 = sys::hsm_copytool_action::HSMA_NONE;
pub const HSMA_ARCHIVE: u32 = sys::hsm_copytool_action::HSMA_ARCHIVE;
pub const HSMA_RESTORE: u32 = sys::hsm_copytool_action::HSMA_RESTORE;
pub const HSMA_REMOVE: u32 = sys::hsm_copytool_action::HSMA_REMOVE;
pub const HSMA_CANCEL: u32 = sys::hsm_copytool_action::HSMA_CANCEL;

// ---- enum hsm_progress_states -------------------------------------------
pub const HPS_NONE: u32 = sys::hsm_progress_states::HPS_NONE;
pub const HPS_WAITING: u32 = sys::hsm_progress_states::HPS_WAITING;
pub const HPS_RUNNING: u32 = sys::hsm_progress_states::HPS_RUNNING;
pub const HPS_DONE: u32 = sys::hsm_progress_states::HPS_DONE;

// ---- KUC magic / transport ----------------------------------------------
pub const KUC_MAGIC: u16 = sys::KUC_MAGIC as u16;

#[cfg(test)]
mod tests {
    use super::*;

    /// The values are the externally-visible kernel ABI. If a Lustre upgrade
    /// silently changes them, bindgen will pick up the new value and this
    /// test will fail — alerting us before downstream conversions corrupt
    /// real actions.
    #[test]
    fn abi_constant_pin() {
        assert_eq!(HUA_NONE, 1);
        assert_eq!(HUA_ARCHIVE, 10);
        assert_eq!(HUA_RESTORE, 11);
        assert_eq!(HUA_RELEASE, 12);
        assert_eq!(HUA_REMOVE, 13);
        assert_eq!(HUA_CANCEL, 14);

        assert_eq!(HSMA_NONE, 10);
        assert_eq!(HSMA_ARCHIVE, 20);
        assert_eq!(HSMA_RESTORE, 21);
        assert_eq!(HSMA_REMOVE, 22);
        assert_eq!(HSMA_CANCEL, 23);

        assert_eq!(HPS_NONE, 0);
        assert_eq!(HPS_WAITING, 1);
        assert_eq!(HPS_RUNNING, 2);
        assert_eq!(HPS_DONE, 3);

        // KUC magic is "Lustre's KUC" sentinel (`0x191C`).
        assert_eq!(KUC_MAGIC, 0x191C);
    }
}
