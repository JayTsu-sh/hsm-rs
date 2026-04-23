//! Raw FFI bindings to the Lustre HSM ABI + liblustreapi.
//!
//! This crate is **deliberately** unsafe — `bindgen`-generated code makes
//! no Rust safety guarantees. Higher-level safe wrappers live in
//! `lustre-llapi` (added in M2). Outside this crate, nothing in hsm-rs
//! should `use lustre_sys::*` directly without going through a wrapper.
//!
//! Linux-only: on other targets `bindings.rs` is empty.

#![allow(unsafe_code)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unsafe_op_in_unsafe_fn)]
#![allow(clippy::all, clippy::pedantic)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: a few HSM constants are present and have the documented
    /// values from `lustre/include/uapi/linux/lustre/lustre_user.h`.
    /// This is the minimum proof that bindgen actually saw the headers.
    #[cfg(target_os = "linux")]
    #[test]
    fn hsm_constants_have_known_values() {
        // bindgen `EnumVariation::ModuleConsts` namespaces enum members
        // under their enum's module name.
        assert_eq!(hsm_user_action::HUA_NONE, 1);
        assert_eq!(hsm_user_action::HUA_ARCHIVE, 10);
        assert_eq!(hsm_user_action::HUA_RESTORE, 11);
        assert_eq!(hsm_user_action::HUA_RELEASE, 12);
        assert_eq!(hsm_user_action::HUA_REMOVE, 13);
        assert_eq!(hsm_user_action::HUA_CANCEL, 14);

        assert_eq!(hsm_copytool_action::HSMA_NONE, 10);
        assert_eq!(hsm_copytool_action::HSMA_ARCHIVE, 20);
        assert_eq!(hsm_copytool_action::HSMA_RESTORE, 21);
        assert_eq!(hsm_copytool_action::HSMA_REMOVE, 22);
        assert_eq!(hsm_copytool_action::HSMA_CANCEL, 23);
    }

    /// `hsm_action_item` is a variable-length struct (trailing `hai_data[]`),
    /// but the fixed prefix has a known size. Catching size drift here would
    /// surface a kernel ABI change that needs Rust attention.
    ///
    /// Layout (packed):
    ///   `hai_len`(u32) + `hai_action`(u32) + `hai_fid`(lu_fid=16)
    ///   + `hai_dfid`(16) + `hai_extent`(16) + `hai_cookie`(u64) + `hai_gid`(u64)
    ///   = 4+4+16+16+16+8+8 = 72 bytes
    #[cfg(target_os = "linux")]
    #[test]
    fn hsm_action_item_minimum_size_matches_abi() {
        assert_eq!(std::mem::size_of::<hsm_action_item>(), 72);
    }

    /// `lu_fid` is the Lustre file identifier — small and load-bearing. Its
    /// layout must stay locked to (u64, u32, u32) packed as 16 bytes.
    #[cfg(target_os = "linux")]
    #[test]
    fn lu_fid_size_is_16() {
        assert_eq!(std::mem::size_of::<lu_fid>(), 16);
    }

    /// kuc_hdr is the message envelope written across the kernel-userspace
    /// pipe. Its size is part of the wire protocol — locked at 8 bytes
    /// (2 + 1 + 1 + 2 + 2 packed).
    #[cfg(target_os = "linux")]
    #[test]
    fn kuc_hdr_size_is_8() {
        assert_eq!(std::mem::size_of::<kuc_hdr>(), 8);
    }
}
