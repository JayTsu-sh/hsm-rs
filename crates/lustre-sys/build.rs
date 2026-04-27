//! Generate Rust FFI bindings for the Lustre HSM subset of the userspace
//! ABI + liblustreapi. Restricted to symbols hsm-rs actually uses, so the
//! generated `bindings.rs` stays small (~1000 lines vs ~50_000 for the full
//! header tree).

use std::env;
use std::path::PathBuf;

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "linux" {
        // Lustre only ships on Linux; emit empty bindings to keep
        // documentation builds (e.g. docs.rs on macOS) functional.
        let out = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("bindings.rs");
        std::fs::write(
            &out,
            b"// Lustre is Linux-only; bindings are empty on this target.\n",
        )
        .unwrap();
        return;
    }

    // Tell cargo to re-run if the wrapper or any included header changes.
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=build.rs");

    // Link against the system liblustreapi.
    println!("cargo:rustc-link-lib=dylib=lustreapi");

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg("-D_GNU_SOURCE")
        .layout_tests(false)
        // Allowlist: just the HSM-relevant ABI + functions.
        // Structs / enums
        .allowlist_type("hsm_action_item")
        .allowlist_type("hsm_action_list")
        .allowlist_type("hsm_progress")
        .allowlist_type("hsm_progress_kernel")
        .allowlist_type("hsm_user_state")
        .allowlist_type("hsm_user_request")
        .allowlist_type("hsm_user_item")
        .allowlist_type("hsm_user_import")
        .allowlist_type("hsm_extent")
        .allowlist_type("hsm_copy")
        .allowlist_type("hsm_current_action")
        .allowlist_type("hsm_states")
        .allowlist_type("hsm_progress_states")
        .allowlist_type("hsm_user_action")
        .allowlist_type("hsm_copytool_action")
        .allowlist_type("hsm_message_type")
        .allowlist_type("hsm_state_set")
        .allowlist_type("hsm_copytool_private")
        .allowlist_type("hsm_copyaction_private")
        .allowlist_type("kuc_hdr")
        .allowlist_type("lustre_kernelcomm")
        .allowlist_type("lu_fid")
        // Functions (liblustreapi userspace HSM surface)
        .allowlist_function("llapi_hsm_copytool_register")
        .allowlist_function("llapi_hsm_copytool_unregister")
        .allowlist_function("llapi_hsm_copytool_get_fd")
        .allowlist_function("llapi_hsm_copytool_recv")
        .allowlist_function("llapi_hsm_action_begin")
        .allowlist_function("llapi_hsm_action_end")
        .allowlist_function("llapi_hsm_action_progress")
        .allowlist_function("llapi_hsm_action_get_dfid")
        .allowlist_function("llapi_hsm_action_get_fd")
        .allowlist_function("llapi_hsm_state_get")
        .allowlist_function("llapi_hsm_state_set")
        .allowlist_function("llapi_hsm_state_get_fd")
        .allowlist_function("llapi_hsm_state_set_fd")
        .allowlist_function("llapi_hsm_current_action")
        .allowlist_function("llapi_hsm_request")
        .allowlist_function("llapi_hsm_user_request_alloc")
        .allowlist_function("llapi_hsm_register_event_fifo")
        .allowlist_function("llapi_hsm_unregister_event_fifo")
        .allowlist_function("llapi_hsm_import")
        // FID ↔ path resolution (needed for shadow namespace in TerrasyncMover).
        .allowlist_function("llapi_fid2path")
        // Constants / vars matching ABI surface.
        .allowlist_var("HSMA_.*")
        .allowlist_var("HUA_.*")
        .allowlist_var("HPS_.*")
        .allowlist_var("HS_.*")
        .allowlist_var("HSM_.*")
        .allowlist_var("HMT_.*")
        .allowlist_var("HUC_.*")
        .allowlist_var("KUC_.*")
        .allowlist_var("HP_FLAG_.*")
        .allowlist_var("HSMS_.*")
        // C enums map naturally to Rust constants we'll re-expose.
        .default_enum_style(bindgen::EnumVariation::ModuleConsts)
        // We manage debug derives ourselves in lustre-hsm-uapi.
        .derive_default(true)
        .derive_debug(false)
        .generate()
        .expect("bindgen failed for Lustre HSM wrapper");

    let out_path = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("bindings.rs");
    bindings
        .write_to_file(&out_path)
        .expect("write bindings.rs");
}
