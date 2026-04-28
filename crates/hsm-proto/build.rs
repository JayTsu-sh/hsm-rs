//! Compile the gRPC `.proto` files into Rust modules.
//!
//! Uses a vendored `protoc` binary (via `protoc-bin-vendored`) so the build
//! works on hosts without a system protobuf compiler.

// std::env::set_var is unsafe in 2024 edition; build scripts are
// single-threaded so this is safe in this context.
#![allow(unsafe_code)]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // tonic-build (0.12) reads the PROTOC env var.
    // SAFETY: build scripts are single-threaded so set_var is fine here.
    unsafe { std::env::set_var("PROTOC", protoc) };

    println!("cargo:rerun-if-changed=proto/hsm_v1.proto");
    println!("cargo:rerun-if-changed=build.rs");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // Serde derives on the leaf message types — handy for log
        // serialization + the M4 coordinatool-compatible TCP/JSON
        // protocol. We deliberately don't apply this universally:
        // the generated `oneof` enums for FromPlugin/ToPlugin would
        // otherwise pull in serde paths through a derive that
        // doesn't fit prost's representation.
        .message_attribute(
            "hsm.v1.BackendObject",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .message_attribute(
            "hsm.v1.ActionItem",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .message_attribute(
            "hsm.v1.ActionStatus",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(&["proto/hsm_v1.proto"], &["proto"])?;

    Ok(())
}
