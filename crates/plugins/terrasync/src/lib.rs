//! `hsm-plugin-terrasync` — reference Mover backed by terrasync-rs's
//! `storage_v2` API.
//!
//! M2e ships the **file://** path; M3 will extend the same crate to
//! S3 / NFS / CIFS by swapping the `StorageEnum` constructor and (per
//! M3.5) using the streaming `copy_file_with_cancel` API once it lands
//! upstream.
//!
//! ## Layout (file://)
//!
//! ```text
//!   primary file: /mnt/lustre/path/to/file
//!                            │
//!                            ▼ archive
//!   backend file: <archive_root>/<archive_id>/<fid>
//!   backend URL:  file://<archive_root>/<archive_id>/<fid>
//! ```
//!
//! - `<archive_root>` comes from the plugin's TOML config (e.g.
//!   `/var/hsm-archive`). The archive root is the LocalStorage `root`
//!   the plugin owns; the per-action subpath is `<archive_id>/<fid>`.
//! - `<fid>` is rendered as `0xseq:0xoid:0xver` (matches kernel
//!   `LPX64i:LPX64i:LPX32i`).
//!
//! ## What lands in `BackendObject`
//!
//! - `uuid`: the same `<fid>` string we use as the relative path.
//!   Stable across archives of the same file.
//! - `hash`: BLAKE3-32 of the bytes we wrote. Restore re-hashes the
//!   bytes it pulled and bails with `MoverError::Integrity` on
//!   mismatch.
//! - `url`: `file://<archive_root>/<archive_id>/<fid>` — fully
//!   self-contained so a future Restore / Remove can act without
//!   re-reading config.
//!
//! ## Known M2e limitations
//!
//! - Whole-file in-memory: `read_file_from` returns `Bytes`. Acceptable
//!   for M2e (small files, dev environment).
//! - Cancel granularity: token is checked **before** read and **before**
//!   write but mid-IO is not interruptible. terrasync's
//!   `copy_file_with_cancel` is merged (good for symmetric copies) but
//!   it requires `entry.relative_path` to be the same on source and
//!   destination — HSM stores at `<archive_root>/<archive_id>/<fid>`,
//!   which never matches the source's Lustre path. Follow-up upstream
//!   work: a cancel-aware `read_file_from` / `write_file_from_bytes`
//!   pair (or `copy_file_with_remap(src_path, dst_path, …)`).
//! - No QoS / no integrity-check piggyback through terrasync: we hash
//!   the in-memory `Bytes` ourselves with `blake3` so the implementation
//!   is identical to whatever terrasync's `HashCalculator` would
//!   produce for the same bytes.

#![warn(missing_docs)]

pub mod config;
pub mod mover;

pub use config::{ArchiveLayout, BackendScheme, BackendUrl, BackendUrlError};
pub use mover::TerrasyncMover;
