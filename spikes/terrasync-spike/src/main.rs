//! terrasync-rs API spike for hsm-rs.
//!
//! Validates the small surface of `storage_v2` we plan to drive from
//! `hsm-plugin-terrasync`:
//!
//!   1. `create_storage(path)` — construct backends from URL/path
//!   2. `get_metadata` + `EntryEnum` — describe an object
//!   3. `copy_file(...)` with `QosManager` + integrity flag + bytes counter
//!      — the archive/restore data path
//!   4. `compute_hash(path, size)` — BLAKE3 verification used as
//!      `trusted.lhsm_hash`
//!   5. `delete_file(entry)` — the remove path
//!   6. `CancellationToken` + chunked QoS acquire — proves we can build
//!      a cancel-aware mover even though terrasync has no per-transfer
//!      cancel today
//!
//! Run with:
//!     cargo run --manifest-path spikes/terrasync-spike/Cargo.toml
//!
//! Cleans up `/tmp/hsm-spike-{src,dst}` on every run.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use storage_v2::{QosManager, StorageEnum, create_storage};
use tokio_util::sync::CancellationToken;

const SRC_DIR: &str = "/tmp/hsm-spike-src";
const DST_DIR: &str = "/tmp/hsm-spike-dst";
const BLOB_NAME: &str = "blob.bin";
const BLOB_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
const BANDWIDTH: &str = "8MiB/s";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    println!("[spike] === hsm-rs / terrasync-rs API validation ===\n");

    println!("[1/7] reset workdirs and write a {} MiB source blob", BLOB_SIZE / 1024 / 1024);
    reset_dirs().await?;
    write_blob(&format!("{SRC_DIR}/{BLOB_NAME}"), BLOB_SIZE).await?;

    println!("\n[2/7] create_storage on local roots");
    let src = create_storage(SRC_DIR, None)
        .await
        .context("create src storage")?;
    let dst = create_storage(DST_DIR, None)
        .await
        .context("create dst storage")?;
    println!("      src.block_size = {} bytes", src.block_size());
    println!("      dst.block_size = {} bytes", dst.block_size());

    println!("\n[3/7] get_metadata on source object");
    let entry = src
        .get_metadata(Path::new(BLOB_NAME))
        .await
        .context("get source metadata")?;
    let total = entry.get_size();
    println!("      entry size = {total} bytes ({} MiB)", total / 1024 / 1024);

    println!("\n[4/7] build QosManager (bandwidth = {BANDWIDTH})");
    let qos = QosManager::try_new(Some(BANDWIDTH), 1.0, None).context("build qos")?;

    println!("\n[5/7] copy_file with QoS + BLAKE3 integrity + live byte counter");
    let counter = Arc::new(AtomicU64::new(0));
    let stop_progress = Arc::new(AtomicBool::new(false));
    spawn_progress_printer(counter.clone(), total, stop_progress.clone());

    let t0 = Instant::now();
    StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        Some(qos),
        true, // enable_integrity_check (BLAKE3 internally)
        true, // is_source_reserved (don't delete src after copy)
        Some(counter.clone()),
    )
    .await
    .context("copy_file")?;
    let elapsed = t0.elapsed();
    stop_progress.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(50)).await; // let printer flush

    let copied = counter.load(Ordering::Relaxed);
    let mibps = (copied as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "      copy done: {copied} bytes in {:.2}s ({mibps:.2} MiB/s, expect ≈ 8 MiB/s)",
        elapsed.as_secs_f64()
    );
    ensure!(copied == total as u64, "byte counter mismatch");

    println!("\n[6/7] BLAKE3 verification (src vs dst — feeds trusted.lhsm_hash)");
    let src_hash = src
        .compute_hash(Path::new(BLOB_NAME), total)
        .await
        .context("compute src hash")?;
    let dst_hash = dst
        .compute_hash(Path::new(BLOB_NAME), total)
        .await
        .context("compute dst hash")?;
    println!("      src BLAKE3 = {src_hash}");
    println!("      dst BLAKE3 = {dst_hash}");
    ensure!(src_hash == dst_hash, "hash mismatch — copy_file corrupted data");

    println!("\n[7/7] delete_file on dst (remove path) + cancel-aware chunk loop");
    let dst_entry = dst.get_metadata(Path::new(BLOB_NAME)).await?;
    dst.delete_file(&dst_entry).await.context("delete dst")?;
    ensure!(
        dst.get_metadata(Path::new(BLOB_NAME)).await.is_err(),
        "dst object still present after delete"
    );
    println!("      delete ok");

    cancel_simulation().await?;

    println!("\n[spike] all checks passed ✓");
    println!("[spike] terrasync-rs storage_v2 is suitable for hsm-plugin-terrasync");
    Ok(())
}

async fn reset_dirs() -> Result<()> {
    for d in [SRC_DIR, DST_DIR] {
        let _ = tokio::fs::remove_dir_all(d).await;
        tokio::fs::create_dir_all(d).await.with_context(|| format!("mkdir {d}"))?;
    }
    Ok(())
}

async fn write_blob(path: &str, size: usize) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::File::create(path).await?;
    let buf = vec![0xABu8; 64 * 1024];
    let mut written = 0;
    while written < size {
        let n = (size - written).min(buf.len());
        f.write_all(&buf[..n]).await?;
        written += n;
    }
    f.flush().await?;
    println!("      wrote {written} bytes to {path}");
    Ok(())
}

fn spawn_progress_printer(counter: Arc<AtomicU64>, total: u64, stop: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let start = Instant::now();
        loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let v = counter.load(Ordering::Relaxed);
            let pct = if total > 0 { v * 100 / total } else { 0 };
            let elapsed = start.elapsed().as_secs_f64();
            let mibps = if elapsed > 0.0 {
                (v as f64) / elapsed / (1024.0 * 1024.0)
            } else {
                0.0
            };
            println!("      [progress] {v:>9} / {total} bytes ({pct:>3}%, {mibps:.2} MiB/s)");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

/// Models the chunk loop we plan to write inside `TerrasyncMover::archive`:
/// QoS-gated, cancel-checked between chunks. This is the workaround for
/// terrasync not exposing per-transfer cancellation today.
async fn cancel_simulation() -> Result<()> {
    println!("\n      --- cancel-aware chunk loop simulation ---");
    let token = CancellationToken::new();
    let token_for_canceller = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        println!("      [cancel] firing token");
        token_for_canceller.cancel();
    });

    let qos = QosManager::try_new(Some("4MiB/s"), 1.0, None)?;
    let chunk_size: u64 = 1024 * 1024; // 1 MiB
    let mut offset: u64 = 0;
    let max_chunks = 16;

    let started = Instant::now();
    for i in 0..max_chunks {
        if token.is_cancelled() {
            println!(
                "      [cancel] observed at chunk {i}, offset {offset} after {:.0} ms — loop exit",
                started.elapsed().as_millis()
            );
            ensure!(i < max_chunks, "cancel did not fire");
            return Ok(());
        }
        qos.acquire_bandwidth(chunk_size).await;
        offset += chunk_size;
    }
    anyhow::bail!("cancel token did not fire within {max_chunks} chunks (unexpected)");
}
