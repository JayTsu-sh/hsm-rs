//! Actual Curvine SDK transport and a non-destructive current-file copy bridge.
//! The copy helper does not claim a fixed `DataView` or grant HSM Release.

use std::io::Write;

use curvine_client_core::file::CurvineFileSystem;
use curvine_fs_api::{Path, Reader};
use curvine_proto::HsmControlRequest;
use hsm_core::{DataEndpoint, DataMover, HsmError, HsmResult, OperationContext, TransferEvidence};
use hsm_local::LocalDataMover;

use crate::ControlTransport;

/// Native Master RPC transport, sharing the Curvine SDK's leader connector.
#[derive(Clone)]
pub struct NativeControl {
    fs: CurvineFileSystem,
}

impl NativeControl {
    /// Use the configured native Curvine client, without a POSIX/FUSE gateway.
    pub const fn new(fs: CurvineFileSystem) -> Self {
        Self { fs }
    }
}

impl ControlTransport for NativeControl {
    async fn exchange(&self, payload: Vec<u8>) -> HsmResult<Vec<u8>> {
        let response = self
            .fs
            .hsm_control(HsmControlRequest {
                protocol_version: 1,
                payload,
            })
            .await
            .map_err(|e| HsmError::Unknown(format!("native Curvine control RPC: {e}")))?;
        if response.protocol_version != 1 {
            return Err(HsmError::Unsupported(
                "incompatible Curvine HSM protocol".into(),
            ));
        }
        Ok(response.payload)
    }
}

/// Bounded native SDK I/O with a private local spool for the existing data-mover
/// Local role. This is the initial data-plane integration, with one extra copy.
#[derive(Clone)]
pub struct CurvineIo {
    fs: CurvineFileSystem,
}

impl CurvineIo {
    /// Build a native read bridge. HSM capability discovery is independent.
    pub const fn new(fs: CurvineFileSystem) -> Self {
        Self { fs }
    }

    /// Copy a current completed file to a local destination via real data-mover.
    /// This is deliberately NOT a `ManagedStorage` `DataView`: metadata observations
    /// can detect some changes but cannot prove immutability under concurrent I/O.
    /// No anchor, Release, ordinary free/delete or Recall publication is performed.
    pub async fn copy_current_file_to_local(
        &self,
        ctx: &OperationContext,
        path: &Path,
        destination: &DataEndpoint,
    ) -> HsmResult<TransferEvidence> {
        ctx.validate()?;
        if self.fs.conf().client.short_circuit {
            return Err(HsmError::Unsupported(
                "disable short_circuit for the native copy bridge".into(),
            ));
        }
        let initial = self.fs.get_status(path).await.map_err(|e| io_error(&e))?;
        if initial.is_dir || !initial.is_complete || initial.id <= 0 || initial.len < 0 {
            return Err(HsmError::Invalid(
                "copy requires a completed native regular file".into(),
            ));
        }
        let spool = tempfile::tempdir()?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(spool.path().join("data"))?;
        let mut reader = self.fs.open(path).await.map_err(|e| io_error(&e))?;
        if reader.status().id != initial.id || reader.len() != initial.len {
            let _ = reader.complete().await;
            return Err(HsmError::Conflict(
                "Curvine source changed before opening".into(),
            ));
        }
        let mut hash = blake3::Hasher::new();
        let mut length = 0u64;
        let read_result: HsmResult<()> = async {
            let mut buffer = vec![0; 256 * 1024];
            loop {
                let count = reader.read(&mut buffer).await.map_err(|e| io_error(&e))?;
                if count == 0 {
                    break;
                }
                file.write_all(&buffer[..count])?;
                hash.update(&buffer[..count]);
                length += count as u64;
                if length > initial.len as u64 {
                    return Err(HsmError::Conflict(
                        "Curvine source grew while reading".into(),
                    ));
                }
            }
            file.sync_all()?;
            Ok(())
        }
        .await;
        let close_result = reader.complete().await.map_err(|e| io_error(&e));
        read_result?;
        close_result?;
        let current = self.fs.get_status(path).await.map_err(|e| io_error(&e))?;
        if length != initial.len as u64
            || current.id != initial.id
            || current.len != initial.len
            || current.mtime != initial.mtime
        {
            return Err(HsmError::Conflict(
                "Curvine current file changed during copy".into(),
            ));
        }
        let source = LocalDataMover::payload_endpoint(spool.path())?;
        let result = LocalDataMover.copy(ctx, &source, destination).await?;
        if result.length != length
            || result.digest.algorithm != "blake3"
            || result.digest.value.as_slice() != hash.finalize().as_bytes()
        {
            return Err(HsmError::Integrity(
                "native read and local transfer evidence differ".into(),
            ));
        }
        Ok(result)
    }
}

fn io_error(error: &curvine_error::FsError) -> HsmError {
    HsmError::Unknown(format!("native Curvine I/O: {error}"))
}
