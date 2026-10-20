//! Native immutable managed files and protected Worker data transfer.
use crate::{ControlTransport, CurvineStorage, NativeControl};
use curvine_client_core::file::{CurvineFileSystem, hsm_io::hsm_worker_rpc};
use curvine_model::hsm::native::{CHUNK_BYTES, Endpoint, WorkerReply, WorkerRequest};
use hsm_core::{
    ContentDigest, DataEndpoint, DataKey, DataMover, DataView, EntityKey, EntityState, HsmError,
    HsmResult, OperationContext, RecallTarget, TransferEvidence,
};
use hsm_local::LocalDataMover;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    path::Path,
};

/// Executor for exact Curvine views and private Recall destinations. The current
/// Local bridge uses a private full-file spool and actual data-mover-rs copying.
#[derive(Clone)]
pub struct CurvineDataMover {
    fs: CurvineFileSystem,
}
impl CurvineDataMover {
    pub const fn new(fs: CurvineFileSystem) -> Self {
        Self { fs }
    }
    async fn download(&self, source: &DataEndpoint, root: &Path) -> HsmResult<TransferEvidence> {
        let endpoint: Endpoint = serde_json::from_slice(&source.capability)
            .map_err(|e| HsmError::Invalid(e.to_string()))?;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(root.join("data"))?;
        let mut length = 0;
        let mut hash = blake3::Hasher::new();
        loop {
            let response = hsm_worker_rpc(
                self.fs.rpc_conf(),
                &endpoint.worker,
                &WorkerRequest::Read {
                    endpoint: endpoint.clone(),
                    offset: length,
                    length: CHUNK_BYTES,
                },
            )
            .await
            .map_err(native_error)?;
            let WorkerReply::Bytes(bytes) = response else {
                return Err(HsmError::Unknown("unexpected Worker read reply".into()));
            };
            if bytes.is_empty() {
                break;
            }
            if bytes.len() > CHUNK_BYTES {
                return Err(HsmError::Integrity("oversized native data chunk".into()));
            }
            file.write_all(&bytes)?;
            hash.update(&bytes);
            length += bytes.len() as u64;
        }
        file.sync_all()?;
        Ok(TransferEvidence {
            length,
            digest: ContentDigest {
                algorithm: "blake3".into(),
                value: hash.finalize().as_bytes().to_vec(),
            },
        })
    }
    async fn upload(
        &self,
        destination: &DataEndpoint,
        root: &Path,
        expected: &TransferEvidence,
    ) -> HsmResult<TransferEvidence> {
        let endpoint: Endpoint = serde_json::from_slice(&destination.capability)
            .map_err(|e| HsmError::Invalid(e.to_string()))?;
        let mut file = std::fs::File::open(root.join("data"))?;
        let mut offset = 0;
        let mut buffer = vec![0; CHUNK_BYTES];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            match hsm_worker_rpc(
                self.fs.rpc_conf(),
                &endpoint.worker,
                &WorkerRequest::Write {
                    endpoint: endpoint.clone(),
                    offset,
                    bytes: buffer[..count].to_vec(),
                },
            )
            .await
            .map_err(native_error)?
            {
                WorkerReply::Done => (),
                _ => return Err(HsmError::Unknown("native write not acknowledged".into())),
            }
            offset += count as u64;
        }
        // Empty payloads still need a concrete allocation file for verification.
        if offset == 0 {
            hsm_worker_rpc(
                self.fs.rpc_conf(),
                &endpoint.worker,
                &WorkerRequest::Write {
                    endpoint: endpoint.clone(),
                    offset: 0,
                    bytes: vec![],
                },
            )
            .await
            .map_err(native_error)?;
        }
        match hsm_worker_rpc(
            self.fs.rpc_conf(),
            &endpoint.worker,
            &WorkerRequest::Verify {
                endpoint: endpoint.clone(),
            },
        )
        .await
        .map_err(native_error)?
        {
            WorkerReply::Verified(actual) => {
                let evidence: TransferEvidence = portable(&actual)?;
                if evidence != *expected {
                    return Err(HsmError::Integrity(
                        "private destination differs from transferred content".into(),
                    ));
                }
                Ok(evidence)
            }
            _ => Err(HsmError::Unknown(
                "private verification not acknowledged".into(),
            )),
        }
    }
}
impl DataMover for CurvineDataMover {
    async fn copy(
        &self,
        ctx: &OperationContext,
        source: &DataEndpoint,
        destination: &DataEndpoint,
    ) -> HsmResult<TransferEvidence> {
        ctx.validate()?;
        match (source.adapter.as_str(), destination.adapter.as_str()) {
            ("local-managed-v1", "local-managed-v1") => {
                LocalDataMover.copy(ctx, source, destination).await
            }
            ("curvine-view-v1", "local-managed-v1") => {
                let root = tempfile::tempdir()?;
                let expected = self.download(source, root.path()).await?;
                let local = LocalDataMover::payload_endpoint(root.path())?;
                let actual = LocalDataMover.copy(ctx, &local, destination).await?;
                if actual != expected {
                    return Err(HsmError::Integrity(
                        "local archive differs from exact native view".into(),
                    ));
                }
                Ok(actual)
            }
            ("local-managed-v1", "curvine-recall-v1") => {
                let root = tempfile::tempdir()?;
                let local = LocalDataMover::payload_endpoint(root.path())?;
                let expected = LocalDataMover.copy(ctx, source, &local).await?;
                self.upload(destination, root.path(), &expected).await
            }
            _ => Err(HsmError::Unsupported(
                "executor endpoint combination".into(),
            )),
        }
    }
}
impl CurvineStorage<NativeControl> {
    async fn native_call<T: DeserializeOwned>(&self, request: Value, kind: &str) -> HsmResult<T> {
        let payload = serde_json::to_vec(&request).map_err(|e| HsmError::Invalid(e.to_string()))?;
        if payload.len() > 64 * 1024 {
            return Err(HsmError::Invalid(
                "native control request exceeds limit".into(),
            ));
        }
        let bytes = self.transport.exchange(payload).await?;
        if bytes.len() > 1024 * 1024 {
            return Err(HsmError::Unknown(
                "oversized native control response".into(),
            ));
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|e| HsmError::Unknown(e.to_string()))?;
        if value["kind"] == "error" {
            return Err(crate::remote_error(
                value["data"]["code"].as_str().unwrap_or("unknown"),
                value["data"]["message"]
                    .as_str()
                    .unwrap_or("invalid remote error")
                    .into(),
            ));
        }
        if value["kind"] != kind {
            return Err(HsmError::Unknown(
                "unexpected native control response".into(),
            ));
        }
        serde_json::from_value(value["data"].clone()).map_err(|e| HsmError::Unknown(e.to_string()))
    }
    /// Create-only import into a fresh protected inode. Parent must already exist.
    pub async fn import_file(
        &self,
        ctx: &OperationContext,
        path: &str,
        source: &DataEndpoint,
    ) -> HsmResult<EntityState> {
        ctx.validate()?;
        if !self.native_capabilities().native_records {
            return Err(HsmError::Unsupported("native managed import".into()));
        }
        let root = tempfile::tempdir()?;
        let local = LocalDataMover::payload_endpoint(root.path())?;
        let evidence = LocalDataMover.copy(ctx, source, &local).await?;
        let prior:Option<EntityState>=self.native_call(json!({"op":"completed_import","context":ctx,"path":path,"length":evidence.length,"digest":evidence.digest}),"completed").await?;
        if let Some(prior) = prior {
            crate::validate_state(&prior)?;
            return Ok(prior);
        }
        let target:RecallTarget=self.native_call(json!({"op":"prepare_import","context":ctx,"path":path,"length":evidence.length,"digest":evidence.digest}),"recall").await?;
        if target.layout_id.is_empty() || target.destination.adapter != "curvine-recall-v1" {
            return Err(HsmError::Integrity("invalid private import target".into()));
        }
        CurvineDataMover::new(self.transport.filesystem().clone())
            .upload(&target.destination, root.path(), &evidence)
            .await?;
        let state: EntityState = self
            .native_call(
                json!({"op":"commit_import","context":ctx,"target":target,"evidence":evidence}),
                "entity",
            )
            .await?;
        crate::validate_state(&state)?;
        if state.data != target.data
            || state.length != evidence.length
            || state.digest != evidence.digest
            || state.residency != hsm_core::Residency::Online
        {
            return Err(HsmError::Integrity(
                "native import publication differs from content".into(),
            ));
        }
        Ok(state)
    }
    /// Resolve a native identity without inventing a generation from stat data.
    pub async fn resolve(&self, path: &str) -> HsmResult<EntityState> {
        let state = self
            .native_call(json!({"op":"resolve","path":path}), "entity")
            .await?;
        crate::validate_state(&state)?;
        Ok(state)
    }
    /// Read with a committed whole-file pin; close it after all transfer I/O ends.
    pub async fn read_to_local(
        &self,
        ctx: &OperationContext,
        data: &DataKey,
        destination: &DataEndpoint,
    ) -> HsmResult<TransferEvidence> {
        ctx.validate()?;
        let view: DataView = self
            .native_call(json!({"op":"open_view","context":ctx,"data":data}), "view")
            .await?;
        if view.data != *data || view.source.adapter != "curvine-view-v1" {
            return Err(HsmError::Integrity("invalid exact native read view".into()));
        }
        let result = CurvineDataMover::new(self.transport.filesystem().clone())
            .copy(ctx, &view.source, destination)
            .await;
        let close: HsmResult<EntityState> = self
            .native_call(
                json!({"op":"close_view","context":ctx,"view":view}),
                "entity",
            )
            .await;
        let evidence = result?;
        close?;
        if evidence.length != view.length || evidence.digest != view.digest {
            return Err(HsmError::Integrity(
                "native read differs from pinned content".into(),
            ));
        }
        Ok(evidence)
    }
    /// Close a pinned source after stopping every reader using it.
    pub async fn close_view(
        &self,
        ctx: &OperationContext,
        view: &DataView,
    ) -> HsmResult<EntityState> {
        self.native_call(
            json!({"op":"close_view","context":ctx,"view":view}),
            "entity",
        )
        .await
    }
    /// Recover a private target by workflow ID after coordinator restart.
    pub async fn pending_recall(
        &self,
        ctx: &OperationContext,
        entity: &EntityKey,
    ) -> HsmResult<RecallTarget> {
        self.native_call(
            json!({"op":"pending_recall","context":ctx,"entity":entity}),
            "recall",
        )
        .await
    }
    /// Recover a persistent source pin by workflow ID.
    pub async fn pending_view(
        &self,
        ctx: &OperationContext,
        entity: &EntityKey,
    ) -> HsmResult<DataView> {
        self.native_call(
            json!({"op":"pending_view","context":ctx,"entity":entity}),
            "view",
        )
        .await
    }
    /// Retry physical deletion of committed retired allocations.
    pub async fn collect_garbage(&self, entity: &EntityKey) -> HsmResult<EntityState> {
        self.native_call(json!({"op":"collect_garbage","entity":entity}), "entity")
            .await
    }
    /// Retire an unpublished Recall allocation; stale write capabilities stay fenced.
    pub async fn abort_recall(
        &self,
        ctx: &OperationContext,
        target: &RecallTarget,
    ) -> HsmResult<EntityState> {
        self.native_call(
            json!({"op":"abort_recall","context":ctx,"target":target}),
            "entity",
        )
        .await
    }
    /// Current committed controller qualification. A new namespace starts at 1.
    pub async fn controller_epoch(&self) -> HsmResult<u64> {
        let value: Value = self
            .native_call(json!({"op":"namespace"}), "namespace")
            .await?;
        value["epoch"]
            .as_u64()
            .ok_or_else(|| HsmError::Unknown("invalid controller epoch".into()))
    }
    /// Conditional durable qualification change. Old controller mutations are fenced.
    pub async fn claim_controller(&self, ctx: &OperationContext, expected: u64) -> HsmResult<u64> {
        self.native_call(
            json!({"op":"claim_controller","context":ctx,"expected_epoch":expected}),
            "epoch",
        )
        .await
    }
}
fn portable<T: DeserializeOwned>(value: &impl Serialize) -> HsmResult<T> {
    serde_json::from_value(
        serde_json::to_value(value).map_err(|e| HsmError::Invalid(e.to_string()))?,
    )
    .map_err(|e| HsmError::Integrity(e.to_string()))
}
fn native_error(error: curvine_model::hsm::native::HsmError) -> HsmError {
    use curvine_model::hsm::native::HsmError as E;
    match error {
        E::Invalid(e) => HsmError::Invalid(e),
        E::Conflict(e) => HsmError::Conflict(e),
        E::Busy(e) => HsmError::Busy(e),
        E::NotFound(e) => HsmError::NotFound(e),
        E::Fenced => HsmError::Fenced,
        E::Integrity(e) => HsmError::Integrity(e),
        E::Unknown(e) => HsmError::Unknown(e),
        E::Unsupported(e) => HsmError::Unsupported(e),
        E::Io(e) => HsmError::Io(e),
    }
}
