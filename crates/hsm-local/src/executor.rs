use std::num::NonZeroUsize;

use data_mover::model::StoragePath;
use data_mover::storage::{BackendConfig, LocalBackendConfig, connect_backend};
use data_mover::transfer::{TransferPolicy, TransferRequest, transfer};
use hsm_core::managed::{
    DataEndpoint, DataMover, HsmError, HsmResult, OperationContext, TransferEvidence,
};
use tokio_util::sync::CancellationToken;

use crate::util::{decode_endpoint, digest_file, transfer_lock};

/// Real data-mover-rs local transfer Adapter, using bounded Checkpointed copying.
/// Endpoints use the same relative payload name inside distinct private roots.
#[derive(Clone, Copy, Default)]
pub struct LocalDataMover;

impl LocalDataMover {
    /// Describe the `data` payload in a trusted private local root. This is a
    /// local reference capability, not a production authentication token.
    pub fn payload_endpoint(root: &std::path::Path) -> HsmResult<DataEndpoint> {
        crate::util::endpoint(&root.canonicalize()?)
    }
}

impl DataMover for LocalDataMover {
    async fn copy(
        &self,
        ctx: &OperationContext,
        source: &DataEndpoint,
        destination: &DataEndpoint,
    ) -> HsmResult<TransferEvidence> {
        ctx.validate()?;
        let source = decode_endpoint(source)?;
        let destination = decode_endpoint(destination)?;
        if source.root == destination.root {
            return Err(HsmError::Invalid(
                "source and destination must be distinct".into(),
            ));
        }
        let _source_session = transfer_lock(&source.root)?;
        let _destination_session = transfer_lock(&destination.root)?;
        if destination.root.join("sealed.json").exists() {
            return Err(HsmError::Fenced);
        }
        let config = |root| {
            BackendConfig::Local(LocalBackendConfig {
                root,
                read_concurrency: NonZeroUsize::MIN,
                write_concurrency: NonZeroUsize::MIN,
                read_chunk_bytes: None,
                write_chunk_bytes: None,
            })
        };
        let src = connect_backend(config(source.root.clone()))
            .await
            .map_err(|e| HsmError::Invalid(e.to_string()))?;
        let dst = connect_backend(config(destination.root.clone()))
            .await
            .map_err(|e| HsmError::Invalid(e.to_string()))?;
        let path =
            StoragePath::new(source.path.clone()).map_err(|e| HsmError::Invalid(e.to_string()))?;
        let request = TransferRequest::new(src, path, dst, CancellationToken::new())
            .with_transfer_policy(TransferPolicy::Checkpointed);
        transfer(request)
            .await
            .map_err(|e| HsmError::Unknown(format!("data transfer: {e}")))?;
        // Independent complete read-back; algorithm is explicit in the receipt.
        let (length, digest) = digest_file(&destination.root.join(&destination.path))?;
        Ok(TransferEvidence { length, digest })
    }
}
