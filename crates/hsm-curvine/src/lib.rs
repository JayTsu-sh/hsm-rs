//! Curvine control Adapter. No local metadata mirror, xattr approximation,
//! ordinary free/delete or rename-based Recall publication.
//!
//! Native HSM effects require an explicitly enabled server and immutable managed
//! files. Capability negotiation keeps ordinary or older servers fail-closed.

mod protocol;
pub use protocol::{ControlReply, ControlRequest, CurvineCapabilities};

#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
pub use native::{CurvineIo, NativeControl};
#[cfg(feature = "native")]
mod managed_io;
#[cfg(feature = "native")]
pub use managed_io::CurvineDataMover;

use hsm_core::{
    ArchiveReceipt, DataKey, DataView, EntityKey, EntityState, HsmError, HsmResult, ManagedAction,
    ManagedStorage, OperationContext, RecallTarget, Residency, StorageCapabilities,
    TransferEvidence,
};

/// Versioned transport; errors after sending an effect must preserve uncertainty.
pub trait ControlTransport: Send + Sync {
    /// Exchange one JSON control body within the Curvine protobuf envelope.
    fn exchange(&self, payload: Vec<u8>) -> impl Future<Output = HsmResult<Vec<u8>>> + Send;
}

/// Generic HSM storage Adapter backed exclusively by native Curvine control.
#[derive(Clone)]
pub struct CurvineStorage<T> {
    transport: T,
    capabilities: CurvineCapabilities,
}

impl<T: ControlTransport> CurvineStorage<T> {
    /// Negotiate native guarantees. Cached guarantees are checked before effects;
    /// the server must also validate the current protocol/qualification on apply.
    pub async fn connect(transport: T) -> HsmResult<Self> {
        let response = exchange(&transport, &ControlRequest::Capabilities).await?;
        let ControlReply::Capabilities(capabilities) = response else {
            return Err(HsmError::Integrity(
                "expected Curvine capability reply".into(),
            ));
        };
        capabilities.validate()?;
        Ok(Self {
            transport,
            capabilities,
        })
    }

    /// Exact advertised native capabilities; ordinary filesystem features do not
    /// imply any of these guarantees.
    pub const fn native_capabilities(&self) -> &CurvineCapabilities {
        &self.capabilities
    }

    fn require(supported: bool, feature: &str) -> HsmResult<()> {
        if !supported {
            return Err(HsmError::Unsupported(format!(
                "Curvine has not enabled {feature}"
            )));
        }
        Ok(())
    }

    async fn state(&self, request: ControlRequest, expected: &DataKey) -> HsmResult<EntityState> {
        match exchange(&self.transport, &request).await? {
            ControlReply::Entity(state) if state.data == *expected => {
                validate_state(&state)?;
                Ok(state)
            }
            _ => Err(HsmError::Integrity(
                "Curvine state reply does not match DataKey".into(),
            )),
        }
    }
}

async fn exchange(
    transport: &impl ControlTransport,
    request: &ControlRequest,
) -> HsmResult<ControlReply> {
    let payload = serde_json::to_vec(request).map_err(|e| HsmError::Invalid(e.to_string()))?;
    if payload.len() > 64 * 1024 {
        return Err(HsmError::Invalid(
            "Curvine control request exceeds limit".into(),
        ));
    }
    let response = transport.exchange(payload).await?;
    if response.len() > 1024 * 1024 {
        return Err(HsmError::Integrity(
            "Curvine control response exceeds limit".into(),
        ));
    }
    let response: ControlReply =
        serde_json::from_slice(&response).map_err(|e| HsmError::Integrity(e.to_string()))?;
    if let ControlReply::Error { code, message } = response {
        return Err(remote_error(&code, message));
    }
    Ok(response)
}

impl<T: ControlTransport> ManagedStorage for CurvineStorage<T> {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            distributed_authority: self.capabilities.distributed_authority,
            durable_success_results: self.capabilities.durable_success_results,
            capacity_reservation: self.capabilities.capacity_reservation,
            transparent_recall: self.capabilities.transparent_recall,
        }
    }

    async fn completed(
        &self,
        ctx: &OperationContext,
        data: &DataKey,
        action: &ManagedAction,
    ) -> HsmResult<Option<EntityState>> {
        ctx.validate()?;
        Self::require(
            self.capabilities.native_records,
            "native records and operation outcomes",
        )?;
        match exchange(
            &self.transport,
            &ControlRequest::Completed {
                context: ctx.clone(),
                data: data.clone(),
                action: action.clone(),
            },
        )
        .await?
        {
            ControlReply::Completed(state) if state.as_ref().is_none_or(|s| s.data == *data) => {
                if let Some(ref state) = state {
                    validate_state(state)?;
                    let matches_action = match action {
                        ManagedAction::Release => state.residency == Residency::Offline,
                        ManagedAction::Archive(key) | ManagedAction::Recall(key) => {
                            state.residency == Residency::Dual
                                && state.anchors.iter().any(|anchor| anchor.key == *key)
                        }
                    };
                    if !matches_action {
                        return Err(HsmError::Integrity(
                            "Curvine stored outcome differs from requested action".into(),
                        ));
                    }
                }
                Ok(state)
            }
            _ => Err(HsmError::Integrity(
                "invalid Curvine operation outcome".into(),
            )),
        }
    }

    async fn inspect(&self, entity: &EntityKey) -> HsmResult<EntityState> {
        Self::require(self.capabilities.native_records, "native entity records")?;
        match exchange(
            &self.transport,
            &ControlRequest::Inspect {
                entity: entity.clone(),
            },
        )
        .await?
        {
            ControlReply::Entity(state) if state.data.entity == *entity => {
                validate_state(&state)?;
                Ok(state)
            }
            _ => Err(HsmError::Integrity(
                "invalid Curvine entity identity".into(),
            )),
        }
    }

    async fn prepare_archive(&self, ctx: &OperationContext, data: &DataKey) -> HsmResult<DataView> {
        ctx.validate()?;
        Self::require(self.capabilities.stable_views, "persistent stable DataView")?;
        match exchange(
            &self.transport,
            &ControlRequest::PrepareArchive {
                context: ctx.clone(),
                data: data.clone(),
            },
        )
        .await?
        {
            ControlReply::View(view)
                if view.data == *data
                    && !view.view_id.is_empty()
                    && view.source.adapter == "curvine-view-v1"
                    && !view.source.capability.is_empty()
                    && !view.digest.algorithm.is_empty()
                    && !view.digest.value.is_empty() =>
            {
                Ok(view)
            }
            _ => Err(HsmError::Integrity("invalid Curvine DataView".into())),
        }
    }

    async fn commit_archive(
        &self,
        ctx: &OperationContext,
        view: &DataView,
        receipt: &ArchiveReceipt,
    ) -> HsmResult<EntityState> {
        ctx.validate()?;
        Self::require(
            self.capabilities.native_records && self.capabilities.stable_views,
            "conditional archive anchor commit",
        )?;
        if receipt.data != view.data
            || receipt.length != view.length
            || receipt.digest != view.digest
        {
            return Err(HsmError::Integrity(
                "archive receipt differs from source view".into(),
            ));
        }
        let state = self
            .state(
                ControlRequest::CommitArchive {
                    context: ctx.clone(),
                    view: view.clone(),
                    receipt: receipt.clone(),
                },
                &view.data,
            )
            .await?;
        if state.residency != Residency::Dual
            || state.length != view.length
            || state.digest != view.digest
            || !state.anchors.contains(receipt)
        {
            return Err(HsmError::Integrity(
                "Curvine did not attach the committed archive anchor".into(),
            ));
        }
        Ok(state)
    }

    async fn release(
        &self,
        ctx: &OperationContext,
        expected: &EntityState,
    ) -> HsmResult<EntityState> {
        ctx.validate()?;
        Self::require(
            self.capabilities.conditional_release,
            "protected native Release",
        )?;
        validate_state(expected)?;
        let state = self
            .state(
                ControlRequest::Release {
                    context: ctx.clone(),
                    expected: expected.clone(),
                },
                &expected.data,
            )
            .await?;
        if state.residency != Residency::Offline
            || state.length != expected.length
            || state.digest != expected.digest
            || state.anchors != expected.anchors
            || state.revision < expected.revision
        {
            return Err(HsmError::Integrity(
                "Curvine Release result changed content or lacks offline residency".into(),
            ));
        }
        Ok(state)
    }

    async fn prepare_recall(
        &self,
        ctx: &OperationContext,
        expected: &EntityState,
        anchor: &ArchiveReceipt,
    ) -> HsmResult<RecallTarget> {
        ctx.validate()?;
        Self::require(
            self.capabilities.private_recall,
            "original-inode private Recall layout",
        )?;
        validate_state(expected)?;
        if anchor.data != expected.data || !expected.anchors.contains(anchor) {
            return Err(HsmError::Integrity(
                "Recall anchor differs from source state".into(),
            ));
        }
        match exchange(
            &self.transport,
            &ControlRequest::PrepareRecall {
                context: ctx.clone(),
                expected: expected.clone(),
                anchor: anchor.clone(),
            },
        )
        .await?
        {
            ControlReply::Recall(target)
                if target.data == expected.data
                    && target.archive == anchor.key
                    && !target.layout_id.is_empty()
                    && target.destination.adapter == "curvine-recall-v1"
                    && !target.destination.capability.is_empty() =>
            {
                Ok(target)
            }
            _ => Err(HsmError::Integrity(
                "invalid Curvine private Recall target".into(),
            )),
        }
    }

    async fn commit_recall(
        &self,
        ctx: &OperationContext,
        target: &RecallTarget,
        evidence: &TransferEvidence,
    ) -> HsmResult<EntityState> {
        ctx.validate()?;
        Self::require(self.capabilities.private_recall, "conditional CommitRecall")?;
        let state = self
            .state(
                ControlRequest::CommitRecall {
                    context: ctx.clone(),
                    target: target.clone(),
                    evidence: evidence.clone(),
                },
                &target.data,
            )
            .await?;
        if state.residency != Residency::Dual
            || state.length != evidence.length
            || state.digest != evidence.digest
            || !state.anchors.iter().any(|a| a.key == target.archive)
        {
            return Err(HsmError::Integrity(
                "Curvine Recall result does not match verified content and archive".into(),
            ));
        }
        Ok(state)
    }
}

fn validate_state(state: &EntityState) -> HsmResult<()> {
    let entity = &state.data.entity;
    let valid_anchors = state.anchors.iter().all(|anchor| {
        anchor.data == state.data
            && anchor.length == state.length
            && anchor.digest == state.digest
            && !anchor.key.backend.is_empty()
            && !anchor.key.object.is_empty()
            && !anchor.receipt_id.is_empty()
            && !anchor.durability_profile.is_empty()
    });
    let valid_residency = match state.residency {
        Residency::Online => state.anchors.is_empty(),
        Residency::Dual | Residency::Offline => !state.anchors.is_empty(),
    };
    if entity.storage.is_empty()
        || entity.namespace.is_empty()
        || entity.entity.is_empty()
        || state.data.generation == 0
        || state.revision == 0
        || state.digest.algorithm.is_empty()
        || state.digest.value.is_empty()
        || !valid_anchors
        || !valid_residency
    {
        return Err(HsmError::Integrity(
            "invalid Curvine authoritative state".into(),
        ));
    }
    Ok(())
}

pub(crate) fn remote_error(code: &str, message: String) -> HsmError {
    match code {
        "unsupported" => HsmError::Unsupported(message),
        "invalid" => HsmError::Invalid(message),
        "conflict" => HsmError::Conflict(message),
        "busy" => HsmError::Busy(message),
        "not_found" => HsmError::NotFound(message),
        "fenced" => HsmError::Fenced,
        "integrity" => HsmError::Integrity(message),
        _ => HsmError::Unknown(message),
    }
}

#[cfg(feature = "native")]
mod lifecycle;
