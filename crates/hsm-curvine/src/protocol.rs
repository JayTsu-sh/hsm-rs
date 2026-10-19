use hsm_core::{
    ArchiveReceipt, DataKey, DataView, EntityKey, EntityState, HsmError, HsmResult, ManagedAction,
    OperationContext, RecallTarget, TransferEvidence,
};
use serde::{Deserialize, Serialize};

/// Native Curvine guarantees, negotiated independently from ordinary FS support.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent advertised capabilities"
)]
pub struct CurvineCapabilities {
    /// Native records/outcomes, not an external metadata mirror.
    pub native_records: bool,
    /// Durable exact-version views and source pins.
    pub stable_views: bool,
    /// Conditional detach with reader/writer/anchor protection.
    pub conditional_release: bool,
    /// Private original-inode layout and atomic publication.
    pub private_recall: bool,
    /// Committed qualification and records survive source-node failure.
    pub distributed_authority: bool,
    /// Original successful effects survive process restart.
    pub durable_success_results: bool,
    /// Enforceable recall capacity claim.
    pub capacity_reservation: bool,
    /// Native offline access triggers a shared Recall.
    pub transparent_recall: bool,
}

impl CurvineCapabilities {
    pub(crate) fn validate(&self) -> HsmResult<()> {
        if (self.stable_views
            || self.private_recall
            || self.conditional_release
            || self.distributed_authority
            || self.durable_success_results
            || self.capacity_reservation
            || self.transparent_recall)
            && !self.native_records
        {
            return Err(HsmError::Integrity(
                "Curvine capability requires native authority records".into(),
            ));
        }
        if (self.capacity_reservation || self.transparent_recall) && !self.private_recall {
            return Err(HsmError::Integrity(
                "Curvine Recall guarantees require native private Recall".into(),
            ));
        }
        if self.conditional_release
            && (!self.stable_views || !self.private_recall || !self.durable_success_results)
        {
            return Err(HsmError::Integrity(
                "Curvine Release requires stable views, private Recall and durable outcomes".into(),
            ));
        }
        Ok(())
    }
}

/// Version-1 JSON body inside the native protobuf `HsmControl` envelope.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    /// Discover implemented native guarantees.
    Capabilities,
    /// Query a protected native entity.
    Inspect { entity: EntityKey },
    /// Query the original workflow outcome.
    Completed {
        context: OperationContext,
        data: DataKey,
        action: ManagedAction,
    },
    /// Acquire a fixed content view.
    PrepareArchive {
        context: OperationContext,
        data: DataKey,
    },
    /// Attach a committed archive receipt and close the view.
    CommitArchive {
        context: OperationContext,
        view: DataView,
        receipt: ArchiveReceipt,
    },
    /// Conditionally detach the online layout.
    Release {
        context: OperationContext,
        expected: EntityState,
    },
    /// Allocate a private original-inode Recall layout.
    PrepareRecall {
        context: OperationContext,
        expected: EntityState,
        anchor: ArchiveReceipt,
    },
    /// Seal and publish complete Recall content.
    CommitRecall {
        context: OperationContext,
        target: RecallTarget,
        evidence: TransferEvidence,
    },
}

/// Version-1 native control result. Unknown failure codes remain uncertain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ControlReply {
    /// Native guarantees.
    Capabilities(CurvineCapabilities),
    /// Entity/effect result.
    Entity(EntityState),
    /// Original workflow result, if observed.
    Completed(Option<EntityState>),
    /// Ready exact-version source.
    View(DataView),
    /// Private restoration target.
    Recall(RecallTarget),
    /// Explicit classified error.
    Error { code: String, message: String },
}
