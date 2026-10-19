//! Storage-independent HSM Interface.
//!
//! Storage owns authoritative metadata, operation results and protected data
//! effects. The external engine coordinates workflows. Executors move bytes;
//! backends alone assert archive commitment. No FID, kernel cookie, path or
//! fixed digest algorithm is required by this Interface.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable identity; paths are not identities.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityKey {
    /// Source storage instance.
    pub storage: String,
    /// Namespace incarnation; clones use a new incarnation.
    pub namespace: String,
    /// Storage-native opaque entity identifier.
    pub entity: String,
}

/// One exact content generation of an entity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataKey {
    /// Stable entity identity.
    pub entity: EntityKey,
    /// Changes before granting permission to modify content.
    pub generation: u64,
}

/// Stable identity on one archive backend.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArchiveKey {
    /// Backend instance, independent of physical media placement.
    pub backend: String,
    /// Immutable object ID; kept across retries.
    pub object: String,
}

/// Algorithm-tagged content digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentDigest {
    /// Algorithm identifier, e.g. `blake3` or `sha256`.
    pub algorithm: String,
    /// Raw digest bytes, never compared across algorithms.
    pub value: Vec<u8>,
}

/// Opaque data capability interpreted only by the executor's Adapter.
/// Debug output intentionally redacts its contents.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataEndpoint {
    /// Adapter protocol identifier, e.g. `local-managed-v1` or `curvine-view-v1`.
    pub adapter: String,
    /// Private descriptor/capability; must not be logged or used as an identity.
    pub capability: Vec<u8>,
}

impl std::fmt::Debug for DataEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataEndpoint")
            .field("adapter", &self.adapter)
            .field("capability", &"<redacted>")
            .finish()
    }
}

/// Workflow identity and distributed execution qualification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationContext {
    /// Caller-generated stable workflow ID. Reuse only for identical inputs.
    pub operation_id: String,
    /// Committed controller qualification, checked by the source Adapter.
    pub controller_epoch: u64,
    /// Current execution attempt; not an archive object ID.
    pub attempt_epoch: u64,
}

/// Portable workflow kind, separate from any kernel's action enumeration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManagedAction {
    /// Archive into an immutable backend identity.
    Archive(ArchiveKey),
    /// Detach the online layout, without deleting the logical entity.
    Release,
    /// Restore from the selected immutable backend identity.
    Recall(ArchiveKey),
}

impl OperationContext {
    /// Validate portable invariants before causing any side effect.
    pub fn validate(&self) -> HsmResult<()> {
        if self.operation_id.is_empty() || self.controller_epoch == 0 || self.attempt_epoch == 0 {
            return Err(HsmError::Invalid(
                "operation ID and nonzero epochs required".into(),
            ));
        }
        Ok(())
    }
}

/// A source view whose bytes remain fixed until explicitly closed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataView {
    /// Exact selected generation.
    pub data: DataKey,
    /// Storage-issued view identity, bound to the operation.
    pub view_id: String,
    /// Full logical length.
    pub length: u64,
    /// Digest of the fixed content.
    pub digest: ContentDigest,
    /// Exact-version read capability; no latest-path fallback.
    pub source: DataEndpoint,
}

/// Backend-private destination, not an assertion of commitment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveTarget {
    /// Stable logical archive identity.
    pub key: ArchiveKey,
    /// Attempt-private writable target.
    pub destination: DataEndpoint,
}

/// Byte-transfer evidence; never authorizes Release on its own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferEvidence {
    /// Complete logical length, including any verified reused bytes.
    pub length: u64,
    /// Verified content digest.
    pub digest: ContentDigest,
}

/// Backend assertion of durable, immutable archive commitment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveReceipt {
    /// Stable backend identity.
    pub key: ArchiveKey,
    /// Exact source generation.
    pub data: DataKey,
    /// Complete logical length.
    pub length: u64,
    /// Content digest with algorithm identity.
    pub digest: ContentDigest,
    /// Backend commit proof ID, queryable after process restart.
    pub receipt_id: String,
    /// Durability profile, e.g. `local-fsync-v1`; not a generic HA promise.
    pub durability_profile: String,
}

/// Exact backend lookup result. Unknown is never interpreted as absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArchiveStatus {
    /// Backend confirms no committed object with this exact key.
    Absent,
    /// Accepted/staged, not committed.
    Pending,
    /// Exact committed archive receipt.
    Committed(Box<ArchiveReceipt>),
    /// Cannot determine the outcome safely.
    Unknown,
}

/// Storage-private restoration target belonging to the original entity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallTarget {
    /// Exact content to restore.
    pub data: DataKey,
    /// Storage-issued private layout identity.
    pub layout_id: String,
    /// Exact archive selected for this operation.
    pub archive: ArchiveKey,
    /// Attempt-bound destination, invisible to ordinary reads.
    pub destination: DataEndpoint,
}

/// Native residency; workflow progress is a separate fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Residency {
    /// Online without a committed anchor.
    Online,
    /// Both online and archived.
    Dual,
    /// No published online layout, committed archive remains.
    Offline,
}

/// Authoritative source observation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityState {
    /// Current content identity.
    pub data: DataKey,
    /// Native control revision for conditional effects.
    pub revision: u64,
    /// Current residency.
    pub residency: Residency,
    /// Current logical length.
    pub length: u64,
    /// Current content digest.
    pub digest: ContentDigest,
    /// Committed recovery anchors, never raw transfer progress.
    pub anchors: Vec<ArchiveReceipt>,
}

/// Portable failure classes. Transport uncertainty is not a negative outcome.
#[derive(Debug, Error)]
pub enum HsmError {
    /// Invalid caller input.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// Generation/revision or owner changed.
    #[error("conditional mutation conflict: {0}")]
    Conflict(String),
    /// Active references prohibit the requested effect.
    #[error("protected data is busy: {0}")]
    Busy(String),
    /// Exact identity does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// Qualification or capability has been revoked.
    #[error("stale execution qualification")]
    Fenced,
    /// Content/receipt mismatch.
    #[error("integrity verification failed: {0}")]
    Integrity(String),
    /// Operation may have taken effect; inspect before compensation.
    #[error("outcome unknown: {0}")]
    Unknown(String),
    /// Backend/Adapter does not implement the required guarantee.
    #[error("unsupported capability: {0}")]
    Unsupported(String),
    /// I/O, with uncertainty preserved by the originating Adapter.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result shared by the managed Interface.
pub type HsmResult<T> = Result<T, HsmError>;

/// Explicit guarantees, so a local reference Adapter cannot imply production HA.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent advertised guarantees, not mutually exclusive states"
)]
pub struct StorageCapabilities {
    /// Outcomes and qualification survive source-node failure.
    pub distributed_authority: bool,
    /// Successful effects and their original results survive process restart.
    pub durable_success_results: bool,
    /// `PrepareRecall` reserves enforceable full-recall capacity.
    pub capacity_reservation: bool,
    /// Native offline access generates/waits for a shared Recall.
    pub transparent_recall: bool,
}

/// Source storage: owns metadata, pins, outcomes and atomic data effects.
///
/// Implementations must condition all effects on the context and exact `DataKey`.
/// Repeated identical calls return their original result; conflicting reuse of
/// an operation/step identity is rejected. Release preserves entity identity and
/// must not run until a current committed anchor and all protection checks hold.
/// Production adapters must replicate outcomes and survive controller failover.
pub trait ManagedStorage: Send + Sync {
    /// Report guarantees supported by this concrete Adapter.
    fn capabilities(&self) -> StorageCapabilities;
    /// Read the original successful workflow result before re-evaluating current
    /// state. Different input under an existing operation ID must conflict.
    fn completed(
        &self,
        ctx: &OperationContext,
        data: &DataKey,
        action: &ManagedAction,
    ) -> impl Future<Output = HsmResult<Option<EntityState>>> + Send;
    /// Read authoritative native state; final mutations recheck current facts.
    fn inspect(&self, entity: &EntityKey) -> impl Future<Output = HsmResult<EntityState>> + Send;
    /// Freeze/pin exact content, before giving any executor access.
    fn prepare_archive(
        &self,
        ctx: &OperationContext,
        data: &DataKey,
    ) -> impl Future<Output = HsmResult<DataView>> + Send;
    /// Conditionally attach a verified committed receipt and close the source view.
    /// Caller must first ensure the transfer no longer reads the view.
    fn commit_archive(
        &self,
        ctx: &OperationContext,
        view: &DataView,
        receipt: &ArchiveReceipt,
    ) -> impl Future<Output = HsmResult<EntityState>> + Send;
    /// Detach the online layout; physical GC may complete later.
    fn release(
        &self,
        ctx: &OperationContext,
        expected: &EntityState,
    ) -> impl Future<Output = HsmResult<EntityState>> + Send;
    /// Create an original-entity private layout. Production Adapters must also
    /// reserve capacity; reference Adapters explicitly advertise its absence.
    fn prepare_recall(
        &self,
        ctx: &OperationContext,
        expected: &EntityState,
        anchor: &ArchiveReceipt,
    ) -> impl Future<Output = HsmResult<RecallTarget>> + Send;
    /// Seal exact content/durability, then atomically publish, retaining identity.
    /// An implementation may internally split `SealRecall` and `CommitRecall` RPCs.
    fn commit_recall(
        &self,
        ctx: &OperationContext,
        target: &RecallTarget,
        evidence: &TransferEvidence,
    ) -> impl Future<Output = HsmResult<EntityState>> + Send;
}

/// Archive backend controls commitment and exact identity lookup, not source GC.
pub trait ArchiveBackend: Send + Sync {
    /// Query by exact immutable identity after unknown outcomes or restart.
    fn inspect(&self, key: &ArchiveKey) -> impl Future<Output = HsmResult<ArchiveStatus>> + Send;
    /// Create an attempt-private target; must not overwrite a committed archive.
    fn prepare(
        &self,
        ctx: &OperationContext,
        key: &ArchiveKey,
        view: &DataView,
    ) -> impl Future<Output = HsmResult<ArchiveTarget>> + Send;
    /// Verify bytes, apply durability barrier, publish immutable archive and receipt.
    fn commit(
        &self,
        ctx: &OperationContext,
        target: &ArchiveTarget,
        view: &DataView,
        evidence: &TransferEvidence,
    ) -> impl Future<Output = HsmResult<ArchiveReceipt>> + Send;
    /// Return exact committed bytes, never the latest object at a mutable path.
    fn open(
        &self,
        receipt: &ArchiveReceipt,
    ) -> impl Future<Output = HsmResult<DataEndpoint>> + Send;
}

/// Executor's data plane, e.g. a data-mover-rs Adapter. No Release/metadata rights.
pub trait DataMover: Send + Sync {
    /// Finish a bounded transfer with complete verification, or report uncertainty.
    /// The returned future must not leave detached readers/writers running.
    fn copy(
        &self,
        ctx: &OperationContext,
        source: &DataEndpoint,
        destination: &DataEndpoint,
    ) -> impl Future<Output = HsmResult<TransferEvidence>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualification_requires_explicit_nonzero_epochs() {
        let mut context = OperationContext {
            operation_id: "op".into(),
            controller_epoch: 1,
            attempt_epoch: 1,
        };
        assert!(context.validate().is_ok());
        context.attempt_epoch = 0;
        assert!(context.validate().is_err());
        context.attempt_epoch = 1;
        context.operation_id.clear();
        assert!(context.validate().is_err());
    }

    #[test]
    fn digest_algorithm_is_part_of_content_identity() {
        let a = ContentDigest {
            algorithm: "blake3".into(),
            value: vec![1; 32],
        };
        let b = ContentDigest {
            algorithm: "sha256".into(),
            value: vec![1; 32],
        };
        assert_ne!(a, b);
    }

    #[test]
    fn capability_debug_does_not_disclose_secrets() {
        let endpoint = DataEndpoint {
            adapter: "test".into(),
            capability: b"secret-token".to_vec(),
        };
        let rendered = format!("{endpoint:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("secret-token"));
        assert!(!rendered.contains("115, 101, 99"));
    }
}
