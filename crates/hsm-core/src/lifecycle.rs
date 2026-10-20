//! Storage-owned lifecycle definitions and durable work, without a namespace mirror.
use crate::{EntityState, HsmError, HsmResult, ManagedAction, OperationContext};
use serde::{Deserialize, Serialize};

/// One action against a configured archive backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    /// Archive an online generation.
    Archive,
    /// Release a dual-resident generation with a verified anchor.
    Release,
    /// Explicitly select offline generations for restoration.
    Recall,
}

/// Evaluated by source storage against its authoritative records.
/// Each revision runs at most once per content generation, preventing rule cycles.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecyclePolicy {
    /// Operator-selected stable policy name.
    pub id: String,
    /// Source-assigned revision; create at one, update with compare-and-swap.
    pub revision: u64,
    /// Disabled policies admit no new work.
    pub enabled: bool,
    /// Source-native path prefix; interpreted by the source Adapter.
    pub prefix: String,
    /// Inclusive minimum logical bytes.
    pub min_size: u64,
    /// Inclusive maximum logical bytes, if present.
    pub max_size: Option<u64>,
    /// Minimum time in the current residency; measured by source storage.
    pub min_residency_ms: u64,
    /// Desired operation, never an instruction to a scanner to move bytes.
    pub action: PolicyAction,
    /// Logical backend ID; no physical media location.
    pub backend: String,
}

impl LifecyclePolicy {
    /// Validate portable limits before admission.
    pub fn validate(&self) -> HsmResult<()> {
        if self.id.is_empty()
            || self.id.len() > 64
            || !self
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || self.revision == 0
            || self.backend.is_empty()
            || self.backend.len() > 128
            || self.max_size.is_some_and(|max| max < self.min_size)
            || self.min_residency_ms > i64::MAX as u64
        {
            return Err(HsmError::Invalid(
                "invalid lifecycle policy bounds or identity".into(),
            ));
        }
        Ok(())
    }
}

/// Source-authoritative task state, distinct from file residency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentStatus {
    /// Ready to be claimed, including an uncertain operation to reconcile.
    Ready,
    /// Exclusively claimed in one controller epoch.
    Running,
    /// The source confirms the original successful effect.
    Done,
    /// A definite error requires operator inspection or a new policy revision.
    Failed,
    /// Conditions or the admitting policy changed before execution.
    Superseded,
}

/// Qualification for one claimed task. Claim IDs are fencing identities, not secrets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentClaim {
    /// Executor identity for diagnosis.
    pub owner: String,
    /// Unreused source-issued claim ID.
    pub id: String,
    /// Controller qualification checked again on finish.
    pub epoch: u64,
}

/// Durable work owned by the source; external executors need no task database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleIntent {
    /// Deterministic policy/revision/generation identity.
    pub id: String,
    /// Admitting policy.
    pub policy: String,
    /// Exact admitting revision.
    pub policy_revision: u64,
    /// Compare-and-swap version of this task record.
    pub version: u64,
    /// Snapshot to validate before a new effect; not a mutable metadata mirror.
    pub expected: EntityState,
    /// Exact action and immutable archive key.
    pub action: ManagedAction,
    /// Stable effect identity; qualification may advance during takeover.
    pub context: OperationContext,
    /// Work state.
    pub status: IntentStatus,
    /// Current source-issued exclusive claim.
    pub claim: Option<IntentClaim>,
    /// Last error for reconciliation or diagnosis.
    pub last_error: Option<String>,
    /// Original source-confirmed successful result.
    pub result: Option<EntityState>,
}

/// Executor report. A source must independently confirm success.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "message", rename_all = "snake_case")]
pub enum IntentCompletion {
    /// Ask the source to confirm the committed original effect.
    Done,
    /// Preserve effect identity and retry/reconcile an uncertain result.
    Retry(String),
    /// Preserve a definite failure for operator inspection.
    Failed(String),
    /// Stop work whose admission conditions no longer hold.
    Superseded(String),
}

/// Bounded page. A cursor is source-opaque and is not a snapshot guarantee.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecyclePage<T> {
    /// At most the requested bound.
    pub items: Vec<T>,
    /// Resume strictly after the last examined native key.
    pub next: Option<String>,
}

/// Candidate scan result: admission is a committed storage transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanBatch {
    /// Number of authoritative records examined, including rejected candidates.
    pub examined: usize,
    /// Newly admitted intent IDs; repeat scans cannot duplicate them.
    pub admitted: Vec<String>,
    /// Resume after the last examined native record, not only matching files.
    pub next: Option<String>,
}

/// Exclusive task claims and reports live at the same source authority as HSM facts.
pub trait LifecycleStorage: Send + Sync {
    /// Claim ready work; takeover of running work requires a newer controller epoch.
    fn claim_intent(
        &self,
        ctx: &OperationContext,
        id: &str,
        owner: &str,
    ) -> impl Future<Output = HsmResult<LifecycleIntent>> + Send;
    /// Atomically check claim/qualification and persist the original result or retry.
    fn finish_intent(
        &self,
        ctx: &OperationContext,
        lease: &LifecycleIntent,
        report: IntentCompletion,
    ) -> impl Future<Output = HsmResult<LifecycleIntent>> + Send;
}
