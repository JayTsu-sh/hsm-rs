//! Storage-owned lifecycle definitions and durable work, without a namespace mirror.
use crate::{EntityState, HsmError, HsmResult, ManagedAction, OperationContext};
use serde::{Deserialize, Serialize};

/// One action against a configured archive backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    /// Archive an online generation.
    Archive,
    /// Migrate an online generation through Archive, then Release its verified dual copy.
    Release,
    /// Explicitly select offline generations for restoration.
    Recall,
}

/// Evaluated by source storage against its authoritative records.
/// Sources deduplicate each selected observation and can re-admit a later cold residency interval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecyclePolicy {
    /// Scheduling, coldness, exclusion and pressure controls.
    #[serde(default)]
    pub selection: PolicySelection,
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

/// Storage-side policy controls; percentages use basis points (100 = 1%).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolicySelection {
    /// Minimum interval between complete passes.
    pub scan_interval_ms: u64,
    /// Maximum source records examined by a policy per worker cycle.
    pub scan_budget: usize,
    /// Maximum newly admitted actions per pass.
    pub max_candidates: usize,
    /// Positive weight for CURRENT_TIMESTAMP - ACCESS_TIME; oldest first.
    pub access_age_weight: u32,
    /// Minimum time since the last data access.
    pub min_access_age_ms: u64,
    /// Minimum time since the last content modification.
    pub min_modify_age_ms: u64,
    /// Excluded path prefixes, evaluated before admission.
    pub exclude_prefixes: Vec<String>,
    /// Optional owning user condition.
    pub owner: Option<String>,
    /// Optional owning group condition.
    pub group: Option<String>,
    /// Optional pressure start/stop thresholds for the online worker pool.
    pub capacity: Option<CapacityWatermarks>,
}
impl Default for PolicySelection {
    fn default() -> Self {
        Self {
            scan_interval_ms: 60_000,
            scan_budget: 1024,
            max_candidates: 128,
            access_age_weight: 1,
            min_access_age_ms: 0,
            min_modify_age_ms: 0,
            exclude_prefixes: vec![],
            owner: None,
            group: None,
            capacity: None,
        }
    }
}
/// Hysteresis: start at high; stop only after actual usage reaches low.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityWatermarks {
    /// Start threshold: 8000 means 80%.
    pub high_basis_points: u16,
    /// Stop threshold: 6000 means 60%.
    pub low_basis_points: u16,
}

impl LifecyclePolicy {
    /// Validate portable limits before admission.
    pub fn validate(&self) -> HsmResult<()> {
        if self.selection.scan_interval_ms == 0
            || self.selection.scan_interval_ms > i64::MAX as u64
            || self.selection.scan_budget == 0
            || self.selection.scan_budget > 65536
            || self.selection.max_candidates == 0
            || self.selection.max_candidates > 4096
            || self.selection.access_age_weight == 0
            || self.selection.min_access_age_ms > i64::MAX as u64
            || self.selection.min_modify_age_ms > i64::MAX as u64
            || self.selection.exclude_prefixes.len() > 64
            || self.selection.capacity.as_ref().is_some_and(|w| {
                w.low_basis_points >= w.high_basis_points || w.high_basis_points > 10000
            })
            || self.id.is_empty()
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
    /// Closed source generations requiring protected content hashing before admission.
    #[serde(default)]
    pub to_seal: Vec<crate::DataKey>,
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
