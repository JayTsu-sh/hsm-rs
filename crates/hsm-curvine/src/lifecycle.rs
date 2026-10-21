//! Lifecycle definitions and durable work live at the native source authority.
use crate::{CurvineStorage, NativeControl};
use hsm_core::{
    HsmResult, IntentCompletion, LifecycleIntent, LifecyclePage, LifecyclePolicy, LifecycleStorage,
    OperationContext, ScanBatch,
};
use serde_json::json;

impl CurvineStorage<NativeControl> {
    /// Create or conditionally revise a source-owned policy.
    pub async fn put_policy(
        &self,
        ctx: &OperationContext,
        policy: &LifecyclePolicy,
        expected_revision: Option<u64>,
    ) -> HsmResult<LifecyclePolicy> {
        policy.validate()?;
        self.native_call(json!({"op":"policy_put","context":ctx,"policy":policy,"expected_revision":expected_revision}),"policy").await
    }
    /// Bounded source-authoritative policy listing.
    pub async fn policies(
        &self,
        ctx: &OperationContext,
        after: Option<&str>,
        limit: usize,
    ) -> HsmResult<LifecyclePage<LifecyclePolicy>> {
        self.native_call(
            json!({"op":"policy_list","context":ctx,"after":after,"limit":limit}),
            "page",
        )
        .await
    }
    /// Examine at most `limit` source records and atomically admit eligible work.
    pub async fn scan_policy(
        &self,
        ctx: &OperationContext,
        id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> HsmResult<ScanBatch> {
        self.native_call(
            json!({"op":"policy_scan","context":ctx,"id":id,"after":after,"limit":limit}),
            "scan",
        )
        .await
    }
    /// Interrupted source verification is resumed even if its original policy is disabled.
    pub async fn pending_seals(&self) -> HsmResult<Vec<hsm_core::DataKey>> {
        self.native_call(json!({"op":"pending_seals"}), "seals")
            .await
    }
    /// Retry source-retired data deletion from the durable storage outbox.
    pub async fn gc_batch(&self) -> HsmResult<usize> {
        self.native_call(json!({"op":"gc_batch"}), "gc").await
    }
    /// Source maintains the cursor, interval and pressure latch across controller restarts.
    pub async fn scan_scheduled(
        &self,
        ctx: &OperationContext,
        id: &str,
        limit: usize,
    ) -> HsmResult<ScanBatch> {
        self.native_call(
            json!({"op":"policy_scan","context":ctx,"id":id,"limit":limit,"scheduled":true}),
            "scan",
        )
        .await
    }
    /// Queue only contains Ready/Running work, with access recalls first.
    pub async fn ready_intents(
        &self,
        ctx: &OperationContext,
        after: Option<&str>,
        limit: usize,
    ) -> HsmResult<LifecyclePage<LifecycleIntent>> {
        self.native_call(
            json!({"op":"intent_list","context":ctx,"after":after,"limit":limit,"ready_only":true}),
            "page",
        )
        .await
    }
    /// Bounded durable task listing; no client-side namespace mirror.
    pub async fn intents(
        &self,
        ctx: &OperationContext,
        after: Option<&str>,
        limit: usize,
    ) -> HsmResult<LifecyclePage<LifecycleIntent>> {
        self.native_call(
            json!({"op":"intent_list","context":ctx,"after":after,"limit":limit}),
            "page",
        )
        .await
    }
    /// Inspect one task after a lost claim, effect or completion response.
    pub async fn intent(&self, ctx: &OperationContext, id: &str) -> HsmResult<LifecycleIntent> {
        self.native_call(json!({"op":"intent_get","context":ctx,"id":id}), "intent")
            .await
    }
}
impl LifecycleStorage for CurvineStorage<NativeControl> {
    async fn claim_intent(
        &self,
        ctx: &OperationContext,
        id: &str,
        owner: &str,
    ) -> HsmResult<LifecycleIntent> {
        self.native_call(
            json!({"op":"intent_claim","context":ctx,"id":id,"owner":owner}),
            "intent",
        )
        .await
    }
    async fn finish_intent(
        &self,
        ctx: &OperationContext,
        lease: &LifecycleIntent,
        report: IntentCompletion,
    ) -> HsmResult<LifecycleIntent> {
        self.native_call(json!({"op":"intent_finish","context":ctx,"id":lease.id,"version":lease.version,"claim":lease.claim,"report":report}),"intent").await
    }
}
