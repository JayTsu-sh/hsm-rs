//! External HSM coordination over portable storage/backend/executor Interfaces.
//! No Lustre, local database or media-specific dependencies.

use hsm_core::managed::{
    ArchiveBackend, ArchiveKey, ArchiveReceipt, ArchiveStatus, DataKey, DataMover, EntityKey,
    EntityState, HsmError, HsmResult, ManagedAction, ManagedStorage, OperationContext, Residency,
};
use hsm_core::{IntentCompletion, IntentStatus, LifecycleIntent, LifecycleStorage};

impl<S: ManagedStorage + LifecycleStorage, B: ArchiveBackend, M: DataMover> HsmEngine<S, B, M> {
    /// Execute one storage-owned intent. Stable effect IDs reconcile a crash after
    /// publication; a new controller can claim old work without a local task DB.
    pub async fn run_intent(
        &self,
        claim_context: &OperationContext,
        id: &str,
        owner: &str,
    ) -> HsmResult<LifecycleIntent> {
        let lease = self.storage.claim_intent(claim_context, id, owner).await?;
        if lease.status != IntentStatus::Running {
            return Ok(lease);
        }
        let result: HsmResult<IntentCompletion> = async {
            if self
                .storage
                .completed(&lease.context, &lease.expected.data, &lease.action)
                .await?
                .is_some()
            {
                return Ok(IntentCompletion::Done);
            }
            if self.storage.inspect(&lease.expected.data.entity).await? != lease.expected {
                return Ok(IntentCompletion::Superseded(
                    "source observation changed".into(),
                ));
            }
            match &lease.action {
                ManagedAction::Archive(key) => {
                    self.archive(&lease.context, &lease.expected.data, key)
                        .await?
                }
                ManagedAction::Release => self.release(&lease.context, &lease.expected).await?,
                ManagedAction::Recall(key) => {
                    self.recall(&lease.context, &lease.expected, key).await?
                }
            };
            Ok(IntentCompletion::Done)
        }
        .await;
        let report = match result {
            Ok(report) => report,
            Err(error @ (HsmError::Unknown(_) | HsmError::Io(_) | HsmError::Busy(_))) => {
                IntentCompletion::Retry(error.to_string().chars().take(512).collect())
            }
            Err(error) => IntentCompletion::Failed(error.to_string().chars().take(512).collect()),
        };
        let finish = OperationContext {
            operation_id: format!(
                "{}:finish",
                lease
                    .claim
                    .as_ref()
                    .ok_or_else(|| HsmError::Integrity("running intent has no claim".into()))?
                    .id
            ),
            controller_epoch: lease.context.controller_epoch,
            attempt_epoch: 1,
        };
        self.storage.finish_intent(&finish, &lease, report).await
    }
}

/// External coordinator; adapters own authority and backend commit facts.
pub struct HsmEngine<S, B, M> {
    storage: S,
    backend: B,
    mover: M,
}

impl<S: ManagedStorage, B: ArchiveBackend, M: DataMover> HsmEngine<S, B, M> {
    /// Compose a source, archive backend and byte mover without hidden discovery.
    pub fn new(storage: S, backend: B, mover: M) -> Self {
        Self {
            storage,
            backend,
            mover,
        }
    }

    /// Observe authoritative source facts.
    pub async fn inspect(&self, entity: &EntityKey) -> HsmResult<EntityState> {
        self.storage.inspect(entity).await
    }

    /// Archive exact content. Accepted/staged/unknown results cannot become anchors.
    /// On error, preserve source protection for explicit recovery by the owner.
    pub async fn archive(
        &self,
        ctx: &OperationContext,
        data: &DataKey,
        key: &ArchiveKey,
    ) -> HsmResult<EntityState> {
        ctx.validate()?;
        if let Some(result) = self
            .storage
            .completed(ctx, data, &ManagedAction::Archive(key.clone()))
            .await?
        {
            return Ok(result);
        }
        let view = self.storage.prepare_archive(ctx, data).await?;
        let receipt = match self.backend.inspect(key).await? {
            ArchiveStatus::Committed(receipt) => *receipt,
            ArchiveStatus::Absent => {
                let target = self.backend.prepare(ctx, key, &view).await?;
                if let Some(receipt) = self.backend.resume_commit(ctx, &target, &view).await? {
                    check_receipt(&receipt, data, key, view.length, &view.digest)?;
                    return self.storage.commit_archive(ctx, &view, &receipt).await;
                }
                let evidence = self
                    .mover
                    .copy(ctx, &view.source, &target.destination)
                    .await?;
                if evidence.length != view.length || evidence.digest != view.digest {
                    return Err(HsmError::Integrity(
                        "archive transfer differs from pinned source".into(),
                    ));
                }
                self.backend.commit(ctx, &target, &view, &evidence).await?
            }
            ArchiveStatus::Pending | ArchiveStatus::Unknown => {
                return Err(HsmError::Unknown(
                    "backend has no confirmed commit result".into(),
                ));
            }
        };
        check_receipt(&receipt, data, key, view.length, &view.digest)?;
        self.storage.commit_archive(ctx, &view, &receipt).await
    }

    /// Release is a conditional storage effect; it never invokes the byte mover.
    pub async fn release(
        &self,
        ctx: &OperationContext,
        expected: &EntityState,
    ) -> HsmResult<EntityState> {
        ctx.validate()?;
        if let Some(result) = self
            .storage
            .completed(ctx, &expected.data, &ManagedAction::Release)
            .await?
        {
            return Ok(result);
        }
        if self.storage.inspect(&expected.data.entity).await? != *expected {
            return Err(HsmError::Conflict("release observation is stale".into()));
        }
        if expected.anchors.is_empty() {
            return Err(HsmError::Conflict("no committed recovery anchor".into()));
        }
        let mut verified = false;
        for anchor in &expected.anchors {
            check_receipt(
                anchor,
                &expected.data,
                &anchor.key,
                expected.length,
                &expected.digest,
            )?;
            match self.backend.inspect(&anchor.key).await? {
                ArchiveStatus::Committed(actual) if actual.as_ref() == anchor => {
                    verified = true;
                    break;
                }
                ArchiveStatus::Pending | ArchiveStatus::Unknown => {
                    return Err(HsmError::Unknown(
                        "release recovery source is uncertain".into(),
                    ));
                }
                _ => {}
            }
        }
        if !verified {
            return Err(HsmError::Integrity(
                "no verified committed recovery source".into(),
            ));
        }
        self.storage.release(ctx, expected).await
    }

    /// Recall selected immutable archive into private storage, then publish.
    pub async fn recall(
        &self,
        ctx: &OperationContext,
        expected: &EntityState,
        key: &ArchiveKey,
    ) -> HsmResult<EntityState> {
        ctx.validate()?;
        if let Some(result) = self
            .storage
            .completed(ctx, &expected.data, &ManagedAction::Recall(key.clone()))
            .await?
        {
            return Ok(result);
        }
        if expected.residency != Residency::Offline {
            return Err(HsmError::Conflict(
                "recall requires an offline entity".into(),
            ));
        }
        let anchor = expected
            .anchors
            .iter()
            .find(|a| &a.key == key)
            .ok_or_else(|| HsmError::NotFound("selected recovery anchor".into()))?;
        check_receipt(
            anchor,
            &expected.data,
            key,
            expected.length,
            &expected.digest,
        )?;
        match self.backend.inspect(key).await? {
            ArchiveStatus::Committed(actual) if actual.as_ref() == anchor => {}
            ArchiveStatus::Pending | ArchiveStatus::Unknown => {
                return Err(HsmError::Unknown("archive lookup is uncertain".into()));
            }
            _ => {
                return Err(HsmError::Integrity(
                    "archive no longer matches source anchor".into(),
                ));
            }
        }
        let target = self.storage.prepare_recall(ctx, expected, anchor).await?;
        let source = self.backend.open(anchor).await?;
        let evidence = self.mover.copy(ctx, &source, &target.destination).await?;
        if evidence.length != expected.length || evidence.digest != expected.digest {
            return Err(HsmError::Integrity(
                "recall content differs from anchor".into(),
            ));
        }
        self.storage.commit_recall(ctx, &target, &evidence).await
    }
}

fn check_receipt(
    receipt: &ArchiveReceipt,
    data: &DataKey,
    key: &ArchiveKey,
    length: u64,
    digest: &hsm_core::managed::ContentDigest,
) -> HsmResult<()> {
    if &receipt.key != key
        || &receipt.data != data
        || receipt.length != length
        || &receipt.digest != digest
        || receipt.receipt_id.is_empty()
        || receipt.durability_profile.is_empty()
    {
        return Err(HsmError::Integrity(
            "unbound or incomplete archive receipt".into(),
        ));
    }
    Ok(())
}
