//! External HSM coordination over portable storage/backend/executor Interfaces.
//! No Lustre, local database or media-specific dependencies.

use hsm_core::managed::{
    ArchiveBackend, ArchiveKey, ArchiveReceipt, ArchiveStatus, DataKey, DataMover, EntityKey,
    EntityState, HsmError, HsmResult, ManagedAction, ManagedStorage, OperationContext, Residency,
};

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
