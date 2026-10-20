use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use hsm_core::managed::{
    ArchiveBackend, ArchiveKey, ArchiveReceipt, ArchiveStatus, ArchiveTarget, DataEndpoint,
    DataView, HsmError, HsmResult, OperationContext, TransferEvidence,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::util::{
    decode, decode_endpoint, digest_file, endpoint, key, lock_root, new_layout, publish_receipt,
    sync_dir, transfer_lock, write_json,
};

/// Durable local archive catalog. Exclusive root ownership; immutable receipts.
/// Its durability profile is local filesystem fsync, without node-failure HA.
#[derive(Clone)]
pub struct LocalArchive(Arc<Inner>);

struct Inner {
    root: PathBuf,
    id: String,
    _owner: File,
    serial: Mutex<()>,
}

#[derive(Serialize, Deserialize)]
struct Prepared {
    context: OperationContext,
    key: ArchiveKey,
    view: DataView,
    target: ArchiveTarget,
}

#[derive(Serialize, Deserialize)]
struct Committed {
    receipt: ArchiveReceipt,
    payload: DataEndpoint,
}

impl LocalArchive {
    /// Open a dedicated private archive root, rejecting concurrent owners.
    pub fn open(root: impl AsRef<Path>, backend_id: impl Into<String>) -> HsmResult<Self> {
        let (root, owner) = lock_root(root.as_ref())?;
        let id = backend_id.into();
        if id.is_empty() {
            return Err(HsmError::Invalid("backend ID required".into()));
        }
        let id_path = root.join("backend-id.json");
        if id_path.exists() {
            let prior: String = decode(&fs::read(&id_path)?)?;
            if prior != id {
                return Err(HsmError::Conflict("archive backend ID changed".into()));
            }
        } else {
            write_json(&id_path, &id)?;
        }
        fs::create_dir_all(root.join("objects"))?;
        fs::create_dir_all(root.join("attempts"))?;
        sync_dir(&root)?;
        Ok(Self(Arc::new(Inner {
            root,
            id,
            _owner: owner,
            serial: Mutex::new(()),
        })))
    }

    fn receipt_path(&self, archive: &ArchiveKey) -> HsmResult<PathBuf> {
        if archive.backend != self.0.id || archive.object.is_empty() {
            return Err(HsmError::Invalid(
                "archive key belongs to a different backend or is empty".into(),
            ));
        }
        Ok(self
            .0
            .root
            .join("objects")
            .join(format!("{}.json", key(archive)?)))
    }

    fn prepared_path(&self, ctx: &OperationContext, archive: &ArchiveKey) -> HsmResult<PathBuf> {
        Ok(self.0.root.join("attempts").join(format!(
            "{}.json",
            key(&(
                &OperationContext {
                    controller_epoch: 1,
                    ..ctx.clone()
                },
                archive
            ))?
        )))
    }

    fn committed(&self, archive: &ArchiveKey) -> HsmResult<Option<Committed>> {
        match fs::read(self.receipt_path(archive)?) {
            Ok(bytes) => {
                let value: Committed = decode(&bytes)?;
                if value.receipt.key != *archive {
                    return Err(HsmError::Integrity("receipt key mismatch".into()));
                }
                let endpoint = decode_endpoint(&value.payload)?;
                let (length, digest) = digest_file(&endpoint.root.join(endpoint.path))?;
                if value.receipt.length != length || value.receipt.digest != digest {
                    return Err(HsmError::Integrity(
                        "committed archive payload changed".into(),
                    ));
                }
                // Also resolves a previous publish with an uncertain dirsync.
                File::open(endpoint.root.join("data"))?.sync_all()?;
                sync_dir(&endpoint.root)?;
                sync_dir(&self.0.root.join("objects"))?;
                Ok(Some(value))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl ArchiveBackend for LocalArchive {
    async fn resume_commit(
        &self,
        ctx: &OperationContext,
        target: &ArchiveTarget,
        view: &DataView,
    ) -> HsmResult<Option<ArchiveReceipt>> {
        let payload = decode_endpoint(&target.destination)?;
        if !payload.root.join("sealed.json").exists() {
            return Ok(None);
        }
        // commit checks ownership, fully reads back bytes and syncs publication.
        let evidence = TransferEvidence {
            length: view.length,
            digest: view.digest.clone(),
        };
        self.commit(ctx, target, view, &evidence).await.map(Some)
    }
    async fn inspect(&self, archive: &ArchiveKey) -> HsmResult<ArchiveStatus> {
        let _guard = self.0.serial.lock().await;
        Ok(match self.committed(archive)? {
            Some(value) => ArchiveStatus::Committed(Box::new(value.receipt)),
            None => ArchiveStatus::Absent,
        })
    }

    async fn prepare(
        &self,
        ctx: &OperationContext,
        archive: &ArchiveKey,
        view: &DataView,
    ) -> HsmResult<ArchiveTarget> {
        ctx.validate()?;
        let _guard = self.0.serial.lock().await;
        if self.committed(archive)?.is_some() {
            return Err(HsmError::Conflict("archive already committed".into()));
        }
        let prepared_path = self.prepared_path(ctx, archive)?;
        if prepared_path.exists() {
            let prior: Prepared = decode(&fs::read(prepared_path)?)?;
            if !same_attempt(&prior.context, ctx) || prior.key != *archive || prior.view != *view {
                return Err(HsmError::Conflict(
                    "archive attempt reused with different inputs".into(),
                ));
            }
            return Ok(prior.target);
        }
        let layout = new_layout(&self.0.root.join("attempts"))?;
        let target = ArchiveTarget {
            key: archive.clone(),
            destination: endpoint(&layout)?,
        };
        write_json(
            &prepared_path,
            &Prepared {
                context: ctx.clone(),
                key: archive.clone(),
                view: view.clone(),
                target: target.clone(),
            },
        )?;
        Ok(target)
    }

    async fn commit(
        &self,
        ctx: &OperationContext,
        target: &ArchiveTarget,
        view: &DataView,
        evidence: &TransferEvidence,
    ) -> HsmResult<ArchiveReceipt> {
        ctx.validate()?;
        let _guard = self.0.serial.lock().await;
        let prior: Prepared = decode(&fs::read(self.prepared_path(ctx, &target.key)?)?)?;
        if !same_attempt(&prior.context, ctx) || prior.target != *target || prior.view != *view {
            return Err(HsmError::Conflict("unowned archive target".into()));
        }
        if let Some(committed) = self.committed(&target.key)? {
            if committed.receipt.data == view.data
                && committed.receipt.length == view.length
                && committed.receipt.digest == view.digest
            {
                return Ok(committed.receipt);
            }
            return Err(HsmError::Conflict(
                "immutable archive key already has different content".into(),
            ));
        }
        let payload = decode_endpoint(&target.destination)?;
        let _session = transfer_lock(&payload.root)?;
        let (length, digest) = digest_file(&payload.root.join(&payload.path))?;
        if length != view.length
            || digest != view.digest
            || evidence.length != length
            || evidence.digest != digest
        {
            return Err(HsmError::Integrity(
                "archive does not match pinned content".into(),
            ));
        }
        File::open(payload.root.join(&payload.path))?.sync_all()?;
        sync_dir(&payload.root)?;
        write_json(
            &payload.root.join("sealed.json"),
            &(&target.key, &view.data, &digest),
        )?;
        let receipt = ArchiveReceipt {
            key: target.key.clone(),
            data: view.data.clone(),
            length,
            digest,
            receipt_id: uuid::Uuid::new_v4().to_string(),
            durability_profile: "local-fsync-v1".into(),
        };
        publish_receipt(
            &self.receipt_path(&target.key)?,
            &Committed {
                receipt: receipt.clone(),
                payload: target.destination.clone(),
            },
        )?;
        Ok(receipt)
    }

    async fn open(&self, receipt: &ArchiveReceipt) -> HsmResult<DataEndpoint> {
        let _guard = self.0.serial.lock().await;
        let actual = self
            .committed(&receipt.key)?
            .ok_or_else(|| HsmError::NotFound("committed archive".into()))?;
        if actual.receipt != *receipt {
            return Err(HsmError::Integrity("receipt mismatch".into()));
        }
        Ok(actual.payload)
    }
}

fn same_attempt(a: &OperationContext, b: &OperationContext) -> bool {
    a.operation_id == b.operation_id && a.attempt_epoch == b.attempt_epoch
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LocalDataMover, LocalStorage};
    use hsm_core::{DataMover, ManagedStorage, Residency};
    #[tokio::test]
    async fn sealed_attempt_resumes_after_receipt_publication_gap_and_takeover() -> HsmResult<()> {
        let root = tempfile::tempdir()?;
        let source = LocalStorage::open(root.path().join("source"))?;
        let state = source
            .import("file", b"sealed data survives the commit gap")
            .await?;
        let ctx = OperationContext {
            operation_id: "archive".into(),
            controller_epoch: 1,
            attempt_epoch: 1,
        };
        let view = source.prepare_archive(&ctx, &state.data).await?;
        let archive = LocalArchive::open(root.path().join("archive"), "local")?;
        let key = ArchiveKey {
            backend: "local".into(),
            object: "one".into(),
        };
        let target = archive.prepare(&ctx, &key, &view).await?;
        LocalDataMover
            .copy(&ctx, &view.source, &target.destination)
            .await?;
        let payload = decode_endpoint(&target.destination)?;
        write_json(
            &payload.root.join("sealed.json"),
            &(&key, &view.data, &view.digest),
        )?;
        drop(archive);
        let archive = LocalArchive::open(root.path().join("archive"), "local")?;
        let mut next = ctx.clone();
        next.controller_epoch = 2;
        assert_eq!(archive.prepare(&next, &key, &view).await?, target);
        let receipt = archive.resume_commit(&next, &target, &view).await?.unwrap();
        assert_eq!(receipt.data, state.data);
        let dual = source.commit_archive(&ctx, &view, &receipt).await?;
        assert_eq!(dual.residency, Residency::Dual);
        Ok(())
    }
}
