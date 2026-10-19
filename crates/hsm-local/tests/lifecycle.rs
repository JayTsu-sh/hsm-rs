use std::path::PathBuf;

use hsm_core::{
    ArchiveBackend, ArchiveKey, ArchiveReceipt, ArchiveStatus, ArchiveTarget, DataEndpoint,
    DataMover, DataView, HsmError, HsmResult, ManagedStorage, OperationContext, Residency,
    TransferEvidence,
};
use hsm_engine::HsmEngine;
use hsm_local::{LocalArchive, LocalDataMover, LocalStorage};

fn operation(id: &str) -> OperationContext {
    OperationContext {
        operation_id: id.into(),
        controller_epoch: 1,
        attempt_epoch: 1,
    }
}

fn archive_key(object: &str) -> ArchiveKey {
    ArchiveKey {
        backend: "local-archive".into(),
        object: object.into(),
    }
}

fn endpoint_path(endpoint: &DataEndpoint) -> PathBuf {
    let value: serde_json::Value = serde_json::from_slice(&endpoint.capability).unwrap();
    PathBuf::from(value["root"].as_str().unwrap()).join(value["path"].as_str().unwrap())
}

#[tokio::test]
async fn archive_release_recall_survives_reopen_and_retries_preserve_identity() {
    let root = tempfile::tempdir().unwrap();
    let source_root = root.path().join("source");
    let archive_root = root.path().join("archive");
    let bytes: Vec<u8> = (0..3 * 1024 * 1024 + 17).map(|i| (i % 251) as u8).collect();
    let data;
    let offline;
    let dual;
    {
        let storage = LocalStorage::open(&source_root).unwrap();
        let backend = LocalArchive::open(&archive_root, "local-archive").unwrap();
        let initial = storage
            .import("entity/name-is-not-a-path", &bytes)
            .await
            .unwrap();
        data = initial.data.clone();
        let engine = HsmEngine::new(storage.clone(), backend, LocalDataMover);
        dual = engine
            .archive(&operation("archive-1"), &data, &archive_key("copy-1"))
            .await
            .unwrap();
        assert_eq!(dual.residency, Residency::Dual);
        assert_eq!(storage.read(&data.entity).await.unwrap(), bytes);
        offline = engine
            .release(&operation("release-1"), &dual)
            .await
            .unwrap();
        assert_eq!(offline.residency, Residency::Offline);
        assert_eq!(offline.data, data);
        assert!(storage.read(&data.entity).await.is_err());
        // The retry returns the original effect, rather than re-evaluating offline.
        assert_eq!(
            engine
                .release(&operation("release-1"), &dual)
                .await
                .unwrap(),
            offline
        );
        assert_eq!(storage.collect_garbage().await.unwrap(), 1);
        assert_eq!(storage.collect_garbage().await.unwrap(), 0);
    }
    {
        let storage = LocalStorage::open(&source_root).unwrap();
        let backend = LocalArchive::open(&archive_root, "local-archive").unwrap();
        assert!(matches!(
            backend.inspect(&archive_key("copy-1")).await.unwrap(),
            ArchiveStatus::Committed(_)
        ));
        let engine = HsmEngine::new(storage.clone(), backend, LocalDataMover);
        let restored = engine
            .recall(&operation("recall-1"), &offline, &archive_key("copy-1"))
            .await
            .unwrap();
        assert_eq!(restored.data, data);
        assert_eq!(restored.residency, Residency::Dual);
        assert_eq!(storage.read(&data.entity).await.unwrap(), bytes);
        assert_eq!(
            engine
                .recall(&operation("recall-1"), &offline, &archive_key("copy-1"))
                .await
                .unwrap(),
            restored
        );
        // Old successful results remain queryable without altering current data.
        assert_eq!(
            engine
                .archive(&operation("archive-1"), &data, &archive_key("copy-1"))
                .await
                .unwrap(),
            dual
        );
        assert_eq!(engine.inspect(&data.entity).await.unwrap(), restored);
        assert!(matches!(
            engine
                .archive(&operation("archive-1"), &data, &archive_key("different"))
                .await,
            Err(HsmError::Conflict(_))
        ));
    }
}

#[tokio::test]
async fn release_without_committed_anchor_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let storage = LocalStorage::open(root.path().join("source")).unwrap();
    let backend = LocalArchive::open(root.path().join("archive"), "local-archive").unwrap();
    let initial = storage.import("a", b"only online copy").await.unwrap();
    let engine = HsmEngine::new(storage.clone(), backend, LocalDataMover);
    assert!(matches!(
        engine.release(&operation("release"), &initial).await,
        Err(HsmError::Conflict(_))
    ));
    assert_eq!(
        storage.read(&initial.data.entity).await.unwrap(),
        b"only online copy"
    );
    assert_eq!(storage.collect_garbage().await.unwrap(), 0);
}

#[tokio::test]
async fn committed_backend_result_can_be_reconciled_after_source_restart() {
    let root = tempfile::tempdir().unwrap();
    let source_root = root.path().join("source");
    let archive_root = root.path().join("archive");
    let context = operation("lost-result");
    let data;
    {
        let storage = LocalStorage::open(&source_root).unwrap();
        let backend = LocalArchive::open(&archive_root, "local-archive").unwrap();
        data = storage
            .import("a", b"committed before controller crash")
            .await
            .unwrap()
            .data;
        let view = storage.prepare_archive(&context, &data).await.unwrap();
        let target = backend
            .prepare(&context, &archive_key("copy"), &view)
            .await
            .unwrap();
        let evidence = LocalDataMover
            .copy(&context, &view.source, &target.destination)
            .await
            .unwrap();
        backend
            .commit(&context, &target, &view, &evidence)
            .await
            .unwrap();
        // Simulate loss of the backend reply before the source anchor was committed.
        assert_eq!(
            storage.inspect(&data.entity).await.unwrap().residency,
            Residency::Online
        );
        assert!(matches!(
            LocalDataMover
                .copy(&context, &view.source, &target.destination)
                .await,
            Err(HsmError::Fenced)
        ));
    }
    let storage = LocalStorage::open(source_root).unwrap();
    let backend = LocalArchive::open(archive_root, "local-archive").unwrap();
    let engine = HsmEngine::new(storage, backend, LocalDataMover);
    assert_eq!(
        engine
            .archive(&context, &data, &archive_key("copy"))
            .await
            .unwrap()
            .residency,
        Residency::Dual
    );
}

#[tokio::test]
async fn immutable_archive_key_cannot_be_reused_for_another_entity() {
    let root = tempfile::tempdir().unwrap();
    let storage = LocalStorage::open(root.path().join("source")).unwrap();
    let backend = LocalArchive::open(root.path().join("archive"), "local-archive").unwrap();
    let a = storage.import("a", b"content-a").await.unwrap();
    let b = storage.import("b", b"content-b").await.unwrap();
    let engine = HsmEngine::new(storage.clone(), backend, LocalDataMover);
    engine
        .archive(&operation("a"), &a.data, &archive_key("immutable"))
        .await
        .unwrap();
    assert!(matches!(
        engine
            .archive(&operation("b"), &b.data, &archive_key("immutable"))
            .await,
        Err(HsmError::Integrity(_))
    ));
    assert_eq!(storage.read(&b.data.entity).await.unwrap(), b"content-b");
}

struct PendingBackend;

impl ArchiveBackend for PendingBackend {
    fn inspect(&self, _: &ArchiveKey) -> impl Future<Output = HsmResult<ArchiveStatus>> + Send {
        std::future::ready(Ok(ArchiveStatus::Pending))
    }
    fn prepare(
        &self,
        _: &OperationContext,
        _: &ArchiveKey,
        _: &DataView,
    ) -> impl Future<Output = HsmResult<ArchiveTarget>> + Send {
        std::future::ready(Err(HsmError::Unsupported(
            "pending must not be re-uploaded".into(),
        )))
    }
    fn commit(
        &self,
        _: &OperationContext,
        _: &ArchiveTarget,
        _: &DataView,
        _: &TransferEvidence,
    ) -> impl Future<Output = HsmResult<ArchiveReceipt>> + Send {
        std::future::ready(Err(HsmError::Unsupported(
            "pending is not committed".into(),
        )))
    }
    fn open(&self, _: &ArchiveReceipt) -> impl Future<Output = HsmResult<DataEndpoint>> + Send {
        std::future::ready(Err(HsmError::Unsupported(
            "pending has no read source".into(),
        )))
    }
}

#[tokio::test]
async fn pending_is_not_an_anchor_and_keeps_source_protected() {
    let root = tempfile::tempdir().unwrap();
    let storage = LocalStorage::open(root.path().join("source")).unwrap();
    let initial = storage.import("a", b"pinned").await.unwrap();
    let engine = HsmEngine::new(storage.clone(), PendingBackend, LocalDataMover);
    assert!(matches!(
        engine
            .archive(&operation("pending"), &initial.data, &archive_key("copy"))
            .await,
        Err(HsmError::Unknown(_))
    ));
    assert_eq!(
        storage.inspect(&initial.data.entity).await.unwrap(),
        initial
    );
    assert_eq!(storage.read(&initial.data.entity).await.unwrap(), b"pinned");
    assert!(matches!(
        storage
            .prepare_archive(&operation("different"), &initial.data)
            .await,
        Err(HsmError::Busy(_))
    ));
}

struct CorruptMover;

impl DataMover for CorruptMover {
    async fn copy(
        &self,
        ctx: &OperationContext,
        source: &DataEndpoint,
        destination: &DataEndpoint,
    ) -> HsmResult<TransferEvidence> {
        let evidence = LocalDataMover.copy(ctx, source, destination).await?;
        std::fs::write(endpoint_path(destination), b"corrupted after transfer")?;
        // Claim the original evidence; storage must independently validate sealing.
        Ok(evidence)
    }
}

#[tokio::test]
async fn corrupt_private_recall_layout_is_never_published() {
    let root = tempfile::tempdir().unwrap();
    let storage = LocalStorage::open(root.path().join("source")).unwrap();
    let backend = LocalArchive::open(root.path().join("archive"), "local-archive").unwrap();
    let initial = storage
        .import("a", b"correct archived content")
        .await
        .unwrap();
    let engine = HsmEngine::new(storage.clone(), backend.clone(), LocalDataMover);
    let dual = engine
        .archive(&operation("archive"), &initial.data, &archive_key("copy"))
        .await
        .unwrap();
    let offline = engine.release(&operation("release"), &dual).await.unwrap();
    let corrupt = HsmEngine::new(storage.clone(), backend, CorruptMover);
    assert!(matches!(
        corrupt
            .recall(&operation("recall"), &offline, &archive_key("copy"))
            .await,
        Err(HsmError::Integrity(_))
    ));
    assert_eq!(
        storage.inspect(&initial.data.entity).await.unwrap(),
        offline
    );
    assert!(storage.read(&initial.data.entity).await.is_err());
}

#[tokio::test]
async fn stale_state_and_wrong_generation_cannot_release() {
    let root = tempfile::tempdir().unwrap();
    let storage = LocalStorage::open(root.path().join("source")).unwrap();
    let backend = LocalArchive::open(root.path().join("archive"), "local-archive").unwrap();
    let initial = storage.import("a", b"content").await.unwrap();
    let engine = HsmEngine::new(storage.clone(), backend, LocalDataMover);
    let dual = engine
        .archive(&operation("archive"), &initial.data, &archive_key("copy"))
        .await
        .unwrap();
    let mut stale = dual.clone();
    stale.revision -= 1;
    assert!(matches!(
        engine.release(&operation("stale"), &stale).await,
        Err(HsmError::Conflict(_))
    ));
    let mut wrong = dual;
    wrong.data.generation += 1;
    assert!(matches!(
        engine.release(&operation("wrong"), &wrong).await,
        Err(HsmError::Conflict(_))
    ));
    assert_eq!(
        storage.read(&initial.data.entity).await.unwrap(),
        b"content"
    );
}

#[test]
fn dedicated_roots_reject_concurrent_owners_and_changed_backend_identity() {
    let root = tempfile::tempdir().unwrap();
    let storage = LocalStorage::open(root.path().join("source")).unwrap();
    assert!(matches!(
        LocalStorage::open(root.path().join("source")),
        Err(HsmError::Busy(_))
    ));
    drop(storage);
    LocalStorage::open(root.path().join("source")).unwrap();
    let backend = LocalArchive::open(root.path().join("archive"), "one").unwrap();
    drop(backend);
    assert!(matches!(
        LocalArchive::open(root.path().join("archive"), "two"),
        Err(HsmError::Conflict(_))
    ));
}

#[tokio::test]
async fn definite_storage_failure_is_sticky_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let archive = root.path().join("archive");
    let data;
    {
        let storage = LocalStorage::open(&source).unwrap();
        let backend = LocalArchive::open(&archive, "local-archive").unwrap();
        data = storage.import("a", b"stable source").await.unwrap().data;
        let owner = operation("owner");
        let view = storage.prepare_archive(&owner, &data).await.unwrap();
        assert!(matches!(
            storage.prepare_archive(&operation("busy"), &data).await,
            Err(HsmError::Busy(_))
        ));
        let target = backend
            .prepare(&owner, &archive_key("copy"), &view)
            .await
            .unwrap();
        let evidence = LocalDataMover
            .copy(&owner, &view.source, &target.destination)
            .await
            .unwrap();
        let receipt = backend
            .commit(&owner, &target, &view, &evidence)
            .await
            .unwrap();
        storage
            .commit_archive(&owner, &view, &receipt)
            .await
            .unwrap();
    }
    let storage = LocalStorage::open(source).unwrap();
    // Conditions have changed, but the old step still returns its original failure.
    assert!(matches!(
        storage.prepare_archive(&operation("busy"), &data).await,
        Err(HsmError::Busy(_))
    ));
    assert!(
        storage
            .prepare_archive(&operation("new-decision"), &data)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn archive_corruption_discovered_before_release_preserves_online_copy() {
    let root = tempfile::tempdir().unwrap();
    let storage = LocalStorage::open(root.path().join("source")).unwrap();
    let backend = LocalArchive::open(root.path().join("archive"), "local-archive").unwrap();
    let initial = storage
        .import("a", b"retain this online copy")
        .await
        .unwrap();
    let engine = HsmEngine::new(storage.clone(), backend.clone(), LocalDataMover);
    let dual = engine
        .archive(&operation("archive"), &initial.data, &archive_key("copy"))
        .await
        .unwrap();
    let payload = backend.open(&dual.anchors[0]).await.unwrap();
    std::fs::write(endpoint_path(&payload), b"damaged archive").unwrap();
    assert!(matches!(
        engine.release(&operation("release"), &dual).await,
        Err(HsmError::Integrity(_))
    ));
    assert_eq!(
        storage.read(&initial.data.entity).await.unwrap(),
        b"retain this online copy"
    );
    assert_eq!(storage.inspect(&initial.data.entity).await.unwrap(), dual);
}
