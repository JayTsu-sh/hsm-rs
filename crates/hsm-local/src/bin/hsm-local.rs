//! Local reference exercise. Creates only private managed data under workdir.
use hsm_core::{ArchiveKey, HsmResult, OperationContext};
use hsm_engine::HsmEngine;
use hsm_local::{LocalArchive, LocalDataMover, LocalStorage};

#[tokio::main]
async fn main() -> HsmResult<()> {
    let root = std::env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .ok_or_else(|| {
            hsm_core::HsmError::Invalid("usage: hsm-local <dedicated-workdir>".into())
        })?;
    let storage = LocalStorage::open(root.join("source"))?;
    let backend = LocalArchive::open(root.join("archive"), "local-demo")?;
    let id = uuid::Uuid::new_v4().to_string();
    let initial = storage
        .import(
            format!("demo-{id}"),
            b"portable HSM: archive, release, recall\n",
        )
        .await?;
    let entity = initial.data.entity.clone();
    let engine = HsmEngine::new(storage.clone(), backend, LocalDataMover);
    let context = |step| OperationContext {
        operation_id: format!("{id}-{step}"),
        controller_epoch: 1,
        attempt_epoch: 1,
    };
    let key = ArchiveKey {
        backend: "local-demo".into(),
        object: id.clone(),
    };
    let dual = engine
        .archive(&context("archive"), &initial.data, &key)
        .await?;
    let offline = engine.release(&context("release"), &dual).await?;
    storage.collect_garbage().await?;
    let restored = engine.recall(&context("recall"), &offline, &key).await?;
    println!(
        "{}",
        serde_json::json!({ "entity": entity, "archive": dual.residency, "release": offline.residency, "recall": restored.residency, "content": String::from_utf8_lossy(&storage.read(&entity).await?) })
    );
    Ok(())
}
