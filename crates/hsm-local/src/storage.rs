use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hsm_core::managed::{
    ArchiveReceipt, DataKey, DataView, EntityKey, EntityState, HsmError, HsmResult, ManagedAction,
    ManagedStorage, OperationContext, RecallTarget, Residency, StorageCapabilities,
    TransferEvidence,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::Mutex;

use crate::util::{
    decode, decode_endpoint, digest_file, endpoint, key, lock_root, new_layout, sync_dir,
    transfer_lock, write_json,
};

/// A private local managed namespace, with native metadata persisted in its root.
/// External programs must not mutate its internal layouts. Single-host reference
/// implementation; fixed epoch 1 deliberately rejects distributed qualification.
#[derive(Clone)]
pub struct LocalStorage(Arc<Inner>);

struct Inner {
    root: PathBuf,
    _owner: File,
    state: Mutex<State>,
    poisoned: AtomicBool,
}

#[derive(Clone, Serialize, Deserialize)]
struct State {
    schema: u32,
    storage_id: String,
    namespace: String,
    entities: HashMap<String, Entity>,
    views: HashMap<String, ViewOwner>,
    recalls: HashMap<String, RecallOwner>,
    steps: HashMap<String, SavedStep>,
    completed: HashMap<String, Completed>,
    gc: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Entity {
    public: EntityState,
    published: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ViewOwner {
    context: OperationContext,
    view: DataView,
}

#[derive(Clone, Serialize, Deserialize)]
struct RecallOwner {
    context: OperationContext,
    target: RecallTarget,
    anchor: ArchiveReceipt,
}

#[derive(Clone, Serialize, Deserialize)]
struct SavedStep {
    fingerprint: String,
    result: Result<serde_json::Value, SavedFailure>,
}

#[derive(Clone, Serialize, Deserialize)]
enum SavedFailure {
    Invalid(String),
    Conflict(String),
    Busy(String),
    NotFound(String),
    Fenced,
    Integrity(String),
    Unsupported(String),
}

impl SavedFailure {
    fn capture(error: &HsmError) -> Option<Self> {
        Some(match error {
            HsmError::Invalid(m) => Self::Invalid(m.clone()),
            HsmError::Conflict(m) => Self::Conflict(m.clone()),
            HsmError::Busy(m) => Self::Busy(m.clone()),
            HsmError::NotFound(m) => Self::NotFound(m.clone()),
            HsmError::Fenced => Self::Fenced,
            HsmError::Integrity(m) => Self::Integrity(m.clone()),
            HsmError::Unsupported(m) => Self::Unsupported(m.clone()),
            HsmError::Unknown(_) | HsmError::Io(_) => return None,
        })
    }

    fn into_error(self) -> HsmError {
        match self {
            Self::Invalid(m) => HsmError::Invalid(m),
            Self::Conflict(m) => HsmError::Conflict(m),
            Self::Busy(m) => HsmError::Busy(m),
            Self::NotFound(m) => HsmError::NotFound(m),
            Self::Fenced => HsmError::Fenced,
            Self::Integrity(m) => HsmError::Integrity(m),
            Self::Unsupported(m) => HsmError::Unsupported(m),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Completed {
    context: OperationContext,
    data: DataKey,
    action: ManagedAction,
    state: EntityState,
}

impl State {
    fn validate_key(&self, entity: &EntityKey) -> HsmResult<()> {
        if entity.storage != self.storage_id
            || entity.namespace != self.namespace
            || entity.entity.is_empty()
        {
            return Err(HsmError::NotFound(
                "entity belongs to a different namespace".into(),
            ));
        }
        Ok(())
    }

    fn entity(&self, data: &DataKey) -> HsmResult<&Entity> {
        self.validate_key(&data.entity)?;
        let entity = self
            .entities
            .get(&data.entity.entity)
            .ok_or_else(|| HsmError::NotFound("entity".into()))?;
        if entity.public.data != *data {
            return Err(HsmError::Conflict("content generation changed".into()));
        }
        Ok(entity)
    }

    fn finish(
        &mut self,
        ctx: &OperationContext,
        data: &DataKey,
        action: ManagedAction,
        state: EntityState,
    ) -> HsmResult<()> {
        if let Some(old) = self.completed.get(&ctx.operation_id)
            && (old.context != *ctx || old.data != *data || old.action != action)
        {
            return Err(HsmError::Conflict(
                "operation ID reused with different input".into(),
            ));
        }
        self.completed.insert(
            ctx.operation_id.clone(),
            Completed {
                context: ctx.clone(),
                data: data.clone(),
                action,
                state,
            },
        );
        Ok(())
    }
}

impl LocalStorage {
    /// Open a dedicated managed root. An OS lock prevents multiple owners.
    pub fn open(root: impl AsRef<Path>) -> HsmResult<Self> {
        let (root, owner) = lock_root(root.as_ref())?;
        fs::create_dir_all(root.join("layouts"))?;
        sync_dir(&root)?;
        let path = root.join("state.json");
        let state: State = if path.exists() {
            decode(&fs::read(&path)?)?
        } else {
            let state = State {
                schema: 1,
                storage_id: uuid::Uuid::new_v4().to_string(),
                namespace: uuid::Uuid::new_v4().to_string(),
                entities: HashMap::new(),
                views: HashMap::new(),
                recalls: HashMap::new(),
                steps: HashMap::new(),
                completed: HashMap::new(),
                gc: Vec::new(),
            };
            write_json(&path, &state)?;
            state
        };
        if state.schema != 1 {
            return Err(HsmError::Unsupported("native metadata schema".into()));
        }
        Ok(Self(Arc::new(Inner {
            root,
            _owner: owner,
            state: Mutex::new(state),
            poisoned: AtomicBool::new(false),
        })))
    }

    fn healthy(&self) -> HsmResult<()> {
        if self.0.poisoned.load(Ordering::Acquire) {
            return Err(HsmError::Unknown(
                "reopen local storage after uncertain metadata commit".into(),
            ));
        }
        Ok(())
    }

    fn qualify(ctx: &OperationContext) -> HsmResult<()> {
        ctx.validate()?;
        if ctx.controller_epoch != 1 || ctx.attempt_epoch != 1 {
            return Err(HsmError::Unsupported(
                "local reference Adapter supports epoch 1 only".into(),
            ));
        }
        Ok(())
    }

    fn persist(&self, state: &State) -> HsmResult<()> {
        if let Err(error) = write_json(&self.0.root.join("state.json"), state) {
            self.0.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    async fn step<T, R>(
        &self,
        ctx: &OperationContext,
        name: &str,
        request: &R,
        effect: impl FnOnce(&mut State) -> HsmResult<T>,
    ) -> HsmResult<T>
    where
        T: Serialize + DeserializeOwned,
        R: Serialize + ?Sized,
    {
        Self::qualify(ctx)?;
        let mut guard = self.0.state.lock().await;
        self.healthy()?;
        let step_key = key(&(ctx, name))?;
        let fingerprint = key(&(ctx, name, request))?;
        if let Some(saved) = guard.steps.get(&step_key) {
            if saved.fingerprint != fingerprint {
                return Err(HsmError::Conflict(
                    "operation step reused with different inputs".into(),
                ));
            }
            return match saved.result.clone() {
                Ok(value) => {
                    serde_json::from_value(value).map_err(|e| HsmError::Integrity(e.to_string()))
                }
                Err(failure) => Err(failure.into_error()),
            };
        }
        let mut next = guard.clone();
        let result = match effect(&mut next) {
            Ok(result) => result,
            Err(error) => {
                if let Some(failure) = SavedFailure::capture(&error) {
                    // Failed conditions never persist partially mutated metadata.
                    let mut failed = guard.clone();
                    failed.steps.insert(
                        step_key,
                        SavedStep {
                            fingerprint,
                            result: Err(failure),
                        },
                    );
                    self.persist(&failed)?;
                    *guard = failed;
                }
                return Err(error);
            }
        };
        let saved = SavedStep {
            fingerprint,
            result: Ok(serde_json::to_value(&result).map_err(|e| HsmError::Invalid(e.to_string()))?),
        };
        next.steps.insert(step_key, saved);
        self.persist(&next)?;
        *guard = next;
        Ok(result)
    }

    /// Create a logical entity inside the managed namespace. Payload is copied;
    /// this never manages or truncates the caller's original file.
    pub async fn import(
        &self,
        entity_id: impl Into<String>,
        bytes: &[u8],
    ) -> HsmResult<EntityState> {
        let entity_id = entity_id.into();
        if entity_id.is_empty() {
            return Err(HsmError::Invalid("entity ID required".into()));
        }
        let mut state = self.0.state.lock().await;
        self.healthy()?;
        if state.entities.contains_key(&entity_id) {
            return Err(HsmError::Conflict("entity already exists".into()));
        }
        let layout = new_layout(&self.0.root.join("layouts"))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(layout.join("data"))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        sync_dir(&layout)?;
        let (length, digest) = digest_file(&layout.join("data"))?;
        write_json(&layout.join("sealed.json"), &digest)?;
        let public = EntityState {
            data: DataKey {
                entity: EntityKey {
                    storage: state.storage_id.clone(),
                    namespace: state.namespace.clone(),
                    entity: entity_id.clone(),
                },
                generation: 1,
            },
            revision: 1,
            residency: Residency::Online,
            length,
            digest,
            anchors: Vec::new(),
        };
        let mut next = state.clone();
        next.entities.insert(
            entity_id,
            Entity {
                public: public.clone(),
                published: Some(
                    layout
                        .file_name()
                        .ok_or_else(|| HsmError::Invalid("layout ID".into()))?
                        .to_string_lossy()
                        .into_owned(),
                ),
            },
        );
        self.persist(&next)?;
        *state = next;
        Ok(public)
    }

    /// Read online bytes through native metadata; Offline is explicit.
    pub async fn read(&self, entity: &EntityKey) -> HsmResult<Vec<u8>> {
        let state = self.0.state.lock().await;
        self.healthy()?;
        state.validate_key(entity)?;
        let entry = state
            .entities
            .get(&entity.entity)
            .ok_or_else(|| HsmError::NotFound("entity".into()))?;
        let layout = entry
            .published
            .as_ref()
            .ok_or_else(|| HsmError::Conflict("offline: request Recall first".into()))?;
        Ok(fs::read(
            self.0.root.join("layouts").join(layout).join("data"),
        )?)
    }

    /// Retry committed local GC. Unknown deletion outcomes are re-observed;
    /// layouts are UUID allocations and are never reused.
    pub async fn collect_garbage(&self) -> HsmResult<usize> {
        let mut state = self.0.state.lock().await;
        self.healthy()?;
        let mut next = state.clone();
        let mut count = 0;
        for layout in &state.gc {
            let path = self.0.root.join("layouts").join(layout);
            if path.exists() {
                let _session = transfer_lock(&path)?;
                fs::remove_dir_all(&path)?;
            }
            count += 1;
        }
        sync_dir(&self.0.root.join("layouts"))?;
        next.gc.clear();
        self.persist(&next)?;
        *state = next;
        Ok(count)
    }
}

impl ManagedStorage for LocalStorage {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            distributed_authority: false,
            durable_success_results: true,
            capacity_reservation: false,
            transparent_recall: false,
        }
    }

    async fn completed(
        &self,
        ctx: &OperationContext,
        data: &DataKey,
        action: &ManagedAction,
    ) -> HsmResult<Option<EntityState>> {
        Self::qualify(ctx)?;
        let state = self.0.state.lock().await;
        self.healthy()?;
        state.validate_key(&data.entity)?;
        match state.completed.get(&ctx.operation_id) {
            Some(old) if old.context == *ctx && old.data == *data && old.action == *action => {
                Ok(Some(old.state.clone()))
            }
            Some(_) => Err(HsmError::Conflict(
                "operation ID reused with different inputs".into(),
            )),
            None => Ok(None),
        }
    }

    async fn inspect(&self, entity: &EntityKey) -> HsmResult<EntityState> {
        let state = self.0.state.lock().await;
        self.healthy()?;
        state.validate_key(entity)?;
        state
            .entities
            .get(&entity.entity)
            .map(|e| e.public.clone())
            .ok_or_else(|| HsmError::NotFound("entity".into()))
    }

    async fn prepare_archive(&self, ctx: &OperationContext, data: &DataKey) -> HsmResult<DataView> {
        self.step(ctx, "prepare_archive", data, |state| {
            let entity = state.entity(data)?;
            let layout = entity
                .published
                .as_ref()
                .ok_or_else(|| HsmError::Conflict("source offline".into()))?;
            if state.views.values().any(|v| v.view.data == *data) {
                return Err(HsmError::Busy("source view already pinned".into()));
            }
            let view = DataView {
                data: data.clone(),
                view_id: uuid::Uuid::new_v4().to_string(),
                length: entity.public.length,
                digest: entity.public.digest.clone(),
                source: endpoint(&self.0.root.join("layouts").join(layout))?,
            };
            state.views.insert(
                view.view_id.clone(),
                ViewOwner {
                    context: ctx.clone(),
                    view: view.clone(),
                },
            );
            Ok(view)
        })
        .await
    }

    async fn commit_archive(
        &self,
        ctx: &OperationContext,
        view: &DataView,
        receipt: &ArchiveReceipt,
    ) -> HsmResult<EntityState> {
        self.step(ctx, "commit_archive", &(view, receipt), |state| {
            let owner = state
                .views
                .get(&view.view_id)
                .ok_or_else(|| HsmError::NotFound("source view".into()))?;
            if owner.context != *ctx || owner.view != *view {
                return Err(HsmError::Fenced);
            }
            let _session = transfer_lock(&decode_endpoint(&view.source)?.root)?;
            state.entity(&view.data)?;
            if receipt.data != view.data
                || receipt.length != view.length
                || receipt.digest != view.digest
                || receipt.receipt_id.is_empty()
                || receipt.durability_profile != "local-fsync-v1"
            {
                return Err(HsmError::Integrity("invalid archive anchor".into()));
            }
            let entity = state
                .entities
                .get_mut(&view.data.entity.entity)
                .ok_or_else(|| HsmError::NotFound("entity".into()))?;
            if entity
                .public
                .anchors
                .iter()
                .any(|a| a.key == receipt.key && a != receipt)
            {
                return Err(HsmError::Conflict(
                    "different receipt for immutable key".into(),
                ));
            }
            if !entity.public.anchors.contains(receipt) {
                entity.public.anchors.push(receipt.clone());
            }
            entity.public.residency = Residency::Dual;
            entity.public.revision += 1;
            let result = entity.public.clone();
            state.views.remove(&view.view_id);
            state.finish(
                ctx,
                &view.data,
                ManagedAction::Archive(receipt.key.clone()),
                result.clone(),
            )?;
            Ok(result)
        })
        .await
    }

    async fn release(
        &self,
        ctx: &OperationContext,
        expected: &EntityState,
    ) -> HsmResult<EntityState> {
        self.step(ctx, "release", expected, |state| {
            let entity = state.entity(&expected.data)?;
            if entity.public != *expected || expected.anchors.is_empty() {
                return Err(HsmError::Conflict(
                    "stale state or no committed anchor".into(),
                ));
            }
            if state.views.values().any(|v| v.view.data == expected.data)
                || state
                    .recalls
                    .values()
                    .any(|r| r.target.data == expected.data)
            {
                return Err(HsmError::Busy("active source/recall reference".into()));
            }
            let layout = entity
                .published
                .clone()
                .ok_or_else(|| HsmError::Conflict("already offline".into()))?;
            let _session = transfer_lock(&self.0.root.join("layouts").join(&layout))?;
            let entity = state
                .entities
                .get_mut(&expected.data.entity.entity)
                .ok_or_else(|| HsmError::NotFound("entity".into()))?;
            entity.published = None;
            entity.public.residency = Residency::Offline;
            entity.public.revision += 1;
            let result = entity.public.clone();
            state.gc.push(layout);
            state.finish(ctx, &expected.data, ManagedAction::Release, result.clone())?;
            Ok(result)
        })
        .await
    }

    async fn prepare_recall(
        &self,
        ctx: &OperationContext,
        expected: &EntityState,
        anchor: &ArchiveReceipt,
    ) -> HsmResult<RecallTarget> {
        self.step(ctx, "prepare_recall", &(expected, anchor), |state| {
            let entity = state.entity(&expected.data)?;
            if entity.public != *expected
                || entity.published.is_some()
                || !entity.public.anchors.contains(anchor)
            {
                return Err(HsmError::Conflict("stale offline state or anchor".into()));
            }
            if state
                .recalls
                .values()
                .any(|r| r.target.data == expected.data)
            {
                return Err(HsmError::Busy("Recall already active".into()));
            }
            let layout = new_layout(&self.0.root.join("layouts"))?;
            let target = RecallTarget {
                data: expected.data.clone(),
                archive: anchor.key.clone(),
                layout_id: layout
                    .file_name()
                    .ok_or_else(|| HsmError::Invalid("layout ID".into()))?
                    .to_string_lossy()
                    .into_owned(),
                destination: endpoint(&layout)?,
            };
            state.recalls.insert(
                target.layout_id.clone(),
                RecallOwner {
                    context: ctx.clone(),
                    target: target.clone(),
                    anchor: anchor.clone(),
                },
            );
            Ok(target)
        })
        .await
    }

    async fn commit_recall(
        &self,
        ctx: &OperationContext,
        target: &RecallTarget,
        evidence: &TransferEvidence,
    ) -> HsmResult<EntityState> {
        self.step(ctx, "commit_recall", &(target, evidence), |state| {
            let owner = state
                .recalls
                .get(&target.layout_id)
                .ok_or_else(|| HsmError::NotFound("private Recall layout".into()))?;
            if owner.context != *ctx || owner.target != *target {
                return Err(HsmError::Fenced);
            }
            let layout = self.0.root.join("layouts").join(&target.layout_id);
            let endpoint = decode_endpoint(&target.destination)?;
            if endpoint.root != layout {
                return Err(HsmError::Fenced);
            }
            let _session = transfer_lock(&layout)?;
            let (length, digest) = digest_file(&layout.join("data"))?;
            let entity = state.entity(&target.data)?;
            if entity.published.is_some() || !entity.public.anchors.contains(&owner.anchor) {
                return Err(HsmError::Conflict("Recall target or anchor changed".into()));
            }
            if evidence.length != length
                || evidence.digest != digest
                || length != entity.public.length
                || digest != entity.public.digest
            {
                return Err(HsmError::Integrity("Recall seal mismatch".into()));
            }
            File::open(layout.join("data"))?.sync_all()?;
            sync_dir(&layout)?;
            write_json(&layout.join("sealed.json"), &target.data)?;
            let entity = state
                .entities
                .get_mut(&target.data.entity.entity)
                .ok_or_else(|| HsmError::NotFound("entity".into()))?;
            entity.published = Some(target.layout_id.clone());
            entity.public.residency = Residency::Dual;
            entity.public.revision += 1;
            let result = entity.public.clone();
            state.recalls.remove(&target.layout_id);
            state.finish(
                ctx,
                &target.data,
                ManagedAction::Recall(target.archive.clone()),
                result.clone(),
            )?;
            Ok(result)
        })
        .await
    }
}
