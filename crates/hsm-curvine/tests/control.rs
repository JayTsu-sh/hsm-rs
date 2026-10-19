use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hsm_core::{
    ContentDigest, DataEndpoint, DataKey, DataView, EntityKey, EntityState, HsmError, HsmResult,
    ManagedStorage, OperationContext, Residency,
};
use hsm_curvine::{
    ControlReply, ControlRequest, ControlTransport, CurvineCapabilities, CurvineStorage,
};

#[derive(Clone)]
struct Transport {
    capabilities: CurvineCapabilities,
    calls: Arc<AtomicUsize>,
    state: EntityState,
    mismatched: bool,
    uncertain: bool,
    completed: bool,
}

impl ControlTransport for Transport {
    fn exchange(&self, payload: Vec<u8>) -> impl Future<Output = HsmResult<Vec<u8>>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request: ControlRequest = serde_json::from_slice(&payload).unwrap();
        let reply = match request {
            ControlRequest::Capabilities => ControlReply::Capabilities(self.capabilities.clone()),
            _ if self.uncertain => ControlReply::Error {
                code: "new-unknown-server-error".into(),
                message: "do not compensate".into(),
            },
            ControlRequest::Inspect { .. }
            | ControlRequest::Release { .. }
            | ControlRequest::CommitArchive { .. }
            | ControlRequest::CommitRecall { .. } => {
                let mut state = self.state.clone();
                if self.mismatched {
                    state.data.generation += 1;
                    state.data.entity.entity.push_str("-wrong");
                }
                ControlReply::Entity(state)
            }
            ControlRequest::Completed { .. } => {
                ControlReply::Completed(self.completed.then(|| self.state.clone()))
            }
            ControlRequest::PrepareArchive { data, .. } => ControlReply::View(DataView {
                data,
                view_id: "view".into(),
                length: self.state.length,
                digest: self.state.digest.clone(),
                source: DataEndpoint {
                    adapter: "curvine-view-v1".into(),
                    capability: b"opaque".to_vec(),
                },
            }),
            ControlRequest::PrepareRecall { .. } => ControlReply::Error {
                code: "busy".into(),
                message: "existing attempt".into(),
            },
        };
        std::future::ready(Ok(serde_json::to_vec(&reply).unwrap()))
    }
}

fn fixture(capabilities: CurvineCapabilities) -> Transport {
    Transport {
        capabilities,
        calls: Arc::new(AtomicUsize::new(0)),
        mismatched: false,
        uncertain: false,
        completed: false,
        state: EntityState {
            data: DataKey {
                entity: EntityKey {
                    storage: "curvine-instance".into(),
                    namespace: "incarnation".into(),
                    entity: "inode-42".into(),
                },
                generation: 3,
            },
            revision: 7,
            residency: Residency::Online,
            length: 5,
            digest: ContentDigest {
                algorithm: "blake3".into(),
                value: vec![1; 32],
            },
            anchors: Vec::new(),
        },
    }
}

fn context() -> OperationContext {
    OperationContext {
        operation_id: "op".into(),
        controller_epoch: 1,
        attempt_epoch: 1,
    }
}

#[tokio::test]
async fn baseline_never_sends_unsupported_effects() {
    let transport = fixture(CurvineCapabilities::default());
    let calls = transport.calls.clone();
    let state = transport.state.clone();
    let storage = CurvineStorage::connect(transport).await.unwrap();
    assert!(!storage.capabilities().distributed_authority);
    assert!(matches!(
        storage.inspect(&state.data.entity).await,
        Err(HsmError::Unsupported(_))
    ));
    assert!(matches!(
        storage.prepare_archive(&context(), &state.data).await,
        Err(HsmError::Unsupported(_))
    ));
    assert!(matches!(
        storage.release(&context(), &state).await,
        Err(HsmError::Unsupported(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn enabled_records_use_native_identity_without_path_or_mtime_generation() {
    let transport = fixture(CurvineCapabilities {
        native_records: true,
        ..CurvineCapabilities::default()
    });
    let state = transport.state.clone();
    let storage = CurvineStorage::connect(transport).await.unwrap();
    assert_eq!(storage.inspect(&state.data.entity).await.unwrap(), state);
    assert!(
        storage
            .completed(&context(), &state.data, &hsm_core::ManagedAction::Release)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn rejects_reply_for_another_inode_or_generation() {
    let mut transport = fixture(CurvineCapabilities {
        native_records: true,
        ..CurvineCapabilities::default()
    });
    transport.mismatched = true;
    let entity = transport.state.data.entity.clone();
    let storage = CurvineStorage::connect(transport).await.unwrap();
    assert!(matches!(
        storage.inspect(&entity).await,
        Err(HsmError::Integrity(_))
    ));
}

#[tokio::test]
async fn unsupported_guarantee_combinations_are_rejected() {
    for capabilities in [
        CurvineCapabilities {
            conditional_release: true,
            ..CurvineCapabilities::default()
        },
        CurvineCapabilities {
            capacity_reservation: true,
            ..CurvineCapabilities::default()
        },
        CurvineCapabilities {
            native_records: true,
            transparent_recall: true,
            ..CurvineCapabilities::default()
        },
    ] {
        assert!(matches!(
            CurvineStorage::connect(fixture(capabilities)).await,
            Err(HsmError::Integrity(_))
        ));
    }
}

#[tokio::test]
async fn unknown_server_error_is_never_absent_or_success() {
    let mut transport = fixture(CurvineCapabilities {
        native_records: true,
        ..CurvineCapabilities::default()
    });
    transport.uncertain = true;
    let entity = transport.state.data.entity.clone();
    let storage = CurvineStorage::connect(transport).await.unwrap();
    assert!(matches!(
        storage.inspect(&entity).await,
        Err(HsmError::Unknown(_))
    ));
}

#[tokio::test]
async fn native_ready_view_is_returned_without_opening_latest_path() {
    let transport = fixture(CurvineCapabilities {
        native_records: true,
        stable_views: true,
        ..CurvineCapabilities::default()
    });
    let data = transport.state.data.clone();
    let storage = CurvineStorage::connect(transport).await.unwrap();
    let view = storage.prepare_archive(&context(), &data).await.unwrap();
    assert_eq!(view.data, data);
    assert_eq!(view.source.adapter, "curvine-view-v1");
}

#[test]
fn wire_uses_release_and_recall_names() {
    let state = fixture(CurvineCapabilities::default()).state;
    let request = ControlRequest::Release {
        context: context(),
        expected: state,
    };
    let value = serde_json::to_value(request).unwrap();
    assert_eq!(value["op"], "release");
    assert!(!value.to_string().contains("fid"));
}

#[tokio::test]
async fn release_cannot_accept_an_online_success_reply() {
    let transport = fixture(CurvineCapabilities {
        native_records: true,
        stable_views: true,
        conditional_release: true,
        private_recall: true,
        durable_success_results: true,
        ..CurvineCapabilities::default()
    });
    let state = transport.state.clone();
    let storage = CurvineStorage::connect(transport).await.unwrap();
    assert!(matches!(
        storage.release(&context(), &state).await,
        Err(HsmError::Integrity(_))
    ));
}

#[tokio::test]
async fn invalid_native_residency_is_rejected() {
    let mut transport = fixture(CurvineCapabilities {
        native_records: true,
        ..CurvineCapabilities::default()
    });
    transport.state.residency = Residency::Offline;
    let entity = transport.state.data.entity.clone();
    let storage = CurvineStorage::connect(transport).await.unwrap();
    assert!(matches!(
        storage.inspect(&entity).await,
        Err(HsmError::Integrity(_))
    ));
}

#[tokio::test]
async fn stored_success_must_match_requested_action() {
    let mut transport = fixture(CurvineCapabilities {
        native_records: true,
        ..CurvineCapabilities::default()
    });
    transport.completed = true;
    let data = transport.state.data.clone();
    let storage = CurvineStorage::connect(transport).await.unwrap();
    assert!(matches!(
        storage
            .completed(&context(), &data, &hsm_core::ManagedAction::Release)
            .await,
        Err(HsmError::Integrity(_))
    ));
}
