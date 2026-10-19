#![cfg(feature = "native-integration")]

use curvine_fs_api::{Path, Writer};
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use hsm_core::{HsmError, ManagedStorage, OperationContext};
use hsm_curvine::{ControlReply, ControlTransport, CurvineIo, CurvineStorage, NativeControl};
use hsm_local::LocalDataMover;

/// A real Master, Worker and native TCP client, in a private temp directory.
/// This tests native negotiation and ordinary file copy, not a stable HSM view.
#[test]
fn native_control_and_data_mover_copy_use_real_curvine()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = tempfile::tempdir()?;
    let mut conf = curvine_config::ClusterConf::format();
    // All files belong to this test; avoid the upstream Testing helper, which
    // clears fixed shared paths. MiniCluster uses detached server threads.
    conf.testing = true;
    conf.log.log_dir = "stderr".into();
    conf.master.log.log_dir = "stderr".into();
    conf.worker.log.log_dir = "stderr".into();
    conf.journal.enable = false;
    conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
    conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
    conf.worker.data_dir = vec![root.path().join("workers").to_string_lossy().into_owned()];
    conf.master.min_block_size = 1;
    conf.client.short_circuit = false;
    conf.client.replicas = 1;
    conf.client.block_size = 1024 * 1024;
    conf.client.block_size_str = "1MB".into();
    conf.client.read_chunk_size = 64 * 1024;
    conf.client.read_chunk_size_str = "64KB".into();
    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let fs = cluster.new_fs();
    let rt = cluster.clone_client_rt();
    rt.block_on(async {
        fs.mkdir(&Path::from_str("/hsm-adapter")?, true).await?;
        let source = Path::from_str("/hsm-adapter/source.bin")?;
        let bytes: Vec<u8> = (0..3 * 1024 * 1024 + 19).map(|i| (i % 251) as u8).collect();
        let mut writer = fs.create(&source, false).await?;
        writer.write(&bytes).await?;
        writer.complete().await?;
        let before = fs.get_status(&source).await?;
        let native = NativeControl::new(fs.clone());
        // Bypass the client capability gate to verify server-side rejection too.
        let denied = native.exchange(br#"{"op":"release"}"#.to_vec()).await?;
        assert!(matches!(serde_json::from_slice::<ControlReply>(&denied)?,
            ControlReply::Error { code, .. } if code == "unsupported"));
        let storage = CurvineStorage::connect(native).await?;
        assert!(!storage.capabilities().distributed_authority);
        assert!(!storage.native_capabilities().stable_views);
        let fake_entity = hsm_core::EntityKey {
            storage: "not-invented-from-masters".into(),
            namespace: "unavailable".into(),
            entity: before.id.to_string(),
        };
        assert!(matches!(
            storage.inspect(&fake_entity).await,
            Err(HsmError::Unsupported(_))
        ));
        let destination_root = root.path().join("local-copy");
        std::fs::create_dir(&destination_root)?;
        let destination = LocalDataMover::payload_endpoint(&destination_root)?;
        let context = OperationContext {
            operation_id: "native-copy".into(),
            controller_epoch: 1,
            attempt_epoch: 1,
        };
        let evidence = CurvineIo::new(fs.clone())
            .copy_current_file_to_local(&context, &source, &destination)
            .await?;
        assert_eq!(evidence.length, bytes.len() as u64);
        assert_eq!(std::fs::read(destination_root.join("data"))?, bytes);
        let after = fs.get_status(&source).await?;
        assert_eq!(before.id, after.id);
        assert_eq!(before.len, after.len);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
}
