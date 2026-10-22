#![cfg(feature = "native-integration")]
//! Real processes, three Raft Masters and a file-backed Worker. Restart keeps
//! the original directories, ports and logs, and omits the previous leader.
use curvine_client_core::file::{CurvineFileSystem, hsm_io::hsm_worker_rpc};
use curvine_config::ClusterConf;
use curvine_model::hsm::native::{Endpoint, WorkerRequest};
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use hsm_core::{
    ArchiveKey, DataMover, HsmError, ManagedStorage, OperationContext, Residency, TransferEvidence,
};
use hsm_curvine::{ControlTransport, CurvineDataMover, CurvineStorage, NativeControl};
use hsm_engine::HsmEngine;
use hsm_local::{LocalArchive, LocalDataMover};
use std::{
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
#[ignore = "subprocess fixture; started only by managed_roundtrip_survives_process_restart"]
fn process_cluster_child() -> Result {
    let root = PathBuf::from(std::env::var_os("HSM_CLUSTER_ROOT").ok_or("fixture root missing")?);
    let manifest = root.join("cluster.json");
    let cluster = if manifest.exists() {
        let (mut masters, mut workers): (Vec<ClusterConf>, Vec<ClusterConf>) =
            serde_json::from_slice(&fs::read(&manifest)?)?;
        for conf in masters.iter_mut().chain(workers.iter_mut()) {
            conf.format_master = false;
            conf.format_worker = false;
        }
        if let Ok(skip) = std::env::var("HSM_SKIP_MASTER") {
            masters.retain(|c| c.master.rpc_port.to_string() != skip);
        }
        MiniCluster::new(masters, workers)
    } else {
        let mut conf = ClusterConf::format();
        conf.testing = false;
        conf.master.hsm_enabled = true;
        conf.journal.enable = true;
        conf.journal.writer_channel_size = 0;
        conf.journal.leader_create_snapshot = Some(false);
        conf.journal.snapshot_entries = u64::MAX;
        conf.journal.snapshot_interval = "24h".into();
        if std::env::var_os("HSM_SCALE_FILES").is_some() {
            conf.journal.snapshot_entries = 20_000;
            conf.journal.snapshot_interval = "30s".into();
            conf.journal.snapshot_min_interval = curvine_runtime::common::DurationUnit::new(30_000);
            conf.journal.retain_checkpoint_num = 2;
            conf.client.rpc_retry_max_duration_ms = 120_000;
        }
        conf.journal.raft_tick_interval_ms = 100;
        conf.journal.raft_election_tick = 100;
        conf.journal.raft_min_election_ticks = 100;
        conf.journal.raft_max_election_ticks = 150;
        conf.master.meta_dir = root.join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.join("journal").to_string_lossy().into_owned();
        conf.worker.data_dir = vec![root.join("worker").to_string_lossy().into_owned()];
        conf.client.conn_timeout_ms = 5000;
        conf.client.conn_retry_max_duration_ms = 10000;
        conf.client.rpc_timeout_ms = 30000;
        conf.client.rpc_retry_max_duration_ms = 30000;
        conf.client.short_circuit = false;
        conf.client.replicas = 1;
        if std::env::var_os("HSM_SCALE_FILES").is_some() {
            conf.client.rpc_timeout_ms = 120_000;
            conf.client.rpc_retry_max_duration_ms = 120_000;
        }
        conf.log.log_dir = "stderr".into();
        conf.master.log.log_dir = "stderr".into();
        conf.worker.log.log_dir = "stderr".into();
        let cluster = MiniCluster::with_num(&conf, 3, 1);
        fs::write(
            &manifest,
            serde_json::to_vec(&(cluster.master_conf.clone(), cluster.worker_conf.clone()))?,
        )?;
        cluster
    };
    cluster.start_cluster();
    if let Ok(count) = std::env::var("HSM_SCALE_FILES") {
        if !root.join("scale-seeded").exists() {
            let files = count.parse::<usize>()?;
            let source = cluster.get_active_master_fs();
            let result = cluster
                .clone_client_rt()
                .block_on(source.hsm_test_prefill("/__hsm_scale_fixture__/records", files))?;
            fs::write(
                root.join("scale-prefill.json"),
                serde_json::to_vec_pretty(&result)?,
            )?;
            fs::write(root.join("scale-seeded"), count)?;
        }
    }
    fs::write(root.join("ready"), b"ready")?;
    loop {
        std::thread::park_timeout(Duration::from_secs(30));
    }
}
struct ProcessCluster {
    root: PathBuf,
    child: Option<Child>,
    scale_files: Option<usize>,
}
impl ProcessCluster {
    fn start(root: &Path) -> Result<Self> {
        Self::start_with_scale(root, None)
    }
    fn start_with_scale(root: &Path, scale_files: Option<usize>) -> Result<Self> {
        let mut cluster = Self {
            root: root.to_path_buf(),
            child: None,
            scale_files,
        };
        cluster.spawn(None)?;
        Ok(cluster)
    }
    fn spawn(&mut self, skip: Option<u16>) -> Result {
        let _ = fs::remove_file(self.root.join("ready"));
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("servers.log"))?;
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--ignored",
                "--exact",
                "process_cluster_child",
                "--nocapture",
            ])
            .env("HSM_CLUSTER_ROOT", &self.root)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        if let Some(port) = skip {
            command.env("HSM_SKIP_MASTER", port.to_string());
        }
        if let Some(files) = self.scale_files {
            command.env("HSM_SCALE_FILES", files.to_string());
        } else {
            command.env_remove("HSM_SCALE_FILES");
        }
        self.child = Some(command.spawn()?);
        let deadline = Instant::now()
            + Duration::from_secs(if self.scale_files.is_some() { 7200 } else { 90 });
        while !self.root.join("ready").exists() {
            if let Some(status) = self.child.as_mut().unwrap().try_wait()? {
                return Err(format!(
                    "cluster exited {status}; log {}",
                    self.root.join("servers.log").display()
                )
                .into());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "cluster startup timeout; log {}",
                    self.root.join("servers.log").display()
                )
                .into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }
    fn restart(&mut self, old_leader: &str) -> Result {
        self.stop();
        let port = old_leader
            .rsplit(':')
            .next()
            .ok_or("missing leader port")?
            .parse()?;
        self.spawn(Some(port))
    }
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl Drop for ProcessCluster {
    fn drop(&mut self) {
        self.stop();
        let artifact = std::env::var_os("HSM_TEST_LOG")
            .map_or_else(|| self.root.with_extension("servers.log"), PathBuf::from);
        if fs::copy(self.root.join("servers.log"), &artifact).is_ok() {
            eprintln!("cluster log: {}", artifact.display());
        }
    }
}
fn context(id: &str) -> OperationContext {
    OperationContext {
        operation_id: id.into(),
        controller_epoch: 1,
        attempt_epoch: 1,
    }
}
fn destination(root: &Path, name: &str) -> Result<hsm_core::DataEndpoint> {
    let dir = root.join(name);
    fs::create_dir(&dir)?;
    Ok(LocalDataMover::payload_endpoint(&dir)?)
}

#[test]
fn managed_roundtrip_survives_process_restart() -> Result {
    let root = tempfile::tempdir()?;
    let mut processes = ProcessCluster::start(root.path())?;
    let (masters, _): (Vec<ClusterConf>, Vec<ClusterConf>) =
        serde_json::from_slice(&fs::read(root.path().join("cluster.json"))?)?;
    let conf = masters[0].clone();
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(conf, rt.clone())?;
    rt.block_on(async {
        let parent=curvine_fs_api::Path::from_str("/.hsm")?;
        let path=curvine_fs_api::Path::from_str("/.hsm/source")?;

        let storage=CurvineStorage::connect(NativeControl::new(fs.clone())).await?;
        assert!(storage.capabilities().durable_success_results);
        assert!(storage.capabilities().distributed_authority);
        assert!(storage.native_capabilities().conditional_release);
        assert!(fs.create(&curvine_fs_api::Path::from_str("/.hsm/foreign")?,false).await.is_err());
        let mover=CurvineDataMover::new(fs.clone());
        let archive=LocalArchive::open(root.path().join("archive"),"archive")?;
        let engine=HsmEngine::new(storage.clone(),archive,mover.clone());
        let bytes:Vec<u8>=(0..3*1024*1024+19).map(|i|(i%251) as u8).collect();
        let input=destination(root.path(),"input")?;fs::write(root.path().join("input/data"),&bytes)?;
        let initial=storage.import_file(&context("import"),"/.hsm/source",&input).await?;
        assert_eq!(storage.import_file(&context("import"),"/.hsm/source",&input).await?,initial);
        let status=fs.get_status(&path).await?;assert_eq!(status.id.to_string(),initial.data.entity.entity);assert!(status.is_complete);
        assert_eq!(status.len,bytes.len() as i64);
        assert!(fs.append(&path).await.is_err());assert!(fs.create(&path,true).await.is_err());
        assert!(fs.delete(&parent,true).await.is_err());assert!(fs.free(&parent,true).await.is_err());
        assert!(fs.rename(&parent,&curvine_fs_api::Path::from_str("/moved")?).await.is_err());
        use curvine_fs_api::Reader;
        let mut ordinary = fs.open(&path).await?;
        let mut received = vec![];
        while ordinary.has_remaining() { received.extend_from_slice(ordinary.async_read(None).await?.as_slice()); }
        ordinary.complete().await?;
        assert_eq!(received, bytes);
        let initial = storage.inspect(&initial.data.entity).await?;
        let read=destination(root.path(),"before")?;
        storage.read_to_local(&context("read-before"),&initial.data,&read).await?;
        assert_eq!(fs::read(root.path().join("before/data"))?,bytes);
        let key=ArchiveKey {backend:"archive".into(),object:"immutable-v1".into()};
        assert!(matches!(engine.release(&context("without-anchor"),&initial).await,Err(HsmError::Conflict(_))));
        let dual=engine.archive(&context("archive"),&initial.data,&key).await?;
        assert_eq!(dual.residency,Residency::Dual);
        assert!(matches!(engine.release(&context("archive"),&dual).await,Err(HsmError::Conflict(_))));
        assert_eq!(storage.inspect(&initial.data.entity).await?,dual);
        let pin=storage.prepare_archive(&context("held-pin"),&initial.data).await?;
        assert!(matches!(engine.release(&context("busy-release"),&dual).await,Err(HsmError::Busy(_))));
        // Replay compact rename/recursive attributes/delete and native resize
        // after a cold leader change; GC survives without inode or Record.
        use curvine_fs_api::Writer;
        let before_dir = curvine_fs_api::Path::from_str("/compact-before-restart")?;
        let after_dir = curvine_fs_api::Path::from_str("/compact-after-restart")?;
        let retired_path = curvine_fs_api::Path::from_str("/compact-before-restart/retire/data")?;
        let mut retired_writer = fs.create(&retired_path, true).await?;
        retired_writer.write(b"retired across restart").await?;
        retired_writer.complete().await?;
        let retired_status = fs.get_status(&retired_path).await?;
        let retired_entity = hsm_core::EntityKey {
            entity: retired_status.id.to_string(), ..initial.data.entity.clone()
        };
        assert!(storage.inspect(&retired_entity).await.is_ok());
        let keep_before = curvine_fs_api::Path::from_str("/compact-before-restart/keep")?;
        let mut keep_writer = fs.create(&keep_before, false).await?;
        keep_writer.write(b"keep across restart").await?;
        keep_writer.complete().await?;
        let keep_id = fs.get_status(&keep_before).await?.id;
        fs.resize(&keep_before, curvine_model::FileAllocOpts::with_truncate(4)).await?;
        assert!(fs.rename(&before_dir, &after_dir).await?);
        // Retain the exact request/response across restart. Randomized xattr
        // map iteration must not change a replicated original response.
        let attribute_request = curvine_proto::NativeMutationRequest {
            version: 1, incarnation: initial.data.entity.namespace.clone(),
            session: format!("{:016x}{}", curvine_runtime::common::LocalTime::mills(), curvine_model::hsm::native::token()),
            sequence: 1, acknowledged: 0, code: curvine_fs_api::RpcCode::SetAttr as u32,
            header: curvine_model::ProtoUtils::encode(curvine_proto::SetAttrRequest {
                path: after_dir.encode(),
                opts: curvine_model::ProtoUtils::set_attr_opts_to_pb(curvine_model::SetAttrOpts { recursive: true,
                    owner: Some("restart-policy-owner".into()), mode: Some(0o750),
                    add_x_attr: (0..16).map(|i| (format!("user.restart-{i}"), vec![i])).collect(),
                    ..Default::default() }),
            })?.to_vec(),
        };
        let attribute_response: curvine_proto::NativeMutationResponse = fs.fs_client()
            .rpc(curvine_fs_api::RpcCode::NativeMutation, attribute_request.clone()).await?;
        assert!(attribute_response.success);
        let retired_path = curvine_fs_api::Path::from_str("/compact-after-restart/retire/data")?;
        let retired_dir = curvine_fs_api::Path::from_str("/compact-after-restart/retire")?;
        let keep_after = curvine_fs_api::Path::from_str("/compact-after-restart/keep")?;
        assert_eq!(fs.delete(&retired_dir, true).await?.inodes, 1);
        let old_leader=fs.get_filesystem_info().await?.active_master;
        processes.restart(&old_leader)?;
        eprintln!("phase: restarted on another leader");
        assert!(!fs.exists(&retired_path).await?, "metadata query served the pre-delete prefix after leader restart");
        assert!(!fs.exists(&before_dir).await?);
        assert!(!fs.exists(&retired_dir).await?);
        let keep_status = fs.get_status(&keep_after).await?;
        assert_eq!(keep_status.id, keep_id);
        assert_eq!(keep_status.len, 4);
        assert_eq!(keep_status.owner, "restart-policy-owner");
        assert_eq!(keep_status.mode, 0o750);
        let mut keep_reader = fs.open(&keep_after).await?;
        let mut kept = vec![];
        while keep_reader.has_remaining() { kept.extend_from_slice(keep_reader.async_read(None).await?.as_slice()); }
        keep_reader.complete().await?;
        assert_eq!(kept, b"keep");
        let retried_attributes: curvine_proto::NativeMutationResponse = fs.fs_client()
            .rpc(curvine_fs_api::RpcCode::NativeMutation, attribute_request).await?;
        assert_eq!(retried_attributes, attribute_response,
            "attribute original result changed after replay on another leader");
        assert_eq!(fs.get_status(&after_dir).await?.children_num, 1);
        assert!(matches!(storage.inspect(&retired_entity).await, Err(HsmError::NotFound(_))));
        let gc = NativeControl::new(fs.clone());
        let collect = || gc.exchange(br#"{"op":"gc_batch"}"#.to_vec());
        let first: serde_json::Value = serde_json::from_slice(&collect().await?)?;
        assert_eq!(first["kind"], "gc");
        assert!(first["data"].as_u64().unwrap_or(0) > 0,
            "retirement obligation was lost when the inode deletion replayed");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let response: serde_json::Value = serde_json::from_slice(&collect().await?)?;
            assert_eq!(response["kind"], "gc");
            if response["data"] == 0 { break; }
            assert!(Instant::now() < deadline, "retired blocks were not acknowledged after restart");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(storage.inspect(&initial.data.entity).await?,dual);
        assert_eq!(engine.archive(&context("archive"),&initial.data,&key).await?,dual);
        assert_eq!(storage.pending_view(&context("held-pin"),&initial.data.entity).await?,pin);
        storage.close_view(&context("held-pin"),&pin).await?;
        // Reader/source pins are transient; the same workflow can retry after close.
        let offline=engine.release(&context("busy-release"),&dual).await?;
        assert_eq!(offline.residency,Residency::Offline);
        storage.collect_garbage(&initial.data.entity).await?;
        let old_endpoint:Endpoint=serde_json::from_slice(&pin.source.capability)?;
        let (_,workers):(Vec<ClusterConf>,Vec<ClusterConf>)=serde_json::from_slice(&fs::read(root.path().join("cluster.json"))?)?;
        let worker_root=curvine_config::WorkerDataDir::from_str(&workers[0].worker.data_dir[0])?;
        assert!(!Path::new(&worker_root.path).join("hsm-v1").join(&old_endpoint.allocation).join("data").exists());

        let old_destination=destination(root.path(),"stale-view")?;
        assert!(matches!(mover.copy(&context("stale-view"),&pin.source,&old_destination).await,Err(HsmError::Fenced)));
        assert_eq!(fs.get_status(&path).await?.id,status.id);assert_eq!(fs.get_status(&path).await?.len,status.len);
        assert!(matches!(storage.read_to_local(&context("offline-read"),&initial.data,&destination(root.path(),"offline")?).await,Err(HsmError::Busy(_))));
        let old_leader=fs.get_filesystem_info().await?.active_master;
        processes.restart(&old_leader)?;
        eprintln!("phase: restarted on another leader");
        assert_eq!(storage.inspect(&initial.data.entity).await?,offline);
        assert_eq!(engine.release(&context("busy-release"),&dual).await?,offline);
        let bad=storage.prepare_recall(&context("bad-recall"),&offline,&offline.anchors[0]).await?;
        assert_eq!(storage.pending_recall(&context("bad-recall"),&initial.data.entity).await?,bad);
        let endpoint:Endpoint=serde_json::from_slice(&bad.destination.capability)?;
        hsm_worker_rpc(fs.rpc_conf(),&endpoint.worker,&WorkerRequest::Write {endpoint:endpoint.clone(),offset:0,bytes:b"bad".to_vec()}).await?;
        let expected=TransferEvidence {length:offline.length,digest:offline.digest.clone()};
        assert!(matches!(storage.commit_recall(&context("bad-recall"),&bad,&expected).await,Err(HsmError::Integrity(_))));
        assert_eq!(storage.inspect(&initial.data.entity).await?,offline);
        storage.abort_recall(&context("bad-recall"),&bad).await?;
        assert!(hsm_worker_rpc(fs.rpc_conf(),&endpoint.worker,&WorkerRequest::Write {endpoint:endpoint.clone(),offset:0,bytes:b"bad".to_vec()}).await.is_err());
        let restored=engine.recall(&context("recall"),&offline,&key).await?;
        assert_eq!(restored.residency,Residency::Dual);assert_eq!(restored.data,initial.data);
        assert_eq!(engine.recall(&context("recall"),&offline,&key).await?,restored);
        let read=destination(root.path(),"after")?;
        storage.read_to_local(&context("read-after"),&initial.data,&read).await?;
        assert_eq!(fs::read(root.path().join("after/data"))?,bytes);
        assert_eq!(fs.get_status(&path).await?.id,status.id);
        assert_eq!(storage.controller_epoch().await?,1);
        assert_eq!(storage.claim_controller(&context("claim"),1).await?,2);
        assert_eq!(storage.claim_controller(&context("claim"),1).await?,2);
        let old_leader=fs.get_filesystem_info().await?.active_master;
        processes.restart(&old_leader)?;
        assert_eq!(storage.controller_epoch().await?,2);
        let mut qualified=context("recall");qualified.controller_epoch=2;
        assert_eq!(engine.recall(&qualified,&offline,&key).await?,restored);

        assert!(matches!(storage.release(&context("old-controller"),&restored).await,Err(HsmError::Fenced)));
        assert!(matches!(storage.import_file(&context("old-import"),"/.hsm/another",&input).await,Err(HsmError::Fenced)));
        println!("verified: native Online -> Dual -> Offline -> Dual; same inode/generation, 3 MiB+19 bytes; three process restarts omitting old leader; persistent pin, GC, corrupt Recall and qualification fencing");
        Ok::<(),Box<dyn std::error::Error+Send+Sync>>(())
    })
}

fn cli(root: &Path, args: &[&str], success: bool) -> Result<Vec<serde_json::Value>> {
    let output = Command::new(env!("CARGO_BIN_EXE_hsm-curvine"))
        .args([
            "--config",
            root.join("client.json").to_str().unwrap(),
            "--archive-root",
            root.join("archive").to_str().unwrap(),
            "--backend-id",
            "local",
        ])
        .args(args)
        .output()?;
    assert_eq!(
        output.status.success(),
        success,
        "CLI {args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect())
}
fn policy_file(
    root: &Path,
    id: &str,
    action: hsm_core::PolicyAction,
    prefix: &str,
    min_size: u64,
) -> Result<hsm_core::LifecyclePolicy> {
    let policy = hsm_core::LifecyclePolicy {
        selection: Default::default(),
        id: id.into(),
        revision: 1,
        enabled: true,
        prefix: prefix.into(),
        min_size,
        max_size: None,
        min_residency_ms: 0,
        action,
        backend: "local".into(),
    };
    fs::write(
        root.join(format!("{id}.json")),
        serde_json::to_vec(&policy)?,
    )?;
    Ok(policy)
}

// Keep the real restart/CLI phases in one fixture; each phase depends on its predecessor.
#[allow(clippy::too_many_lines)]
#[test]
fn local_file_policies_survive_takeover_and_execute_through_cli() -> Result {
    use hsm_core::{
        ArchiveBackend, IntentCompletion, IntentStatus, LifecycleStorage, PolicyAction,
    };
    let root = tempfile::tempdir()?;
    let mut processes = ProcessCluster::start(root.path())?;
    let (masters, _): (Vec<ClusterConf>, Vec<ClusterConf>) =
        serde_json::from_slice(&fs::read(root.path().join("cluster.json"))?)?;
    fs::write(
        root.path().join("client.json"),
        serde_json::to_vec(&masters[0])?,
    )?;
    let bytes: Vec<u8> = (0..3 * 1024 * 1024 + 19).map(|i| (i % 251) as u8).collect();
    for (name, data) in [
        ("cold-a", bytes.as_slice()),
        ("cold-b", &bytes[..4099]),
        ("cold-small", &b"small"[..]),
        ("hot-a", &b"hot"[..]),
    ] {
        let input = root.path().join(format!("{name}.bin"));
        fs::write(&input, data)?;
        cli(
            root.path(),
            &[
                "--op-id",
                name,
                "import",
                input.to_str().unwrap(),
                &format!("/.hsm/{name}"),
            ],
            true,
        )?;
    }
    let mut archive_policy = policy_file(
        root.path(),
        "archive",
        PolicyAction::Archive,
        "/.hsm/cold-",
        1000,
    )?;
    archive_policy.enabled = false;
    fs::write(
        root.path().join("archive.json"),
        serde_json::to_vec(&archive_policy)?,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("archive.json").to_str().unwrap(),
        ],
        true,
    )?;
    assert_eq!(
        cli(root.path(), &["scan", "archive"], true)?[0]["admitted"],
        serde_json::json!([])
    );
    archive_policy.enabled = true;
    archive_policy.revision = 2;
    fs::write(
        root.path().join("archive.json"),
        serde_json::to_vec(&archive_policy)?,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("archive.json").to_str().unwrap(),
            "--expected-revision",
            "1",
        ],
        true,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("archive.json").to_str().unwrap(),
            "--expected-revision",
            "1",
        ],
        false,
    )?;
    let mut age = policy_file(
        root.path(),
        "age",
        PolicyAction::Archive,
        "/.hsm/cold-",
        1000,
    )?;
    age.min_residency_ms = 365 * 24 * 60 * 60 * 1000;
    fs::write(root.path().join("age.json"), serde_json::to_vec(&age)?)?;
    cli(
        root.path(),
        &["policy-put", root.path().join("age.json").to_str().unwrap()],
        true,
    )?;
    assert_eq!(
        cli(root.path(), &["scan", "age"], true)?[0]["admitted"],
        serde_json::json!([])
    );

    let rt = Arc::new(masters[0].client_rpc_conf().create_runtime());
    let fs_client = CurvineFileSystem::with_rt(masters[0].clone(), rt.clone())?;
    rt.block_on(async {
        let storage = CurvineStorage::connect(NativeControl::new(fs_client.clone())).await?;
        let mut after = None;
        let mut admitted = vec![];
        let mut examined = 0;
        loop {
            let batch = storage
                .scan_policy(
                    &context(&format!("scan-{examined}")),
                    "archive",
                    after.as_deref(),
                    1,
                )
                .await?;
            assert!(batch.examined <= 1);
            examined += batch.examined;
            admitted.extend(batch.admitted);
            after = batch.next;
            if after.is_none() {
                break;
            }
        }
        assert_eq!(examined, 4);
        assert_eq!(admitted.len(), 2);
        assert!(
            storage
                .scan_policy(&context("dedupe"), "archive", None, 16)
                .await?
                .admitted
                .is_empty()
        );
        let overlap = policy_file(
            root.path(),
            "overlap",
            PolicyAction::Archive,
            "/.hsm/cold-",
            1000,
        )?;
        storage
            .put_policy(&context("overlap-put"), &overlap, None)
            .await?;
        assert!(
            storage
                .scan_policy(&context("overlap-scan"), "overlap", None, 16)
                .await?
                .admitted
                .is_empty()
        );
        let first = storage
            .claim_intent(&context("claim-first"), &admitted[0], "before-crash")
            .await?;
        assert!(matches!(
            storage
                .claim_intent(&context("competing-claim"), &first.id, "other")
                .await,
            Err(HsmError::Busy(_))
        ));
        assert!(matches!(
            storage
                .finish_intent(&context("fake-success"), &first, IntentCompletion::Done)
                .await,
            Err(HsmError::Busy(_))
        ));
        let second = storage
            .claim_intent(&context("claim-second"), &admitted[1], "before-crash")
            .await?;
        let ready = storage
            .finish_intent(
                &context("uncertain-report"),
                &second,
                IntentCompletion::Retry("injected transport uncertainty".into()),
            )
            .await?;
        assert_eq!(ready.status, IntentStatus::Ready);
        assert_eq!(ready.context.operation_id, second.context.operation_id);
        let second = storage
            .claim_intent(&context("reclaim-second"), &second.id, "before-crash")
            .await?;
        let archive = LocalArchive::open(root.path().join("archive"), "local")?;
        let mover = CurvineDataMover::new(fs_client.clone());
        let engine = HsmEngine::new(storage.clone(), archive.clone(), mover.clone());
        let hsm_core::ManagedAction::Archive(first_key) = &first.action else {
            panic!("archive intent")
        };
        let original = engine
            .archive(&first.context, &first.expected.data, first_key)
            .await?;
        // Crash window A: source effect committed, task finish not persisted.
        let hsm_core::ManagedAction::Archive(second_key) = &second.action else {
            panic!("archive intent")
        };
        let view = storage
            .prepare_archive(&second.context, &second.expected.data)
            .await?;
        let target = archive.prepare(&second.context, second_key, &view).await?;
        mover
            .copy(&second.context, &view.source, &target.destination)
            .await?;
        let descriptor: serde_json::Value = serde_json::from_slice(&target.destination.capability)?;
        let payload_root = PathBuf::from(descriptor["root"].as_str().ok_or("local root missing")?);
        fs::File::open(payload_root.join("data"))?.sync_all()?;
        fs::write(
            payload_root.join("sealed.json"),
            serde_json::to_vec(&(second_key, &view.data, &view.digest))?,
        )?;
        fs::File::open(payload_root.join("sealed.json"))?.sync_all()?;
        fs::File::open(&payload_root)?.sync_all()?;
        // Crash window B: data sealed, backend receipt not published; no source anchor.
        drop(engine);
        drop(archive);
        let leader = fs_client.get_filesystem_info().await?.active_master;
        processes.restart(&leader)?;
        assert_eq!(
            storage
                .intent(&context("persisted-intent"), &first.id)
                .await?
                .status,
            IntentStatus::Running
        );
        assert_eq!(
            storage
                .policies(&context("persisted-policy"), None, 16)
                .await?
                .items
                .iter()
                .find(|p| p.id == "archive")
                .unwrap()
                .revision,
            2
        );
        cli(root.path(), &["worker", "--once"], true)?; // current epoch cannot steal running work.
        cli(root.path(), &["takeover", "--expected-epoch", "1"], true)?;
        assert!(matches!(
            storage
                .finish_intent(&context("old-finish"), &first, IntentCompletion::Done)
                .await,
            Err(HsmError::Fenced)
        ));
        cli(root.path(), &["worker", "--once"], true)?;
        let mut ctx = context("recovered");
        ctx.controller_epoch = 2;
        let done = storage.intent(&ctx, &first.id).await?;
        assert_eq!(done.status, IntentStatus::Done);
        assert_eq!(done.result, Some(original));
        let done = storage.intent(&ctx, &second.id).await?;
        assert_eq!(done.status, IntentStatus::Done);
        assert_eq!(
            fs::read_dir(root.path().join("archive/objects"))?.count(),
            2
        );
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;
    // Separate CLI invocations reopen the backend and coordinator for every phase.
    let release = policy_file(
        root.path(),
        "release",
        PolicyAction::Release,
        "/.hsm/cold-",
        1000,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("release.json").to_str().unwrap(),
        ],
        true,
    )?;
    // Corruption must fail closed, retaining that online source copy.
    let object: serde_json::Value = serde_json::from_slice(&fs::read(
        fs::read_dir(root.path().join("archive/objects"))?
            .next()
            .unwrap()?
            .path(),
    )?)?;
    let descriptor: serde_json::Value =
        serde_json::from_value(object["payload"]["capability"].clone())
            .and_then(|bytes: Vec<u8>| serde_json::from_slice(&bytes))?;
    let corrupt = PathBuf::from(descriptor["root"].as_str().unwrap()).join("data");
    let saved = fs::read(&corrupt)?;
    fs::write(&corrupt, b"corrupt")?;
    cli(root.path(), &["worker", "--once"], false)?;
    let jobs = cli(root.path(), &["jobs"], true)?;
    assert!(jobs.iter().any(|i| i["status"] == "failed"));
    let states = [
        cli(root.path(), &["status", "/.hsm/cold-a"], true)?[0].clone(),
        cli(root.path(), &["status", "/.hsm/cold-b"], true)?[0].clone(),
    ];
    assert_eq!(
        states.iter().filter(|s| s["residency"] == "Dual").count(),
        1
    );
    assert_eq!(
        states
            .iter()
            .filter(|s| s["residency"] == "Offline")
            .count(),
        1
    );
    fs::write(&corrupt, saved)?;
    let mut revised = release;
    revised.revision = 2;
    fs::write(
        root.path().join("release.json"),
        serde_json::to_vec(&revised)?,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("release.json").to_str().unwrap(),
            "--expected-revision",
            "1",
        ],
        true,
    )?;
    cli(root.path(), &["worker", "--once"], true)?;
    for name in ["cold-a", "cold-b"] {
        assert_eq!(
            cli(root.path(), &["status", &format!("/.hsm/{name}")], true)?[0]["residency"],
            "Offline"
        );
    }
    cli(
        root.path(),
        &[
            "read",
            "/.hsm/cold-a",
            root.path().join("offline-output").to_str().unwrap(),
        ],
        false,
    )?;
    // Disable the revised release rule before the explicit restore policy.
    revised.revision = 3;
    revised.enabled = false;
    fs::write(
        root.path().join("release.json"),
        serde_json::to_vec(&revised)?,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("release.json").to_str().unwrap(),
            "--expected-revision",
            "2",
        ],
        true,
    )?;
    policy_file(
        root.path(),
        "recall",
        PolicyAction::Recall,
        "/.hsm/cold-",
        1000,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("recall.json").to_str().unwrap(),
        ],
        true,
    )?;
    cli(root.path(), &["worker", "--once"], true)?;
    for (name, expected) in [("cold-a", bytes.as_slice()), ("cold-b", &bytes[..4099])] {
        let output = root.path().join(format!("{name}.restored"));
        cli(
            root.path(),
            &["read", &format!("/.hsm/{name}"), output.to_str().unwrap()],
            true,
        )?;
        assert_eq!(fs::read(output)?, expected);
        assert_eq!(
            cli(root.path(), &["status", &format!("/.hsm/{name}")], true)?[0]["residency"],
            "Dual"
        );
    }
    let mut cancel = policy_file(
        root.path(),
        "cancel",
        PolicyAction::Archive,
        "/.hsm/hot-",
        0,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("cancel.json").to_str().unwrap(),
        ],
        true,
    )?;
    let cancelled = cli(root.path(), &["scan", "cancel"], true)?;
    assert_eq!(cancelled[0]["admitted"].as_array().unwrap().len(), 1);
    cancel.enabled = false;
    cancel.revision = 2;
    fs::write(
        root.path().join("cancel.json"),
        serde_json::to_vec(&cancel)?,
    )?;
    cli(
        root.path(),
        &[
            "policy-put",
            root.path().join("cancel.json").to_str().unwrap(),
            "--expected-revision",
            "1",
        ],
        true,
    )?;
    cli(root.path(), &["worker", "--once"], true)?;
    assert!(
        cli(root.path(), &["jobs"], true)?
            .iter()
            .any(|i| i["policy"] == "cancel" && i["status"] == "superseded")
    );
    assert_eq!(
        cli(root.path(), &["worker", "--once"], true)?
            .last()
            .unwrap()["executed"],
        0
    );
    assert_eq!(
        cli(root.path(), &["status", "/.hsm/hot-a"], true)?[0]["residency"],
        "Online"
    );
    assert_eq!(
        cli(root.path(), &["status", "/.hsm/cold-small"], true)?[0]["residency"],
        "Online"
    );
    Ok(())
}

/// Normal SDK I/O uses the same path as FUSE: no native import and no Recall policy.
#[test]
fn ordinary_files_are_tiered_by_policy_and_recalled_by_concurrent_reads() -> Result {
    use curvine_fs_api::{Path as CvPath, Reader, Writer};
    use hsm_core::{LifecyclePolicy, PolicyAction, PolicySelection};
    struct WorkerProcess(Child);
    impl Drop for WorkerProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root = tempfile::tempdir()?;
    let _cluster = ProcessCluster::start(root.path())?;
    let (masters, _): (Vec<ClusterConf>, Vec<ClusterConf>) =
        serde_json::from_slice(&fs::read(root.path().join("cluster.json"))?)?;
    let config = root.path().join("client.json");
    fs::write(&config, serde_json::to_vec(&masters[0])?)?;
    let rt = Arc::new(masters[0].client_rpc_conf().create_runtime());
    let source = CurvineFileSystem::with_rt(masters[0].clone(), rt.clone())?;
    rt.block_on(async {
        let storage = CurvineStorage::connect(NativeControl::new(source.clone())).await?;
        let epoch = storage.controller_epoch().await?;
        let ctx = |id: &str| OperationContext {
            operation_id: id.into(),
            controller_epoch: epoch,
            attempt_epoch: 1,
        };
        source.mkdir(&CvPath::from_str("/ordinary")?, true).await?;
        let a: Vec<u8> = (0..2 * 1024 * 1024 + 19).map(|i| (i % 251) as u8).collect();
        let b: Vec<u8> = (0..1024 * 1024 + 71).map(|i| (i % 239) as u8).collect();
        for (name, bytes) in [
            ("a", a.as_slice()),
            ("b", b.as_slice()),
            ("small", b"small".as_slice()),
            ("excluded", a.as_slice()),
        ] {
            let mut writer = source
                .create(&CvPath::from_str(format!("/ordinary/{name}"))?, false)
                .await?;
            writer.write(bytes).await?;
            writer.complete().await?;
        }
        let initial = storage.resolve("/ordinary/a").await?;
        assert_eq!(initial.residency, Residency::Online);
        assert_eq!(initial.digest.algorithm, "unsealed");
        // Simulate a coordinator stopping after it pins a selected native generation.
        // The new CLI worker must find and resume this source-owned seal.
        let token = format!(
            "seal-{}-{}",
            initial.data.entity.entity, initial.data.generation
        );
        let pinned = NativeControl::new(source.clone())
            .exchange(serde_json::to_vec(&serde_json::json!({
                "op":"seal_begin", "entity":initial.data.entity, "data":initial.data, "token":token
            }))?)
            .await?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&pinned)?["kind"],
            "blocks"
        );
        // No policy exists yet: a fresh process must finish the persisted seal independently.
        cli(root.path(), &["worker", "--once"], true)?;
        assert_eq!(
            storage.resolve("/ordinary/a").await?.digest.algorithm,
            "blake3"
        );
        let policy = LifecyclePolicy {
            id: "ordinary-release".into(),
            revision: 1,
            enabled: true,
            prefix: "/ordinary/".into(),
            min_size: 1024 * 1024,
            max_size: None,
            min_residency_ms: 0,
            action: PolicyAction::Release,
            backend: "local".into(),
            selection: PolicySelection {
                scan_interval_ms: 100,
                min_access_age_ms: 3000,
                exclude_prefixes: vec!["/ordinary/excluded".into()],
                ..Default::default()
            },
        };
        storage
            .put_policy(&ctx("ordinary-policy"), &policy, None)
            .await?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.path().join("worker.log"))?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_hsm-curvine"));
        command
            .arg("--config")
            .arg(&config)
            .arg("--archive-root")
            .arg(root.path().join("archive"))
            .args(["worker", "--poll-ms", "100"])
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        let _worker = WorkerProcess(command.spawn()?);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if storage.resolve("/ordinary/a").await?.residency == Residency::Offline
                && storage.resolve("/ordinary/b").await?.residency == Residency::Offline
            {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "ordinary release timeout: {}",
                    fs::read_to_string(root.path().join("worker.log"))?
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            storage.resolve("/ordinary/small").await?.residency,
            Residency::Online
        );
        assert_eq!(
            storage.resolve("/ordinary/excluded").await?.residency,
            Residency::Online
        );
        async fn read_all(fs: &CurvineFileSystem, path: &str) -> Result<Vec<u8>> {
            let mut reader = fs.open(&CvPath::from_str(path)?).await?;
            let mut bytes = vec![];
            while reader.has_remaining() {
                bytes.extend_from_slice(reader.async_read(None).await?.as_slice());
            }
            reader.complete().await?;
            Ok(bytes)
        }
        // The application only reads; the storage authority creates the Recall intent.
        let (one, two, three) = tokio::join!(
            read_all(&source, "/ordinary/a"),
            read_all(&source, "/ordinary/a"),
            read_all(&source, "/ordinary/b")
        );
        assert_eq!(one?, a);
        assert_eq!(two?, a);
        assert_eq!(three?, b);
        let recalled = storage.resolve("/ordinary/a").await?;
        assert_eq!(recalled.data, initial.data);
        assert_eq!(recalled.residency, Residency::Dual);
        // Stop further cold release while checking ordinary replacement semantics.
        let mut disabled = policy;
        disabled.enabled = false;
        disabled.revision = 2;
        storage
            .put_policy(&ctx("disable-ordinary-policy"), &disabled, Some(1))
            .await?;
        let replacement = b"new ordinary generation after recall";
        let mut writer = source
            .create(&CvPath::from_str("/ordinary/a")?, true)
            .await?;
        writer.write(replacement).await?;
        writer.complete().await?;
        assert_eq!(read_all(&source, "/ordinary/a").await?, replacement);
        let rewritten = storage.resolve("/ordinary/a").await?;
        assert!(rewritten.data.generation > initial.data.generation);
        assert!(rewritten.anchors.is_empty());
        assert_eq!(rewritten.residency, Residency::Online);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
}

#[test]
fn ordinary_access_fences_stale_release_and_allows_later_readmission() -> Result {
    use curvine_fs_api::{Path as CvPath, Reader, Writer};
    use hsm_core::{IntentStatus, LifecyclePolicy, PolicyAction, PolicySelection};
    let root = tempfile::tempdir()?;
    let _cluster = ProcessCluster::start(root.path())?;
    let (masters, _): (Vec<ClusterConf>, Vec<ClusterConf>) =
        serde_json::from_slice(&fs::read(root.path().join("cluster.json"))?)?;
    let rt = Arc::new(masters[0].client_rpc_conf().create_runtime());
    let source = CurvineFileSystem::with_rt(masters[0].clone(), rt.clone())?;
    rt.block_on(async {
        let storage = CurvineStorage::connect(NativeControl::new(source.clone())).await?;
        let epoch = storage.controller_epoch().await?;
        let ctx = |id: &str| OperationContext {
            operation_id: id.into(),
            controller_epoch: epoch,
            attempt_epoch: 1,
        };
        let path = CvPath::from_str("/read-fence")?;
        let mut writer = source.create(&path, false).await?;
        writer.write(&vec![73; 1024 * 1024]).await?;
        writer.complete().await?;
        let initial = storage.resolve("/read-fence").await?;
        storage.seal_ordinary(&initial.data).await?;
        let compact = storage
            .prepare_archive(&ctx("compact-view-probe"), &initial.data)
            .await?;
        assert_eq!(compact.source.adapter, "curvine-block-view-v2");
        assert!(compact.source.capability.len() < 1024);
        storage
            .close_view(&ctx("compact-view-probe"), &compact)
            .await?;
        let stale = destination(root.path(), "stale-compact-view")?;
        assert!(matches!(
            CurvineDataMover::new(source.clone())
                .copy(&ctx("stale-compact-copy"), &compact.source, &stale)
                .await,
            Err(HsmError::Fenced)
        ));
        let engine = HsmEngine::new(
            storage.clone(),
            LocalArchive::open(root.path().join("archive"), "local")?,
            CurvineDataMover::new(source.clone()),
        );
        let dual = engine
            .archive(
                &ctx("archive-read-fence"),
                &initial.data,
                &ArchiveKey {
                    backend: "local".into(),
                    object: "read-fence".into(),
                },
            )
            .await?;
        let policy = LifecyclePolicy {
            id: "read-fence".into(),
            revision: 1,
            enabled: true,
            prefix: "/read-fence".into(),
            min_size: 1,
            max_size: None,
            min_residency_ms: 0,
            action: PolicyAction::Release,
            backend: "local".into(),
            selection: PolicySelection {
                min_access_age_ms: 500,
                ..Default::default()
            },
        };
        storage
            .put_policy(&ctx("read-fence-policy"), &policy, None)
            .await?;
        tokio::time::sleep(Duration::from_millis(600)).await;
        let batch = storage
            .scan_policy(&ctx("before-access-scan"), "read-fence", None, 16)
            .await?;
        assert_eq!(batch.admitted.len(), 1);
        let mut reader = source.open(&path).await?;
        assert!(!reader.async_read(Some(1)).await?.is_empty());
        reader.complete().await?;
        let observed = storage.resolve("/read-fence").await?;
        assert!(observed.revision > dual.revision);
        assert_eq!(observed.residency, Residency::Dual);
        tokio::time::sleep(Duration::from_millis(600)).await;
        let superseded = engine
            .run_intent(
                &ctx("claim-stale-release"),
                &batch.admitted[0],
                "reader-test",
            )
            .await?;
        assert_eq!(superseded.status, IntentStatus::Superseded);
        assert_eq!(
            storage.resolve("/read-fence").await?.residency,
            Residency::Dual
        );
        let next = storage
            .scan_policy(&ctx("after-access-scan"), "read-fence", None, 16)
            .await?;
        assert_eq!(next.admitted.len(), 1);
        assert_ne!(next.admitted[0], batch.admitted[0]);
        let done = engine
            .run_intent(
                &ctx("claim-fresh-release"),
                &next.admitted[0],
                "reader-test",
            )
            .await?;
        assert_eq!(done.status, IntentStatus::Done);
        assert_eq!(
            storage.resolve("/read-fence").await?.residency,
            Residency::Offline
        );
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
}

#[test]
fn ordinary_rename_and_truncate_preserve_authority_after_leader_restart() -> Result {
    use curvine_fs_api::{Path as CvPath, Reader, Writer};
    let root = tempfile::tempdir()?;
    let mut cluster = ProcessCluster::start(root.path())?;
    let (masters, _): (Vec<ClusterConf>, Vec<ClusterConf>) =
        serde_json::from_slice(&fs::read(root.path().join("cluster.json"))?)?;
    let rt = Arc::new(masters[0].client_rpc_conf().create_runtime());
    let source = CurvineFileSystem::with_rt(masters[0].clone(), rt.clone())?;
    rt.block_on(async {
        let storage = CurvineStorage::connect(NativeControl::new(source.clone())).await?;
        storage.controller_epoch().await?;
        let path = CvPath::from_str("/ordinary-before-rename")?;
        let renamed = CvPath::from_str("/ordinary-after-rename")?;
        let mut writer = source.create(&path, false).await?;
        writer.write(&vec![31; 1024 * 1024]).await?;
        writer.complete().await?;
        let initial = storage.resolve(path.path()).await?;
        storage.seal_ordinary(&initial.data).await?;
        let engine = HsmEngine::new(
            storage.clone(),
            LocalArchive::open(root.path().join("archive"), "local")?,
            CurvineDataMover::new(source.clone()),
        );
        let dual = engine
            .archive(
                &context("archive-before-rename"),
                &initial.data,
                &ArchiveKey {
                    backend: "local".into(),
                    object: "rename-truncate".into(),
                },
            )
            .await?;
        source.rename(&path, &renamed).await?;
        let moved = storage.resolve(renamed.path()).await?;
        assert_eq!(moved.data, dual.data);
        assert_eq!(moved.residency, Residency::Dual);
        source
            .resize(&renamed, curvine_model::FileAllocOpts::with_truncate(4096))
            .await?;
        let changed = storage.resolve(renamed.path()).await?;
        assert!(changed.data.generation > initial.data.generation);
        assert_eq!(changed.length, 4096);
        assert!(changed.anchors.is_empty());
        assert_eq!(changed.residency, Residency::Online);
        let leader = source.get_filesystem_info().await?.active_master;
        cluster.restart(&leader)?;
        assert_eq!(storage.resolve(renamed.path()).await?, changed);
        let mut reader = source.open(&renamed).await?;
        let mut bytes = vec![];
        while reader.has_remaining() {
            bytes.extend_from_slice(reader.async_read(None).await?.as_slice());
        }
        reader.complete().await?;
        assert_eq!(bytes, vec![31; 4096]);
        source.delete(&renamed, false).await?;
        assert!(storage.resolve(renamed.path()).await.is_err());
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
}

/// Expensive, opt-in scale acceptance: real namespace + Raft + ordinary SDK I/O.
/// Setup batches normal internal create/complete calls; it does not measure
/// ordinary-RPC ingestion throughput. All three Masters run in one fixture
/// process with independent stores, and the HSM controller is a separate process.
#[test]
#[ignore = "million-file namespace, policy scan, local mover and checkpoint recovery"]
fn million_namespace_policy_recall_and_checkpoint_recovery() -> Result {
    use curvine_fs_api::{Path as CvPath, Reader, Writer};
    use hsm_core::{IntentStatus, LifecyclePolicy, PolicyAction, PolicySelection};
    struct Worker(Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let count = std::env::var("HSM_SCALE_FILES")
        .unwrap_or_else(|_| "1000000".into())
        .parse::<usize>()?;
    assert!((1..=1_000_000).contains(&count));
    let temp = tempfile::tempdir()?;
    let root =
        std::env::var_os("HSM_SCALE_ROOT").map_or_else(|| temp.path().to_path_buf(), PathBuf::from);
    fs::create_dir_all(&root)?;
    assert!(
        fs::read_dir(&root)?.next().is_none(),
        "scale root must be empty"
    );
    let begin = Instant::now();
    let mut cluster = ProcessCluster::start_with_scale(&root, Some(count))?;
    let (masters, workers): (Vec<ClusterConf>, Vec<ClusterConf>) =
        serde_json::from_slice(&fs::read(root.join("cluster.json"))?)?;
    let config = root.join("client.json");
    fs::write(&config, serde_json::to_vec(&masters[0])?)?;
    let rt = Arc::new(masters[0].client_rpc_conf().create_runtime());
    let source = CurvineFileSystem::with_rt(masters[0].clone(), rt.clone())?;
    let mut report = serde_json::json!({"kind":"real-curvine-namespace-scale","namespace_files":count,"payload_files":2,
        "masters":3,"workers":1,"server_processes":1,"controller_processes":1,"pve_cluster":false,
        "prefill":serde_json::from_slice::<serde_json::Value>(&fs::read(root.join("scale-prefill.json"))?)?});
    rt.block_on(async {
        // Verify through the public SDK, rather than trusting a fixture counter.
        let start=Instant::now();let mut files=0usize;
        for part in 0..count.div_ceil(1000) {
            let path=CvPath::from_str(format!("/__hsm_scale_fixture__/records/part-{part:06}"))?;
            let children=source.list_status(&path).await?;
            assert_eq!(children.len(),(count-part*1000).min(1000));
            assert!(children.iter().all(|s|s.len==0&&s.is_complete));files+=children.len();
        }
        assert_eq!(files,count);report["sdk_count_ms"]=serde_json::json!(start.elapsed().as_millis());
        let storage=CurvineStorage::connect(NativeControl::new(source.clone())).await?;
        let epoch=storage.controller_epoch().await?;
        let ctx=|id:&str|OperationContext{operation_id:id.into(),controller_epoch:epoch,attempt_epoch:1};
        for id in [0,count/2,count-1] {
            let path=format!("/__hsm_scale_fixture__/records/part-{:06}/item-{id:012}",id/1000);
            let state=storage.resolve(&path).await?;assert_eq!(state.residency,Residency::Online);assert_eq!(state.length,0);
        }
        source.mkdir(&CvPath::from_str("/scale-live")?,true).await?;
        let a:Vec<u8>=(0..1024*1024+17).map(|i|(i%251)as u8).collect();
        let b:Vec<u8>=(0..2*1024*1024+31).map(|i|(i%239)as u8).collect();
        for (name,bytes) in [("a",a.as_slice()),("b",b.as_slice())] {
            let mut writer=source.create(&CvPath::from_str(format!("/scale-live/{name}"))?,false).await?;
            writer.write(bytes).await?;writer.complete().await?;
        }
        let initial=[storage.resolve("/scale-live/a").await?,storage.resolve("/scale-live/b").await?];
        let zero=LifecyclePolicy{id:"scale-zero".into(),revision:1,enabled:true,prefix:"/scale-live/".into(),min_size:u64::MAX,
            max_size:None,min_residency_ms:0,action:PolicyAction::Release,backend:"local".into(),selection:PolicySelection::default()};
        storage.put_policy(&ctx("scale-zero-put"),&zero,None).await?;
        let start=Instant::now();let mut cursor=None;let mut examined=0;let mut pages=0;
        loop {
            let batch=storage.scan_policy(&ctx(&format!("scale-zero-page-{pages}")),&zero.id,cursor.as_deref(),256).await?;
            assert!(batch.examined<=256);assert!(batch.admitted.is_empty()&&batch.to_seal.is_empty());
            examined+=batch.examined;pages+=1;cursor=batch.next;if cursor.is_none(){break;}
        }
        assert_eq!(examined,count+2);report["zero_match_scan"]=serde_json::json!({"examined":examined,"pages":pages,"ms":start.elapsed().as_millis(),"admitted":0});
        let mut disabled=zero;disabled.enabled=false;disabled.revision=2;
        storage.put_policy(&ctx("scale-zero-disable"),&disabled,Some(1)).await?;
        let mut policy=LifecyclePolicy{id:"scale-release".into(),revision:1,enabled:true,prefix:"/scale-live/".into(),min_size:1024*1024,
            max_size:None,min_residency_ms:0,action:PolicyAction::Release,backend:"local".into(),selection:PolicySelection{
                scan_interval_ms:100,scan_budget:65536,max_candidates:2,..Default::default()}};
        storage.put_policy(&ctx("scale-release-put"),&policy,None).await?;
        let log=OpenOptions::new().create(true).append(true).open(root.join("worker.log"))?;
        let _worker=Worker(Command::new(env!("CARGO_BIN_EXE_hsm-curvine")).arg("--config").arg(&config)
            .arg("--archive-root").arg(root.join("archive")).args(["worker","--poll-ms","100"])
            .stdout(Stdio::from(log.try_clone()?)).stderr(Stdio::from(log)).spawn()?);
        let start=Instant::now();
        loop {
            let a=storage.resolve("/scale-live/a").await?;let b=storage.resolve("/scale-live/b").await?;
            if a.residency==Residency::Offline&&b.residency==Residency::Offline{break;}
            if start.elapsed()>Duration::from_secs(1800){return Err("scale release timeout".into());}
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        report["release_ms"]=serde_json::json!(start.elapsed().as_millis());
        policy.enabled=false;policy.revision=2;storage.put_policy(&ctx("scale-release-disable"),&policy,Some(1)).await?;
        async fn read(source:&CurvineFileSystem,path:&str)->Result<Vec<u8>> {
            let mut reader=source.open(&CvPath::from_str(path)?).await?;let mut result=vec![];
            while reader.has_remaining(){result.extend_from_slice(reader.async_read(None).await?.as_slice());}
            reader.complete().await?;Ok(result)
        }
        let start=Instant::now();let(one,two,three)=tokio::join!(read(&source,"/scale-live/a"),read(&source,"/scale-live/a"),read(&source,"/scale-live/b"));
        assert_eq!(one?,a);assert_eq!(two?,a);assert_eq!(three?,b);
        let recalled=[storage.resolve("/scale-live/a").await?,storage.resolve("/scale-live/b").await?];
        for i in 0..2 {assert_eq!(recalled[i].data,initial[i].data);assert_eq!(recalled[i].residency,Residency::Dual);}
        let mut after=None;let mut recalls=0;
        loop {
            let page=storage.intents(&ctx(&format!("scale-intents-{recalls}")),after.as_deref(),16).await?;
            for intent in &page.items {if intent.policy=="__access_recall" {assert_eq!(intent.status,IntentStatus::Done);recalls+=1;}}
            after=page.next;if after.is_none(){break;}
        }
        assert_eq!(recalls,2);report["automatic_recall"]=serde_json::json!({"application_reads":3,"recall_intents":recalls,"ms":start.elapsed().as_millis(),"bytes_and_data_keys_preserved":true,"explicit_recall_calls":0});
        fs::write(root.join("scale-states.json"),serde_json::to_vec(&recalled)?)?;
        Ok::<(),Box<dyn std::error::Error+Send+Sync>>(())
    })?;
    // Allow followers to publish a supported committed checkpoint, then kill
    // the whole server fixture and restart without the former leader.
    std::thread::sleep(Duration::from_secs(35));
    let old = rt.block_on(source.get_filesystem_info())?.active_master;
    let offset = fs::metadata(root.join("servers.log"))?.len() as usize;
    let start = Instant::now();
    cluster.restart(&old)?;
    report["recovery_ms"] = serde_json::json!(start.elapsed().as_millis());
    let restarted = CurvineFileSystem::with_rt(masters[0].clone(), rt.clone())?;
    rt.block_on(async {
        let storage = CurvineStorage::connect(NativeControl::new(restarted.clone())).await?;
        let expected: Vec<hsm_core::EntityState> =
            serde_json::from_slice(&fs::read(root.join("scale-states.json"))?)?;
        assert_eq!(storage.resolve("/scale-live/a").await?, expected[0]);
        assert_eq!(storage.resolve("/scale-live/b").await?, expected[1]);
        // The restored private layout must be readable without a controller.
        for (name, length, modulus) in [
            ("a", 1024 * 1024 + 17, 251),
            ("b", 2 * 1024 * 1024 + 31, 239),
        ] {
            let mut reader = restarted
                .open(&CvPath::from_str(format!("/scale-live/{name}"))?)
                .await?;
            let mut actual = vec![];
            while reader.has_remaining() {
                actual.extend_from_slice(reader.async_read(None).await?.as_slice());
            }
            reader.complete().await?;
            let expected: Vec<u8> = (0..length).map(|i| (i % modulus) as u8).collect();
            assert_eq!(actual, expected);
        }
        let mut total = 0;
        for part in 0..count.div_ceil(1000) {
            let children = restarted
                .list_status(&CvPath::from_str(format!(
                    "/__hsm_scale_fixture__/records/part-{part:06}"
                ))?)
                .await?;
            assert!(children.iter().all(|s| s.len == 0 && s.is_complete));
            total += children.len();
        }
        assert_eq!(total, count);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;
    let log = fs::read_to_string(root.join("servers.log"))?;
    let recovery = &log[offset..];
    assert!(
        recovery.contains("apply_snapshot:"),
        "checkpoint restore was not observed"
    );
    let mut resets = vec![];
    for line in recovery
        .lines()
        .filter(|l| l.contains("reset HSM projection:"))
    {
        let number = |name: &str| -> Result<u64> {
            Ok(line
                .split(&format!("{name}="))
                .nth(1)
                .ok_or("reset field missing")?
                .split([',', ' '])
                .next()
                .ok_or("reset value missing")?
                .parse()?)
        };
        let keys = number("keys")?;
        let batch = number("max_batch_keys")?;
        let bytes = number("max_batch_bytes")?;
        assert!(keys >= count as u64 * 2 && batch <= 1024 && bytes <= 256 * 1024);
        resets.push(serde_json::json!({"keys":keys,"max_batch_keys":batch,"max_batch_bytes":bytes,"elapsed_ms":number("elapsed_ms")?}));
    }
    assert_eq!(resets.len(), 2);
    report["restart_resets"] = serde_json::json!(resets);
    report["checkpoint_restore_observed"] = serde_json::json!(true);
    report["elapsed_ms"] = serde_json::json!(begin.elapsed().as_millis());
    report["worker_nodes"] = serde_json::json!(workers.len());
    if let Ok(path) = std::env::var("HSM_SCALE_REPORT") {
        fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    println!(
        "HSM_NAMESPACE_SCALE_RESULT={}",
        serde_json::to_string(&report)?
    );
    Ok(())
}
