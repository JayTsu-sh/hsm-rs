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
use hsm_curvine::{CurvineDataMover, CurvineStorage, NativeControl};
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
    fs::write(root.join("ready"), b"ready")?;
    loop {
        std::thread::park_timeout(Duration::from_secs(30));
    }
}
struct ProcessCluster {
    root: PathBuf,
    child: Option<Child>,
}
impl ProcessCluster {
    fn start(root: &Path) -> Result<Self> {
        let mut cluster = Self {
            root: root.to_path_buf(),
            child: None,
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
        self.child = Some(command.spawn()?);
        let deadline = Instant::now() + Duration::from_secs(90);
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
        assert!(fs.open(&path).await.is_err()); // legacy data access must fail, never return holes.
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
        let old_leader=fs.get_filesystem_info().await?.active_master;
        processes.restart(&old_leader)?;
        eprintln!("phase: restarted on another leader");
        assert_eq!(storage.inspect(&initial.data.entity).await?,dual);
        assert_eq!(engine.archive(&context("archive"),&initial.data,&key).await?,dual);
        assert_eq!(storage.pending_view(&context("held-pin"),&initial.data.entity).await?,pin);
        storage.close_view(&context("held-pin"),&pin).await?;
        // A definite Busy result remains the same after the condition changes.
        assert!(matches!(engine.release(&context("busy-release"),&dual).await,Err(HsmError::Busy(_))));
        let offline=engine.release(&context("release"),&dual).await?;
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
        assert_eq!(engine.release(&context("release"),&dual).await?,offline);
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
