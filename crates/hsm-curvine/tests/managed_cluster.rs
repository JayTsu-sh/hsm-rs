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
