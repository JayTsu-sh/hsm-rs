// Included by managed_cluster: independent OS processes make leader loss real.
use super::*;
use curvine_fs_api::{Reader, Writer};
use curvine_model::{CreateFileOpts, OpenFlags, ProtoUtils};
use hsm_core::{IntentCompletion, IntentStatus, LifecyclePolicy, LifecycleStorage};

#[test]
#[ignore = "subprocess fixture for live leader pause/rejoin"]
fn node_child() -> Result {
    let root = PathBuf::from(std::env::var_os("HSM_LIVE_ROOT").ok_or("missing live root")?);
    let manifest = root.join("live-cluster.json");
    if std::env::var_os("HSM_LIVE_BOOTSTRAP").is_some() {
        let mut conf = ClusterConf::format();
        conf.testing = false;
        conf.master.hsm_enabled = true;
        conf.master.meta_dir = root.join("meta").to_string_lossy().into_owned();
        conf.worker.data_dir = vec![root.join("worker").to_string_lossy().into_owned()];
        conf.journal.enable = true;
        conf.journal.journal_dir = root.join("journal").to_string_lossy().into_owned();
        conf.journal.leader_create_snapshot = Some(false);
        conf.journal.snapshot_entries = u64::MAX;
        conf.journal.raft_tick_interval_ms = 100;
        conf.journal.raft_election_tick = 50;
        conf.journal.raft_min_election_ticks = 50;
        conf.journal.raft_max_election_ticks = 75;
        conf.client.short_circuit = false;
        conf.client.replicas = 1;
        conf.client.block_size_str = "1MB".into();
        conf.client.conn_timeout_ms = 1000;
        conf.client.conn_retry_max_duration_ms = 2000;
        conf.client.rpc_timeout_ms = 2000;
        conf.client.rpc_retry_max_duration_ms = 90_000;
        conf.master.min_block_size = 4096;
        // Keep Worker detection shorter than this fault test, with enough
        // election grace for the healthy Worker's client to find the Leader.
        conf.master.heartbeat_interval = "100ms".into();
        conf.master.worker_check_interval = "200ms".into();
        conf.master.worker_blacklist_interval = "4s".into();
        conf.master.worker_lost_interval = "8s".into();
        conf.log.level = "WARN".into();
        conf.log.log_dir = "stderr".into();
        conf.master.log = conf.log.clone();
        conf.worker.log = conf.log.clone();
        // Bootstrap exits before nodes start, releasing its reserved sockets.
        let layout = MiniCluster::with_num(&conf, 3, 1);
        fs::write(
            &manifest,
            serde_json::to_vec(&(layout.master_conf.clone(), layout.worker_conf.clone()))?,
        )?;
        return Ok(());
    }
    let index: usize = std::env::var("HSM_LIVE_NODE")?.parse()?;
    let (mut masters, mut workers): (Vec<ClusterConf>, Vec<ClusterConf>) =
        serde_json::from_slice(&fs::read(manifest)?)?;
    // Duration caches are serde-skipped. Rebuild them after the IPC manifest
    // roundtrip so the fixture uses its configured detection/absence windows.
    for conf in masters.iter_mut().chain(workers.iter_mut()) {
        conf.master.init()?;
        assert_eq!(conf.master.worker_lost_interval_ms(), 8_000);
    }
    let node = if index < masters.len() {
        MiniCluster::new(vec![masters[index].clone()], vec![])
    } else {
        MiniCluster::new(
            vec![masters[0].clone()],
            vec![workers[index - masters.len()].clone()],
        )
    };
    if index < masters.len() {
        node.start_master();
    } else {
        node.start_worker();
    }
    fs::write(root.join(format!("live-ready-{index}")), b"spawned")?;
    loop {
        // Local fixture IPC only; no test RPC is added to the product. It
        // inspects committed rows on Followers and pauses between real steps.
        let input = root.join(format!("cleanup-command-{index}.json"));
        if index < masters.len() && input.exists() {
            let request: serde_json::Value = serde_json::from_slice(&fs::read(&input)?)?;
            fs::remove_file(input)?;
            let response = root.join(format!(
                "cleanup-result-{}.json",
                request["id"].as_str().ok_or("missing fixture id")?
            ));
            let master = node.hsm_test_local_master_fs();
            let worker =
                u32::try_from(request["worker"].as_u64().ok_or("missing fixture worker")?)?;
            let result = (|| -> Result<serde_json::Value> {
                match request["action"].as_str().ok_or("missing fixture action")? {
                    "seed" => {
                        let ids: Vec<i64> = serde_json::from_value(request["blocks"].clone())?;
                        master.hsm_test_seed_cleanup_locations(worker, &ids)?;
                    }
                    "pause" => {
                        let _ = master.hsm_test_run_cleanup(worker, |phase| {
                            if phase == "deleted" {
                                let state = master.hsm_test_cleanup_snapshot(worker)?;
                                fs::write(
                                    &response,
                                    serde_json::to_vec(&state).map_err(|e| {
                                        curvine_error::FsError::common(e.to_string())
                                    })?,
                                )?;
                                while !root.join("cleanup-resume").exists() {
                                    std::thread::sleep(Duration::from_millis(20));
                                }
                                return Err(curvine_error::FsError::common(
                                    "fixture interrupted before cursor acknowledgement",
                                ));
                            }
                            Ok(())
                        });
                    }
                    "snapshot" => {}
                    _ => return Err("unknown cleanup fixture command".into()),
                }
                Ok(master.hsm_test_cleanup_snapshot(worker)?)
            })();
            if !response.exists() {
                let value = match result {
                    Ok(value) => value,
                    Err(error) => serde_json::json!({"error":error.to_string()}),
                };
                fs::write(response, serde_json::to_vec(&value)?)?;
            }
        }
        std::thread::park_timeout(Duration::from_millis(20));
    }
}

struct LiveCluster {
    root: PathBuf,
    children: [Option<Child>; 4],
    masters: Vec<ClusterConf>,
}
impl LiveCluster {
    fn master_index(&self, address: &str) -> Result<usize> {
        let port: u16 = address
            .rsplit(':')
            .next()
            .ok_or("missing Master port")?
            .parse()?;
        self.masters
            .iter()
            .position(|conf| conf.master.rpc_port == port)
            .ok_or_else(|| "unknown Master".into())
    }
    async fn cleanup_command(
        &self,
        index: usize,
        worker: u32,
        action: &str,
        blocks: &[i64],
    ) -> Result<serde_json::Value> {
        let id = curvine_model::hsm::native::token();
        let response = self.root.join(format!("cleanup-result-{id}.json"));
        let input = self.root.join(format!("cleanup-command-{index}.json"));
        let temporary = input.with_extension("tmp");
        fs::write(
            &temporary,
            serde_json::to_vec(
                &serde_json::json!({"id":id,"worker":worker,"action":action,"blocks":blocks}),
            )?,
        )?;
        fs::rename(temporary, input)?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(bytes) = fs::read(&response) {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if !value["error"].is_null() {
                        return Err(format!("cleanup fixture: {value}").into());
                    }
                    return Ok(value);
                }
            }
            if Instant::now() > deadline {
                return Err(
                    format!("cleanup fixture command {action} timed out on node {index}").into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn cleanup_recovered(
        &self,
        address: &str,
        worker: u32,
        minimum_repairs: usize,
    ) -> Result<serde_json::Value> {
        let index = self.master_index(address)?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let state = self.cleanup_command(index, worker, "snapshot", &[]).await?;
            if state["job"].is_null()
                && state["locations"] == 0
                && state["repairs"].as_u64().unwrap_or(0) >= minimum_repairs as u64
            {
                return Ok(state);
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "cleanup did not automatically recover: term={} job_term={} pending={} locations={} repairs={}",
                    state["term"], state["job"]["term"],
                    state["job"]["pending"].as_array().map_or(0, Vec::len),
                    state["locations"], state["repairs"]
                ).into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    fn command(root: &Path) -> Result<Command> {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("live-servers.log"))?;
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["--ignored", "--exact", "live_ha::node_child", "--nocapture"])
            .env("HSM_LIVE_ROOT", root)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        Ok(command)
    }
    fn start(root: &Path) -> Result<Self> {
        let status = Self::command(root)?
            .env("HSM_LIVE_BOOTSTRAP", "1")
            .status()?;
        if !status.success() {
            return Err("live layout bootstrap failed".into());
        }
        let (masters, _): (Vec<ClusterConf>, Vec<ClusterConf>) =
            serde_json::from_slice(&fs::read(root.join("live-cluster.json"))?)?;
        let mut cluster = Self {
            root: root.into(),
            children: std::array::from_fn(|_| None),
            masters,
        };
        for index in 0..4 {
            cluster.children[index] = Some(
                Self::command(root)?
                    .env("HSM_LIVE_NODE", index.to_string())
                    .spawn()?,
            );
            let deadline = Instant::now() + Duration::from_secs(30);
            while !root.join(format!("live-ready-{index}")).exists() {
                if let Some(status) = cluster.children[index].as_mut().unwrap().try_wait()? {
                    return Err(format!("live node {index} exited {status}").into());
                }
                if Instant::now() > deadline {
                    return Err(format!("live node {index} startup timeout").into());
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        Ok(cluster)
    }
    fn signal(&self, address: &str, signal: &str) -> Result {
        let port: u16 = address
            .rsplit(':')
            .next()
            .ok_or("leader port missing")?
            .parse()?;
        let index = self
            .masters
            .iter()
            .position(|conf| conf.master.rpc_port == port)
            .ok_or("unknown live leader")?;
        let pid = self.children[index]
            .as_ref()
            .ok_or("live node absent")?
            .id();
        if !Command::new("kill")
            .args([signal, &pid.to_string()])
            .status()?
            .success()
        {
            return Err(format!("could not signal live node {index}").into());
        }
        Ok(())
    }
}
impl Drop for LiveCluster {
    fn drop(&mut self) {
        for child in &mut self.children {
            if let Some(mut child) = child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        let artifact = self.root.with_extension("live-servers.log");
        let _ = fs::copy(self.root.join("live-servers.log"), &artifact);
        eprintln!("live HA log: {}", artifact.display());
    }
}
async fn changed_leader(fs: &CurvineFileSystem, old: &str) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Ok(info) = fs.get_filesystem_info().await {
            if info.active_master != old {
                return Ok(info.active_master);
            }
        }
        if Instant::now() > deadline {
            return Err("live leader did not change".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn retry_action<T, F, Fut>(mut action: F) -> hsm_core::HsmResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = hsm_core::HsmResult<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match action().await {
            Err(error @ (HsmError::Busy(_) | HsmError::Unknown(_) | HsmError::Io(_)))
                if Instant::now() < deadline =>
            {
                eprintln!("live same-identity retry: {error}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            result => return result,
        }
    }
}

async fn read_all(fs: &CurvineFileSystem, path: &curvine_fs_api::Path) -> Result<Vec<u8>> {
    let mut reader = fs.open(path).await?;
    let mut bytes = vec![];
    while reader.has_remaining() {
        bytes.extend_from_slice(reader.async_read(None).await?.as_slice());
    }
    reader.complete().await?;
    Ok(bytes)
}

#[test]
fn ordinary_hsm_and_batch_results_survive_live_leader_pause_and_rejoin() -> Result {
    let root = tempfile::tempdir()?;
    let cluster = LiveCluster::start(root.path())?;
    let conf = cluster.masters[0].clone();
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(conf, rt.clone())?;
    rt.block_on(async {
        let ready_by = Instant::now() + Duration::from_secs(90);
        loop {
            if fs.get_filesystem_info().await.is_ok_and(|info| info.live_workers.len() == 1) { break; }
            if Instant::now() > ready_by { return Err("live cluster did not register its Worker".into()); }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let storage = CurvineStorage::connect(NativeControl::new(fs.clone())).await?;
        let mover = CurvineDataMover::new(fs.clone());
        let archive = LocalArchive::open(root.path().join("live-archive"), "live-archive")?;
        let engine = HsmEngine::new(storage.clone(), archive, mover);
        let path = curvine_fs_api::Path::from_str("/live-hsm")?;
        let bytes: Vec<_> = (0..2 * 1024 * 1024 + 29).map(|i| (i % 251) as u8).collect();
        let mut writer = fs.create(&path, false).await?;
        writer.write(&bytes).await?;
        writer.complete().await?;
        let initial = storage.resolve(path.path()).await?;
        storage.seal_ordinary(&initial.data).await?;
        eprintln!("live phase: sealed ordinary generation");
        let key = ArchiveKey { backend: "live-archive".into(), object: "immutable-live".into() };
        let dual = engine.archive(&context("live-archive"), &initial.data, &key).await?;
        assert_eq!(dual.residency, Residency::Dual);
        eprintln!("live phase: archived");
        // Persist policy admission and a live executor claim before losing the
        // Master. Master elections must not reset controller ownership or
        // require rebuilding an external task/namespace mirror.
        let policy: LifecyclePolicy = serde_json::from_value(serde_json::json!({
            "id":"live-release", "revision":1,"enabled":true,"prefix":"/live-hsm",
            "min_size":0,"max_size":null,"min_residency_ms":0,"action":"release","backend":"live-archive"
        }))?;
        let policy_context = context("live-policy-put");
        assert_eq!(storage.put_policy(&policy_context, &policy, None).await?, policy);
        let selected = storage.scan_policy(&context("live-scan-before-failover"), &policy.id, None, 256).await?;
        let first_task = selected.admitted.first().ok_or("release policy did not admit the Dual file")?;
        let claim_context = context("live-claim-before-failover");
        let first_claim = storage.claim_intent(&claim_context, first_task, "live-executor").await?;
        assert_eq!(first_claim.status, IntentStatus::Running);

        let item = |path: &str| curvine_proto::CreateFileRequest {
            path: path.into(), opts: ProtoUtils::create_opts_to_pb(CreateFileOpts::with_create(true), "live-batch"),
            flags: OpenFlags::new_create().set_overwrite(false).value(),
        };
        let batch = curvine_proto::NativeMutationRequest {
            version: 1, incarnation: initial.data.entity.namespace.clone(),
            session: format!("{:016x}{}", curvine_runtime::common::LocalTime::mills(), curvine_model::hsm::native::token()),
            sequence: 1, acknowledged: 0, code: curvine_fs_api::RpcCode::CreateFilesBatch as u32,
            header: ProtoUtils::encode(curvine_proto::CreateFilesBatchRequest {
                requests: vec![item("/live-batch-old"), item("/live-batch-old"), item("/live-batch-never")],
            })?.to_vec(),
        };
        let original: curvine_proto::NativeMutationResponse = fs.fs_client()
            .rpc(curvine_fs_api::RpcCode::NativeMutation, batch.clone()).await?;
        assert!(!original.success && original.status == curvine_proto::NativeMutationStatusProto::Committed as i32);
        let batch_old = curvine_fs_api::Path::from_str("/live-batch-old")?;
        let batch_new = curvine_fs_api::Path::from_str("/live-batch-new")?;
        let batch_inode = fs.get_status(&batch_old).await?.id;
        assert!(fs.rename(&batch_old, &batch_new).await?);
        // Use real current file blocks for durable repair demand, plus a
        // synthetic derived inventory to force multiple cleanup pages. One
        // actual Worker remains, so the requested two-copy policy stays unmet.
        let repair_path = curvine_fs_api::Path::from_str("/live-repair")?;
        let repair_bytes: Vec<_> = (0..1024 * 1024 + 53).map(|i| (i % 239) as u8).collect();
        let mut repair_writer = fs.create(&repair_path, false).await?;
        repair_writer.write(&repair_bytes).await?;
        repair_writer.complete().await?;
        fs.set_attr(&repair_path, curvine_model::SetAttrOpts { replicas: Some(2), ..Default::default() }).await?;
        let actual_blocks: Vec<_> = fs.fs_client().get_block_locations(&repair_path).await?.block_locs
            .into_iter().map(|located| located.block.id).collect();
        assert!(!actual_blocks.is_empty());
        let fake_worker = u32::MAX - 20;
        let mut inventory = actual_blocks.clone();
        inventory.extend((1..=2050-actual_blocks.len()).map(|id| i64::MAX-id as i64));
        let first_leader = fs.get_filesystem_info().await?.active_master;
        let first_index = cluster.master_index(&first_leader)?;
        for index in (0..3).filter(|index| *index != first_index).chain(std::iter::once(first_index)) {
            let seeded = cluster.cleanup_command(index, fake_worker, "seed", &inventory).await?;
            assert_eq!(seeded["locations"], 2050);
        }
        let paused = cluster.cleanup_command(first_index, fake_worker, "pause", &[]).await?;
        assert_eq!(paused["job"]["pending"].as_array().unwrap().len(), 1024);
        assert_eq!(paused["locations"], 1026, "fault must occur after deletion but before durable cursor acknowledgement");
        for index in (0..3).filter(|index| *index != first_index) {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let state = cluster.cleanup_command(index, fake_worker, "snapshot", &[]).await?;
                if state["job"]["pending"].as_array().is_some_and(|ids| ids.len() == 1024) {
                    assert_eq!(state["locations"], 2050, "replicated advisory jobs must not delete Follower locations");
                    break;
                }
                if Instant::now() > deadline { return Err("Follower did not apply staged cleanup checkpoint".into()); }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        cluster.signal(&first_leader, "-STOP")?;
        let next_leader = changed_leader(&fs, &first_leader).await?;
        assert_ne!(first_leader, next_leader);
        eprintln!("live phase: replacement elected {next_leader}");
        let recovered_cleanup = cluster.cleanup_recovered(&next_leader, fake_worker, actual_blocks.len()).await?;
        assert!(recovered_cleanup["term"].as_u64() > paused["term"].as_u64());
        assert_eq!(read_all(&fs, &repair_path).await?, repair_bytes);
        assert_eq!(read_all(&fs, &path).await?, bytes);
        let recovered_claim = retry_action(|| storage.claim_intent(&claim_context, first_task, "live-executor")).await?;
        assert_eq!(recovered_claim, first_claim, "lost claim response did not retain its original token/result");
        assert_eq!(storage.intent(&context("live-inspect-claim"), first_task).await?, first_claim);
        assert_eq!(storage.put_policy(&policy_context, &policy, None).await?, policy);
        let superseded = storage.finish_intent(&context("live-supersede-accessed"), &first_claim,
            IntentCompletion::Superseded("foreground read changed the release observation".into())).await?;
        assert_eq!(superseded.status, IntentStatus::Superseded);
        let repeated: curvine_proto::NativeMutationResponse = fs.fs_client()
            .rpc(curvine_fs_api::RpcCode::NativeMutation, batch.clone()).await?;
        assert_eq!(repeated, original);
        assert!(!fs.exists(&batch_old).await?);
        assert_eq!(fs.get_status(&batch_new).await?.id, batch_inode);
        assert!(!fs.exists(&curvine_fs_api::Path::from_str("/live-batch-never")?).await?);
        // Resuming the old process exercises a stale term, not a clean
        // restarted group. It must rejoin rather than overwrite authority.
        fs::write(root.path().join("cleanup-resume"), b"interrupt old cleanup owner")?;
        cluster.signal(&first_leader, "-CONT")?;
        // The verified foreground read advanced the release observation.
        // Re-observe it; the pre-read snapshot must not authorize release.
        let current = storage.inspect(&initial.data.entity).await?;
        assert_eq!(current.residency, Residency::Dual);
        let selected = storage.scan_policy(&context("live-scan-after-access"), &policy.id, None, 256).await?;
        let release_task = selected.admitted.first().ok_or("policy did not re-admit the new observation")?;
        let release_claim = storage.claim_intent(&context("live-claim-new-observation"), release_task, "live-executor").await?;
        assert_eq!(release_claim.expected, current);
        let offline = retry_action(|| engine.release(&release_claim.context, &release_claim.expected)).await?;
        let finish_context = context("live-finish-release");
        let finished = storage.finish_intent(&finish_context, &release_claim, IntentCompletion::Done).await?;
        assert_eq!(finished.status, IntentStatus::Done);
        assert_eq!(finished.result.as_ref(), Some(&offline));
        assert_eq!(offline.residency, Residency::Offline);
        eprintln!("live phase: released");
        assert_eq!(storage.resolve(path.path()).await?.residency, Residency::Offline);
        assert_eq!(fs.get_status(&path).await?.id.to_string(), initial.data.entity.entity);
        // Lose the new Leader as well. The resumed old process must now
        // participate in the surviving quorum; merely staying alive is not
        // enough to let Recall commit on the remaining node.
        cluster.signal(&next_leader, "-STOP")?;
        let recall_leader = changed_leader(&fs, &next_leader).await?;
        assert_ne!(recall_leader, next_leader);
        eprintln!("live phase: resumed quorum elected {recall_leader}");
        cluster.cleanup_recovered(&recall_leader, fake_worker, actual_blocks.len()).await?;
        assert_eq!(read_all(&fs, &repair_path).await?, repair_bytes);
        assert_eq!(storage.intent(&context("live-recovered-done"), release_task).await?, finished);
        assert_eq!(storage.finish_intent(&finish_context, &release_claim, IntentCompletion::Done).await?, finished);
        assert_eq!(storage.put_policy(&policy_context, &policy, None).await?, policy);

        let recall_context = context("live-recall");
        let restored = retry_action(|| engine.recall(&recall_context, &offline, &key)).await?;
        assert_eq!(restored.residency, Residency::Dual);
        assert_eq!(restored.data, initial.data);
        assert_eq!(read_all(&fs, &path).await?, bytes);
        cluster.signal(&next_leader, "-CONT")?;
        assert_eq!(storage.finish_intent(&finish_context, &release_claim, IntentCompletion::Done).await?, finished,
            "finish retry regenerated the current Dual state instead of the original Offline result");
        let repeated: curvine_proto::NativeMutationResponse = fs.fs_client()
            .rpc(curvine_fs_api::RpcCode::NativeMutation, batch).await?;
        assert_eq!(repeated, original);
        // Acknowledgement is based on current metadata and live placement,
        // not merely on having queued a repair into an in-memory executor.
        fs.set_attr(&repair_path, curvine_model::SetAttrOpts { replicas: Some(1), ..Default::default() }).await?;
        let deadline = Instant::now() + Duration::from_secs(60);
        let final_index = cluster.master_index(&fs.get_filesystem_info().await?.active_master)?;
        loop {
            let state = cluster.cleanup_command(final_index, fake_worker, "snapshot", &[]).await?;
            if state["repairs"] == 0 { break; }
            if Instant::now() > deadline { return Err(format!("healthy repair hints were not retired: {state}").into()); }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(read_all(&fs, &repair_path).await?, repair_bytes);
        println!("verified live HA: pause active Master, elect with two survivors, resume stale Master, pause replacement Leader and elect again; policy, original claim/finish and batch results survive; HSM Dual -> Offline -> Dual retains inode/content");
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
}
