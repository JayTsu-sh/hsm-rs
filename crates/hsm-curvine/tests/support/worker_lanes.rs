use super::*;
use curvine_fs_api::{Reader, Writer};

struct WorkerProcess(Child);
impl Drop for WorkerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn recall_completes_while_a_background_executor_turn_is_suspended() -> Result {
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
        let ctx = |id: &str| OperationContext { operation_id: id.into(), controller_epoch: epoch, attempt_epoch: 1 };
        let path = curvine_fs_api::Path::from_str("/lane-recall")?;
        let bytes: Vec<_> = (0..1024 * 1024 + 37).map(|i| (i % 251) as u8).collect();
        let mut writer = source.create(&path, false).await?;
        writer.write(&bytes).await?;
        writer.complete().await?;
        let initial = storage.resolve(path.path()).await?;
        storage.seal_ordinary(&initial.data).await?;
        let engine = HsmEngine::new(storage.clone(), LocalArchive::open(root.path().join("archive"), "local")?, CurvineDataMover::new(source.clone()));
        let key = ArchiveKey { backend: "local".into(), object: "lane-recall".into() };
        let dual = engine.archive(&ctx("lane-archive"), &initial.data, &key).await?;
        let offline = engine.release(&ctx("lane-release"), &dual).await?;
        assert_eq!(offline.residency, Residency::Offline);
        drop(engine); // Transfer exclusive local archive ownership to the CLI.
        source.mkdir(&curvine_fs_api::Path::from_str("/lane-background")?, false).await?;
        let background = curvine_fs_api::Path::from_str("/lane-background/payload")?;
        let mut writer = source.create(&background, false).await?;
        writer.write(b"archive on the independent background lane").await?;
        writer.complete().await?;
        let state = storage.resolve(background.path()).await?;
        storage.seal_ordinary(&state.data).await?;
        let policy = hsm_core::LifecyclePolicy {
            id: "lane-background".into(), revision: 1, enabled: true, prefix: "/lane-background/".into(),
            min_size: 0, max_size: None, min_residency_ms: 0,
            action: hsm_core::PolicyAction::Archive, backend: "local".into(), selection: Default::default(),
        };
        storage.put_policy(&ctx("lane-policy"), &policy, None).await?;
        let pause = root.path().join("pause");
        fs::create_dir(&pause)?;
        let log = OpenOptions::new().create(true).append(true).open(root.path().join("worker-lanes.log"))?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_hsm-curvine"));
        command.arg("--config").arg(&config).arg("--archive-root").arg(root.path().join("archive"))
            .args(["worker", "--poll-ms", "20", "--max-actions", "1"])
            .env("HSM_TEST_BACKGROUND_PAUSE", &pause)
            .stdout(Stdio::from(log.try_clone()?)).stderr(Stdio::from(log));
        let mut worker = WorkerProcess(command.spawn()?);
        let deadline = Instant::now() + Duration::from_secs(45);
        while !pause.join("paused").exists() {
            if let Some(status) = worker.0.try_wait()? {
                return Err(format!("worker exited {status}: {}", fs::read_to_string(root.path().join("worker-lanes.log"))?).into());
            }
            if Instant::now() > deadline {
                return Err(format!("background did not reach suspension: {}", fs::read_to_string(root.path().join("worker-lanes.log"))?).into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // This is an ordinary application read. There is no explicit Recall
        // call, and the background executor remains suspended throughout it.
        let read = async {
            let mut reader = source.open(&path).await?;
            let mut received = vec![];
            while reader.has_remaining() { received.extend_from_slice(reader.async_read(None).await?.as_slice()); }
            reader.complete().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(received)
        };
        let received = tokio::time::timeout(Duration::from_secs(20), read).await??;
        assert_eq!(received, bytes);
        assert!(!pause.join("resume").exists());
        assert_eq!(storage.resolve(path.path()).await?.residency, Residency::Dual);
        assert_eq!(storage.resolve(background.path()).await?.residency, Residency::Online);
        // Now let background progress and verify that reserving Recall
        // capacity did not prevent a real Archive on the other lane.
        fs::write(pause.join("resume"), b"resume")?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while storage.resolve(background.path()).await?.residency != Residency::Dual {
            if Instant::now() > deadline { return Err(format!("background Archive stalled: {}", fs::read_to_string(root.path().join("worker-lanes.log"))?).into()); }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        println!("verified: background executor suspended; ordinary read independently recalls and verifies 1 MiB+37 bytes; resumed background Archive completes");
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
}
