//! Local-file HSM operator and lifecycle worker. Authority always stays in Curvine.
use clap::{Parser, Subcommand};
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_runtime::runtime::RpcRuntime;
use hsm_core::{HsmError, HsmResult, IntentStatus, LifecyclePolicy, OperationContext};
use hsm_curvine::{CurvineDataMover, CurvineStorage, NativeControl};
use hsm_engine::HsmEngine;
use hsm_local::{LocalArchive, LocalDataMover};
use serde::Serialize;
use std::{path::PathBuf, sync::Arc, time::Duration};

#[derive(Parser)]
#[command(about = "Curvine HSM with storage-owned policies and a local-file archive")]
struct Args {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    archive_root: Option<PathBuf>,
    #[arg(long, global = true, default_value = "local")]
    backend_id: String,
    #[arg(long, global = true)]
    op_id: Option<String>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Import one local file as a new immutable managed inode.
    Import { file: PathBuf, path: String },
    /// Show source-authoritative identity, residency and archive receipts.
    Status { path: String },
    /// Read an online generation to a new local file; Offline requires Recall.
    Read { path: String, output: PathBuf },
    /// Create or CAS-update a JSON policy (revision is in the document).
    PolicyPut {
        file: PathBuf,
        #[arg(long)]
        expected_revision: Option<u64>,
    },
    /// List source-owned policy definitions.
    Policies,
    /// Admit one bounded candidate page; next is an opaque resume cursor.
    Scan {
        id: String,
        #[arg(long)]
        after: Option<String>,
        #[arg(long, default_value_t = 16)]
        limit: usize,
    },
    /// List durable intents and their original results.
    Jobs,
    /// Poll policy admission and execute matching-backend tasks.
    Worker {
        #[arg(long)]
        once: bool,
        #[arg(long, default_value_t = 1000)]
        poll_ms: u64,
        #[arg(long, default_value_t = 64)]
        max_actions: usize,
    },
    /// Advance controller qualification; required to reclaim an old running task.
    Takeover {
        #[arg(long)]
        expected_epoch: u64,
    },
    /// Retry physical deletion of source-retired allocations.
    Gc { path: String },
    /// Close a failed Archive/read pin after its old I/O has stopped.
    CloseView { path: String, operation_id: String },
    /// Abort a failed unpublished Recall after its old I/O has stopped.
    AbortRecall { path: String, operation_id: String },
    /// Start a persistent local 3-Master/1-Worker development cluster.
    #[cfg(feature = "native-integration")]
    ServeLocal { root: PathBuf },
}
fn context(epoch: u64, operation_id: Option<String>) -> OperationContext {
    OperationContext {
        operation_id: operation_id.unwrap_or_else(curvine_model::hsm::native::token),
        controller_epoch: epoch,
        attempt_epoch: 1,
    }
}
fn print(value: &impl Serialize) -> HsmResult<()> {
    println!(
        "{}",
        serde_json::to_string(value).map_err(|e| HsmError::Invalid(e.to_string()))?
    );
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    #[cfg(feature = "native-integration")]
    if let Command::ServeLocal { root } = &args.command {
        return serve_local(root);
    }
    let path = args.config.as_ref().ok_or("--config is required")?;
    let conf: ClusterConf = if path.extension().is_some_and(|e| e == "json") {
        serde_json::from_slice(&std::fs::read(path)?)?
    } else {
        ClusterConf::from(path.to_str().ok_or("invalid configuration path")?)?
    };
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(conf, rt.clone())?;
    rt.block_on(run(args, fs))?;
    Ok(())
}
async fn run(args: Args, fs: CurvineFileSystem) -> HsmResult<()> {
    let storage = CurvineStorage::connect(NativeControl::new(fs.clone())).await?;
    let epoch = storage.controller_epoch().await?;
    let ctx = context(epoch, args.op_id);
    match args.command {
        Command::Import { file, path } => {
            let input = tempfile::tempdir()?;
            std::fs::copy(file, input.path().join("data"))?;
            let source = LocalDataMover::payload_endpoint(input.path())?;
            print(&storage.import_file(&ctx, &path, &source).await?)
        }
        Command::Status { path } => print(&storage.resolve(&path).await?),
        Command::Read { path, output } => read_file(&storage, &ctx, &path, &output).await,
        Command::PolicyPut {
            file,
            expected_revision,
        } => {
            let policy: LifecyclePolicy = serde_json::from_slice(&std::fs::read(file)?)
                .map_err(|e| HsmError::Invalid(e.to_string()))?;
            print(&storage.put_policy(&ctx, &policy, expected_revision).await?)
        }
        Command::Policies => {
            let mut after = None;
            loop {
                let page = storage.policies(&ctx, after.as_deref(), 16).await?;
                for item in page.items {
                    print(&item)?;
                }
                after = page.next;
                if after.is_none() {
                    break;
                }
            }
            Ok(())
        }
        Command::Scan { id, after, limit } => print(
            &storage
                .scan_policy(&ctx, &id, after.as_deref(), limit)
                .await?,
        ),
        Command::Jobs => {
            let mut after = None;
            loop {
                let page = storage.intents(&ctx, after.as_deref(), 16).await?;
                for item in page.items {
                    print(&item)?;
                }
                after = page.next;
                if after.is_none() {
                    break;
                }
            }
            Ok(())
        }
        Command::Worker {
            once,
            poll_ms,
            max_actions,
        } => {
            worker(
                storage,
                fs,
                args.archive_root,
                args.backend_id,
                epoch,
                WorkerOptions {
                    once,
                    poll_ms,
                    max_actions,
                },
            )
            .await
        }
        Command::Takeover { expected_epoch } => {
            print(&storage.claim_controller(&ctx, expected_epoch).await?)
        }
        Command::Gc { path } => {
            let state = storage.resolve(&path).await?;
            print(&storage.collect_garbage(&state.data.entity).await?)
        }
        Command::CloseView { path, operation_id } => {
            let state = storage.resolve(&path).await?;
            let ctx = context(epoch, Some(operation_id));
            let view = storage.pending_view(&ctx, &state.data.entity).await?;
            print(&storage.close_view(&ctx, &view).await?)
        }
        Command::AbortRecall { path, operation_id } => {
            let state = storage.resolve(&path).await?;
            let ctx = context(epoch, Some(operation_id));
            let target = storage.pending_recall(&ctx, &state.data.entity).await?;
            print(&storage.abort_recall(&ctx, &target).await?)
        }
        #[cfg(feature = "native-integration")]
        Command::ServeLocal { .. } => unreachable!(),
    }
}

#[cfg(feature = "native-integration")]
fn serve_local(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use curvine_server::test::MiniCluster;
    std::fs::create_dir_all(root)?;
    let root = root.canonicalize()?;
    let manifest = root.join("cluster.json");
    let cluster = if manifest.exists() {
        let (mut masters, mut workers): (Vec<ClusterConf>, Vec<ClusterConf>) =
            serde_json::from_slice(&std::fs::read(&manifest)?)?;
        for conf in masters.iter_mut().chain(workers.iter_mut()) {
            conf.format_master = false;
            conf.format_worker = false;
        }
        MiniCluster::new(masters, workers)
    } else {
        if std::fs::read_dir(&root)?.next().is_some() {
            return Err(
                "serve-local requires an empty new root or its existing cluster.json".into(),
            );
        }
        let conf = local_conf(&root);
        let cluster = MiniCluster::with_num(&conf, 3, 1);
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&manifest)?;
        std::io::Write::write_all(
            &mut file,
            &serde_json::to_vec(&(&cluster.master_conf, &cluster.worker_conf))?,
        )?;
        file.sync_all()?;
        std::fs::File::open(&root)?.sync_all()?;
        cluster
    };
    cluster.start_cluster();
    let mut conf = cluster.master_conf[0].clone();
    conf.format_master = false;
    conf.format_worker = false;
    std::fs::write(root.join("client.json"), serde_json::to_vec_pretty(&conf)?)?;
    eprintln!(
        "local HSM cluster ready; client configuration: {}",
        root.join("client.json").display()
    );
    let stop = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    stop.block_on(async {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! { result=tokio::signal::ctrl_c()=>result, _=terminate.recv()=>Ok(()) }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await
        }
    })?;
    Ok(())
}

#[cfg(feature = "native-integration")]
fn local_conf(root: &std::path::Path) -> ClusterConf {
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
    conf.client.short_circuit = false;
    conf.client.conn_timeout_ms = 5000;
    conf.client.conn_retry_max_duration_ms = 10000;
    conf.client.rpc_timeout_ms = 30000;
    conf.log.log_dir = "stderr".into();
    conf.master.log.log_dir = "stderr".into();
    conf.worker.log.log_dir = "stderr".into();
    conf
}

type Storage = CurvineStorage<NativeControl>;
type Engine = HsmEngine<Storage, LocalArchive, CurvineDataMover>;
struct WorkerOptions {
    once: bool,
    poll_ms: u64,
    max_actions: usize,
}
#[derive(Serialize)]
struct CycleReport {
    admitted: usize,
    executed: usize,
    failed: usize,
    retry: usize,
}

async fn worker(
    storage: Storage,
    fs: CurvineFileSystem,
    archive_root: Option<PathBuf>,
    backend_id: String,
    epoch: u64,
    options: WorkerOptions,
) -> HsmResult<()> {
    if options.poll_ms == 0 || options.max_actions == 0 {
        return Err(HsmError::Invalid(
            "positive poll-ms and max-actions required".into(),
        ));
    }
    let archive = LocalArchive::open(
        archive_root.ok_or_else(|| HsmError::Invalid("worker requires --archive-root".into()))?,
        &backend_id,
    )?;
    let engine = HsmEngine::new(storage.clone(), archive, CurvineDataMover::new(fs));
    let owner = curvine_model::hsm::native::token();
    loop {
        match cycle(
            &storage,
            &engine,
            &backend_id,
            epoch,
            &owner,
            options.max_actions,
        )
        .await
        {
            Ok(report) => {
                print(&report)?;
                if options.once {
                    return if report.failed + report.retry == 0 {
                        Ok(())
                    } else {
                        Err(HsmError::Busy(
                            "tasks require reconciliation; inspect jobs".into(),
                        ))
                    };
                }
            }
            Err(
                error @ (HsmError::Busy(_)
                | HsmError::Conflict(_)
                | HsmError::Unknown(_)
                | HsmError::Io(_)),
            ) if !options.once => eprintln!("cycle deferred: {error}"),
            Err(error) => return Err(error),
        }
        tokio::time::sleep(Duration::from_millis(options.poll_ms)).await;
    }
}
async fn definitions(
    storage: &Storage,
    epoch: u64,
) -> HsmResult<std::collections::BTreeMap<String, LifecyclePolicy>> {
    let mut result = std::collections::BTreeMap::new();
    let mut after = None;
    loop {
        let page = storage
            .policies(&context(epoch, None), after.as_deref(), 16)
            .await?;
        for policy in page.items {
            result.insert(policy.id.clone(), policy);
        }
        after = page.next;
        if after.is_none() {
            return Ok(result);
        }
    }
}
async fn cycle(
    storage: &Storage,
    engine: &Engine,
    backend_id: &str,
    epoch: u64,
    owner: &str,
    max_actions: usize,
) -> HsmResult<CycleReport> {
    let _ = storage.gc_batch().await;
    let definitions = definitions(storage, epoch).await?;
    let mut report = CycleReport {
        admitted: 0,
        executed: 0,
        failed: 0,
        retry: 0,
    };
    execute_ready(
        storage,
        engine,
        &definitions,
        backend_id,
        epoch,
        owner,
        max_actions,
        &mut report,
    )
    .await?;
    for data in storage.pending_seals().await? {
        storage.seal_ordinary(&data).await?;
        execute_ready(
            storage,
            engine,
            &definitions,
            backend_id,
            epoch,
            owner,
            max_actions,
            &mut report,
        )
        .await?;
    }
    for policy in definitions
        .values()
        .filter(|p| p.enabled && p.backend == backend_id)
    {
        let mut examined = 0usize;
        loop {
            let batch = storage
                .scan_scheduled(
                    &context(epoch, None),
                    &policy.id,
                    256.min(policy.selection.scan_budget - examined),
                )
                .await?;
            report.admitted += batch.admitted.len();
            examined += batch.examined;
            for data in &batch.to_seal {
                storage.seal_ordinary(data).await?;
            }
            if batch.next.is_none() || examined >= policy.selection.scan_budget {
                break;
            }
        }
    }
    execute_ready(
        storage,
        engine,
        &definitions,
        backend_id,
        epoch,
        owner,
        max_actions,
        &mut report,
    )
    .await?;
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn execute_ready(
    storage: &Storage,
    engine: &Engine,
    definitions: &std::collections::BTreeMap<String, LifecyclePolicy>,
    backend_id: &str,
    epoch: u64,
    owner: &str,
    max_actions: usize,
    report: &mut CycleReport,
) -> HsmResult<()> {
    if report.executed >= max_actions {
        return Ok(());
    }
    let mut after = None;
    'jobs: loop {
        let page = storage
            .ready_intents(&context(epoch, None), after.as_deref(), 16)
            .await?;
        for intent in page.items {
            let belongs = match &intent.action {
                hsm_core::ManagedAction::Recall(key) | hsm_core::ManagedAction::Archive(key) => {
                    key.backend == backend_id
                }
                hsm_core::ManagedAction::Release => definitions
                    .get(&intent.policy)
                    .is_some_and(|p| p.backend == backend_id),
            };
            if !belongs {
                continue;
            }
            let eligible = intent.status == IntentStatus::Ready
                || intent.status == IntentStatus::Running
                    && intent.claim.as_ref().is_some_and(|c| c.epoch < epoch);
            if !eligible {
                continue;
            }
            match engine
                .run_intent(&context(epoch, None), &intent.id, owner)
                .await
            {
                Ok(result) => {
                    report.failed += usize::from(result.status == IntentStatus::Failed);
                    report.retry += usize::from(result.status == IntentStatus::Ready);
                    print(&result)?;
                }
                Err(HsmError::Busy(_) | HsmError::Conflict(_)) => continue,
                Err(error) => return Err(error),
            }
            report.executed += 1;
            if report.executed >= max_actions {
                break 'jobs;
            }
        }
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    Ok(())
}

async fn read_file(
    storage: &Storage,
    ctx: &OperationContext,
    path: &str,
    output: &std::path::Path,
) -> HsmResult<()> {
    if output.exists() {
        return Err(HsmError::Conflict("output already exists".into()));
    }
    let state = storage.resolve(path).await?;
    let spool = tempfile::tempdir()?;
    let destination = LocalDataMover::payload_endpoint(spool.path())?;
    let evidence = storage
        .read_to_local(ctx, &state.data, &destination)
        .await?;
    // create_new prevents races and never overwrites a caller's file.
    let mut target = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)?;
    std::io::copy(
        &mut std::fs::File::open(spool.path().join("data"))?,
        &mut target,
    )?;
    target.sync_all()?;
    print(&evidence)
}
