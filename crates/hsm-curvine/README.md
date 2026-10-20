# Curvine 原生 HSM Adapter

当前支持新建、不可变 `/.hsm/<name>` 文件的 **Archive → Release → Recall** 闭环。hsm-rs 协调工作流，Curvine 保存复制的权威记录和原始结果，data-mover-rs 搬运，LocalArchive 断言后端提交。没有外部全量 namespace 镜像，没有旧文件系统兼容路径。

## 启用和范围

所有 Curvine Master/Worker 使用本分支协议，配置：

```toml
[master]
hsm_enabled = true
[journal]
enable = true
writer_channel_size = 0
leader_create_snapshot = false
```

必须 `testing=false`。Master 以 `hsm-format-v1` 标记该卷的格式要求；不能在已有 HSM 卷上关闭该开关。首次状态请求通过提交日志创建保留目录 `/.hsm`；该目录应事先不存在。这里只支持直接子文件。普通 SDK 的创建/修改/open 不能进入这个作用域；读数据使用下面的完整文件 view 接口。其他 namespace 保持原来的路径。

原生能力包括 committed records、稳定 view、条件 Release、私有 Recall 和持久成功结果；至少三 Master 的配置报告 distributed_authority。容量预留、透明 Recall 为 false，使用者应显式 Recall。本地 archive 的 `local-fsync-v1` 不是介质或整机 HA 承诺。

## 组合

调用方已有原生 SDK `CurvineFileSystem`，并创建独立本地 input root，其中 payload 名为 `data`：

```rust,ignore
let storage = CurvineStorage::connect(NativeControl::new(fs.clone())).await?;
let mover = CurvineDataMover::new(fs.clone());
let input = LocalDataMover::payload_endpoint(input_root)?;
let initial = storage.import_file(&ctx("import"), "/.hsm/file", &input).await?;
let backend = LocalArchive::open(archive_root, "local-archive")?;
let engine = HsmEngine::new(storage.clone(), backend, mover);
let key = ArchiveKey { backend: "local-archive".into(), object: unique_archive_id };
let dual = engine.archive(&ctx("archive"), &initial.data, &key).await?;
let offline = engine.release(&ctx("release"), &dual).await?;
let restored = engine.recall(&ctx("recall"), &offline, &key).await?;
assert_eq!(restored.data, initial.data);
```

`ctx` 产生稳定、全局唯一工作流 ID、当前 controller epoch、attempt epoch 1。不同动作使用不同 ID，重试同一动作保持 ID 和输入；确定失败后改变条件重试应使用新 ID。`controller_epoch()` 读取资格，接管时 `claim_controller(ctx, expected_epoch)` 条件提升；旧 epoch 不能修改存储。归档 Key 独立于 attempt 和物理位置。

`resolve(path)` 获取原生 DataKey；`read_to_local(ctx, data, destination)` 取得持久 pin、复制并关闭 view；`collect_garbage(entity)` 重试 committed GC。协调者重开后可通过 `pending_view` / `pending_recall` 找到 pin/私有布局，停止旧 I/O 后 `close_view` / `abort_recall`。这些事实全部来自 Curvine。

## 执行器与持久性

`CurvineDataMover` 实现通用 DataMover，支持 Curvine view → Local、Local → Curvine private layout 以及 Local → Local。当前使用私有全文件 spool 和真实 LocalDataMover Checkpointed transfer，内存有界，但需要完整临时空间和额外复制。

Worker 按块接收，重复前缀必须内容一致，Verify 独立读取完整目标。Master CommitRecall 再要求 Worker Seal：同步数据、验证 length/digest、持久撤销写权限，然后提交原 inode 的 published 引用。旧写/删除不会作用于另一 allocation；墓碑跨重启保留。Release 先提交撤销引用和 GC outbox，再做物理删除；GC 错误保留待重试状态。

默认无 native feature，只编译协议 Adapter 和通用合同测试。`native` 引入 SDK/执行器；`native-integration` 额外引入 server 测试 harness，不属于生产客户端依赖。

```sh
cargo test --workspace --locked
CXXFLAGS=-g0 cargo test --workspace --features hsm-curvine/native-integration --locked -j 6
CXXFLAGS=-g0 cargo clippy --workspace --all-targets \
  --features hsm-curvine/native-integration --locked -j 6 -- -D warnings
```

需要 Rust/C++ 工具链、protoc、Clang/libclang；`CXXFLAGS=-g0` 减少 RocksDB C++ 调试信息。managed_cluster 启动真实三 Master/单 Worker 子进程，三次强制重启均排除旧 leader，保留实际数据目录；验证完整字节、身份、原始结果、pin、epoch、物理 GC 和坏 Recall 不发布。ignore 的 process_cluster_child 只由测试父进程启动。

普通 `copy_current_file_to_local` 仍是独立诊断 helper，不参与此闭环，也不会构造稳定 view 或授权释放。

当前不支持存量普通文件直接转管、可变文件、嵌套 managed 目录、attempt > 1、自动生命周期/任务 daemon、透明 offline 访问、容量预留、普通 Worker 块统计/副本修复、SPDK 或磁带。未验证断电、盘损坏和全部故障矩阵；初版不自动清除失败 import/pin/private layout，避免把未知结果误当失败。
