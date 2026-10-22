# Curvine 原生 HSM Adapter

当前支持普通 FUSE 文件自动纳管、策略驱动归档释放，以及普通读取触发的整文件自动 Recall。原生不可变 `/.hsm/<name>` import 继续用于原语诊断。hsm-rs 协调工作流，Curvine 保存复制的权威记录和原始结果，data-mover-rs 搬运，LocalArchive 断言后端提交。没有外部全量 namespace 镜像，没有旧文件系统兼容路径。

策略/任务、CLI、一键本地集群场景及恢复步骤见 [本地 HSM 与生命周期](../../docs/LOCAL-HSM.md)。

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

必须 `testing=false`。Master 以 `hsm-format-v1` 标记该卷的格式要求；不能在已有 HSM 卷上关闭该开关。首次状态请求通过提交日志创建保留目录 `/.hsm`；该目录应事先不存在。该保留目录的创建和修改仍走原生接口；标准使用方式是在普通目录通过 FUSE 写入。单硬链接、非 UFS 常规文件自动进入存储侧 HSM 记录。全部 Master、FUSE 与 SDK 应一起升级，不能混用旧的受管读取路径。

原生能力包括 committed records、稳定 view、条件 Release、私有 Recall 和持久成功结果；至少三 Master 的配置报告 distributed_authority。透明 Recall 为 true；普通读取自动排队并等待恢复。容量预留仍为 false。本地 archive 的 `local-fsync-v1` 不是介质或整机 HA 承诺。

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

`ctx` 产生稳定、全局唯一工作流 ID、当前 controller epoch、attempt epoch 1。不同动作使用不同 ID，重试同一动作保持 ID 和输入。Curvine 的临时 Busy 可以在保护解除后重试；源观察变化会终止旧任务，再按新观察入队。`controller_epoch()` 读取资格，接管时 `claim_controller(ctx, expected_epoch)` 条件提升；旧 epoch 不能修改存储。归档 Key 独立于 attempt 和物理位置。

`resolve(path)` 获取原生 DataKey；`read_to_local(ctx, data, destination)` 取得持久 pin、复制并关闭 view；`collect_garbage(entity)` 重试 committed GC。协调者重开后可通过 `pending_view` / `pending_recall` 找到 pin/私有布局，停止旧 I/O 后 `close_view` / `abort_recall`。这些事实全部来自 Curvine。

## 执行器与持久性

`CurvineDataMover` 实现通用 DataMover，支持 Curvine view → Local、Local → Curvine private layout 以及 Local → Local。当前使用私有全文件 spool 和真实 LocalDataMover Checkpointed transfer，内存有界，但需要完整临时空间和额外复制。

Worker 按块接收，重复前缀必须内容一致，Verify 独立读取完整目标。Master CommitRecall 再要求 Worker Seal：同步数据、验证 length/digest、持久撤销写权限，然后提交原 inode 的 published 引用。旧写/删除不会作用于另一 allocation；墓碑跨重启保留。Release 先提交撤销引用和 GC outbox，再做物理删除；GC 错误保留待重试状态。

默认无 native feature，只编译协议 Adapter 和通用合同测试。`native` 引入 SDK/执行器；`native-integration` 额外引入 server 测试 harness，不属于生产客户端依赖。

```sh
CXXFLAGS=-g0 cargo test --workspace --locked \
  --features hsm-curvine/native-integration -j 6
cargo fmt --all --check
```

需要 Rust/C++ 工具链、protoc、Clang/libclang；`CXXFLAGS=-g0` 减少 RocksDB C++ 调试信息。managed_cluster 启动真实三 Master/单 Worker 子进程，三次强制重启均排除旧 leader，保留实际数据目录；验证完整字节、身份、原始结果、pin、epoch、物理 GC 和坏 Recall 不发布。ignore 的 process_cluster_child 只由测试父进程启动。

普通 `copy_current_file_to_local` 仍是独立诊断 helper，不参与此闭环，也不会构造稳定 view 或授权释放。

策略字段、元数据索引、容量水位、周期预算、普通写入钩子及访问触发过程见 [实现设计](../../docs/DESIGN.md)。配置和跨节点实测见 [验收记录](../../docs/VALIDATION.md)。

当前仍不提供完整 GPFS 策略文本解析、恢复布局上的追加写、受管硬链接、硬容量预留或磁带后端。Recall 的私有 Worker 布局仍为单副本。没有完整断电、盘损坏和生产故障矩阵验证；失败资源的保护以存储事实为准。


常驻 `worker` 现在为 Recall 和 Archive/Release 各保留一个执行位置；后台轮次继续扫描/Seal，Recall 单独取 Source 的 ready work class。`--max-actions` 为每通道每轮上限，JSON 报告的 `lane` 区分 recall/background；`--once` 仍只执行一个顺序轮次。使用匹配的 Source 版本，分类分页仍返回不透明游标，不允许跨类别复用。工作认领、结果及失败恢复仍由 Curvine 控制。


Curvine 文件级控制和 Seal 现在共享资格等待 Raft，提交时核对版本与当前路径。并发读写使准备过期时 Source 返回 Busy，不创建永久失败 outcome；重试保持原 operation_id 和动作输入，由 Source 重新准备。业务观察已经变化时仍按 Conflict/Superseded 处理，不能把旧 Release 观察改成当前状态继续执行。Native 创建与私有 Import 使用同一个 inode 预留器，首次 namespace 创建/升级才独占；完整设计与格式 `hsm_native_v7` 的版本要求见[文件级条件提交](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/conditional-controls.md)。策略更新/入队、claim/finish 等跨记录控制仍保留隔离。
