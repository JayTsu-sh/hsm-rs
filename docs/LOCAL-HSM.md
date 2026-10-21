# 本地文件后端与 Curvine 生命周期

本文保留 v0.5 原生 import 与显式策略动作的诊断操作。当前 v0.6 的普通 FUSE 自动纳管、周期容量/访问年龄策略及访问自动 Recall，应使用 [PVE-FUSE-HSM.md](PVE-FUSE-HSM.md)；架构与限制见 [DESIGN.md](DESIGN.md)。当前发布版本使用固定 Git 依赖和 Cargo.lock 构建，命令见 [VALIDATION.md](VALIDATION.md)。

v0.5 原语诊断：策略、候选筛选、任务、资格和 HSM 结果均在 Curvine 原权威域；hsm-rs 的外部 worker 只轮询和执行，data-mover-rs 负责字节搬运。本地 archive catalog 记录自己的不可变对象，不镜像源 namespace。当前不需要带库。

## 一键运行

准备 Rust 1.98.1、C++、protoc、Clang/libclang，在 hsm-rs checkout 运行：

```sh
CXXFLAGS=-g0 examples/local-hsm.sh /tmp/my-hsm-scenario
```

指定一个新的专用目录。脚本构建一次，启动正常 journal 的三 Master/单 Worker 本地开发集群；后续每个命令都是独立进程：

1. 生成并导入 3 MiB 左右的文件至 `/.hsm/demo-file`。
2. 安装 Archive 策略，worker 扫描、领取任务，经真实 data-mover Local transfer 归档，状态为 Dual。
3. 安装 Release 策略，worker 独立核对归档内容与回执，源提交撤销在线引用并 GC，状态为 Offline；显式读取被拒绝。
4. 安装 Recall 策略，worker 写入新私有 allocation；Worker 校验并封存，Master 在原 inode 发布，状态回到 Dual。
5. 完整文件比对，再次轮询没有重复动作，保存 `jobs.jsonl`。

退出时仅停止开发集群，保留数据、配置和日志。脚本拒绝重复使用已有 cluster 的目录。该集群由一个进程承载多个真实服务和独立 Raft/元数据目录，适用于开发验证。

## 已有集群

```sh
cargo install --path crates/hsm-curvine --features native --locked
hsm-curvine --config curvine-cluster.toml import ./input.bin /.hsm/demo-file
hsm-curvine --config curvine-cluster.toml policy-put examples/local-policies/archive.json
hsm-curvine --config curvine-cluster.toml --archive-root /srv/hsm/archive --backend-id local worker --once
hsm-curvine --config curvine-cluster.toml jobs
```

重复安装 release/recall 示例策略并执行 worker，即完成三个阶段。省略 `--once` 则按 `--poll-ms`（默认 1000）轮询；`--max-actions` 默认每轮最多执行 64 项。RPC 每页最多检查 16 条记录，单轮遍历策略的全部页；当前按 native key 线性扫描，尚无代价优化索引。首次扫描完成后再执行本轮任务，下一轮重新从头筛选以包含新文件。

默认 native 构建不包含本地服务 harness；`native-integration` 额外提供 `serve-local <directory>`，可重启该目录保存的开发集群。其 `client.json` 是连接/服务配置，不是文件元数据镜像。CLI 同时支持原 Curvine TOML 配置和此 JSON 配置。

## 策略合同

策略示例见 [examples/local-policies](../examples/local-policies)。字段均在源端校验：

| 字段 | 语义 |
|---|---|
| id / revision | 稳定规则名和 CAS 版本；创建为 1，更新需 `--expected-revision N` 且文档 revision=N+1 |
| enabled | 关闭后不再入队；未执行的旧版本任务在领取时变为 Superseded |
| prefix | `/.hsm/` 内的文件名前缀 |
| min_size / max_size | 包含边界的逻辑大小；max_size=null 表示无上限 |
| min_residency_ms | 当前 Online/Dual/Offline 驻留状态的最短持续时间，使用源端提交时间；不是 atime/空闲时长 |
| action / backend | archive、release、recall，以及逻辑后端 ID；规则不含物理介质位置 |

Archive 选择有在线布局的 Online 文件；Release 选择 Dual、有匹配 backend anchor、无 pin/私有布局的文件；Recall 选择有对应 anchor 的 Offline 文件。尚未完成的 import 不会被筛选。旧版本记录缺少驻留起始时间时，不满足正数时间门槛。

扫描器提交一个有界事务：策略原值、候选 record version、已有任务和每实体 active index 都做 CAS；任何条件变化使整个批次不入队，调用方需用新的扫描请求 ID 重试。cursor 指向最后检查的记录，包括未匹配项；分页不是全局快照，下一轮从头补扫。

任务 ID 来自 policy ID、policy revision 和完整 DataKey。**同一规则版本对同一内容 generation 最多一次**；重复扫描、多个扫描器和换主不会重复入队，每个实体同时最多一个未完成策略任务。因此示例三条规则不会反复迁移。修改规则版本会重新开放该版本所覆盖的 generation；修复错误后重试可用新规则版本。长期自动冷热振荡/访问热度策略不在此版本内。

## 领取、完成与恢复

Ready → Running → Done / Failed / Superseded；不确定结果回 Ready，保留同一个 effect operation ID，以便查询原始效果或续传。每次 claim 有独立 ID 和 record version。当前 epoch 的 Running 任务不允许另一个 worker 抢占；进程死亡后，显式接管：

```sh
hsm-curvine --config curvine-cluster.toml takeover --expected-epoch 1
hsm-curvine --config curvine-cluster.toml --archive-root /srv/hsm/archive worker --once
```

接管把全局 controller epoch 条件提升，旧控制者的新源端修改和完成上报被拒绝。新 worker 可恢复较旧 epoch 的 Running 任务；原任务动作 ID 不变。资格不计入逻辑请求指纹，pending 布局仍检查动作 ID/attempt，所有修改仍在 apply 校验当前 epoch。源端已成功、任务尚未 finish 时，原成功结果优先，既不再次搬运，也不把它误记成失败。

本地 backend 的 attempt 身份同样独立于 controller epoch；复制结束但 seal/receipt 发布之间发生中断时，由 backend 独立完整验证、同步数据并续完提交。失败或 Unknown 不能被扫描器当作 anchor；执行器也没有正式发布/源删除权限。

Done 必须能在源端查询到匹配 DataKey/action 的原始成功结果；只上报 Done 会被拒绝。取消或更新规则不撤销已经运行的数据 I/O。需要先停止旧 I/O，再使用显式恢复入口：

```sh
hsm-curvine --config curvine-cluster.toml close-view /.hsm/demo-file <job.context.operation_id>
hsm-curvine --config curvine-cluster.toml abort-recall /.hsm/demo-file <job.context.operation_id>
hsm-curvine --config curvine-cluster.toml gc /.hsm/demo-file
```

Failed 保留诊断与资源引用，不按超时删除；修复原因、必要时关闭 pin/abort 后，再用新策略版本选择。Unknown 自动重试原动作。归档内容损坏时 Release 失败，保留在线副本。

## 范围

沿用首版受管范围：新建的 `/.hsm` 直接子文件、不可变内容、file-backed Worker、本地 fsync 归档、显式 Recall、native attempt epoch 1。task claim 次数与 attempt epoch 是不同概念。当前 worker 一次打开一个本地后端，archive root 使用 OS 独占锁；同一后端 ID 应始终指向同一目录。

尚无普通文件原地转管、可变 generation、FUSE 透明召回、访问热度/容量水位策略、普通块统计/副本修复集成或生产掉电/盘损坏矩阵。带库后续通过后端/执行器接口接入。
