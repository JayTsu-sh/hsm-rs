# 普通 FUSE 文件 HSM 验收与规模验证

2026-10-20。本轮工作区新增普通文件自动纳管、容量与访问年龄策略、周期索引扫描，以及普通读取触发的透明 Recall。详细操作和 PVE 证据见 [PVE-FUSE-HSM.md](PVE-FUSE-HSM.md)，责任与一致性设计见 [DESIGN.md](DESIGN.md)。

## 构建来源

首次发布版本（2026-10-21）合入 hsm-rs main 与 Jay fork 的 Curvine main。当时 hsm-curvine 的 Cargo.toml 固定过渡微批代码 `ad7fdaa2667cf1ce1c7fd614989c56e82d6560ec`；下文 P1 验收所用的历史 Curvine 提交是 `826a2ae9be052b2bf86d1a1647551e396adfcc5f`。Curvine 固定 hsm-core `ef5167661da40345a25f5d028dbeb4f1a70b1398`；实际 data-mover-rs 固定为 `11fac12eee028c679293e4df6220584512561487`。Cargo.lock 包含可从 GitHub 获取的源地址，无需本地路径补丁。本轮没有修改带库后端。

从本仓库克隆后，可直接执行：

```sh
CXXFLAGS=-g0 cargo test --workspace --locked \
  --features hsm-curvine/native-integration -j 6
cargo fmt --all --check
```

2026-10-20 的本机及 PVE 验收在发布前使用联合工作区 Cargo patch；下表记录该轮实际测试。多仓库联合开发仍可用 `tools/curvine-workspace-config.py` 生成 path patch，发布构建不需要该步骤。

需要 Rust/C++、protoc、Clang/libclang 和真实 RocksDB。默认 feature 不启动原生集群测试，不能用默认测试为零推断闭环完成。上述全工作区命令是复现入口。下面的分组结果记录 2026-10-20 验收；2026-10-21 固定 Git 依赖的完整重跑见发布检查，没有把历史严格 Clippy 结果记为本轮结果。

## 已通过的测试组

| 组 | 结果 | 验证内容 |
|---|---|---|
| hsm-core、Curvine 合同、LocalArchive、本地生命周期 | 24 通过 | 模型、适配合同、真实本地搬运和后端恢复 |
| Curvine Master 单元 | 3 通过 | 高低水位滞回；真实条件函数的大小、访问年龄、保护与排除筛选；启动格式保护 |
| 普通文件真实进程集群 | 3 通过 | 普通写入、策略 Release、并发自动 Recall；访问废弃旧 Release 并可重新选择；重命名、截断及换主重放 |
| 既有原生真实进程集群 | 2 通过 | 三次强制重启的原语保护；策略任务接管与 CLI 执行 |

原生进程集群为三个 Master 与一个 Worker，独立真实目录。ignored 的 process_cluster_child 是父测试显式启动的 fixture，不是未执行的业务验收。

普通文件测试还验证：中断的摘要校验由独立 CLI worker 在没有启用策略时恢复；普通块视图使用小于 1 KiB 的紧凑能力引用，关闭后再使用被 Fenced；读取后的旧 Release 任务 Superseded；内容更新推进 generation 并清除旧锚点。既有原生测试验证持久 pin、物理 GC、损坏 Recall 不发布、旧 epoch 被拒绝及源已提交而任务尚未 finish 的接管窗口。

编译完成 curvine-server、curvine-fuse、curvine-cli 与 hsm-curvine；格式和差异检查通过。日志保存在 `/work/jay/hsm/research/hsm-upgrade-*.log`。本轮未执行最终版严格 Clippy、盘损坏或完整断电矩阵。

## 2026-10-21 发布检查

移除本地 path patch，使用上述 Git rev 与 Cargo.lock 执行 `cargo test --workspace --locked --features hsm-curvine/native-integration -j 6 -- --test-threads=1`（`CXXFLAGS=-g0`）：**30 通过、0 失败、1 个子进程 fixture 标记 ignored**。其中五项真实多进程受管集群测试全部通过，另含一项原生 SDK 搬运测试；ignored fixture 由父测试显式启动。

Curvine 使用已发布 hsm-core Git 依赖运行 `cargo test -p curvine-master --lib master::hsm:: -j 6`：**3 通过**。两个仓库的相关格式、差异检查通过；hsm-rs 文档本地链接检查通过。锁文件中的 29 个 Curvine package 均指向固定 Git 提交，没有残留本地 patch 条目。

日志位于开发环境 `research/hsm-publish-workspace-tests.log`、`research/hsm-publish-curvine-unit.log`，路径相对于 `/work/jay/hsm`。这是本次发布前的本机重新验证；PVE 验收仍为下面记录的 2026-10-20 部署，未因源码推送而重新部署。

## 2026-10-21 规模优化后复验

固定 Curvine `16885bb4`，完整工作区 native-integration 回归 **30 通过、0 失败、2 ignored**；ignored 分别为被父测试启动的子进程 fixture 和需显式运行的规模测试。Curvine HSM/有界清理回归 **8 通过、0 失败**，另一个 ignored 为独立运行的元数据基准。相关格式与差异检查通过；没有将历史严格 Clippy 结果记为本轮结果。

真实 RocksDB 的百万 Record/两百万 KV 基准，优化前后各重复三次：排序键不变的更新由每次 5 个 KV 操作降至 1 个，真实 atime 更新由 5 个降至 3 个；清理最大批次从两百万键降至 1024 键，采样峰值 RSS 中位数从 349.6 MiB 降至 193.9 MiB。清理耗时中位数 14.461 → 14.917 秒，主要收益是内存边界和减少冗余写。

显式运行的真实 Curvine 百万 namespace 测试通过：1,000,000 个空文件使用正常 create/complete 钩子与 journal 内部批量预置，SDK 实际计数；另有两个 SDK 写入的非空文件。零匹配策略检查 1,000,002 项、3908 页，耗时 139.310 秒；两个文件经实际 data-mover LocalArchive Release 后，三次普通 SDK 读合并为两个自动 Recall。强制停止、排除原 leader，从 follower checkpoint 恢复后再次核对计数、身份和完整内容。整个测试约 1043 秒。三 Master 和一 Worker 在同一 fixture 进程、独立目录，控制器为独立进程；这不是百万普通客户端写入吞吐或物理节点故障隔离测试。

真实 PVE 另行完成 2000 个普通 SDK 空文件写入（16 并发、33.232 秒、零错误），以及新目录的普通 FUSE 写入、容量/访问时间策略、双副本物理 Release 和四读合并两次自动 Recall。三 Master 日志迁至各自独立 16 GiB 本地 SSD，旧日志保留；该硬件变化的收益与源码优化分开报告。Worker 仍为各节点本地盘，无 UFS。

复现命令、环境、原始 JSON 与精确边界见 [完整规模测试报告](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/scale-validation.md)。本仓库测试入口为 `crates/hsm-curvine/tests/managed_cluster.rs` 的 `million_namespace_policy_recall_and_checkpoint_recovery`；native-integration 才启用 Curvine 的可选 `hsm-scale-test` fixture，普通服务器默认不启用。

## 原版 Curvine 与 HSM 的前台性能

另外使用完全相同的 release 基准，分别测试原版 Curvine `feb3c9e2`、当前服务器/SDK 关闭 HSM、开启 HSM 自动纳管。每组独立新建三个 Master/一 Worker 的同进程 MiniCluster，SDK 通过真实 TCP/Raft 操作，禁用短路、无 UFS；三轮交替执行。普通读写保持 Online，不运行 Archive/Recall 搬运。

16 并发创建并关闭空文件的吞吐中位数从原版 5690.5 降到开启 HSM 后的 96.9 ops/s，P99 从 3.242 增到 195.820 ms；空文件 open+complete 为 13292.9 → 94.0 ops/s。get_status 基本持平。该历史基线的主要结构性成本是全局提交串行边界和额外 barrier/受保护读取提交；本次没有修改这些生产路径。HSM 关闭组也有 SDK open 探测开销，不能等同于原版。

完整元数据、小文件、加长大文件测试和扫描并发结果、原始 JSON、源码/二进制指纹及复现方法见 [原版与 HSM 性能对比](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/performance-comparison.md)。这些同主机 SDK 工程基线与百万 namespace 验证属于不同范围，不是 PVE 上原版/HSM 的 A/B 或生产吞吐 SLO。

## 2026-10-21 普通日志提交回执改造验收

hsm-curvine 的 Curvine Git 依赖更新到 `826a2ae9be052b2bf86d1a1647551e396adfcc5f`，全工作区 `--locked`、`native-integration`、单测试线程的发布来源回归 **30 通过、0 失败、2 ignored**。五项真实多进程受管集群测试均通过；两个 ignored 分别是子进程 fixture 和需显式运行的百万规模测试。Curvine Master journal 单元测试 **26 通过**，HSM/恢复单元测试 **8 通过、1 ignored**，真实 Raft term/多数派提交测试 **2 通过**。

另显式运行百万 namespace 测试 **1 通过**。内部批量预置 1,000,000 个文件，零匹配策略扫描 1,000,002 项、3908 页；两份普通 SDK 写入的非空文件经策略 Release 和 LocalArchive 搬运，三次普通读取触发两次自动 Recall。排除旧 leader 后从 checkpoint 恢复，身份、驻留和内容逐项保持。结果原始 JSON 与改造前后 release 性能对照见 [Curvine 提交回执验收](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/commit-receipts.md)。百万建库采用内部批量 fixture，三个 Master 与一个 Worker 仍在同一测试进程内的独立目录；不能视为物理机 HA 或百万普通 RPC 写入吞吐验收。规模测试与 Git 依赖编译曾重叠，其耗时不用作性能收益对比。

相同 HSM 开启语义下的三轮 release 配对测量：16 并发 create+complete 从 95.7 提高到 198.1 ops/s，rename 从 205.3 到 389.2 ops/s，delete 从 202.0 到 400.4 ops/s；普通修改区间空 HSM barrier 为 0。64 MiB 4 并发写入中位吞吐 1040.8 → 1166.3 MiB/s，但 P99 369.44 → 489.81 ms，读取中位吞吐 2497.7 → 2399.1 MiB/s；不能宣称大文件路径稳定改善。测试方法、全部轮次和二进制指纹见 [改造前后性能结果](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/commit-receipt-results.md)。

## 2026-10-21 普通元数据过渡微批验收

hsm-curvine 的八个 Curvine Git 依赖及 Cargo.lock 均固定 `ad7fdaa2667cf1ce1c7fd614989c56e82d6560ec`。取消本地 path patch，执行 `CXXFLAGS=-g0 cargo test --workspace --locked --features hsm-curvine/native-integration -j 6 -- --test-threads=1`：**30 通过、0 失败、2 ignored**。五项真实多进程集群测试全部通过，覆盖策略 Release、普通读取自动 Recall、访问废弃旧 Release、进程重启及换主；两个 ignored 分别为子进程 fixture 与需显式启动的百万规模测试。`cargo fmt --all --check` 与 `git diff --check` 通过。开发环境日志位于 `/work/jay/hsm/research/p2-git-integration-final.log`。

Curvine 在最终 `ad7fdaa2` 上 journal 单元 **27 通过**，Master HSM 相关单元 **8 通过、1 ignored**。百万 namespace 闭环另在首个 P2 提交 `76c624f5` 上显式通过；后续提交只把批前固定 750 微秒等待改为一次调度让出，并增加基准连接池参数。三轮 P1/P2 同语义 release 对照中，16 条 Master 连接下 256 次并发 create+complete 的实际 Raft flush 从 512 降到 64，中位吞吐 202.2→1322.8 ops/s、P99 93.89→13.26 ms；默认单连接 smoke 为 187.19→189.90 ops/s，flush 均 32。读侧尚未合批，`get_status` 在该 VM 上约慢 13%，大文件结果波动大。方法、全部逐轮数据、限制和二进制指纹见 [Curvine 过渡微批验收](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/mutation-microbatch-results.md)。这不代表已完成统一 committed 状态机、跨物理故障域 HA 或 PVE 集群升级验收。

## 真实 PVE 集群

三个 Master、三个 Worker、两个普通块副本及独立 HSM 控制节点，不配置 UFS。应用通过 FUSE 写入和读取，没有 import 或显式 Recall。候选和任务位于 Curvine 权威元数据，不依赖外部 namespace 镜像。两轮测试及物理块检查见 [PVE-FUSE-HSM.md](PVE-FUSE-HSM.md)。

升级历史 journal 到 ordinary_format=2 后，上一轮 A、B 的公开 EntityState、原 inode、DataKey 和归档锚点逐字段保持一致，三 Worker 全部恢复。该检查独立于全新目录的闭环测试。

## 2026-10-22 累计性能改造与 HA 回归

当前 Curvine 固定版本以 [Cargo.toml](../crates/hsm-curvine/Cargo.toml) 和 Cargo.lock 为准。Native 创建/覆盖、块分配、关闭/flush、可写 open 和非递归 SetAttr/Delete 使用稳定请求身份、条件提交及原始结果。普通元数据查询合批取 ReadIndex；仅操作私有准备数据的路径共享投影资格，旧式修改/控制独占。相邻已准备好的 Native/HSM entry 可以合用 proposal，每个 ticket 保留自身回执。Curvine 模块 247 项和两个优化集群用例已通过。八个 Curvine 依赖及 Cargo.lock 固定 `122d9535f941b6b738422935dedbee89aa13928c`，取消本地 path patch 后执行 `CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test --locked --workspace --features hsm-curvine/native-integration -j 6`，30 项功能测试（含五个 managed 集群场景和一个 native 场景）及全部文档测试通过，两个规模/子进程入口保持 ignored。没有沿用只测试本地 path 的结果作为固定 Git 版本的验收。

进程重启用例增加普通文件已提交删除、尚待物理清理的场景。保留磁盘目录并排除旧 Leader 后，要求首次 Exists 自身正确、HSM Record 不存在、退休义务仍能处理并最终得到 Worker 删除确认。此测试曾复现普通 Exists 读取旧前缀；ReadIndex 后即正确，说明删除日志未丢失，修正的是元数据查询资格。该用例使用 Master/Worker 进程组重启，不能等同于独立物理节点/磁盘故障验收。

以上为截至 `122d9535` 的累计记录。本轮没有升级 PVE，没有运行当前版本的百万验收或生成优化后的原版性能差值；历史数值保持各自冻结版本的适用范围。后续目录和后台改造见下节。

## 2026-10-22 目录、后台删除与 Native Resize

八个 Curvine 直接 Git 依赖与 lock 的 29 个 Curvine 包固定 `6a60ef60f6dc1b10f0d830a9f265e4a0d20a3b9a`，其他包的版本和依赖关系不变。该版本新增紧凑 namespace journal v5 和 `hsm_native_v6` checkpoint 栅栏，要求全部 Master 使用匹配实现；未升级 PVE。

目录 rename/替换/交换、递归 SetAttr/Delete 和无挂载 TTL Delete 由已提交输入原子发布。Resize/AssignWorker 同样保存原始结果与退休事实。共享 ReadIndex 队列只合并证明开始前入队的请求；旧任期、证明失败及 I/O 期间后来到达的请求不能借用资格。配额跳过不计回收进度，同步回收工作进入 blocking 线程。Curvine 的 260 项 Master 测试和两个真实集群优化用例已通过。

增强的进程组重启回归先写入两个普通文件，对保留文件做部分截断，改名父目录并递归更新 owner/mode，再删除另一个子目录。保留磁盘目录、排除旧 Leader 重启后，首次 Exists 必须正确；保留文件的 inode、长度、属性及内容不变，已删除文件的 HSM Record 不存在，持久清理最终取得 Worker 确认。它仍是进程组冷恢复，不能充当单节点在线故障、网络分区或磁盘丢失验收。

属性响应的 xattr 按键排序编码，保持原 protobuf wire tag/type。重启测试还保存原始 Native 属性请求及响应，在删除子目录、改变目录状态后换主，要求重试返回完全相同的原始响应字节，不能重新生成当前目录状态。

固定来源的组合验证命令（不使用 path patch）：

```bash
CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test --locked --workspace \
  --features hsm-curvine/native-integration -j 6
```

固定 Git 来源回归结果：**30 项功能测试通过、0 失败、2 ignored**，全部文档测试执行成功。五个 managed 场景全部通过；增强的进程组重启用例包含部分截断、目录改名、递归属性、持久删除和原始属性响应逐字节比较。独立 native SDK 搬运场景也通过。两个 ignored 分别为父测试显式启动的子进程 fixture 与未执行的百万规模测试。日志：`research/hsm-namespace-canonical-git-pinned.log`（相对于 `/work/jay/hsm`）。

默认 feature 的 workspace 测试不启动真实 Curvine 场景；必须显式开启 `native-integration`。当前仍未完成所有稀有 RPC 迁移、完整业务优先级调度和组合故障矩阵，没有把资格证明合批描述为完整业务调度器。当前百万验收及最终原版对比继续暂缓。

## 2026-10-22 批量写、加权调度与独立执行通道

八个直接依赖和 29 个 Curvine Git 包固定 `222a32549739d91d9e685fad29647f9fe6532918`，其他依赖与原 lock 完全一致，没有路径覆盖。Curvine 当前 270 项 Master 测试及两个 TCP/Raft 优化用例通过。

三个公开批量 API 使用一个稳定请求身份、一份原始结果和一次原子发布；创建/块分配遇错时保存成功前缀及原始错误，关闭保留逐项结果。目录 owned 命令共享资格等待 Raft，apply 复核准备后提交的读者。Master 的 Recall/前台/后台按 4:8:1 调度，各类接入名额独立；实际提交后的不确定结果仍需确认前缀。

常驻 worker 为 Recall 与 Archive/Release 保留独立执行通道，各最多一个动作；后台仍进行扫描与 Seal。`--once` 保留原顺序语义，常驻 `max-actions` 为每通道每轮上限，报告新增 `lane`。Source 按既有 work:0:/work:1: 索引分别分页，客户端只持有有界页面，不维护外部任务/文件镜像。

本轮新增或增强的场景：

- 实际 Raft commit/local apply 后丢失指定 Native 会话回执，首次返回 Uncertain；改名后用原身份重试返回原始批量错误，不重建旧路径（Curvine 优化集群用例）。
- 全进程冷恢复时，对 CreateFilesBatch、AddBlocksBatch、CompleteFilesBatch 保存完整请求/响应；成功前缀后来改名后，在新 Leader 上逐字节比较原始结果。
- 三个 Master 分别运行于独立进程。暂停当前 Leader、两个存活节点选主、恢复旧进程，再暂停替代 Leader；恢复的旧节点必须参加下一次 quorum，随后完成 Release/Recall 并校验 2 MiB+29 字节及 inode/data identity。使用相同操作身份重试可重试错误。
- 暂停真实 CLI worker 的后台执行轮次，普通读取仍自动 Recall 并校验 1 MiB+37 字节；此时后台文件仍 Online。恢复后台后，其 Archive 也完成。暂停 hook 仅随 native-integration 编译。

在线测试曾发现旧 Leader 因读批尚未提交时的资格失败而退出；修复将无修改的预检失败与真正提交未知分开。保留原失败日志 `research/hsm-live-ha-seal-fixed.log`；修复后的旧版本固定来源回归 `research/hsm-owned-batches-live-ha-fixed-git.log` 已通过 31 项功能与全部文档测试。本节的新通道组合日志为 `research/hsm-worker-lanes-full-fixed-git.log`（均相对于 `/work/jay/hsm`）。最终固定来源组合回归：**32 项功能测试通过、0 失败、3 ignored**，全部文档测试执行成功；七个 managed 场景全部通过（含独立进程在线两次选主、后台暂停期间自动 Recall），单独 native 搬运场景也通过。三个 ignored 为百万规模测试与两个由父测试显式启动的子进程 fixture。

暂停/恢复进程验证在线失联和旧任期处理，不代表磁盘故障或选择性网络分区。LocalArchive 仍单节点、私有 Recall 仍单副本、控制者接管仍需显式 epoch；本轮未升级 PVE，未执行新版百万验收或最终原版性能对照。普通 HSM 控制仍保留独占提交，少数兼容/后台路径的收敛继续按优化方案推进。

## 边界与历史记录

当前支持单硬链接、非 UFS 常规文件。策略是 GPFS 对应概念的类型化子集，不解析 GPFS 语法或任意权重表达式。索引分页减少目录扫描；大小、owner 等条件已在当前路径解析前检查，但仍需 Record 回查，路径仍后过滤。已完成百万 Record 基准及内部批量预置的百万 namespace 验证，尚未验收百万普通客户端写入吞吐；当前 owned 路径使用共享准备与条件发布，旧式修改仍有独占边界；当前代码的完整百万验收尚未执行。

Recall 为整文件恢复，私有恢复布局单副本，尚不参与普通块副本修复；全量覆盖可用，恢复布局上的追加与完整可变 POSIX 语义尚未覆盖。有活跃读取或归档保护时修改可能被拒绝。LocalArchive 单节点，控制者 Running 接管仍需显式 epoch。所有虚拟机在同一 PVE 物理机，没有生产 HA 承诺。搬运需要完整临时 spool。

[v0.5 PVE 原语测试记录](PVE-HSM-SCENARIO.md)保留为历史：当时是 import、策略显式 Recall，普通 FUSE 返回 EIO。本轮 v0.6 的普通 FUSE 自动纳管和透明 Recall 已新增，不能混用两轮结论。旧的严格 Clippy、固定 Git pin 和不可变文件限制也仅对应当时版本。


## 2026-10-22 文件级条件提交与 namespace 共享绑定

八个直接依赖及 lock 的 29 个 Curvine Git 包固定 `ad3391bd3cc21cf6ab6f721026c3cc069bbfa3f1`；其他 lock 内容逐字节保持，没有路径覆盖。执行 `CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test --locked --workspace --features hsm-curvine/native-integration -j 6`：**32 项功能测试通过、0 失败、3 ignored**，四个 crate 的全部文档测试执行成功。格式与差异检查通过。

本轮 Source 改为文件级 HSM 控制、Seal 和只读查询共享资格，Native/Import 统一 inode 预留，已有 namespace 的新 SDK 会话绑定也共享读取。新条件日志在版本/路径/create 观察过期时返回 Busy，既不发布状态，也不固化失败绑定；同一逻辑身份可重新准备。业务拒绝则重新独占检查。新增 `hsm_native_v7` checkpoint 栅栏，全部 Master 必须匹配代码；详见[Source 设计](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/conditional-controls.md)。

七个真实 managed 场景全部通过（86.71 秒）：普通策略归档/释放与并发自动 Recall；旧 Release 观察废弃；目录/截断冷恢复；CLI 策略及控制者接管；批量原始结果跨进程重启；独立 Master 两次在线选主/旧 Leader 恢复；后台执行暂停时普通读取仍独立 Recall。单独 native 搬运与原语保护用例也通过（17.81 秒）。三个 ignored 仍是百万规模用例和两个由父测试显式启动的子进程 fixture。

Curvine Root 另有 **276 项 Master、两个 TCP/Raft 用例通过**。新增集群检查同时执行 16 个 Import 与 16 个 Native 创建，保持一个共享资格不释放，并验证 32 个 inode 不重复；回归先暴露 SDK 绑定已有 namespace 仍申请独占资格，修复后通过。还检查了新读者拒绝已准备 Release 且不丢失读租约、过期计划不写 RocksDB/绑定、同身份重试及旧日志语义。

原始日志为开发环境 `research/hsm-conditional-final-root.log`、`research/hsm-conditional-fixed-git.log`，相对 `/work/jay/hsm`。没有升级 PVE，没有执行本版本百万规模负载或最终原版性能比较；策略/任务跨记录协调及旧后台路径尚在优化，物理 Worker/磁盘故障矩阵仍按 HA 设计推进。


## 2026-10-22 生命周期共享提交与任务跨换主恢复

八个直接依赖及 lock 的 29 个 Curvine 包固定 `d5e88f1d03f399ffd31099b8a4887ecfea2fbc7e`。其他锁文件内容逐字节保持，没有路径覆盖。执行 `CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test --locked --workspace --features hsm-curvine/native-integration -j 6`，最终回归 **32 项功能通过、0 失败、3 ignored，四个 crate 的全部文档测试执行成功**；格式与差异检查通过。

Source 的策略更新、扫描入队、claim/finish 和显式 GC 使用共享条件提交。新的有界检查集包括 namespace、策略/任务/游标/活动标记、候选版本/路径/副本数、动作 outcome（含不存在）和容量策略逻辑待释放预算；失效不写永久失败结果。Recall 任务的认领、完成和详情按存储中 action 分类，后台接入满额不占 Recall 名额。新 CF 栅栏 `hsm_native_v8` 要求全部 Master 匹配；水位遥测与物理空间预留的区别见[Source 设计](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/lifecycle-shared-commit.md)。

七个 managed 场景通过（88.49 秒），其中独立 Master 的在线故障测试现在覆盖完整任务状态：

1. 真正写入约 2 MiB 的普通文件，通过 data-mover/LocalArchive 归档为 Dual；Source 保存 Release 策略、入队并取得 Running claim。
2. 暂停当前 Leader，由两个存活节点选主。普通读取校验全部内容，原 claim 请求仍返回原 token/结果，原 policy_put 请求也返回原成功结果。
3. 因读取改变观察，执行器报告旧任务 Superseded；策略重新选择当前观察，认领新任务并实际 Release，finish 必须由 Source 的动作成功结果确认 Done/Offline。
4. 恢复旧 Master，再暂停替代 Leader，剩余节点再次选主。任务 Done 与原 finish 结果保持；执行真实 Recall 为 Dual，校验 inode/完整内容后重试旧 finish，仍必须返回原 Offline 结果。

这验证 Master 选举不重置 controller 资格或 claim，不要求在外部重建任务镜像；它不是执行器进程丢失后的自动接管、网络分区或 Worker 磁盘故障测试。常驻 worker 后台暂停时普通读取独立 Recall 的测试、冷恢复和其他策略场景也继续通过；单独 native 搬运用例通过（23.80 秒）。三个 ignored 仍为百万规模用例及两个父测试显式启动的子进程 fixture。

Curvine Root 另有 **283 项 Master 与两个 TCP/Raft 用例通过**，包括不同候选并行入队、容量预算重检、动作成功结果与旧失败报告的竞争、新读者阻止旧 claim 发布，以及后台接入满额时 Recall 的独立名额。测试暂停 hook 只在 hsm-scale-test 编译。原始日志为开发环境 `research/hsm-lifecycle-priority-final.log` 和 `research/hsm-lifecycle-fixed-git-final.log`（相对 `/work/jay/hsm`）；首次固定来源回归另有 `research/hsm-lifecycle-fixed-git.log`。

未升级 PVE，未执行本版本百万规模持续负载或最终原版性能比较。少数原始 RPC、失联 Worker 清理/UFS 缓存失效等旧后台路径仍保留独占协调；完整优化与物理 HA 矩阵尚未全部完成。


## 2026-10-22 失联 Worker 分页清理与固定来源回归

八个直接依赖和锁文件中的 29 个 Curvine 包固定 `a1878b22f7fdbb5cb4cad532c520015912a511e8`。除这组来源版本外，manifest 与 Cargo.lock 内容保持原样，没有 path patch 或其他依赖更新。执行：

```bash
CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test --locked --workspace \
  --features hsm-curvine/native-integration -j 6
```

最终 **32 项功能测试通过、0 失败、3 ignored，四个 crate 的全部文档测试通过**。七个 managed 场景通过（88.58 秒），覆盖普通写入纳管、策略筛选与实际 Release/自动 Recall、旧访问观察失效、目录/截断与批量结果冷恢复、CLI 控制者接管、后台暂停期间独立 Recall，以及三 Master 独立进程的两次在线选主。后者继续校验 Source 保存的策略、Running claim、实际 Release 后的 Done 与原 finish 结果；真实 Recall 后原 finish 仍返回其原 Offline 结果。单独 native 搬运场景通过（22.79 秒）。三个 ignored 仍为百万用例及两个父测试显式启动的子进程 fixture。

Source 的 heartbeat 清理现在每页最多 1024 个块位置，按字节游标读取并原子删除双向索引；页间释放 authority gate，逐页交接副本修复，不在生产路径预先聚合整个 Worker 的块清单。纯位置清理不等待没有日志的 journal 回执；UFS 缓存失效与可能已有部分修改的错误仍确认其日志前缀。每次失联 token 和原始 Leader 任期控制旧任务资格：Start/Running 或新位置上报取消旧任务，已经失联的 Worker 发送 End 则保留清理。token 是 Source 内部的本地身份，未引入外部文件镜像，也没有改变公开 HSM 接口或持久格式。

Source 完整回归 **290 项 Master、2 项真实 TCP/Raft、64 项文件系统和 6 项 inode 测试通过，0 失败**。新增覆盖旧 token 与重连/再次失联、End 保留清理、页间任期变化、Running 心跳之前的新位置上报、跨页 UFS 缓存不可读判定，以及不依赖 Raft 回执的纯位置清理。真实 TCP/Raft 用例注入 2050 个派生位置，首个 1024 项页面之后暂停清理，仍通过 SDK 写入/关闭/读回普通文件，恢复后按 1024/1024/2 完成清理。原有 UFS 缓存失效、存活副本和 Follower journal 重放也通过。

最终日志（相对于 `/work/jay/hsm`）：`research/hsm-worker-cleanup-end-final.log`、`research/hsm-worker-cleanup-end-fixed-git.log`。旧 `837aa6b1` pin 的 `research/hsm-worker-cleanup-fixed-git.log` 是被最终修正替代的中断编译，不计入验收。详细设计见[Source 分页清理](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/lost-worker-cleanup.md)。

该集群清理用例的 inventory 是注入的派生位置记录，前台数据读写使用真实 Worker；它不代表物理 Worker/磁盘故障。单个大 UFS 文件解码/失效仍随布局增长，完整块报告仍有全清单成本，旧缓存删除尚缺完整持久 outbox，清理调度尚未实现换主后的持久游标与自动接续。LocalArchive 单节点和私有 Recall 单副本限制保持。未升级 PVE，未执行新版百万验收或最终原版性能对比。


## 2026-10-22 Worker 清理持久接续与修复需求跨换主

八个直接依赖及锁文件中的 29 个 Curvine 包固定 `dcea3446e80448093f08a986cc398cb260e202d3`。只替换原先的 Curvine SHA，其他 manifest / lock 字节保持；没有本地路径覆盖。Source 新增清理 job、pending 页面、cursor 与独立修复需求，保存于自身复制元数据，没有外部文件镜像。新内部 envelope 与 hsm_native_v9 checkpoint 栅栏要求全部 Master 匹配版本；历史日志解释保留。

删除前同批提交最多 1024 项 pending 与修复需求；位置删除重新检查原始任期和本地失联 token。每个新任期重新等待 worker_lost_interval，保留 pending，并从新 Leader 的实际位置清单重新扫描。修复每轮最多 32 页、每页 64 项，不扩大单次检查/提交边界；入队不算完成，当前 inode / 需求 observation / 新任期 Live 联系共同约束确认。恢复扫描等待 Raft 时不持有心跳检测器的 recovery mutex，旧扫描不能恢复新任期已重置的游标或 token。

增强的独立进程 HA 场景按以下顺序验证：

1. 延续已有真实文件归档、策略与 Running claim；另写入 1 MiB+53 字节的普通文件，将当前副本目标设为 2。集群只有一个真实 Worker，需求因此尚未满足。
2. 在三个独立 Master 上分别注入同一失联 Worker 的 2050 个派生位置，包含普通文件的真实块 ID。旧 Leader 执行真实清理，在首批 1024 项已经删除、游标确认尚未提交处暂停；两份 Follower 已保存 pending，但位置仍各为 2050 项。
3. SIGSTOP 旧 Leader，两个存活 Master 选主。由生产心跳恢复扫描器自动接续，无手工触发恢复；检查 job 已移除、位置归零，真实文件的未满足修复需求继续保留，普通内容仍可读。
4. 恢复旧 Master，再暂停替代 Leader并第二次选主；再次核对接续与需求保留。原策略、任务 token、Done/finish 原始结果、实际 Release/Recall 以及内容/inode 的已有断言同时执行。
5. 将该文件当前副本目标改回 1，验证需求自动确认移除，再次完整读回内容。该步骤证明依据当前策略确认需求，没有验证实际创建第二份物理副本。

Worker inventory 使用 fixture 注入，Master 停止/恢复及选举使用真实独立进程；它不是物理 Worker/磁盘丢失测试。fixture 在反序列化配置后执行 MasterConf::init，确保 serde-skipped 的 duration 缓存真正使用 8 秒失联窗口。此前一次失败来自写死块数量，另一次失败来自实际缓存仍为默认 10 分钟，均保留失败日志，没有放宽自动恢复的断言。

Source 最终回归：**298 项 Master、2 项真实 TCP/Raft、64 项文件系统及 6 项 inode 测试通过，0 失败**；三个规模/手工基准维持 ignored。新增 2050 条需求的批次回放测试检查游标接续、入队保留、每页边界及任期变化停止。Source 日志为 `research/hsm-cleanup-recovery-root-final-v3.log`（相对 `/work/jay/hsm`）。

UFS 旧缓存删除仍缺完整持久 outbox，大 UFS 文件及完整块报告仍有随清单增长的成本；原始 inode 条件确认可能超过 payload 上限，需求会保留等待后续优化。LocalArchive 单节点和私有 Recall 单副本限制保持。本轮未升级 PVE、未运行新版百万验收、未给出最终原版性能差值。详细设计见 [Source 持久接续](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/worker-cleanup-recovery.md)。

最终固定来源命令与结果：

```bash
CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test --locked --workspace \
  --features hsm-curvine/native-integration -j 6
```

**32 项功能测试通过、0 失败、3 ignored，四个 crate 的文档测试流程成功**。七个 managed 场景 93.68 秒全部通过，包含增强的两次在线选主/自动接续及全部原始结果断言；单独 native 搬运用例 26.82 秒通过。三个 ignored 为百万规模用例及两个由父测试启动的子进程 fixture。格式与差异检查通过。最终日志 `research/hsm-cleanup-recovery-fixed-git-final.log`；早期失败日志 `research/hsm-cleanup-recovery-fixed-git.log`、`research/hsm-cleanup-recovery-live-layout-fixed.log` 不计为通过；配置修正后、批次优化前的单用例验证为 `research/hsm-cleanup-recovery-live-config-fixed.log`。
