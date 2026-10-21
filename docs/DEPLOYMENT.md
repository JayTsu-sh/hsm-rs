# Curvine HSM 当前部署设计

更新：2026-10-20；状态：当前工作区已实现并在 PVE 完成普通 FUSE 全流程验证。本文描述实际部署；[实现设计](DESIGN.md)中多控制者、完整副本修复和磁带调度等内容仍属后续目标。

## 架构图与流程入口

- [写入自动纳管：1—6](architecture/write.html)
- [策略与 Release：1—11](architecture/policy-release.html)
- [普通读取自动 Recall：1—12](architecture/automatic-recall.html)

三张均为独立部署流程图，每张的箭头从 **1** 开始连续编号，数字表示执行顺序；按编号读，不按箭头在页面上的上下位置读。本文 W/P/R/C 表示逻辑阶段名称，图内数字是该张图的具体步骤。第 2 张包含策略筛选、Archive、Release 与物理回收；第 3 张从普通应用读取开始，不需要显式 Recall 命令。

图基于本次实际部署、当前源码和验收证据，源文件已冻结在图目录的本地 evidence 仓库；不是将尚未提交的改动冒充上游基线。Curvine 改动主要位于 Master HSM/普通元数据钩子、SDK FsReader 与生命周期模块，hsm-rs 改动位于策略模型、worker、适配器和集成测试。源码通过 Git 固定依赖发布，重现使用 Cargo.lock，无需本地路径补丁，见 [构建与验证](VALIDATION.md)。

## 部署拓扑

| 地址 | 运行进程 | 持久存储与责任 |
|---|---|---|
| 10.131.9.80 | Master、Worker、FUSE；普通写入客户端 | 独立元数据/journal、Worker 本地盘 |
| 10.131.9.81 | Master、Worker、FUSE | 独立元数据/journal、Worker 本地盘 |
| 10.131.9.82 | Master、Worker、FUSE | 独立元数据/journal、Worker 本地盘 |
| 10.131.9.83 | 外部 hsm-curvine worker/controller、FUSE；普通读取客户端 | `/srv/hsm-data/archive` 与 spool |

三个 Master 通过既有 journal/Raft 维护元数据权威。三个 Worker 的本地磁盘共同提供统一文件系统，普通文件初始配置两个副本；没有 UFS。HSM controller 在一个进程中包含 hsm-engine、CurvineStorage、CurvineDataMover、实际 data-mover-rs Local transfer 和 LocalArchive。逻辑模块分层不等于独立进程：当前尚未把 executor 部署为独立远端服务。

配置 `/etc/curvine-hsm/cluster.toml`，程序 `/opt/curvine-hsm/bin`，FUSE `/mnt/curvine`。Master 数据 `/var/lib/curvine-hsm/{meta,journal}`，Worker 数据 `/srv/curvine/worker`。全部虚拟机在 PVE 10.131.9.20，同物理机不构成物理故障隔离。服务已启用并运行；旧 VM402 不参与本次升级。

## 五层责任与权威边界

| 层 | 当前责任 | 权威记录 |
|---|---|---|
| 存储侧 Curvine | 文件身份与 generation、普通写入钩子、读取保护、稳定视图、条件 Release、私有恢复及原 inode 发布 | 文件/HSM 记录、pin/租约、原始结果、GC outbox，由原有日志复制 |
| 策略层 | Curvine 维护规则并按索引筛选和原子入队；外部 worker 周期触发 | 规则版本、候选索引、持久游标、next_due、压力锁存、共享待回收预算，均在 Curvine |
| HSM 核心 hsm-rs | 领取、归档/释放/召回编排、回执检查、重试和结果对账 | 通过存储接口更新任务和结果；无外部全量 namespace 镜像 |
| 执行器 data-mover-rs | 固定源视图与私有目标之间搬运、摘要验证、传输证据 | 临时 spool；不能单独修改正式驻留状态 |
| 后端 LocalArchive | 归档对象封存、fsync、精确查询、持久回执 | 自身 catalog 与 payload；不是源文件系统元数据镜像 |

未来 tape-rs/EE 类或其他介质适配在执行器及后端边界，本轮没有部署带库。源端提供存储安全原语，外部核心选择动作顺序并推进工作流。

## W：普通写入自动纳管

W1 应用正常 create/write/fsync/close，经 FUSE 与 SDK 调用 Curvine，不使用 import。W2 普通数据先写入两个 Worker 副本。W3 Master 的正常完成日志创建或更新 HSM 记录及候选索引，未关闭文件标记 writing；关闭后为 Online、unsealed。只有被策略选中的文件才在源端保护下计算摘要。已有内容被覆盖或截断会推进 generation、撤销旧归档锚点；原生布局的重命名更新路径字段。

写入图第 6 步是**返回写入完成确认**。普通 CompleteFileResponse 只有 result 和可选 file_blocks，没有 Online 字段；FUSE close 向应用返回完成或错误。Online 保存在 Master 的 HSM Record.state.residency，记录位于原 RocksDB common CF 的 `\x03hsm:record:<inode>`，沿原 journal/Raft 恢复，可用 `hsm-curvine --config /etc/curvine-hsm/cluster.toml status /path` 查询。状态不是独立数据库镜像，也不是普通 stat 的字段。

### 写入文件如何对应 Worker 块文件

Curvine 提供统一文件命名空间与文件访问。应用看到一个完整路径，Master 保存 inode、长度、块大小、块列表及副本位置，SDK 按偏移定位并向 Worker 写入 block。Worker 独立本地盘使用普通本地文件承载数据块，例如 `active/b*/b*/blk_<block_id>`；不需要另一个共享底层文件系统或 UFS，也不在每个 Worker 重建业务目录。

本次 A 的 8,388,625 B 小于 128 MiB 块大小，因此 Release 前只包含 block 17129537536，在 .80、.81 各有一个副本。大文件可以有多个块；同一块的不同副本保留同一个块 ID。当前 Recall 的私有 allocation/data 布局是另一种恢复布局，尚未转入普通块副本体系。

### 为 HSM 新增的权威元数据

精确 CF/键值编码、Record JSON、候选项排序、WriteBatch、CAS 入队与启动重放见 [RocksDB 记录与索引详细设计](https://github.com/JayTsu-sh/curvine/blob/039631f4ea8aa5a06250aa8928e2328edc003475/docs/hsm/rocksdb-records.md)。

| 类别 | 字段与用途 | 新文件完成写入时 |
|---|---|---|
| 身份与内容版本 | DataKey：storage、namespace、inode、generation；区分文件与内容版本 | 绑定原 inode 和当前 generation |
| 驻留与恢复依据 | state.residency、length、digest、anchors | Online；当前长度；digest.algorithm=unsealed；anchors 为空 |
| 修订与驻留区间 | Record.version、state.revision、state_since_ms | 更新版本，支持旧观察失效与驻留条件 |
| 写入与布局识别 | ordinary、native_blocks、writing、sealed | true、true、false、false |
| 并发保护 | views、readers、seal_token | 初始为空；后续稳定归档、读取与摘要校验按需登记 |
| 私有恢复与回收 | pending、published、garbage、native_garbage | 初始为空；后续 Recall 发布及 Release 回收按需更新 |
| 策略属性与索引 | path、atime_ms、mtime_ms、owner/group；状态/atime/inode 候选键 | 从原生属性投影并维护候选索引 |

上述为 Master 同一权威域内的 HSM 记录，不是对外部数据库的 namespace 镜像。HSM record 与派生索引同 batch 更新，沿正常 journal/Raft 恢复；不等同于声明所有原生 inode 字段和 HSM 字段在一个 RocksDB batch 写入。写入图第 5 步内部箭头对应这些记录的维护，第 6 步只返回写入完成确认。HSM namespace 尚未初始化时先写 enroll 待纳管记录，初始化后补登记。

普通修改与 HSM 控制共享提交串行边界并等待提交屏障，避免预更新与 HSM CAS 交错。新增普通钩子使用 ordinary_format=2，历史日志按原语义重放，格式升级通过提交激活；实测旧文件状态和归档锚点保持。该串行边界的元数据吞吐仍待压测。

## P/R：策略、归档与 Release

P1 运维 policy-put 将规则写入 Curvine，revision CAS 防止覆盖未知版本；worker 按 scan_interval_ms 周期调用。P2 Master 按驻留状态、atime、inode 索引读取，Dual 优先于 Online，状态内按访问年龄排序；筛选容量水位、路径与排除、大小、owner/group、访问/修改/驻留年龄。不是递归 readdir。正权重表示访问年龄优先，尚不支持任意权重表达式和 GPFS 策略语法。

每 RPC 最多处理 256 个候选项，额外预读 1 项判断下一页；scan_budget 控制每轮计入 examined 的项，max_candidates 控制入选项。游标和下次运行时间持久化。达到高水位启动、低水位停止；所有规则共享在途预计回收预算，减少超额选择。这不是 Recall 的硬容量预留。路径等条件仍后过滤，未宣称大规模性能已验证。

P3 同一存储提交校验规则与 record version，写 Ready intent、工作索引和 active index；worker 领取后再查资格和原始结果。读取改变访问观察，旧 Release 被 Superseded，变冷后可以按新观察重新选择。

R1 action=release 对 Online 候选先产生 Archive：hsm-rs 取得稳定块视图，调用 data-mover 把完整版本复制到 LocalArchive，后端校验/fsync 后返回回执。R2 Master 登记匹配 DataKey 的锚点，Online → Dual；归档本身不释放。

R3 后续扫描生成 Release 任务，hsm-rs 再验证归档仍成立；Master 检查 expected/generation/epoch、锚点及活动 pin/读取，原子撤销在线布局，写 Offline、GC outbox 与成功 outcome。R4 Worker 根据提交后的删除任务回收全部旧块副本。DataMover 不把文件设为 Offline；Release Done 与物理 GC 分开，重启后继续清理。

## C：访问自动 Recall

C1 普通应用 open/read，更新后的 SDK 向 Master 请求受保护读取；Offline 时原子生成或合并 `__access_recall` intent，应用等待。该流程无需 Recall 策略或显式命令。

C2 常驻 worker 优先领取召回并检查恢复锚点。C3 data-mover 将归档内容复制到 Worker 私有布局，保持原 inode 仍 Offline。C4 校验 length/digest，Master 要求 Worker Seal，持久撤销旧写权限。C5 Master 原子发布布局到原 inode，Offline → Dual。C6 等待的普通读取取得读取租约并继续返回数据。同一离线版本的多个读取共享一次任务。

读等待最多 300 秒，每 200 ms 查询恢复；当前为整文件恢复，不提供部分召回/边恢复边读。控制器和归档后端需可用。读租约 120 秒、30 秒续租，保护在线布局不被并发 Release。

## 策略配置与实测

[正常策略示例](../examples/policies/cold-local.json)：80%/60% 水位，访问年龄一天、修改年龄五分钟，60 秒扫描、预算 4096、每轮最多 128 项。当前部署保留启用的 `transparent-final-release` revision=4，限定本轮目录并排除 excluded；不是让低测试水位规则长期运行。

真实 .80 FUSE 写入五文件，.83 普通读取验证：A=8,388,625 B，B=6,291,487 B，依访问年龄先后 Online → Dual → Offline；hot、小文件、排除文件保持 Online。三 Worker 上原块全部删除，两副本回收 29,360,224 B。随后四次并发读只生成两个自动 Recall Done，SHA256、长度、inode 与 DataKey 全部相同；无显式 Recall、无启用 Recall 策略。完整操作及证据见 [PVE 测试描述](PVE-FUSE-HSM.md)。

## 当前边界

单硬链接、非 UFS 常规文件在范围内。普通初始布局两副本；Recall 私有布局当前单副本，不参加普通块修复与完整容量统计。恢复布局上的读取和全量覆盖可用，追加与完整可变 POSIX 语义尚未覆盖；活动保护期间修改可能被拒绝。控制器和 LocalArchive 单节点，旧 Running 接管仍需显式提升 epoch。完整 spool 和控制通道影响性能，尚未做大规模扫描或完整盘损坏/断电矩阵，不宣称 GPFS/EE 全功能或生产 HA。
