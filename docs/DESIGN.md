# 通用 HSM 实现设计 v0.6

Curvine 保存文件身份、驻留状态、策略、任务及操作结果；hsm-rs 在外部协调执行，data-mover-rs 搬运数据，归档后端提供持久化回执。普通 FUSE 写入的文件在写入和关闭路径自动纳管，读取 Offline 文件时自动触发整文件 Recall。当前后端使用本地文件，带库接入留在执行器和后端边界。

## 1 责任划分

| 组件 | 责任 |
|---|---|
| Curvine | 原 inode 和内容 generation、文件与 HSM 元数据、候选索引、条件筛选、任务持久化、稳定视图、条件 Release、私有恢复布局与原子发布 |
| hsm-core | ManagedStorage、ArchiveBackend、DataMover、身份、回执与生命周期模型 |
| hsm-engine | Archive、Release、Recall 编排；验证恢复依据，查询原始效果，报告完成或重试 |
| hsm-curvine worker | 周期触发扫描、推进持久游标、领取任务、优先处理召回、恢复中断的源校验及 GC |
| CurvineDataMover | 从固定原生块视图读取、写私有恢复目标，通过实际 data-mover-rs Local transfer 完成复制 |
| LocalArchive | 后端对象、封存、内容校验、fsync 回执和精确查询 |

策略、任务和状态放在 Curvine 现有 RocksDB common CF，并由原有 Raft journal 提交；没有外部文件元数据镜像。LocalArchive catalog 是归档对象自身的权威记录，不是文件系统 namespace 的镜像。

## 2 文件身份与普通写入

EntityKey 由存储实例、namespace incarnation 和 inode ID 组成。DataKey 再绑定内容 generation。路径、Worker 地址和传输 capability 不作为内容身份。

普通非 UFS、单硬链接的常规文件由写入完成日志纳管。未关闭文件标记 writing，不参与归档。新普通文件先记录 Online 和 unsealed，只有筛选出的候选才取得源保护并计算 BLAKE3 摘要；扫描不会给全体文件计算内容摘要。摘要确认后，归档动作取得持久 DataView，归档期间禁止改变所选数据。普通块视图使用只含 DataKey、操作 ID 和 view ID 的紧凑引用，执行器凭该能力查询固定布局；提交归档的控制请求不再携带完整块列表。

原生块布局的重新写入、覆盖和截断使旧锚点失效，并推进 generation；重命名和元数据变更更新路径、条件字段及状态版本。读取推进访问时间和公开状态版本，已入队的旧 Release 观察因此失效；文件再次变冷后可以重新入队。

源校验的 seal token 和待恢复校验索引保存在 Curvine。执行器重启后先完成中断的校验，即使原策略已关闭也会清除这项源保护。状态不是执行器进程内的锁或本地文件清单。

## 3 提交顺序与日志重放

HSM 卷上的普通元数据修改与 HSM 控制共享提交串行边界。普通修改完成预更新后，等待本节点应用提交屏障再释放边界；HSM 的比较与交换不会与未提交的普通元数据更新交错。数据传输、心跳、stat 和普通目录读取保留现有路径。

普通文件钩子使用 namespace 的 ordinary_format=2。历史日志缺少该字段时按原有规则重放；升级到新钩子本身是一条提交日志。不能用新增回调重新解释历史操作，否则会改变历史 HSM record version，破坏旧条件提交的结果。重放已接受的普通修改时，也不使用另一个节点当前的时钟重判读取租约。

当前使用全局提交串行边界保证正确性；大规模元数据吞吐还需要压测和进一步并行化。TTL 不直接处理已受 HSM 管理的文件，避免绕过该生命周期。

## 4 策略筛选与周期扫描

| GPFS 策略概念 | 当前配置 |
|---|---|
| THRESHOLD 高低水位 | selection.capacity.high_basis_points / low_basis_points，8000 表示 80% |
| WEIGHT 当前时间减访问时间 | access_age_weight 为正，等价按该权重乘以访问年龄降序选择 |
| WHERE 条件 | 路径前缀、排除前缀、逻辑大小上下限、owner、group、访问年龄、修改年龄及驻留时间 |
| 周期应用策略 | scan_interval_ms，后台 worker 发起，进度保存在存储侧 |
| MIGRATE 至外部池 | action=release，Online 先 Archive，再对 Dual 执行 Release |
| PREMIGRATE | action=archive，保留在线数据 |

这是上述策略语义的实现，不解析 GPFS 的策略文本，也不提供任意 SQL 或权重表达式。

候选索引按驻留状态、访问时间和 inode 排序。Release 优先处理已有归档的 Dual，再选择需要先归档的 Online；各状态内较冷的文件优先。扫描到超过访问年龄边界的记录后结束该状态范围，不递归 readdir。路径、大小等其余条件仍在读取候选记录后判断。

单次最多处理 256 个候选索引项，并额外预读 1 项判断下一页。scan_budget 限制每个策略每轮计入 examined 的索引项，max_candidates 限制每轮选中的动作和校验候选。持续扫描的游标、完成一轮后的下一次时间和容量触发锁存值保存在 Curvine，控制器重启不必丢弃进度。分页不是整个 namespace 的一致性快照；入队与动作提交另有版本检查。

达到高水位后启动，降到低水位后停止；两者之间继续已经启动的迁移。占用按存活 Worker 报告的 allocatable_capacity 减 allocatable_available 计算，包含分配器保留的不可用空间。所有规则共享在线池待执行任务的预计回收预算，降低重复选择造成的超发。这种预算不是 Recall 的硬容量预留，capacity_reservation 仍为 false。

一个提交批次检查策略版本、候选 record version、任务键和实体 active index，然后原子入队。任务 ID 绑定规则版本、DataKey、动作阶段、驻留区间、访问时间和状态版本。同一观察不重复；再次访问或修改后的新观察可以重新选择。Ready/Running 使用独立工作索引，召回优先，历史完成任务不必每轮遍历。

RocksDB common CF 的精确键值、Record JSON、WriteBatch 与索引扫描/重放实现见配套 [Curvine RocksDB 设计](https://github.com/JayTsu-sh/curvine/blob/039631f4ea8aa5a06250aa8928e2328edc003475/docs/hsm/rocksdb-records.md)。

## 5 从 Online 到 Offline

```text
普通 FUSE 写入并关闭
  → Curvine 原生 Online 记录
  → 策略索引筛出候选，取得固定内容摘要
  → hsm-rs 领取 Archive 任务
  → data-mover 复制固定视图到归档后端
  → 后端校验并持久化回执
  → Curvine 登记锚点，变为 Dual
  → 下一轮生成 Release 任务
  → hsm-rs 验证后端回执仍成立
  → Curvine 条件撤销在线布局，提交 Offline 与 GC outbox
  → Worker 删除所有旧原生块副本并确认回收
```

DataMover 不修改驻留状态，也不执行删除 namespace 的动作。Release 的安全检查和 Offline 提交属于存储侧。Release 任务 Done 表示逻辑离线提交成功；物理删除异步进行，GC 可在进程或主节点恢复后重试。因此验收同时检查 Worker 块文件，不能只凭 Done 判断磁盘空间已经回收。

## 6 读取触发 Recall

```text
应用 read 普通路径
  → 更新后的 Curvine SDK 向 Master 请求受保护读取
  → Offline 时原子创建共享 Recall intent，应用等待
  → 外部 worker 从工作队列领取 Recall
  → data-mover 从归档复制至私有恢复布局
  → 验证摘要和长度，Worker 封存并撤销旧写权限
  → Curvine 在原 inode 发布 Dual
  → 等待的读取获得在线布局并继续返回内容
```

不需要安装 Recall 策略，不需要用户运行 Recall 命令。同一离线版本的并发读取共享一个召回任务。恢复整文件之后才返回数据；当前不提供部分召回或边恢复边读。读取租约保护在线布局，续租和关闭可在记录竞争后重试。

## 7 执行恢复与边界

操作结果保存原始成功事实。控制器重启可以查询已完成效果；旧 Running 任务仍通过显式 controller epoch 接管恢复，不能凭超时假定旧动作失败。源读取、源校验、归档视图、私有恢复布局和 GC 的事实均在存储侧。

目前普通文件首次写入及原生块上的更新可以纳管；Recall 发布使用独立 Worker 私有布局，普通读取和全量覆盖可用，恢复布局上的追加及完整可变文件语义尚未覆盖。有活动读取或归档保护时，修改可能被拒绝并需要重试。受管硬链接和 UFS 文件不在此范围。

普通初始数据遵循 Curvine replicas 配置；私有恢复布局仍是单副本，不参与普通块副本修复和完整容量统计。LocalArchive 也是单节点本地后端。所有客户端和 Master 必须使用匹配版本；没有宣称生产 HA、全套 GPFS ILM 兼容或磁带调度已经完成。

发布版本通过 Cargo.toml 的 Git rev 与 Cargo.lock 固定 Curvine 和 data-mover 依赖，无需本地路径补丁。联合开发时可用 tools/curvine-workspace-config.py 生成本地 Cargo patch。部署图见 [DEPLOYMENT.md](DEPLOYMENT.md)，验证结果见 [VALIDATION.md](VALIDATION.md)。
