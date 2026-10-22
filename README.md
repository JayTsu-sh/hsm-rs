# hsm-rs

通用外部 HSM 核心。存储保存权威事实和受保护数据效果，核心协调 Archive / Release / Recall，data-mover-rs 执行字节搬运。本地闭环已实现，Curvine 已接入普通 FUSE 文件自动纳管、周期策略驱动的 Archive / Release，以及普通读取触发的自动 Recall，带库接入后置。

旧文件系统专有实现已移除，不保留兼容路径；这是一次不兼容的 Interface 更新。

| 模块 | 内容 |
|---|---|
| [hsm-core](crates/hsm-core/src/managed.rs) | 通用身份、状态、回执、ManagedStorage / ArchiveBackend / DataMover |
| [hsm-engine](crates/hsm-engine/src/lib.rs) | Archive / Release / Recall 外部协调 |
| [hsm-local](crates/hsm-local/src/lib.rs) | 本地参考存储、归档后端、真实 data-mover 集成 |
| [hsm-curvine](crates/hsm-curvine/README.md) | Curvine 原生权威记录、稳定 view、条件 Release、原 inode Recall 与 data-mover 本地桥接 |
| [详细设计](docs/DESIGN.md) | 合同、持久化、保证范围、限制和 Curvine 接入顺序 |

策略和任务保存在 Curvine；容量高低水位、访问年龄和条件筛选已实现。普通 FUSE 写入、双副本 Release 及并发自动 Recall 已在真实 PVE 集群通过，见 [部署设计与流程图](docs/DEPLOYMENT.md)、[完整测试描述](docs/PVE-FUSE-HSM.md)、[实现设计](docs/DESIGN.md)和[验证记录](docs/VALIDATION.md)。原生诊断脚本见 [本地 HSM 与生命周期](docs/LOCAL-HSM.md)。2026-10-21 已完成百万 Record 基准、内部批量预置的真实百万 namespace 策略/Release/自动 Recall/checkpoint 恢复，以及 PVE 复验；范围和复现方法见 [规模测试报告](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/scale-validation.md)。

## 运行

本地参考实现要求 Unix 文件系统的目录 fsync、advisory lock 和 hard-link 支持。当前依赖锁要求 Rust 1.94.1 或更新版本；已使用 Rust 1.98.1 验证。data-mover 当前没有后端 feature gates，因此本地调用仍会编译其其他后端依赖。

```sh
cargo test --workspace --locked
cargo run -p hsm-local --bin hsm-local -- /tmp/hsm-local-example
```

演示仅在指定目录中建立专用 source/archive，运行 Archive → Release → GC → Recall，并输出原实体身份和恢复内容。源与归档应使用专用、互不重叠的目录。

本地参考是单主机、独占 root、经 Interface 访问的逻辑 namespace；支持持久成功结果及重开恢复，不提供跨节点 authority、透明 POSIX 召回或容量预留。Curvine 支持非 UFS 单硬链接常规文件自动纳管及本地归档；默认不开启，需按文档配置并使用匹配的 Master/Worker/FUSE 版本。Recall 私有布局仍是单副本，完整可变文件语义与生产 HA 尚未完成。构建及集群验证命令见 [Curvine Adapter](crates/hsm-curvine/README.md)。完整的生产协调和故障验收仍按设计推进。

历史版本的前台吞吐、尾延迟及扫描影响见[性能对比报告](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/performance-comparison.md)。当前已增加 Native owned 提交、读取保护合批、访问 overlay、快照筛查、Worker I/O 解耦及元数据查询 ReadIndex；目录 rename/交换/替换、递归属性/删除及无挂载 TTL Delete 使用紧凑 committed 输入，Resize/AssignWorker 也纳入原始结果与持久块退休。各业务共用有界 ReadIndex 资格 FIFO，owned 路径共享准备，相邻 Native/HSM 条目可共用 proposal，少数旧式修改仍保留独占边界。细节见[目录和后台命令](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/native-namespace.md)。完整优化尚未结束，本轮百万验收和最终原版对比尚未执行，当前进度见[优化方案](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/optimization-plan.md)。


最新改造增加公开批量写的单一持久身份、目录共享等待、Master 的 Recall/前台/后台加权资格，以及常驻 worker 的独立 Recall/后台执行通道。任务仍在 Curvine，按 Source 已有索引分类分页。独立进程在线两次选主、原始批量结果重放和后台暂停期间普通读取自动 Recall 的范围见[最新验证记录](docs/VALIDATION.md#2026-10-22-批量写加权调度与独立执行通道)。这些改造不表示百万性能验收、最终原版对照或全部生产 HA 能力已经完成。


文件级 HSM 控制与 Seal 已接入共享条件提交，Native 创建与 Import 共用 inode 预留器；已有 namespace 的新会话绑定也不再独占。过期准备返回可重试 Busy，保留原操作身份重新准备；持久业务拒绝会重新独占检查。Curvine 侧设计与保留边界见[文件级条件提交](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/conditional-controls.md)。没有增加外部元数据镜像。


生命周期控制现已使用 Source 有界检查集共享提交：策略更新/扫描入队、任务 claim/finish 和 GC 不再独占等待 Raft；Recall 认领/完成按任务实际 action 使用独立接入类别。容量逻辑预算、候选版本/当前路径/副本数和动作原始结果均在提交前检查。固定源码及增强的在线 HA 结果见[最新验证](docs/VALIDATION.md#2026-10-22-生命周期共享提交与任务跨换主恢复)，Source 的采样/资源边界见[详细设计](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/lifecycle-shared-commit.md)。


Source 的失联 Worker 清理已按 1024 项分页，页间重新排队并逐页交接副本修复；原始 Leader 任期和每次失联 token 阻止旧任务删除重连位置。纯位置清理不等待无日志回执，UFS 缓存失效仍保留确认保护。设计、单文件成本与非持久 outbox 的限制见[Source 分页清理](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/lost-worker-cleanup.md)，配套固定来源回归见[最新验证](docs/VALIDATION.md#2026-10-22-失联-worker-分页清理与固定来源回归)。


失联 Worker 的清理检查点和修复需求现保存在 Curvine 的复制元数据内：删除前同批保存 pending 页面与需求，换主后重新确认失联并自动接续；修复仅入内存队列不会删除需求。每轮按有界游标扫描，Raft 等待不占心跳检测器的恢复锁。新格式要求全部 Master 匹配版本，详见 [Source 持久接续设计](https://github.com/JayTsu-sh/curvine/blob/main/docs/hsm/worker-cleanup-recovery.md)。没有增加外部元数据镜像。

固定来源组合验收见 [Worker 清理持久接续验证](docs/VALIDATION.md#2026-10-22-worker-清理持久接续与修复需求跨换主)。
