# 通用 HSM：实现设计 v0.3

当前工作区直接采用通用 HSM Interface，不保留旧文件系统 ABI、copytool daemon、v1 插件协议或其兼容模块。旧实现可从 Git 历史查看，不作为运行路径。

## 1. 当前可执行结构

| crate | 责任 |
|---|---|
| hsm-core | EntityKey、DataKey、ArchiveKey、OperationContext、算法标记 digest；ManagedStorage / ArchiveBackend / DataMover Interface |
| hsm-engine | 外部协调 Archive / Release / Recall；校验回执、查询未知结果、请求源存储原子效果 |
| hsm-curvine | 原生控制协议、能力门控、结果校验；可选 SDK/current-file 数据桥接，无元数据镜像 |
| hsm-local | 专用本地 managed namespace、不可变归档 catalog、真实 data-mover-rs 传输 Adapter、演示命令 |

依赖方向：hsm-engine → hsm-core；hsm-local → hsm-engine + data-mover-rs。通用核心不依赖任何具体文件系统、数据库、传输协议或介质。

## 2. 身份与状态

EntityKey 使用 storage instance、namespace incarnation 和 opaque entity ID。DataKey 额外绑定内容 generation。ArchiveKey 使用 backend instance 和不可变 object ID。路径及传输 endpoint 均不作为稳定身份。

OperationContext 绑定 operation ID、controller epoch 和 attempt。不同业务动作不得复用 operation ID。存储保存成功效果的原结果，并在重试时先返回结果，之后才重判当前事实。通用 Interface 提供 completed 查询；具体 RPC 的 mutation ID、线性化结果和接管仍由生产存储 Adapter 实现。

DataEndpoint 仅含 Adapter 类型和不透明 capability。Debug 隐藏 capability；适配器负责验证其权限、版本和生命周期。

驻留事实独立于操作进度：Online → Dual → Offline → Dual。Archive 不自动 Release；Recall 在私有布局完整验证并发布前保持 Offline。

## 3. 三个 Interface

ManagedStorage 保存权威原生记录、源 view、归档锚点、私有 Recall 布局和操作结果；它执行 Release、seal/publish。原语内部必须检查当前内容和保护，外部先读后判断不能代替源侧原子检查。

ArchiveBackend 负责 prepare、commit、inspect 和精确读取。inspect 区分 Absent、Pending、Committed、Unknown；只有 Committed receipt 可进入当前数据的恢复锚点。

DataMover 只在给定 endpoint 间复制和验证 bytes，无 Release、namespace 删除或正式布局发布权限。本地 Adapter 使用实际 data-mover-rs Checkpointed transfer；两端独立 root 内统一使用 data 作为相对 payload 名，满足该库普通 transfer 的同相对路径合同，不通过整文件 Bytes 绕开接口。

## 4. 本地参考实现

源根目录拥有自己的 state.json 与 layouts/<UUID>/data。它是这套专用逻辑 namespace 的原生记录，不是对任意 POSIX namespace 的外部全量镜像。导入复制 caller 的内容，Release 不会删除导入前的原文件。

state.json 原子替换并同步目录，数据效果和成功 outcome 同次持久化；存储原语的确定条件失败也保存原结果，条件变化后使用新操作 ID。状态提交不确定时停止使用缓存，要求重开。OS advisory lock 限制同一 root 一个 Adapter owner；其 clone 共用 owner 和内存串行化。

归档写 attempt-private 目录，数据同步、独立完整校验后，create-only 发布 synced receipt。稳定 archive key 哈希只用于内部路径编码；同 key 不同 DataKey/内容冲突。精确 inspect 每次验证 catalog 对应 payload；没有 receipt 不冒充 Committed。

Data-mover 复制与 seal/GC 使用本地 session 锁。sealed marker 禁止旧 destination 再经此执行器写入。Release 先持久撤销 published layout 并登记 GC，collect_garbage 随后删除不复用的 UUID allocation。Recall 写原实体下的新私有 UUID layout，验证完整 digest/length 后切换 published 引用，保留原 EntityKey/DataKey。

本地参考仅支持 controller/attempt epoch 1，其他资格明确拒绝。它报告：持久成功结果=true；跨节点 authority=false；容量预留=false；透明访问召回=false。所有客户端必须经此 Adapter，内部目录不得被其他程序修改。它不防同一 OS 用户绕过 Interface 直接改文件，不提供磁盘/节点故障 HA，也不是透明 POSIX HSM。

## 5. 当前验收与限制

运行 cargo test --workspace 验证真实本地复制、Archive/Release/Recall、源与后端重开、原成功结果重试、不可变归档 ID 冲突、Pending 保护、stale state、错误 generation、Recall 损坏不发布和 root 所有权。

当前核心是可组合的同步工作流入口，还没有分布式 daemon/worker RPC、控制者选举、独立 mutation 查询、Abort/取消/资源清理工作流、容量 claim、自动策略或透明 offline 事件。失败和 Unknown 保留 pin/private layout 以供对账，不能在错误时自动删资源。不能据本地闭环宣称生产 HA 或完整 HSM 验收。

## 6. Curvine 与后续顺序

1. 先稳定通用 Interface 与 hsm-rs ↔ data-mover ↔ local archive 可执行闭环。
2. Curvine 提供同一 ManagedStorage Interface 的远端 Adapter：原生身份/generation、committed 控制记录与 outcome、固定 DataView、Release、原 inode RecallLayout 和 CommitRecall。
3. data-mover 增加受约束的自定义角色装配入口或原生 Curvine backend，实现 view source/private-layout destination；当前公开 Storage 构造器不能直接装入外部角色。
4. 原型逐步增加 durable attempt/reconciliation/Abort、offline event、capacity claim、native lifecycle 和实际故障测试。Curvine 可靠提交和写者 fencing 未完成前禁止唯一在线副本 Release。
5. 最后接 tape-rs 和第二介质后端；Tape Adapter 在 data-mover 内，物理带库调度由 tape-rs 保留。

SourceStorage 语义和所需 Curvine 改造详见 [Curvine HSM 设计](https://github.com/JayTsu-sh/curvine/tree/docs/hsm-design/docs/hsm)。实现阶段对当前 Interface 的简化均已在此列明，生产扩展不得以本地锁或 sidecar 文件替代 Curvine 的复制权威记录。

## 7. Curvine 接入当前进度

第一阶段已实现 `CurvineStorage<ControlTransport>`，把通用 Interface 映射到版本化原生 HsmControl RPC；不通过路径、mtime 或 xattr 猜测 generation。能力协商后逐项门控，响应需匹配实体/版本、内容和效果。服务端在实现复制权威记录及数据保护之前保持能力全 false。

可选 native feature 实现 SDK TCP 控制和 `copy_current_file_to_local`：先按块读入私有本地 spool，再用真实 LocalDataMover 复制和独立验证。此读取针对普通已完成文件，前后 stat 检查不构成稳定 DataView。它没有源状态发布权，也没有接入 hsm-engine Archive/Release/Recall 工作流；不能据复制成功登记 anchor 或释放 Curvine 文件。

下一阶段应按上节第 2 项实现 Curvine 原生合同，再替换 spool 为精确 view/private-layout 的执行器角色；目前不会以额外数据库、普通 free 或临时文件 rename 绕过这些改造。实现入口、feature 选择及测试边界见 [hsm-curvine](../crates/hsm-curvine/README.md)。
