# 通用 Interface 与本地闭环验收

日期：2026-10-19。这是本地参考实现的实际软件验证记录，不是 Curvine/磁带或生产 HA 验收。

| 检查 | 结果 |
|---|---|
| `cargo test --workspace --locked -j 2` | 通过：3 个核心测试、10 个真实本地集成测试 |
| `cargo clippy --workspace --all-targets --locked -j 2 -- -D warnings` | 通过 |
| `cargo fmt --all --check` | 通过 |
| `cargo run --locked -p hsm-local --bin hsm-local -- <dedicated-workdir>` | 成功：Archive=Dual、Release=Offline、Recall=Dual，恢复原内容 |
| 原工作区专有实现 | 已移除；成员仅为 hsm-core、hsm-engine、hsm-local |

本地集成使用实际 data-mover-rs，固定提交为 `11fac12eee028c679293e4df6220584512561487`，没有连接磁带、NFS、S3 或其他远端存储。

覆盖内容：

- 3 MiB 以上内容的归档、独立释放、物理 GC、恢复与身份保持。
- 源/后端 Adapter 重开，原成功结果重试不重复生效，同 ID 不同输入冲突。
- 后端已提交而源尚未登记锚点时重启，通过精确 ArchiveKey 对账。
- 无锚点不得释放；Pending 不产生锚点；归档 ID 不可绑定另一实体。
- 原语确定失败跨重开保持原结果；陈旧 revision/错误 generation 被拒绝。
- 损坏的私有 Recall 不发布；释放前发现归档损坏时保留在线副本。
- 独占 root 所有权、后端身份不变、旧 destination 在 seal 后被拒绝。

未覆盖的生产能力：跨节点 authority/控制者接管、真实进程 kill/掉电与磁盘损坏矩阵、透明离线访问、容量预留、Abort/取消/资源回收工作流、生命周期批量策略、Curvine 数据协议及带库持久性。这些限制由 Interface 能力声明和设计共同记录，不能从本地测试通过推导为已完成。
