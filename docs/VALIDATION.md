# 通用 Interface、本地闭环与 Curvine 接入验收

日期：2026-10-19。本地闭环、Curvine 客户端合同和原生链路分别记录；它们均不是生产 HA 或磁带验收。

| 检查 | 结果 |
|---|---|
| `CXXFLAGS=-g0 cargo test --workspace --features hsm-curvine/native-integration --locked -j 6` | 通过：24 项，包含 3 个核心、10 个真实本地集成、10 个 Curvine 合同、1 个真实集群测试 |
| `CXXFLAGS=-g0 cargo clippy --workspace --all-targets --features hsm-curvine/native-integration --locked -j 6 -- -D warnings` | 通过 |
| `cargo fmt --all --check` | 通过 |
| `cargo run --locked -p hsm-local --bin hsm-local -- <dedicated-workdir>` | 成功：Archive=Dual、Release=Offline、Recall=Dual，恢复原内容 |
| 原工作区专有实现 | 已移除；成员为 hsm-core、hsm-engine、hsm-local、hsm-curvine，无旧专有兼容模块 |

本地集成使用实际 data-mover-rs，固定提交为 `11fac12eee028c679293e4df6220584512561487`，没有连接磁带、NFS、S3 或其他远端存储。

覆盖内容：

- 3 MiB 以上内容的归档、独立释放、物理 GC、恢复与身份保持。
- 源/后端 Adapter 重开，原成功结果重试不重复生效，同 ID 不同输入冲突。
- 后端已提交而源尚未登记锚点时重启，通过精确 ArchiveKey 对账。
- 无锚点不得释放；Pending 不产生锚点；归档 ID 不可绑定另一实体。
- 原语确定失败跨重开保持原结果；陈旧 revision/错误 generation 被拒绝。
- 损坏的私有 Recall 不发布；释放前发现归档损坏时保留在线副本。
- 独占 root 所有权、后端身份不变、旧 destination 在 seal 后被拒绝。

未覆盖的生产能力：跨节点 authority/控制者接管、真实进程 kill/掉电与磁盘损坏矩阵、透明离线访问、容量预留、Abort/取消/资源回收工作流、生命周期批量策略、Curvine 原生 HSM 效果及带库持久性。这些限制由 Interface 能力声明和设计共同记录，不能从本地测试通过推导为已完成。

## Curvine 第一阶段

Curvine SDK 固定提交 `8cc0babc27a39aed5ad22c18d5f62562a6a0664c`；原生控制模型的 3 项单元测试通过（`cargo test -p curvine-model hsm::tests -j 2`，在 Curvine checkout 内运行）。

10 项客户端合同测试使用 fixture transport，覆盖能力拒绝、原生身份、错误 inode/generation、能力矛盾、未知结果、opaque view、Release wire 名称、错误 Release 成功结果、无 anchor 的 Offline 状态，以及原始成功结果与请求动作不一致。模拟能力不表示服务端已支持效果。最终加强了 Recall 能力组合检查，另以 `cargo test -p hsm-curvine --locked --test control -j 6` 重跑全部 10 项合同测试并通过。

真实 native 集群测试通过：1 个临时 Master、1 个 Worker，原生 TCP RPC 和 SDK；写入 3 MiB + 19 bytes（4 个 block），读取到私有 spool，再调用实际 data-mover Local transfer；核对全部内容、证据长度及源 inode/长度不变。绕过客户端门控直接发送 Release，服务端仍返回 Unsupported。该测试需要 `native-integration` feature；默认 `cargo test --workspace` 输出中的 native_cluster 为 0 项，不代表完成了集群验证。

本阶段没有为 Curvine 建立元数据镜像、没有进行唯一在线副本释放；服务端所有 HSM 能力为 false。

带 native-integration feature 的严格 Clippy 检查通过：

```sh
CXXFLAGS=-g0 cargo clippy --workspace --all-targets \
  --features hsm-curvine/native-integration --locked -j 6 -- -D warnings
```

本机补齐了 protoc、Clang/libclang 和相应 C++ 构建环境。CXXFLAGS=-g0 仅关闭 C++ 调试信息；验证使用真实 RocksDB 库，无存储层 mock。测试仍为 testing 模式，未验证正常 journal 队列提交、跨节点持久性、稳定视图、受保护 Release 或 Recall 发布。
