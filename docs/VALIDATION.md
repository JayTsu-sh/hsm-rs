# Curvine HSM 闭环验收

日期：2026-10-20。验证对象为不可变受管文件、本地归档和真实 data-mover；不是生产 HA、断电或磁带验收。

固定源码：

- Curvine：`b53bf69ae818a9143f4ff3792fd812ed747984a3`，分支 `feat/hsm-adapter`。
- data-mover-rs：`11fac12eee028c679293e4df6220584512561487`，本次没有修改。
- Curvine 引用的通用 hsm-core：`2909ceb71b7a1cd8e2db1238fcff9253761ad74d`；源端仅依赖类型，不引入 HsmEngine。
- hsm-rs 的 Cargo.lock 中全部 29 个 Curvine 包来自上述 Git 提交；最终验证不使用本地 path patch。

## 结果

| 检查 | 结果 |
|---|---|
| 固定 Git 依赖的 native-integration 全工作区测试 | 通过：25 项，另 1 个子进程 fixture 按设计忽略；rustdoc 通过 |
| 真实 managed_cluster | 通过，91.95 秒；三次 SIGKILL / 换 leader 重启 |
| Curvine Worker 持久性 | 通过：2 项 |
| Curvine 旧 journal 兼容 | 通过：5 项 |
| HSM 格式启动保护 | 通过：1 项 |
| 严格 Clippy（全部 target，native-integration，固定 lock） | 通过：`-D warnings` |
| 两仓库 fmt / diff 检查 | 通过 |

最终测试第一次运行允许 Cargo 更新新提交的 lock；后续可按下面的 `--locked` 命令复现。开发用 path patch 未进入仓库或最终验收。

## 运行

hsm-rs checkout：

```sh
CXXFLAGS=-g0 cargo test --workspace --features hsm-curvine/native-integration --locked -j 6
CXXFLAGS=-g0 cargo clippy --workspace --all-targets \
  --features hsm-curvine/native-integration --locked -j 6 -- -D warnings
cargo fmt --all --check
```

Curvine checkout：

```sh
CXXFLAGS=-g0 cargo test -p curvine-worker worker::hsm::tests -j 6
CXXFLAGS=-g0 cargo test -p curvine-master journal_entry_compat -j 6
CXXFLAGS=-g0 cargo test -p curvine-master master::hsm::tests -j 6
cargo fmt --all --check
```

Rust 1.98.1；需要 C++、protoc、Clang/libclang。`CXXFLAGS=-g0` 减少 RocksDB C++ 调试信息；使用真实 RocksDB。默认不启用 native-integration 时集群测试为 0 项，不能据此判断闭环已验收。

## 覆盖

通用核心 3 项、本地集成 10 项、Curvine 合同 10 项、原生关闭 HSM 的诊断集群 1 项、原生 managed 集群 1 项，共 25 项；另有 1 个被父测试显式调用的 ignored 子进程 fixture。

managed_cluster 使用正常提交日志（testing=false），启动 3 Master / 1 Worker。3 MiB + 19 bytes 经真实 SDK、TCP RPC 和执行器完成：

1. 导入新建 `/.hsm/file`，重试保持原身份与内容。
2. Archive → Dual，源端登记本地归档的独立回执；归档不自动释放。
3. 活跃 view 阻止 Release；强制杀进程并保留目录，排除原 leader 后重启，pin 和原始 Archive 结果仍在。
4. 关闭 pin，以新 ID Release → Offline；验证实际 payload 删除，旧 view 被拒绝，inode、generation 和逻辑长度保持。
5. 再次强制重启并排除原 leader，Offline 和原始 Release 结果保持。
6. 写入错误的私有 Recall，提交被 Integrity 拒绝且保持 Offline；Abort 后旧写 capability 被拒绝。
7. 正确 Recall → Dual，在原 inode 发布新 allocation，核对全部字节和原始成功结果。
8. controller epoch 1 → 2；第三次强制重启、排除原 leader后，查询仍为 2，原 claim/Recall 结果可重查，旧控制者的新修改被 Fenced。

还验证无 anchor 不得 Release、ID 不得跨动作复用、普通覆盖/追加/删除/free/重命名/open 无法绕过受管保护。

Curvine Worker 的 2 项测试覆盖重开后的 sealed/deleted 权限、删除墓碑、前缀幂等及错误数据不能 Seal。Journal 兼容 5 项测试覆盖现有 bincode enum 编号；新 Hsm 变体追加在末尾。Authority 的 1 项启动测试验证关闭 HSM 时，格式校验在 DB 打开/格式化前拒绝启动，保留已有元数据。

## 保证范围

权威记录、pin、私有布局、anchor、epoch、原始结果和 GC outbox 均在 Curvine 原生权威域，不维护外部全量文件元数据镜像。hsm-rs 的工作流逻辑和执行器仍在存储外。

当前只支持新建的 `/.hsm` 直接子文件、不可变内容、file-backed Worker、显式 Recall、attempt epoch 1、本地归档；执行器使用完整临时 spool。未实现自动生命周期候选批次/daemon、存量文件转管、可变文件、透明 Recall、容量预留、普通块统计/副本修复、SPDK 或带库，也未覆盖断电/盘损坏和完整生产故障矩阵。Pin/private layout/失败 import 不按超时自动清理，避免未知结果下误删；可通过查询与显式关闭/Abort/GC 恢复。

此前第一阶段仅提供 capability 握手和诊断复制；本次启用 HSM 后已提供真实保护效果。HSM 默认关闭时仍报告全部能力 false；容量预留和透明 Recall 在启用后也仍为 false。
