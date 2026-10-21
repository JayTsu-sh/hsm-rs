# 普通 FUSE 文件 HSM 验收 v0.6

2026-10-20。本轮工作区新增普通文件自动纳管、容量与访问年龄策略、周期索引扫描，以及普通读取触发的透明 Recall。详细操作和 PVE 证据见 [PVE-FUSE-HSM.md](PVE-FUSE-HSM.md)，责任与一致性设计见 [DESIGN.md](DESIGN.md)。

## 构建来源

发布版本（2026-10-21）合入 hsm-rs main 与 Jay fork 的 Curvine main。hsm-curvine 的 Cargo.toml 固定 Curvine `039631f4ea8aa5a06250aa8928e2328edc003475`；Curvine 固定 hsm-core `ef5167661da40345a25f5d028dbeb4f1a70b1398`；实际 data-mover-rs 固定为 `11fac12eee028c679293e4df6220584512561487`。Cargo.lock 包含可从 GitHub 获取的源地址，无需本地路径补丁。本轮没有修改带库后端。

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

## 真实 PVE 集群

三个 Master、三个 Worker、两个普通块副本及独立 HSM 控制节点，不配置 UFS。应用通过 FUSE 写入和读取，没有 import 或显式 Recall。候选和任务位于 Curvine 权威元数据，不依赖外部 namespace 镜像。两轮测试及物理块检查见 [PVE-FUSE-HSM.md](PVE-FUSE-HSM.md)。

升级历史 journal 到 ordinary_format=2 后，上一轮 A、B 的公开 EntityState、原 inode、DataKey 和归档锚点逐字段保持一致，三 Worker 全部恢复。该检查独立于全新目录的闭环测试。

## 边界与历史记录

当前支持单硬链接、非 UFS 常规文件。策略是 GPFS 对应概念的类型化子集，不解析 GPFS 语法或任意权重表达式。索引分页减少目录扫描，但路径和部分条件仍后过滤，未做百万文件吞吐测试；元数据提交使用全局串行边界。

Recall 为整文件恢复，私有恢复布局单副本，尚不参与普通块副本修复；全量覆盖可用，恢复布局上的追加与完整可变 POSIX 语义尚未覆盖。有活跃读取或归档保护时修改可能被拒绝。LocalArchive 单节点，控制者 Running 接管仍需显式 epoch。所有虚拟机在同一 PVE 物理机，没有生产 HA 承诺。搬运需要完整临时 spool。

[v0.5 PVE 原语测试记录](PVE-HSM-SCENARIO.md)保留为历史：当时是 import、策略显式 Recall，普通 FUSE 返回 EIO。本轮 v0.6 的普通 FUSE 自动纳管和透明 Recall 已新增，不能混用两轮结论。旧的严格 Clippy、固定 Git pin 和不可变文件限制也仅对应当时版本。
