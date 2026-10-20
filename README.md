# hsm-rs

通用外部 HSM 核心。存储保存权威事实和受保护数据效果，核心协调 Archive / Release / Recall，data-mover-rs 执行字节搬运。本地闭环已实现，Curvine 已接入受管不可变文件的原生 Archive / Release / Recall 闭环，带库接入后置。

旧文件系统专有实现已移除，不保留兼容路径；这是一次不兼容的 Interface 更新。

| 模块 | 内容 |
|---|---|
| [hsm-core](crates/hsm-core/src/managed.rs) | 通用身份、状态、回执、ManagedStorage / ArchiveBackend / DataMover |
| [hsm-engine](crates/hsm-engine/src/lib.rs) | Archive / Release / Recall 外部协调 |
| [hsm-local](crates/hsm-local/src/lib.rs) | 本地参考存储、归档后端、真实 data-mover 集成 |
| [hsm-curvine](crates/hsm-curvine/README.md) | Curvine 原生权威记录、稳定 view、条件 Release、原 inode Recall 与 data-mover 本地桥接 |
| [详细设计](docs/DESIGN.md) | 合同、持久化、保证范围、限制和 Curvine 接入顺序 |

策略、持久任务与可运行的本地文件全场景已实现，见 [本地 HSM 与生命周期](docs/LOCAL-HSM.md)。

## 运行

本地参考实现要求 Unix 文件系统的目录 fsync、advisory lock 和 hard-link 支持。当前依赖锁要求 Rust 1.94.1 或更新版本；已使用 Rust 1.98.1 验证。data-mover 当前没有后端 feature gates，因此本地调用仍会编译其其他后端依赖。

```sh
cargo test --workspace --locked
cargo run -p hsm-local --bin hsm-local -- /tmp/hsm-local-example
```

演示仅在指定目录中建立专用 source/archive，运行 Archive → Release → GC → Recall，并输出原实体身份和恢复内容。源与归档应使用专用、互不重叠的目录。

本地参考是单主机、独占 root、经 Interface 访问的逻辑 namespace；支持持久成功结果及重开恢复，不提供跨节点 authority、透明 POSIX 召回或容量预留。Curvine 原生闭环已接入，限于显式管理的不可变文件及本地归档；默认不开启，需按文档配置。构建及集群验证命令见 [Curvine Adapter](crates/hsm-curvine/README.md)。完整的生产协调和故障验收仍按设计推进。
