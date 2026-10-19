# Curvine Adapter：第一阶段

此 crate 把通用 `ManagedStorage` 合同接到 Curvine 原生控制 RPC，不保存 inode、generation、驻留状态或 archive anchor 的本地镜像。身份、原始操作结果和受保护数据效果必须由 Curvine 提供。

当前实现分两条路径：

- `CurvineStorage<T>`：能力协商、控制命令映射、错误分类、响应身份和效果校验；默认只依赖通用核心。原生服务端尚未实现权威 HSM 效果，所以目前所有 HSM 能力为 false，相应操作在发送前返回 Unsupported。
- `NativeControl` / `CurvineIo`（`native` feature）：原生 Master TCP RPC，以及 SDK → 私有本地 spool → 真实 data-mover-rs Local transfer 的初始数据链路。没有 FUSE 或 POSIX 网关依赖。每次复制按块读取，内存不随文件增长，但 spool 需要完整文件空间，多一次本地复制。

`copy_current_file_to_local` 复制普通、已完成文件。读取前后的 inode/length/mtime 检查只能发现部分变化，不能证明读到单一稳定版本；它不会构造 DataKey、归档锚点或授权 Release。调用方应选择静止的测试文件。目标是受信任的专用本地 root，`LocalDataMover::payload_endpoint` 也不是生产认证机制。

## 原生协议

Curvine fork 的 `RpcCode::HsmControl = 61` 使用 protobuf envelope `{protocol_version, payload}`；payload 为带 `op` / `kind` 的版本 1 JSON。请求最大 64 KiB，客户端响应最大 1 MiB。不兼容协议、缺少能力、错误实体、错误 generation、不匹配内容和不正确驻留状态明确失败；未知服务端错误或传输中断保持 Unknown，不解释为 Absent。

`ControlTransport` 可以注入测试，但 fixture 结果不代表 Curvine 已具备相关保证。生产 native transport 使用固定 Git revision，直接复用 SDK 的 Master connector。

## 运行

```sh
# 默认测试不编译 Curvine SDK 或 server。
cargo test -p hsm-curvine --locked

# 原生客户端；Rust/C++ 构建环境和 protoc 必须可用。
cargo check -p hsm-curvine --features native --locked

# 真实临时 Master + Worker + TCP SDK + data-mover。
# 额外需要 Clang/libclang 和 RocksDB 所需的 C++ 构建环境。
CXXFLAGS=-g0 cargo test -p hsm-curvine --features native-integration --locked \
  --test native_cluster -- --nocapture
```

`CXXFLAGS=-g0` 只减少 C++ 调试信息，降低首次 RocksDB 构建开销。`native-integration` 仅用于集群测试，才引入 `curvine-server`；生产 `native` 不引入 server。测试使用独立临时数据目录及动态端口，单 Master/Worker，`testing=true`、journal 关闭；因此不验证 Raft 提交、跨节点恢复或 HSM Release/Recall。

## 后续实现的必要条件

1. 在 Curvine journal 队列内增加 HSM 命令，使用 committed apply 保存原生记录、资格及原始结果；与普通 Create/Complete 保持顺序。
2. 从创建开始限定 managed 文件范围，所有修改入口和 Worker 数据路径检查 generation/epoch；处理既有 writer、复制、删除和 short-circuit 引用。
3. 提供跨进程保留的 DataView/pin，以及精确 view 读取。只有固定版本可进入 `prepare_archive`。
4. 实现条件 Release：保留原 inode、逻辑长度和已提交 anchor，撤销 published layout 后再 GC。
5. 实现原 inode 私有 RecallLayout、完整 seal、原子 publish、未知结果查询和旧执行器 fencing。
6. 将 Curvine view/private-layout 作为 data-mover 原生角色接入，替换本阶段普通读取 spool。上述保证通过真实故障验收后逐项启用能力。

不能把普通 free/delete 当成 Release，不能把临时文件 rename 当成 Recall publish，也不能用配置开关直接启用尚未实现的能力。
