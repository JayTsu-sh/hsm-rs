# 本地文件 HSM 与策略验收 v0.5

2026-10-20。固定 Curvine `4ebad541ef9c93ac0e4d5cd7053c8222a29f5c5f`，data-mover-rs `11fac12eee028c679293e4df6220584512561487`。Curvine 引用共享 hsm-core 类型的提交为 `1c19c4a2932dd33066ad16f7975ff5cdac28e3a1`。全部 29 个 Curvine 包来自固定 Git 提交，最终验收不使用 path patch。

## 结果与命令

```sh
CXXFLAGS=-g0 cargo test --workspace --features hsm-curvine/native-integration --locked -j 6
CXXFLAGS=-g0 cargo clippy --workspace --all-targets \
  --features hsm-curvine/native-integration --locked -j 6 -- -D warnings
cargo fmt --all --check
CXXFLAGS=-g0 examples/local-hsm.sh <new-dedicated-workdir>
```

全工作区 27 项：核心 3、Curvine 合同 10、真实 managed 集群 2、关闭 HSM 的诊断集群 1、本地后端恢复单元测试 1、本地完整集成 10；另 1 个 ignored 子进程 fixture 由测试父进程显式启动。rustdoc、严格 Clippy、fmt/diff 与脚本语法检查通过。

一键场景使用 3,146,044 字节。它启动正常 journal 的三 Master/单 Worker 开发集群，经独立 CLI 进程完成：导入 → 安装策略 → 源端筛选与原子入队 → 领取 → 实际 data-mover → 本地回执 → Archive/Dual → Release/Offline + GC → Recall/Dual → 完整文件比较。再次轮询 admitted=0、executed=0；三个任务均为 Done。退出保留集群配置、日志、输入、归档、恢复文件及 jobs.jsonl。

## 策略与恢复覆盖

- enabled、规则 revision CAS、文件名前缀、最小大小、驻留时间门槛；小文件与未匹配前缀保持 Online。
- 单条分页最终检查全部记录；重复扫描和重叠规则受确定性任务 ID/active index 约束。
- 同一 epoch 重复领取 Running 被拒绝；虚假的 Done 因无源端成功 outcome 被拒绝。
- 不确定报告回 Ready，保持相同 effect ID；规则关闭/改版后未执行任务为 Superseded。
- 强制杀集群进程，保留独立 metadata/journal/Worker 目录，重启排除旧 leader；策略版本、Running 任务、源原始结果持久保存。
- 源动作已完成、任务尚未 finish 的窗口：提升 controller epoch 后，新 CLI worker 查询原结果，完成任务而不重复搬运；旧控制者上报被 Fenced。
- 后端数据已封存、receipt 未发布的窗口：新控制者恢复相同 attempt，后端独立校验、同步并发布 receipt，再登记源 anchor。
- 归档 payload 损坏：Release 任务失败，对应在线副本保持 Dual；修复并更新规则版本后才释放。
- 多文件按策略 Release/Recall；恢复后完整字节一致，其他文件保持原状态。

## 既有保护回归

原生闭环测试仍使用三 Master/单 Worker、3 MiB + 19 bytes，以及三次 SIGKILL/换 leader 重启，验证：

- import 幂等；原 inode、generation、逻辑长度及全部内容保持。
- Archive 不隐式释放；无 anchor 不得 Release；活跃 pin 阻止 Release，pin 跨重启存在。
- Release 提交后真实 payload 被删除，旧 view 拒绝，Offline 不伪造普通文件空洞。
- 错误 Recall 保持 Offline；显式 Abort 后旧写 capability 不得重建 allocation。
- 原始成功/确定失败结果跨重启保持；epoch 提升后旧控制者的新修改被拒绝。
- 普通覆盖、追加、删除、free、重命名和 open 不能绕过受管保护。

此前 Curvine 额外单元回归通过：Worker 持久性 2、旧 journal 兼容 5、启动格式保护 1；本轮策略/CLI 的新增源端行为通过上述真实集群验证。

## 环境与范围

Rust 1.98.1，真实 RocksDB，需要 C++、protoc、Clang/libclang；CXXFLAGS=-g0 减少 C++ 调试信息。默认不启用 native-integration 时真实集群测试为 0，不能据此判断闭环完成。

权威策略、任务与元数据保持在 Curvine，外部没有维护全量 namespace 镜像；本地归档 catalog 仅是后端对象权威。数据范围为新建不可变 `/.hsm` 直接子文件、file-backed Worker、本地 fsync 后端、显式 Recall、attempt epoch 1。策略每版本/内容 generation 最多一次；时间筛选是当前驻留状态持续时间，不是访问热度。当前 worker 顺序执行，一个 worker 打开一个本地归档目录，使用完整 spool。

尚无存量文件原地转管、可变 generation、透明召回、访问热度/容量水位策略、普通块统计/副本修复、带库或完整生产故障矩阵。配置、运行和故障恢复步骤见 [LOCAL-HSM.md](LOCAL-HSM.md)。
