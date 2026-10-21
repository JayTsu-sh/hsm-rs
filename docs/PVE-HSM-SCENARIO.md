# Curvine 多节点 HSM 策略与读取验收

2026-10-20，在已部署的真实多虚拟机集群上执行。**原生接口下的导入、策略筛选、Archive、Release、策略驱动 Recall 和内容校验通过；“应用再次打开文件触发自动 Recall 并继续读取”尚未实现。** 受管文件即使 Online 或已经 Recall 回到 Dual，普通 FUSE 读取也返回 EIO，因此本次不能将访问透明的完整 HSM 场景标记为通过。

## 1 环境与执行范围

| 地址 | 角色 | 数据位置 |
|---|---|---|
| 10.131.9.80 | 独立 Master、Worker、FUSE 客户端；执行访问探针 | /srv/curvine/worker |
| 10.131.9.81 | 独立 Master、Worker、FUSE 客户端；本轮 Master leader | /srv/curvine/worker |
| 10.131.9.82 | 独立 Master、Worker、FUSE 客户端 | /srv/curvine/worker |
| 10.131.9.83 | 外部 HSM worker，进程内包含 HsmEngine、data-mover-rs 和 LocalArchive；FUSE 客户端 | /srv/hsm-data/archive |

四台均挂载 `/mnt/curvine`。Curvine 在线存储来自三个 Worker 的独立磁盘，普通文件配置 2 副本，`cv mount` 实测为 `Mount Table: (empty)`。归档目录位于外部 HSM 节点，经 ArchiveBackend 接口使用，不是 Curvine 的 UFS 挂载。

固定版本：Curvine `4ebad541ef9c93ac0e4d5cd7053c8222a29f5c5f`；hsm-rs `9d060c87b8675e9db53d0a1231c7eba7c29b31f5`；data-mover-rs `11fac12eee028c679293e4df6220584512561487`。本轮没有修改或升级服务端实现。

场景目录：`/srv/hsm-data/scenarios/acceptance-20261020-132409`，位于 10.131.9.83。本地脚本和证据：`/work/jay/hsm/research/pve-hsm-acceptance-20261020-132409/`。主要测试动作的北京时间为 13:27 至 13:33，事件日志保存 UTC 时间。

以下命令在 10.131.9.83 的 root Bash 中执行；FUSE 访问探针在 10.131.9.80 执行。它们复述已经完成的测试。重新执行应使用新的 TAG、策略 ID、文件名和场景目录；现有文件和策略版本不能当作空环境重复创建。

```bash
CONF=/etc/curvine-hsm/cluster.toml
ROOT=/srv/hsm-data/scenarios/acceptance-20261020-132409
TAG=acc20261020-132409
HSM=(hsm-curvine --config "$CONF" \
  --archive-root /srv/hsm-data/archive --backend-id local)
```

## 2 写入数据与对照文件

首先在 10.131.9.80 经 FUSE 写入一个普通文件，再在 10.131.9.83 下载并核验 SHA256，确认共享命名空间和远程 Worker 读写可用。

```bash
# 10.131.9.80
python3 - <<'PY'
from pathlib import Path
import os
p = Path('/mnt/curvine/hsm-acceptance-20261020-132409')
p.mkdir()
with (p / 'ordinary.bin').open('xb') as f:
    f.write(bytes(range(256)) * 4096)
    f.flush()
    os.fsync(f.fileno())
PY

# 10.131.9.83
cv fs get /hsm-acceptance-20261020-132409/ordinary.bin "$ROOT/ordinary-cross-node.bin"
cv fs blocks /hsm-acceptance-20261020-132409/ordinary.bin --format json
```

结果：1,048,576 字节，SHA256 为 `fbbab289f7f94b25736c58be46a994c441fd02552cc6022352e3d86d2fab7c83`。块记录显示 `replicas=2`、`storage_policy.state=Cv`，没有 UFS 挂载。

**当前普通 FUSE 写入的文件不会自动成为 HSM 受管文件。** HSM 首版范围是 `/.hsm/` 下的新建不可变直接子文件，必须使用原生 import。不能将普通 `cp` 写入和 HSM 纳管视为同一个步骤。

本轮生成五份独立内容，文件大小均为精确的二进制 MiB。每份数据使用不同的 256 字节重复模式，原始内容保留在场景目录。

| 简称 | 受管文件名，省略相同的 `/.hsm/` 前缀 | 大小 | inode | 用途 |
|---|---|---:|---:|---|
| A | acc20261020-132409-cold-a | 8 MiB | 1007 | 归档、释放、离线访问、恢复 |
| B | acc20261020-132409-cold-b | 6 MiB | 1008 | 归档、释放；不经应用访问，由 Recall 策略恢复 |
| small | acc20261020-132409-cold-small | 256 KiB | 1009 | 小于策略最小大小 |
| keep | acc20261020-132409-keep-d | 4 MiB | 1010 | 不匹配 cold 前缀 |
| noanchor | acc20261020-132409-cold-noanchor | 16 MiB | 1011 | 大于 Archive 上限，但满足 Release 大小范围；无归档副本 |

生成及导入示例，完整五文件生成逻辑见证据目录中的 `scenario.py`：

```bash
python3 - "$ROOT/a.bin" <<'PY'
import sys
pattern = bytes((i + 17) % 256 for i in range(256))
with open(sys.argv[1], 'xb') as f:
    f.write(pattern * (8 * 1024 * 1024 // 256))
PY

"${HSM[@]}" --op-id "$TAG-import-a" \
  import "$ROOT/a.bin" "/.hsm/$TAG-cold-a"
"${HSM[@]}" status "/.hsm/$TAG-cold-a"
"${HSM[@]}" read "/.hsm/$TAG-cold-a" "$ROOT/online-native-read.bin"
```

五个 import 均返回 Online。A 经原生 read 读取的完整内容 SHA256 与输入一致。记录的身份包含 storage、namespace、entity 和 generation；后续按这个 DataKey 约束动作，路径仅用于定位。

同时在 10.131.9.80 对 A 执行普通 FUSE `open(..., 'rb').read()`：`stat` 可见 inode 1007、长度 8,388,608，但读取返回 `EIO (5)`。这说明当前受管读取与普通 FUSE 路径尚未对接，与是否离线无关。

## 3 配置 Archive 策略并形成归档凭据

为展示 Ready 中间态，确认已有任务无 Running/Ready 后，暂时停止常驻 HSM worker。单次执行器和常驻 worker 不能同时占用同一本地 archive root。

```bash
systemctl stop curvine-hsm-controller
```

`archive-policy.json` 的完整内容：

```json
{
  "id": "acc20261020-132409-archive",
  "revision": 1,
  "enabled": true,
  "prefix": "/.hsm/acc20261020-132409-cold-",
  "min_size": 1048576,
  "max_size": 12582912,
  "min_residency_ms": 0,
  "action": "archive",
  "backend": "local"
}
```

```bash
"${HSM[@]}" policy-put "$ROOT/archive-policy.json"
"${HSM[@]}" scan "$TAG-archive" --limit 2
# 如果 next 非 null，使用 --after '<next>' 继续，直到 next=null。
"${HSM[@]}" jobs
"${HSM[@]}" worker --once
```

`scan` 会提交准入事务并创建 Ready 任务，**不是只读的候选预览**。本轮 Archive 分三页检查 6 条原生 HSM record，其中一条是既有场景的记录；只有 A、B 入队。两个任务执行后为 Done，A、B 从 Online 变为 Dual，另外三个对照文件保持 Online。

归档执行过程为：源端建立稳定视图和 pin → LocalArchive.prepare → Curvine Worker 读取到完整 spool → data-mover Local transfer 写归档 → 后端完整核验并 fsync、提交 receipt → Master 登记 anchor 并关闭视图。仅传输成功不会直接形成可释放的归档凭据。

两份归档 payload 均再次完整读取并核验 SHA256，与输入一致，receipt 的 durability_profile 为 `local-fsync-v1`。Archive 不释放在线副本。

## 4 配置 Release 策略与筛选候选

`release-policy.json`：

```json
{
  "id": "acc20261020-132409-release",
  "revision": 1,
  "enabled": true,
  "prefix": "/.hsm/acc20261020-132409-cold-",
  "min_size": 1048576,
  "max_size": 33554432,
  "min_residency_ms": 120000,
  "action": "release",
  "backend": "local"
}
```

```bash
"${HSM[@]}" policy-put "$ROOT/release-policy.json"
"${HSM[@]}" scan "$TAG-release" --limit 2
"${HSM[@]}" scan "$TAG-release" --limit 2 --after '<上页 next>'
# 继续到 next=null；生产常驻 worker 使用同一接口，每页最多检查 16 条。
```

两分钟是**当前驻留状态的持续时间**。对于本轮 A/B，它从进入 Dual 开始计时，不是“最近两分钟未被读取”，也不是文件 mtime/atime、热度或容量水位。

筛选发生在 Master，由存储权威记录执行：

1. 检查 enabled、路径前缀、逻辑大小、当前状态驻留时间；拒绝 pending 私有布局。
2. Release 还必须满足 Dual、有 published 在线布局、无视图/pin、有匹配 backend 的 anchor。
3. 对策略原值、候选 record version、确定性 intent key、实体 active index 做 CAS 校验；同一个 Raft 事务写入 Ready intent 和 active index。
4. 外部 worker 领取任务，源端持久化 Running/claim；执行前再次查询原始效果并核对源状态。

本轮使用 `--limit 2` 是为了显示分页。这个 limit 指每页检查的 record 数，不是每页要选出两个文件。cursor 指向最后检查的原生记录，包含未匹配项。扫描是 key 线性分页，不是一次 namespace 全局快照，也不依赖外部元数据镜像。

实际结果：

| 观察点 | 页数 | 检查 record | 新入队 |
|---|---:|---:|---:|
| Dual 驻留时间未达到两分钟 | 3 | 6 | 0 |
| 等待满足驻留时间后 | 3 | 6 | 2 |
| 立即重复同一策略版本扫描 | 3 | 6 | 0 |

Ready 阶段捕获到的候选：

| 文件 | 任务 ID 前缀 | 扫描时源状态 | 任务状态 |
|---|---|---|---|
| A | 6cd18474… | Dual | ready |
| B | 8d99a3e9… | Dual | ready |

此时尚未运行执行器，A、B 仍为 Dual，说明“识别并入队”与“实际释放”是两个步骤。完整任务 ID 和 DataKey 对应关系保存在 `release-candidates.json`。

排除情况：small 小于 1 MiB，keep 不匹配 cold 前缀，这两份文件也尚未归档；noanchor 的前缀和 16 MiB 大小符合 Release 规则，但仍为 Online、没有有效 anchor，不能释放。small/keep 在本轮不是对 Release 单一谓词的独立消融测试，它们同时缺少归档；对应大小和前缀条件也已在 Archive 选择时验证。

## 5 执行 Release 并核验物理删除

```bash
"${HSM[@]}" worker --once
"${HSM[@]}" status "/.hsm/$TAG-cold-a"
"${HSM[@]}" status "/.hsm/$TAG-cold-b"
"${HSM[@]}" jobs
```

该周期返回 `executed=2, failed=0, retry=0`。两个 Release 任务为 Done，A、B 变为 Offline，原 inode、逻辑长度、generation 和 anchor 保留。

执行顺序：HsmEngine 再次核验源状态和后端已提交回执 → Master 条件校验并通过 Raft 原子撤销 published、写 Offline、登记 garbage 和原始成功结果 → Master 请求 Worker.Delete → 源端确认 GC → finish_intent 独立核对原始成功结果。

Release 不调用 data-mover。逻辑离线提交和物理 GC 分两个阶段；Done 的权威依据是源端原始效果，不能只根据执行器声称完成作判断。本轮另外读取 Worker 的持久化 allocation 状态和实际文件，验证 GC 确实完成：

| 文件 | 原 allocation | 数据文件变化 |
|---|---|---|
| A | 1587252c77b04279ace6645fbcd90bd6 | 8,388,608 字节 → 文件不存在，deleted=true |
| B | 4be3694047854c1f9da4413e10f41761 | 6,291,456 字节 → 文件不存在，deleted=true |
| small / keep / noanchor | 各自原 allocation | 数据大小不变，deleted=false |

合计删除在线 payload **14,680,064 字节，即 14 MiB**。归档 payload 仍存在且完整内容校验通过。没有根据普通块 fs_used 推断 HSM 释放量，因为专用 HSM allocation 尚未接入普通块统计。

本轮五份受管文件都分配在 Worker 10.131.9.81。当前原生 HSM 分配选择 live_workers 的第一个 Worker，没有复用普通块的轮询分配和副本机制。不能把普通文件的 2 副本设置解释为 HSM allocation 也有 2 副本。

## 6 再访问是否触发自动 Recall

先恢复常驻 HSM worker，确保测试不是因为执行器被停止才没有召回；此时没有安装匹配本轮文件的 Recall 策略。

```bash
systemctl start curvine-hsm-controller
systemctl is-active curvine-hsm-controller

# 10.131.9.80
python3 - <<'PY'
p = '/mnt/curvine/.hsm/acc20261020-132409-cold-a'
with open(p, 'rb') as f:
    print(len(f.read()))
PY
```

实测：`stat` 仍可见 inode 1007 和逻辑长度 8 MiB，读取返回 `EIO (5)`，没有阻塞等待召回。观察三秒后，A/B 都仍为 Offline，没有新增 Recall 任务，既有四个本轮任务保持 Done。

原生读取同样不能隐式召回：

```bash
"${HSM[@]}" read "/.hsm/$TAG-cold-a" "$ROOT/offline-native-read.bin"
```

返回：

```text
Error: Busy("protected data is busy: entity is offline; Recall required")
```

原因与实现一致：普通文件位置读取会调用 `FsDir::get_file_locations`，其中执行 `hsm_guard_id(file.id())`；有原生 HSM record 的 inode 被要求使用 native HSM Interface。这里尚未返回受保护在线视图，也没有创建访问触发的 Recall 意图。

因此，访问触发的透明 Recall 是本轮明确发现的功能缺口。普通 FUSE open 不能用一次定时策略扫描或一个显式脚本替代后宣称已经通过。

## 7 测试实际支持的策略自动 Recall

为了区分策略触发和访问触发，先只为 **B** 安装 Recall 策略。离线后被应用访问的是 A；B 在恢复前没有应用读取。

`recall-b-policy.json`：

```json
{
  "id": "acc20261020-132409-recall",
  "revision": 1,
  "enabled": true,
  "prefix": "/.hsm/acc20261020-132409-cold-b",
  "min_size": 1048576,
  "max_size": 33554432,
  "min_residency_ms": 0,
  "action": "recall",
  "backend": "local"
}
```

```bash
"${HSM[@]}" policy-put "$ROOT/recall-b-policy.json"
# 不手动调用 worker --once；常驻服务轮询筛选并执行。
"${HSM[@]}" status "/.hsm/$TAG-cold-b"
```

实测 B 自动变为 Dual，A 仍为 Offline。这证明当前自动化入口是策略轮询，应用访问不是触发条件。

Recall 的实际执行过程为：

1. Master 筛选 Offline 且有匹配 anchor 的 B，持久化任务。
2. HsmEngine 领取并核验归档；请求 prepare_recall。
3. Master 持久化 pending 私有布局并让 Worker.Allocate，返回仅能写私有目标的 capability；文件此时仍 Offline。
4. LocalArchive.open 返回本地源端点；data-mover 从归档复制到完整 spool，再经 HsmData.Write 写入私有布局并 Verify，返回长度和摘要证据。
5. Master 在 commit_recall 时请求 Worker.Seal，核对 evidence，再通过 Raft 在原 inode 原子发布布局并置 Dual，保存原始成功结果。
6. finish_intent 核对源端原始成功结果后置 Done。

随后将同一 Recall 策略的 revision 改为 2、prefix 改为 cold 公共前缀，并以 revision CAS 更新，召回剩余的 A：

```bash
# recall-a-policy.json 与上述相同，只有 revision=2、prefix 为 cold 公共前缀。
"${HSM[@]}" policy-put "$ROOT/recall-a-policy.json" --expected-revision 1
"${HSM[@]}" status "/.hsm/$TAG-cold-a"
```

A 恢复到 Dual。两个文件都通过原生 read 完整读出并核验 SHA256：

| 文件 | SHA256，输入与恢复结果一致 |
|---|---|
| A，8 MiB | 3480e36fd09a0e1fb7fc22645b44a0332ee0c2fd9cd6f62ef44fd76522b708fe |
| B，6 MiB | a4276dcbf069dac2d5b3d70fbf8bce5363f0afaf9c12b73fa6ccd833c1a0a5aa |

另对 A/B 各执行一次 `cmp`，恢复文件与输入逐字节一致，结果保存在 `byte-comparison.log`。inode 1007/1008、generation=1、DataKey 和源摘要均保持。归档保留，因此恢复后状态为 Dual。

再次分别通过普通 FUSE 读取恢复后的 A/B，仍返回 EIO。**显式/策略 Recall 后原生数据已可读，不代表当前普通 FUSE 路径已可读。**

同一规则版本对同一 DataKey 最多一次，重复扫描不重复入队。A/B 恢复后不会被同一 Release 规则版本再次反复释放；本轮不是冷热振荡或访问热度策略测试。

## 8 最终结果与需要补齐的访问链路

| 验收项 | 结果 |
|---|---|
| 普通文件跨 VM FUSE 写入、读取；无 UFS | 通过 |
| 原生 import、Online 读取 | 通过 |
| Archive 候选过滤与提交回执 | 通过，A/B 两项 |
| Release 驻留时间、分页、候选入队、重复扫描去重 | 通过，0 → 2 → 0 新任务 |
| Release 物理 GC 与保留逻辑身份 | 通过，14 MiB 在线 payload 删除 |
| Offline 原生读取保护性拒绝 | 通过 |
| 策略驱动 Recall，无需手动执行器 | 通过，B 未经应用访问即恢复 |
| Recall 后原生读取内容、身份校验 | 通过 |
| 普通 FUSE 读取 Online / Offline / 恢复后的受管文件 | 未接入，均为 EIO |
| 应用访问触发 Recall、等待发布并继续读取 | 未实现，本轮完整请求场景仅部分满足 |

最终 A/B 为 Dual，small/keep/noanchor 为 Online。本轮 2 Archive、2 Release、2 Recall 共 6 个任务全部 Done。此前已有的三个任务记录保持不变。常驻 HSM worker 和 FUSE 已恢复运行，所有测试文件与策略保留供检查。

要实现用户期望的透明访问流程，还需要完成以下链路，尚不能作为本轮测试结果：

```text
应用 open/read
  → FUSE/SDK 识别原生受管 inode
  → Online/Dual：获取稳定受保护视图并正常读取
  → Offline：向存储提交可去重的访问型 Recall 意图
  → 外部 HSM worker 领取，校验归档并调 data-mover 写私有布局
  → Worker Verify/Seal，Master 原 inode 原子发布 Dual
  → 唤醒等待的访问请求，获取稳定视图并继续读取
```

这要求普通读取桥接、访问型任务准入、同文件并发访问合并、发布等待/通知，以及超时、取消和失败传播；仅安装一条 Recall 策略不能完成这条访问链路。本轮没有改写服务实现，也没有用自动重试包装器模拟透明 Recall。

## 9 证据与复现入口

本地证据目录：`/work/jay/hsm/research/pve-hsm-acceptance-20261020-132409/`。

- `scenario.py`：分阶段测试驱动；`probe-fuse.py`：真实 FUSE 访问探针。
- `SUMMARY.json`：汇总，完整访问场景状态为 partial。
- `evidence/fixtures.json`：输入大小、SHA256 和导入身份。
- `evidence/archive-admission-scan.json`、`release-too-young-scan.json`、`release-eligible-scan.json`、`release-repeat-scan.json`：完整分页及准入证据。
- `release-candidates.json`、`evidence/release-ready-jobs.json`：实际候选与 Ready 任务。
- `physical-release-proof.json`、`before-release-*.json`、`after-release-*.json`：Worker 实际 allocation 文件变化，不包含管理 capability。
- `evidence/archive-payload-proof.json`：两份归档内容和回执的独立校验。
- `evidence/access-trigger-result.json`、`evidence/fuse/*-fuse-probe.json`：访问失败且没有触发 Recall 的观察结果。
- `evidence/after-recall-b-states.json`：B 已恢复、A 仍离线，区分两种触发机制。
- `evidence/final-jobs.json`、`final-states.json`、`events.jsonl`：最终任务、状态和时间线。

测试驱动按 prepare → archive → release-admit → release-execute → access-check → recall-b → recall-a 执行；各阶段之间按本报告执行 FUSE 探针和物理文件检查。`release-admit` 等待驻留条件后只入队，`release-execute` 才执行动作。手动扫描期间停常驻 worker，访问/策略 Recall 阶段必须启动常驻 worker。完整输入和恢复数据仍保留在 10.131.9.83 的场景目录。

源码依据：Curvine `curvine-master/src/master/hsm/lifecycle.rs` 的 selected 和 policy_scan；`curvine-master/src/master/meta/fs_dir.rs:898` 的普通读取保护；`curvine-master/src/master/hsm.rs` 的 release、prepare_recall、commit_recall、hsm_gc；hsm-rs `crates/hsm-engine/src/lib.rs` 的 release/recall；`crates/hsm-curvine/src/bin/hsm-curvine.rs` 的 cycle 和原生 CLI。

所有虚拟机共用一台 PVE。该验收不覆盖物理主机故障、HSM allocation 副本修复、归档后端 HA、掉电/盘损坏矩阵或带库。
