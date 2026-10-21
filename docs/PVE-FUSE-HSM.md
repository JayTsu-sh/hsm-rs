# 普通 FUSE 写入、策略 Release 与自动 Recall 实测

2026-10-20，PVE `10.131.9.20` 上的四台虚拟机。当前版本使用普通 Curvine FUSE 文件，不需要 import；策略存储在 Curvine，外部 hsm-rs 调度执行，data-mover-rs 使用本地文件归档后端。未接带库、未配置 UFS。设计见 [DESIGN.md](DESIGN.md)，构建来源和回归覆盖见 [VALIDATION.md](VALIDATION.md)。

当前工作区的[部署设计](DEPLOYMENT.md)附三张独立 Archify 部署流程图：普通写入自动纳管（1—6）、策略归档与 Release（1—11）、访问自动 Recall（1—12），每条箭头按执行顺序编号。这些图使用当前未提交源码和实际运行证据的冻结快照。

## 1 部署和状态位置

| 节点 | 服务 | 数据位置 |
|---|---|---|
| 10.131.9.80 | Master、Worker、FUSE；应用写入节点 | `/srv/curvine/worker` |
| 10.131.9.81 | Master、Worker、FUSE | `/srv/curvine/worker` |
| 10.131.9.82 | Master、Worker、FUSE | `/srv/curvine/worker` |
| 10.131.9.83 | 外部 HSM controller、FUSE；应用读取节点 | `/srv/hsm-data/archive`、`/srv/hsm-data/spool` |

Master 为三个独立进程，Worker 为三个独立磁盘目录；普通文件初始两副本。配置 `/etc/curvine-hsm/cluster.toml`，二进制 `/opt/curvine-hsm/bin/`，全部节点的 FUSE 挂载点为 `/mnt/curvine`。Curvine 元数据与 journal 位于 `/var/lib/curvine-hsm/`。归档目录经 ArchiveBackend 使用，属于外部后端，不是 UFS。

文件身份、驻留状态、索引、策略、任务和原始操作结果保存在 Curvine 的既有元数据存储，由 Raft journal 提交；hsm-rs 不持有全量文件元数据镜像。LocalArchive 的 catalog 保存归档对象与持久化回执，是后端自身的权威。

本轮一起升级 Master、Worker、FUSE 和控制器。升级前停止 FUSE/控制器，先停止全部 Worker 再停止 Master，并保存旧二进制、配置及 metadata/journal/Worker 备份。旧测试策略保留但关闭。旧集群 VM402 不在本轮操作范围。备份见各节点 `/opt/curvine-hsm/backups/transparent-20261020/` 和 `transparent-format2-20261020/`。

## 2 如何写入并自动纳管

最终验证目录 `/hsm-transparent-final-20261020/`，在 .80 使用标准 Python 文件 I/O 写入、fsync、关闭，没有调用 HSM import。示例仅说明操作方式；复测请使用新目录与新策略 ID：

```python
from pathlib import Path
import os
root = Path('/mnt/curvine/new-hsm-test')
root.mkdir()
with (root / 'a').open('xb') as f:
    f.write(bytes(range(256)) * (8 * 1024 * 1024 // 256))
    f.flush()
    os.fsync(f.fileno())
```

普通非 UFS、单硬链接常规文件在正常写入日志和完成路径进入 HSM Online 记录。正在写入的文件不参与归档。只有筛选中的候选才取得校验保护并计算内容摘要；不会在每次扫描中读取全部文件内容。

```bash
hsm-curvine --config /etc/curvine-hsm/cluster.toml status /new-hsm-test/a
cv fs blocks /new-hsm-test/a --format json
```

最终运行的五个文件：

| 文件 | 精确字节数 | inode | 用途 |
|---|---:|---:|---|
| a | 8,388,625 | 1021 | 最早访问，先归档释放 |
| b | 6,291,487 | 1022 | 第二个冷文件 |
| small | 262,144 | 1023 | 小于策略最小大小 |
| hot | 1,048,576 | 1024 | 每 0.5 秒普通读取一次，维持热度 |
| excluded | 4,194,304 | 1025 | 被排除前缀过滤 |

完整写入脚本、原始 SHA256、inode 和时间保存在本地 `research/pve-hsm-transparent-final-20261020/write-fuse.sh` 与 `manifest.json`。以上 research 路径相对于 `/work/jay/hsm`。

## 3 配置策略与周期筛选

日常配置示例见 [cold-local.json](../examples/policies/cold-local.json)：80% 高水位开始，60% 低水位停止，每 60 秒扫描，按访问年龄选择冷文件，并设置大小、修改年龄、驻留年龄及排除路径。

实际测试使用以下第二版规则。为避免写满磁盘，容量高水位仅为 0.01%，低水位为 0%；这是测试触发参数，不是日常配置：

```json
{
  "id": "transparent-final-release",
  "revision": 2,
  "enabled": true,
  "prefix": "/hsm-transparent-final-20261020/",
  "min_size": 1048576,
  "max_size": 67108864,
  "min_residency_ms": 0,
  "action": "release",
  "backend": "local",
  "selection": {
    "scan_interval_ms": 1000,
    "scan_budget": 128,
    "max_candidates": 1,
    "access_age_weight": 1,
    "min_access_age_ms": 5000,
    "min_modify_age_ms": 1000,
    "exclude_prefixes": ["/hsm-transparent-final-20261020/excluded"],
    "owner": "root",
    "group": "root",
    "capacity": {"high_basis_points": 1, "low_basis_points": 0}
  }
}
```

安装新规则不传 expected revision；更新规则必须带预期旧版本：

```bash
hsm-curvine --config /etc/curvine-hsm/cluster.toml policy-put policy-r1.json
hsm-curvine --config /etc/curvine-hsm/cluster.toml \
  policy-put policy-r2.json --expected-revision 1
systemctl start curvine-hsm-controller
hsm-curvine --config /etc/curvine-hsm/cluster.toml policies
hsm-curvine --config /etc/curvine-hsm/cluster.toml jobs
```

第一版容量阈值为 high=10000、low=9000（100%/90%）。后台扫描运行 7 秒后，五个文件全部 Online，没有为该规则生成任务。第二版降低测试水位后，A 先于 B 归档释放；持续访问的 hot、小文件和 excluded 保持 Online。第三版关闭快速测试规则，隔离物理 GC 和召回观察。

扫描由外部 worker 周期发起，Curvine 执行权威筛选与原子入队。它读取按“驻留状态、访问时间、inode”排序的元数据索引，优先处理 Dual，再从 Online 中选择，状态内较冷的文件优先；不是 FUSE readdir 或递归目录遍历。超过访问年龄边界可提前结束该状态范围；路径、大小、owner 等条件仍后过滤。

每请求最多 256 个索引项，scan_budget 限制每轮读取项数，max_candidates 限制选中项数。游标、下一次执行时间和容量滞回状态保存在 Curvine。所有规则共享待回收字节预算；该预算不是 Recall 硬容量预留。任务领取时重查策略、文件版本、条件与容量，读取会改变访问观察并使旧 Release 失效。

这里与 GPFS 对齐的是高低水位、访问年龄优先和条件选择等概念；没有实现 GPFS 策略语言、任意权重表达式或全部 ILM 能力。正的 access_age_weight 等价于按“权重 × 访问年龄”排序，当前不支持多维评分。

## 4 A、B 如何变为 Offline

```text
Curvine 识别候选并保存 Archive intent
  → 外部 hsm-rs 领取并取得稳定数据视图
  → CurvineDataMover 调用实际 data-mover-rs Local transfer
  → LocalArchive 校验、fsync 并返回持久回执
  → Curvine 登记归档锚点，Online → Dual
  → 后续扫描生成 Release intent
  → hsm-rs 重新验证归档回执
  → Curvine 比较内容身份、版本和活动保护，撤销在线块布局
  → 同一提交记录 Offline 与 GC outbox
  → Worker 心跳删除所有旧普通块副本
```

因此不是让 data-mover 把文件标记为 Offline，也不是删除路径。data-mover 负责数据复制，源端负责条件状态转换和回收授权；inode、路径、逻辑长度和 DataKey 保持。任务 Done 只证明逻辑提交成功，物理删除需另行观察。

最终运行中，A、B 合计 14,680,112 字节。释放前，块 `17129537536` 和 `17146314752` 均各有两副本；释放后直接检查 .80/.81/.82 的 `/srv/curvine/worker`，这些块文件全部不存在，small/hot/excluded 仍各有两副本。共回收旧块 29,360,224 字节。证据为 `physical-offline-*.json`、`physical-summary.json` 和 `blocks-before.json`，不是仅根据 HSM 状态推断空间回收。

## 5 普通读取自动 Recall

在 .83 对离线 A 启动三个并发普通文件读取，对 B 启动一个。每个读取用 Python `open(..., 'rb')`，计算完整 SHA256；没有调用 HSM read、Recall 原语或安装 Recall 策略。

```python
from pathlib import Path
import concurrent.futures, hashlib
root = Path('/mnt/curvine/hsm-transparent-final-20261020')
def read(name):
    return hashlib.sha256((root / name).read_bytes()).hexdigest()
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    print(list(pool.map(read, ['a', 'a', 'a', 'b'])))
```

更新后的 SDK/FUSE 发现 Offline 后在 Curvine 中合并创建 `__access_recall` 任务，阻塞等待；外部 worker 优先执行召回，data-mover 从归档恢复到 Worker 私有布局，校验摘要、长度并封存，Master 在原 inode 原子发布 Dual，等待的读取继续。后台 controller 必须运行，归档后端必须可用；这不是 FUSE 进程直接访问归档路径。

最终测试通过：四次并发读取只生成两个 `__access_recall` Done 任务；A 三个读取约 12.52 秒，B 约 18.44 秒，完整 SHA256、长度、inode 和 DataKey 均保持。本地读取等待上限为 300 秒，当前是整文件恢复后再读，不是部分恢复或边恢复边读。关闭快速 Release 规则是为了观察稳定中间态，自动 Recall 本身不依赖该规则是否启用。

## 6 实测结果与证据

最终版本的自动场景结果为 **PASS**。五个普通文件自动纳管，两个 Archive 与两个 Release 完成；A、B 离线后四次普通读取产生两个 Recall Done，显式 Recall 调用为零、启用的 Recall 策略为零。五文件筛选与两个原始副本的物理回收均通过。详细结果见 `evidence/summary.json`、状态快照、完整任务列表和 SHA256 记录。前一轮更大文件验证位于 `research/pve-hsm-transparent-20261020/`，A、B 合计 58,720,304 字节，两副本释放 117,440,608 字节；同样是四次普通 FUSE 读取、两个自动 Recall 任务，内容和 inode 一致。

服务端随后升级了普通钩子日志格式及紧凑数据视图。升级重放历史 journal 后，上一轮 A、B 的公开 EntityState 与升级前逐字段相等；证据 `verify-compact.log`。最终运行使用新目录和最终二进制，不能将前一轮测试冒充最终版本结果。二进制摘要保存在 `research/pve-hsm-transparent-20261020/compact-binaries.sha256`。

测试后将 `transparent-final-release` 更新为 revision=4：启用、80%/60% 水位、访问年龄一天、修改年龄五分钟、驻留一分钟、60 秒扫描、预算 4096、每轮最多 128 项；仍限定本次目录并排除 excluded。快速测试条件不再启用。

两轮 .83 场景目录分别是 `/srv/hsm-data/scenarios/transparent-20261020` 和 `/srv/hsm-data/scenarios/transparent-final-20261020`；各自含 `evidence/`、manifest、脚本和实际读取输出。本地证据归集在对应 research 目录的 `remote-evidence/`；不需要读取私有 Worker capability 文件。

## 7 能力边界

普通文件首次写入及普通块上的 generation 更新已纳管；归档中或读取保护期间修改可能需要重试。Recall 私有布局当前单副本，不参与普通块副本修复；读取和全量覆盖可用，追加及完整可变文件语义尚未覆盖。LocalArchive 与控制器是单节点，所有虚拟机也在同一物理 PVE 上，不能据此宣称整机 HA。控制器旧 Running 的恢复仍需显式 epoch 接管。完整临时 spool 和控制通道影响召回耗时，测试耗时不是存储吞吐基准。扫描和全局元数据提交边界还未做大规模性能测试，带库接入本轮未做。
