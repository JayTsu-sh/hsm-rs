# 普通写入如何自动纳入 HSM？ {#write-reading}

应用通过 Curvine 写入文件。文件内容按块存入 Worker，Master 维护文件元数据和 HSM 记录。**写入完成后，文件处于 Online，供后续策略筛选。**

@answer scope="当前已实现工作区；普通非 UFS 单硬链接文件；KV 为结构示例"
@meta locale=zh-CN repo_url="ssh://git@localhost/work/jay/hsm/.archify/architecture-curvine-hsm-deployed-20261020-164157/evidence" repo_revision=c9cc378ac025657b19948e0ea73fb982be89bf42
@source writer title="SDK 按文件偏移定位并写入块" path=curvine/crates/client/curvine-client-core/src/file/fs_writer_base.rs lines=367:444
@source inode title="原生文件元数据与块列表" path=curvine/curvine-master/src/master/meta/inode/inode_file.rs lines=30:49
@source ordinary title="普通文件纳管与版本更新" path=curvine/curvine-master/src/master/hsm/ordinary.rs lines=51:151
@source kvbatch title="主记录与候选索引同批维护" path=curvine/curvine-master/src/master/hsm/ordinary.rs lines=6:44
@source cf title="现有 RocksDB CF 与 WAL 配置" path=curvine/curvine-master/src/master/meta/store/rocks_inode_store.rs lines=35:54
@source model title="HSM Record 字段定义" path=curvine/crates/common/curvine-model/src/hsm/native.rs lines=102:137
@source scans title="候选扫描与回查 Record" path=curvine/curvine-master/src/master/hsm/lifecycle.rs lines=281:366
@source runstate title="持久扫描进度" path=curvine/curvine-master/src/master/hsm/lifecycle.rs lines=469:499
@source response title="普通写入完成响应" path=curvine/crates/common/curvine-proto/proto/master.proto lines=198:213
@source localfile title="块在 Worker 本地落盘" path=curvine/crates/adapters/curvine-storage-local/src/layout/file_layout.rs lines=61:89
@source naming title="块文件命名" path=curvine/crates/adapters/curvine-storage-local/src/block_meta.rs lines=24:45
@source conditional title="提交时核对源记录版本" path=curvine/curvine-master/src/master/hsm.rs lines=159:199
@source commit title="HSM 记录与原始结果提交" path=curvine/curvine-master/src/master/hsm.rs lines=290:381
@source reset title="投影清理与范围读取" path=curvine/curvine-master/src/master/meta/store/rocks_inode_store.rs lines=150:183
@source boot title="启动时清理 HSM 投影" path=curvine/curvine-master/src/master/meta/fs_dir.rs lines=114:119

## 按 1—6 看一次完整写入 {#write-steps span=12}

@diagram architecture id=write-flow source=write-diagram.json title="写入与 HSM 元数据落盘"

图中 `5a` 和 `5b` 属于第 5 步内部操作，不是额外一次客户端请求。

| 步骤 | 谁做什么 | 产生什么 |
| --- | --- | --- |
| 1 创建文件 | FUSE / SDK 向 Master 创建文件、申请块位置。 | 文件 inode、待写入块和副本位置。 |
| 2 返回布局 | Master 将 inode、块 ID 和 Worker 位置交给 SDK。 | SDK 知道每段内容应写到哪里。 |
| 3 写入数据 | SDK 按文件偏移定位 block，向 Worker 发送字节。 | 数据进入普通块写入通道。 |
| 4 保存副本 | Worker 将 block 保存为本地文件。 | 当前普通块配置要求的两份副本。 |
| 5 完成写入 | SDK 提交 close / complete，Master 更新元数据并执行纳管钩子。 | 写入完成事实、HSM Record 和候选索引。 |
| 6 返回确认 | Master 返回结果，FUSE 向应用返回成功或错误。 | 应用知道写入是否完成。 |

SDK 负责按块写入，Master 的完成钩子负责纳管。`fsync` 刷新数据，最终完成关闭后才将 `writing` 更新为 `false`。[](source:writer)[](source:ordinary)

@callout level=note title="第 6 步不返回 Online 字段"
| `Online` 保存在 Master 的 HSM 记录中。普通完成响应只有 `result` 和可选 `file_blocks`。[](source:response)
| 此时归档尚未发生，归档锚点为空。后续策略才决定是否归档和 Release。

## 文件内容和元数据分别在哪里？ {#where-data-lives span=12}

Curvine 提供统一目录和文件访问。应用看到完整文件，Worker 保存其中的数据块。[](source:inode)[](source:writer)

| 层次 | 保存什么 | 具体形式 |
| --- | --- | --- |
| 应用 / FUSE | 完整文件视图。 | 例如 `/data/a`。 |
| Master 原生元数据 | 目录、inode、长度、块列表和副本位置。 | 原有 `inodes`、`edges`、`block`、`location` CF。 |
| Worker 本地盘 | 各个 block 及其副本。 | 例如 `active/b0/b4/blk_17129537536`。 |
| Master HSM 元数据 | 当前 HSM 状态与候选排序入口。 | 原有 `common` CF 中的 HSM 键空间。 |

`CF` 是 RocksDB 内的一组键值集合。HSM 复用 `common`，不新增独立数据库。[](source:cf)

Worker 用本地文件承载 block。一个业务文件可以包含多个块，同一块可以在不同 Worker 上保存副本。[](source:localfile)[](source:naming)

## RocksDB 先理解两类数据：记录与索引 {#two-kinds span=12}

**Record 记录文件事实，候选索引帮助策略找到文件。** 两者都在 `common` CF。[](source:kvbatch)

| 数据 | 回答什么问题 | 值中保存什么 |
| --- | --- | --- |
| 主记录 `Record` | 内容是谁？是否在线？归档在哪里？能否释放？ | 身份、状态、摘要、归档回执和保护事实。 |
| 候选索引 | 先检查哪个状态、哪个较冷文件？ | 指向 Record 的 inode，不复制整份属性。 |

共同前缀写作 `\x03hsm:`。`0x03` 是真实字节，后面接 UTF-8 文本；HSM 值采用 JSON 编码。[](source:kvbatch)

**主记录：按 inode 查完整状态。**

```text title="主记录的键"
common CF
key = \x03hsm:record:1021
```

**候选索引：按状态和访问时间排序。**

```text title="候选索引的键和值"
common CF
key   = \x03hsm:candidate:o:00000001792485229722:1021
value = "1021"
```

| 键中的一段 | 含义 |
| --- | --- |
| `o` | Online；`d` 表示 Dual，`f` 表示 Offline。 |
| `00000001792485229722` | 访问时间 `1792485229722` 毫秒，左补零到 20 位。 |
| `1021` | inode，用于回查 `record:1021`。 |

时间补齐位数后，字节序与时间先后保持一致。同一状态内可以先检查较冷文件。索引负责排序，策略仍会检查文件条件。[](source:kvbatch)[](source:scans)

## 新文件写完后，Record 里有什么？ {#record-fields span=12}

先看新文件最相关的字段。其他保护和恢复字段留待后续动作更新。[](source:ordinary)[](source:model)

| 字段 | 用途 | 新文件关闭后的值 |
| --- | --- | --- |
| `state.data` | 绑定 storage、namespace、inode 和内容 generation。 | 指向原 inode 与当前内容版本。 |
| `state.residency` | 表示在线与归档的关系。 | `Online`。 |
| `state.length` | 供归档校验和大小条件使用。 | 等于文件逻辑长度。 |
| `state.digest`、`sealed` | 保存摘要和摘要确认状态。 | `unsealed`、`sealed=false`。 |
| `state.anchors` | 保存已提交归档的恢复依据。 | 空数组。 |
| `ordinary`、`native_blocks`、`writing` | 标明普通文件、原生块布局、是否仍在写。 | `true`、`true`、`false`。 |
| `version`、`state.revision` | 识别变化后的源观察，拒绝过期动作。 | 随对应更新推进。 |
| 路径、时间、owner/group | 供策略检查路径、冷热和归属。 | 从原生属性维护。 |

被策略选中后，文件才在保护下计算完整摘要。覆盖或截断会推进 generation，并撤销旧归档依据。[](source:ordinary)

```json title="Record 的关键部分"
{
  "ordinary": true,
  "native_blocks": true,
  "writing": false,
  "sealed": false,
  "state": {
    "data": {
      "entity": {
        "storage": "<storage-id>",
        "namespace": "<incarnation>",
        "entity": "1021"
      },
      "generation": 1
    },
    "residency": "Online",
    "length": 8388625,
    "digest": {"algorithm": "unsealed", "value": []},
    "anchors": []
  }
}
```

这段 JSON 只展开关键字段。`views`、`readers` 保存归档视图和读租约；`pending`、`published` 保存私有恢复布局。`garbage`、`native_garbage` 保存待清理内容。[](source:model)

## 第五步怎样一起维护 Record 和索引？ {#write-batch span=12}

Master 用一个 HSM WriteBatch 更新主记录和派生索引。一次批次完成后，二者对应同一份新状态。[](source:kvbatch)

```text title="5a 和 5b：一次 HSM WriteBatch"
如果旧 Record 存在：
    Delete(旧候选索引键)

Put(record:<inode>, 新 Record JSON)
Put(新候选索引键, inode JSON 字符串)

按需更新 seal:<inode> 和 gc:<inode>
Commit()
```

| 后续变化 | Record 如何变化 | 候选键如何变化 |
| --- | --- | --- |
| 文件被再次读取 | 更新访问时间、租约和源观察。 | 删除旧时间键，写入新时间键。 |
| 归档提交完成 | Online → Dual，登记归档锚点。 | `candidate:o:` → `candidate:d:`。 |
| Release 提交 | Dual → Offline，登记待回收块。 | `candidate:d:` → `candidate:f:`。 |
| Recall 发布 | Offline → Dual，发布恢复布局。 | `candidate:f:` → `candidate:d:`。 |

同批更新保持记录和索引对应；源端版本检查阻止旧动作继续使用过期观察。[](source:kvbatch)[](source:conditional)

@callout level=note title="同批更新的范围"
| 这里指 HSM Record、候选索引和相应派生键。普通 inode 更新与 HSM 更新并非全部在一个 RocksDB batch。
| 正常元数据路径和 HSM 控制通过提交串行边界、journal 与屏障协调。HSM 条件事务还会提交必要 inode、控制记录和原始 outcome。[](source:commit)

## 后续策略怎样从索引找到冷文件？ {#policy-reads span=12}

Release 策略先检查已有归档的 Dual，再检查需要先归档的 Online。每个状态内按访问时间读取。[](source:scans)

| 顺序 | 查询或动作 | 目的 |
| --- | --- | --- |
| 1 读取索引页 | 按 `candidate:d:`，再按 `candidate:o:` 做范围读取。 | 优先处理已有归档的冷文件。 |
| 2 回查 Record | 从索引值取 inode，再 Get 主记录。 | 获得完整状态与保护事实。 |
| 3 检查当前条件 | 解析当前路径，检查大小、年龄、归属和保护。 | 索引负责排序，条件仍需校验。 |
| 4 校验并入队 | 比较策略和源记录版本，保存持久任务。 | 不接纳已经失效的候选观察。 |

每页最多处理 256 项，额外预读 1 项判断下一页。`scan_budget` 限制每轮计入的候选数，回查主记录和路径解析仍有成本。[](source:scans)

`scan:<policy>:<revision>` 保存游标、下次执行时间、已选数量和容量触发状态。分页不是全局快照；移动后的键可能到下一轮才看到。[](source:runstate)

写入是否已纳管，可通过状态接口检查：

```bash title="在已配置节点上查询普通文件"
hsm-curvine --config /etc/curvine-hsm/cluster.toml status /data/a
```

查询返回 `record.state`。刚写完且尚未归档时，查看 `residency` 是否为 `Online`；普通 `close` 不返回这个字段。

## 深入阅读：控制记录与恢复 {#further-details span=12}

这些键用于后续工作流，不会在每次普通写入时全部创建。

| 键后缀 | 何时使用 |
| --- | --- |
| `enroll:<inode>` | namespace 尚未初始化，先登记待纳管文件。 |
| `seal:<inode>` | 摘要校验中断后，继续完成源端校验。 |
| `gc:<inode>` | 释放提交后，继续回收物理数据。 |
| `policy:`、`scan:` | 保存规则与周期扫描进度。 |
| `intent:`、`active:`、`work:` | 保存任务、实体去重和工作入口。 |
| `binding:`、`outcome:` | 绑定操作身份，重查原始效果。 |

RocksDB 是本地投影。当前配置关闭 WAL，跨节点持久性依赖 journal/Raft。保护、归档锚点和 pending 等事实来自提交历史。[](source:cf)[](source:commit)

非 testing 启动先清理 HSM 投影，再按受支持的 journal/checkpoint 流程重建。不能从 Worker 块文件推测归档与保护状态。[](source:boot)[](source:reset)

当前仍有三项规模成本：访问时间更新会迁移索引；部分条件仍需后过滤；启动清理与重放随记录数增长。尚未做百万文件性能验收。

完整字段、精确键表和恢复约束见 `curvine/docs/hsm/rocksdb-records.md`。当前 Recall 使用私有恢复布局，与普通初始块布局分开。
