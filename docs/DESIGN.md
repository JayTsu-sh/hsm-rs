# hsm-rs 系统设计

> 版本：v0.1（2026-04-23）
> 状态：草案，作为 M1 之前的工作基准

## 0. TL;DR

用 Rust 重写 Lustre HSM 用户态栈，融合三个参考项目优势：

| 来源 | 借鉴的部分 |
|---|---|
| `lustre-release/lustre/{mdt,utils}` | 与内核 coordinator 的 ABI 契约：KUC 协议、`hsm_action_*` 结构、`llapi_hsm_*` 语义 |
| `lemur` | 进程隔离的插件架构、gRPC 双向流、xattr 元数据持久化、多后端搬运 |
| `coordinatool` | 用户态二级调度（host-affinity / 一致性哈希 / 批处理）、Redis 持久化、grace-period 重连、JSON/TCP 兼容协议 |
| `terrasync-rs`（本仓库依赖） | 多后端搬运引擎（Local/NFS/CIFS/S3）、BLAKE3 校验、QoS 限速 |

**核心定位**：内核 ABI 兼容 + 用户态可控调度 + 多后端搬运（复用 terrasync-rs）+ 可观测可恢复。

## 1. 设计目标

### 1.1 硬约束（不可妥协）

| 约束 | 来源 |
|---|---|
| KUC 协议（`kuc_hdr`, `KUC_TRANSPORT_HSM`, `lustre_kernelcomm`） | 内核 ABI |
| `hsm_action_list` / `hsm_action_item` 二进制布局 | 内核 ABI |
| `LL_IOC_HSM_*` ioctl 编号 / `llapi_hsm_*` 语义 | 内核 ABI |
| Action 状态机 `ARS_WAITING → STARTED → SUCCEED|FAILED|CANCELED` | 内核 coordinator |
| Group lock + cookie + extent 单调推进 | llapi 契约 |

### 1.2 软目标

- **类型安全**：FID / Cookie / ArchiveId / HsmFlags 全部 newtype，杜绝裸 `u64` 串味
- **异步优先**：tokio multi-thread runtime；KUC fd / TCP / Redis 全部 epoll 驱动
- **进程隔离的插件**：沿用 lemur"插件即子进程 + gRPC 双向流"，崩溃不带翻 daemon
- **可选用户态二级调度**：coordinatool 模式作为 feature——关闭则是纯 copytool，开启则 daemon 接管所有 archive_id 再分发给真实 mover
- **Sqlite 默认 + Redis 可选** 的状态持久化，coordinator 重启 in-flight 不丢
- **OpenTelemetry**：metrics / traces / logs 三件套统一出
- **复用 terrasync-rs**：多后端数据路径（POSIX/NFS/CIFS/S3）不重写，统一走 `storage_v2`

### 1.3 非目标

- 不重写 Lustre 内核 coordinator
- 不替换 `liblustreapi`，FFI 包装它（或在 sys crate 提供原生 ioctl 路径，二选一）
- 第一阶段不做 PCC（HS_PCCRW/RO），留接口位
- 不做 daemon 内嵌的 backend 搬运——所有数据路径走子进程插件

## 2. 总体架构

```
                 ┌────────────────────────────────────────────────┐
                 │              Lustre MDT (kernel)                │
                 │   mdt_hsm_cdt — Action LLOG / Cookie / Agents   │
                 └───────────┬─────────────────────┬───────────────┘
                       KUC pipe                ioctl (begin/end/progress)
                             │                     │
        ┌────────────────────▼─────────────────────▼─────────────────────┐
        │                  hsmd  (Rust daemon, 单进程)                    │
        │ ┌──────────────┐ ┌─────────────┐ ┌───────────────┐ ┌─────────┐│
        │ │ kuc_listener │ │  scheduler  │ │ action_store  │ │  redis   ││
        │ │ (epoll)      │ │ (policy)    │ │ (sqlite/mem)  │ │  (opt)   ││
        │ └──────┬───────┘ └──────┬──────┘ └──────┬────────┘ └────┬─────┘│
        │        │                │               │               │      │
        │   ┌────▼────────────────▼───────────────▼───────────────▼────┐ │
        │   │             dispatch core (tokio runtime)                 │ │
        │   └───┬─────────┬────────────────────────────────────┬───────┘ │
        │       │ gRPC    │ gRPC                               │ TCP/JSON│
        └───────┼─────────┼────────────────────────────────────┼─────────┘
                │ UDS     │ UDS                                │
        ┌───────▼─────────┴───┐    ┌───────────────────┐  ┌────▼──────────┐
        │ hsm-plugin-terrasync│    │ hsm-plugin-noop   │  │ external mover│
        │ (storage_v2 backend)│    │ (test only)       │  │ (LD_PRELOAD)  │
        │   ↓ Local/NFS/      │    └───────────────────┘  └───────────────┘
        │   ↓ CIFS/S3         │
        └─────────────────────┘
```

**两种部署形态共用一套二进制**：

- **mode = `agent`**（lemur 形态）：`hsmd` 直接以 copytool 身份注册 archive_id，调度本地插件子进程
- **mode = `coordinator`**（coordinatool 形态）：`hsmd` 注册"门面" archive_id，再用 TCP/JSON 把请求分发给远程 `hsmd-agent` 节点（兼容 coordinatool 协议，C 版 lhsmtool + LD_PRELOAD 可直连）

## 3. Crate / Workspace 划分

```
hsm-rs/
├── Cargo.toml                       # workspace
├── docs/                            # 设计文档（本目录）
├── spikes/                          # 临时验证项目（不入主 workspace）
│   └── terrasync-spike/             # M0 阶段验证 terrasync 可用性
├── crates/
│   ├── lustre-sys/                  # bindgen 生成，对外只暴露 unsafe
│   ├── lustre-hsm-uapi/             # #[repr(C)] 纯 Rust 结构（hsm_action_list 等）
│   ├── lustre-llapi/                # safe 包装（HsmCopytool, HsmActionHandle）
│   ├── hsm-core/                    # Domain 类型（Fid, Cookie, ArchiveId, Action）
│   ├── hsm-proto/                   # tonic gRPC + serde JSON 协议
│   ├── hsm-store/                   # ActionStore trait + memory/sqlite/redis 实现
│   ├── hsm-scheduler/               # 调度策略（FIFO / batch / host-affinity / consistent-hash）
│   ├── hsm-plugin-sdk/              # 插件作者用的 SDK（Mover trait）
│   ├── hsm-credentials/             # 凭据注入（env/file/vault → URL）
│   ├── hsm-keymap/                  # FID → backend object key 映射
│   ├── hsm-metrics/                 # OTel + prometheus exporter
│   ├── hsmd/                        # 主 daemon 二进制
│   ├── hsmctl/                      # CLI（status / lock / drain / requeue）
│   └── plugins/
│       ├── terrasync/               # 通用：用 terrasync-rs::storage_v2 实现 Mover
│       └── noop/                    # 测试参考
└── tests/
    ├── e2e/                         # 起 mock-mdt + plugin 跑端到端
    └── fixtures/
```

### 3.1 切分原则

- `lustre-sys` / `lustre-hsm-uapi` 是唯一能 `unsafe`/`#[repr(C)]` 的地方，外面纯 safe Rust
- `hsm-core` 不依赖 Lustre，能在 macOS 跑单元测试
- `hsm-plugin-sdk` 单独发布，第三方写后端不用拉整个仓库
- **terrasync-rs 只被 `plugins/terrasync` 依赖**，daemon 不沾任何 backend SDK（aws-sdk / nfs-rs / smb-rs）

## 4. 核心领域模型（hsm-core）

```rust
// 强类型 newtype
#[derive(Copy, Clone, Eq, Hash)]
pub struct Fid { pub seq: u64, pub oid: u32, pub ver: u32 }

#[derive(Copy, Clone, Eq, Hash)]
pub struct Cookie(pub u64);                 // hai_cookie

#[derive(Copy, Clone, Eq, Hash)]
pub struct ArchiveId(pub u32);

pub enum ActionKind { Archive, Restore, Remove, Cancel }

pub struct Extent { pub offset: u64, pub length: u64 }

pub struct Action {
    pub cookie: Cookie,
    pub fid: Fid,
    pub dfid: Fid,
    pub archive_id: ArchiveId,
    pub kind: ActionKind,
    pub extent: Extent,
    pub gid: u64,
    pub data: Bytes,                         // hai_data，opaque hint
}

pub enum ArState {                           // 镜像 ARS_*
    Waiting,
    Started { agent: AgentId, since: Instant },
    Succeed { rc: i32 },
    Failed { rc: i32 },
    Canceled,
}

pub struct ActionRecord {
    pub action: Action,
    pub state: ArState,
    pub progress: AtomicExtent,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

// xattr 字段（沿用 lemur 名字以兼容）
const XATTR_UUID: &str = "trusted.lhsm_uuid";
const XATTR_HASH: &str = "trusted.lhsm_hash";
const XATTR_URL:  &str = "trusted.lhsm_url";

pub struct BackendObject {
    pub uuid: String,        // backend 内的 key
    pub hash: [u8; 32],      // BLAKE3
    pub url: Url,            // 完整 URI
}
```

## 5. KUC 与 llapi 边界（lustre-llapi）

**两条路线，编译期选择**：

1. `feature = "ffi-llapi"`（默认）：bindgen 包 `liblustreapi`，调用 `llapi_hsm_copytool_register/recv/begin/end/progress`
2. `feature = "raw-ioctl"`：直接 `ioctl(LL_IOC_HSM_*)` + 手写 KUC 解析（参考 `lustre_kernelcomm.h`）

**安全包装**：

```rust
pub struct HsmCopytool { /* opaque */ }

impl HsmCopytool {
    pub fn register(mnt: &Path, archives: &[ArchiveId]) -> Result<Self>;
    pub fn raw_fd(&self) -> RawFd;            // 给 tokio AsyncFd
    pub async fn recv(&mut self) -> Result<HsmActionList>;
}

// 持有 group lock 的 RAII；end(self) 消费所有权防止重复完成
pub struct HsmActionHandle<'ct> { /* opaque */ }

impl<'ct> HsmActionHandle<'ct> {
    pub fn begin(...) -> Result<Self>;
    pub fn progress(&self, e: Extent, flags: u16) -> Result<()>;
    pub fn end(self, e: Extent, rc: i32) -> Result<()>;
}
```

`HsmActionHandle::end(self)` 取所有权——编译期消除"忘了 end / end 两次"这类经典 C bug。

## 6. Daemon 主循环（hsmd）

```rust
// 单任务 KUC reader（KUC 是单消费者）
async fn main_loop(ct: HsmCopytool, store: Store, sched: Scheduler, dispatcher: Dispatcher) {
    let async_fd = AsyncFd::new(ct.raw_fd())?;
    loop {
        async_fd.readable().await?;
        let hal = ct.recv().await?;
        for hai in hal.items() {
            let action = Action::from(hai);
            store.insert(action.clone()).await?;       // 持久化先于调度
            sched.enqueue(action).await;
        }
    }
}

// 调度任务独立
async fn scheduler_task(sched: Scheduler, dispatcher: Dispatcher, store: Store) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tick.tick().await;
        for assignment in sched.pick_ready() {
            let h = HsmActionHandle::begin(&ct, &assignment.action)?;
            store.transition(assignment.cookie, ArState::Started { agent }).await?;
            dispatcher.dispatch(assignment, h).await;
        }
    }
}
```

**关键点**：

- `store.insert` 在调度前完成 → 重启可恢复
- `HsmActionHandle::begin` 失败 → store 回退到 Waiting
- dispatcher 内部对每个 (agent, action) 起一个 task，进度通过 mpsc channel 汇总到一个进度 task，进度 task 独占 `handle.progress()` 调用

## 7. 调度器（hsm-scheduler）

```rust
pub trait Scheduler: Send + Sync {
    fn enqueue(&self, action: Action);
    fn pick_ready(&self, agents: &AgentRegistry) -> Vec<Assignment>;
    fn on_complete(&self, cookie: Cookie, rc: i32);
    fn on_disconnect(&self, agent: AgentId);
}
```

**内置实现**（叠加链，按优先级匹配）：

| 策略 | 来源 | 配置示例 |
|---|---|---|
| `FifoPerKind` | lustre 默认 | 默认 |
| `HostAffinity` | coordinatool `archive_on_hosts` | `tag=n0 -> [mover0, mover1]` |
| `ConsistentHash` | coordinatool `archive_on_hosts_ch` | `grouping=10 ring=[m0,m1,m2]` |
| `BatchByHint` | coordinatool 批处理槽 | `slices_sec=300 slots_per_client=1` |
| `ArchiveTypeRoute` | 新增 | `restore→fast pool, archive→bulk pool` |

策略链：`Action → 第一个匹配的策略 → Assignment(agent_id)`。配置可热加载（SIGHUP）。

## 8. 状态存储（hsm-store）

```rust
#[async_trait]
pub trait ActionStore: Send + Sync {
    async fn insert(&self, rec: ActionRecord) -> Result<()>;
    async fn transition(&self, c: Cookie, new: ArState) -> Result<()>;
    async fn update_progress(&self, c: Cookie, e: Extent) -> Result<()>;
    async fn delete(&self, c: Cookie) -> Result<()>;
    async fn load_all(&self) -> Result<Vec<ActionRecord>>;
    async fn list_by_agent(&self, a: AgentId) -> Result<Vec<ActionRecord>>;
}
```

| 后端 | 何时用 | 备注 |
|---|---|---|
| `MemStore` | 单元测试 / 不要持久化 | `DashMap` |
| `SqliteStore` | 默认，无外部依赖 | `sqlx`，WAL 模式 |
| `RedisStore` | 兼容 coordinatool 部署 | `redis-rs` async；keys 沿用 `<cookie><dfid>` |
| `LayeredStore` | 生产 | 内存 hot path + Sqlite/Redis WAL |

**恢复流程**：

```
启动 → store.load_all() → 重建内存索引 → 等待 agent EHLO（带 in-flight）
       → 已存在 cookie 视为 Started、未匹配 agent 的 grace_period 内保留
       → 超时未认领 → 回 Waiting，重新调度
```

## 9. 通信协议（hsm-proto）

### 9.1 daemon ↔ 本机插件：gRPC over UDS（lemur 模式）

```proto
service DataMover {
    rpc Register(RegisterReq) returns (RegisterResp);
    rpc GetActions(stream Ack) returns (stream ActionItem);     // 双向流
    rpc StatusStream(stream ActionStatus) returns (Empty);
}

message ActionItem {
    uint64 cookie = 1;
    uint32 archive_id = 2;
    ActionKind kind = 3;
    string primary_path = 4;     // 读路径
    string write_path = 5;       // 写路径（restore，FID open）
    uint64 offset = 6;
    uint64 length = 7;
    bytes data = 8;              // hai_data
    optional BackendObject existing = 9;  // restore 用
}

message ActionStatus {
    uint64 cookie = 1;
    uint64 offset = 2;
    uint64 length = 3;
    bool completed = 4;
    int32 errno = 5;
    optional BackendObject result = 6;
}
```

### 9.2 coordinator ↔ 远程 mover：JSON over TCP（coordinatool 兼容）

为了**直接复用现存 coordinatool 客户端 / LD_PRELOAD shim**，TCP 协议保持 1:1 兼容：`EHLO / RECV / DONE / QUEUE / STATUS / LOCK`，jansson JSON 风格。

Rust 侧用 `serde_json` + `tokio::net::TcpListener`，消息编解码放 `hsm-proto::wire`。

## 10. 数据搬运层：基于 terrasync-rs

### 10.1 为什么用 terrasync-rs

- 多后端抽象 `StorageEnum`（Local / NFS / CIFS / S3）一套接口覆盖
- `read_file(offset, length)` / `write_file` / `delete_file` 已支持 extent 语义
- 内置 BLAKE3 校验（`ConsistencyCheck` trait）→ 直接落 `trusted.lhsm_hash`
- 内置 `QosManager`（governor 令牌桶，带宽 + IOPS 双限速，`ArcSwap` 热更新）
- `unwrap`/`expect` deny lint，质量门槛符合 daemon 嵌入要求
- MIT 协议，本地仓库就在 `/root/rust/github/terrasync-rs`，可 `path =` 依赖

### 10.2 集成层级

**只用 `storage_v2` crate**（不用 `app::sync` 那套树同步），原因：

- HSM 是单文件 + extent 语义，不是树同步
- 需要精确控制 chunk loop 才能上报 byte-level 进度和响应 cancel
- `app::sync` 拖入 db / transport / consumer，依赖太重

### 10.3 `hsm-plugin-terrasync` 形态

```rust
pub struct TerrasyncMover {
    storage: Arc<StorageEnum>,            // terrasync 后端
    keymap: KeyMap,                       // FID → relative path
    chunk_size: u64,                      // 默认 4 MiB
    qos: Arc<QosManager>,
}

#[async_trait]
impl Mover for TerrasyncMover {
    async fn archive(&self, ctx: ActionCtx, mut src: impl AsyncRead + Unpin)
        -> Result<BackendObject>
    {
        let key = self.keymap.encode(&ctx.fid);
        let mut hasher = blake3::Hasher::new();
        let mut offset = ctx.extent.offset;
        let end = offset + ctx.extent.length;

        let mut buf = BytesMut::with_capacity(self.chunk_size as usize);
        while offset < end {
            ctx.cancel_token.check()?;                       // ← cancel 注入点
            let n = read_at(&mut src, &mut buf, self.chunk_size).await?;
            if n == 0 { break; }
            self.qos.acquire_bandwidth(n as u64).await;      // ← 限速
            self.storage.write_chunk(&key, offset, &buf[..n]).await?;
            hasher.update(&buf[..n]);
            offset += n as u64;
            ctx.progress.advance(n as u64).await;            // ← llapi progress 上报
            buf.clear();
        }
        Ok(BackendObject {
            uuid: key.clone(),
            hash: hasher.finalize().into(),
            url: self.storage.url_for(&key),
        })
    }

    async fn restore(&self, ctx: ActionCtx, obj: BackendObject,
                     mut dst: impl AsyncWrite + Unpin) -> Result<()>
    { /* 对称 */ }

    async fn remove(&self, _ctx: ActionCtx, obj: BackendObject) -> Result<()> {
        self.storage.delete_file(&obj.uuid).await
    }
}
```

### 10.4 概念映射表

| HSM 概念 | terrasync 概念 | 落地方式 |
|---|---|---|
| `archive_id` (u32) | 一个 `StorageEnum` + URL 前缀 | 每个 plugin 实例对应一个 archive_id |
| `Action.fid` | object key (`relative_path`) | `hsm-keymap` 默认沿用 lhsmtool_posix V2：`{oid_low16}/{fid_hex}` |
| `BackendObject.uuid` | `relative_path` 字符串 | 写 `trusted.lhsm_uuid` |
| `BackendObject.hash` | BLAKE3 摘要 | 写 `trusted.lhsm_hash` |
| `BackendObject.url` | `storage.url_for(key)` | 写 `trusted.lhsm_url` |
| `Action.extent` | `read_file(offset, length)` 参数 | 直接传 |
| 进度 | 自写 chunk loop + `ProgressReporter` | terrasync broadcast 太粗（entry 级），不用 |
| `HSMA_CANCEL` | `CancelToken::check()` chunk 边界 | terrasync 没有 cancel，自己埋 |
| 限速 | `QosManager` | 每 archive_id 一份，SIGHUP 热更 |
| 校验 | BLAKE3 (`HashCalculator`) | 替代 SHA256，更快 |

### 10.5 必须解决的非典型问题

1. **凭据不能落明文**
   `terrasync` 用 `s3://ak:sk@host/bucket` URL 形式，配置里只能放轮廓（`template = "s3+https://${ENDPOINT}/${BUCKET}"` + `credentials.source = "env|vault|imds"`）。`hsm-credentials` 在 spawn plugin 之前 resolve，把完整 URL 通过 **stdin pipe**（不是命令行参数，避免 `ps` 泄露）传给 plugin 进程。日志里的 URL 走 `redact()` 过滤。

2. **进度粒度**
   terrasync 的进度事件是 entry 级，HSM 需要 byte 级。结论：自写 chunk loop，SDK `ProgressReporter` 双阈值节流（5s 或 64 MiB 任一触发一次 `llapi_hsm_action_progress`）。

3. **Cancel**
   terrasync 不提供单 transfer cancel。`ActionCtx::cancel_token` 持有 `CancellationToken`，chunk loop 每次循环开头 `check()`。最坏取消延迟 ≈ 一次 chunk 时间 + QoS 等待。

4. **Multipart 与 extent 的关系**
   S3 multipart 最小 5 MiB（除最后一段）。HSM 大文件整文件 archive 时**不调** terrasync 的 `write_multipart_data`（一次性吞整流，进度黑盒），而是手动分段调 `upload_part`。这需要在 `StorageEnum::S3` 上加细粒度接口——**作为给 terrasync-rs 的上游 PR**。短期 fallback：用 `tokio_util::io::ReaderStream` 包装我们的 chunk loop 喂给 `write_multipart_data`，进度通过包装 reader 拦截（part-level 进度抖动可接受）。

5. **Restore 的写入路径**
   llapi 给的是 data FID 打开的 fd（striped 布局），不能让 terrasync 直接写 Lustre 文件。必须：
   ```
   terrasync.read_file(uuid, offset, length) → Bytes
                 ↓
          自己 pwrite() 到 llapi 给的 dst fd
   ```
   即 restore 的 sink 始终是手写的 fd 写入循环。

6. **跨进程 panic 隔离**
   terrasync 自身代码 deny `unwrap`/`expect`，但传递依赖（`aws-sdk-s3` / `nfs-rs` / `smb-rs`）没此保证。把 plugin 跑在子进程而不是 daemon 内嵌——保留 lemur 式进程隔离。

### 10.6 反向取舍：要不要 daemon 直接 link terrasync？

**不要**。理由：

- daemon 必须 7×24 长稳，依赖越少越好。aws-sdk-s3 / nfs-rs / smb-rs 加起来编译产物 +30 MB，daemon 没需要
- NFS/SMB 客户端有 bug 卡死整个 tokio runtime 时，daemon 跟着挂——违反 lemur 隔离原则
- plugin 进程崩溃 → daemon watchdog 重启 → in-flight 从 store 恢复，故障域清晰

**结论**：daemon 不依赖 terrasync，只有 `hsm-plugin-terrasync` 依赖。

## 11. 插件 SDK（hsm-plugin-sdk）

```rust
#[async_trait]
pub trait Mover: Send + Sync {
    async fn archive(&self, ctx: ActionCtx, src: impl AsyncRead) -> Result<BackendObject>;
    async fn restore(&self, ctx: ActionCtx, obj: BackendObject, dst: impl AsyncWrite) -> Result<()>;
    async fn remove(&self, ctx: ActionCtx, obj: BackendObject) -> Result<()>;
}

// 插件作者只写：
fn main() -> Result<()> {
    hsm_plugin_sdk::run(MyMover::from_env()?)
}
```

`run()` 内部：

- 从 `HSM_AGENT_SOCKET` 环境变量读 daemon UDS
- 注册 → 双向流接 ActionItem → spawn 任务 → `Mover::archive` → 进度通过 `ProgressReporter` 节流上报
- 失败重试由 SDK 统一处理（指数退避，最大次数可配）
- 进程退出码语义化：0=正常、2=配置错、75=临时错（daemon 会重启）

**进程隔离**沿用 lemur：daemon 用 `tokio::process::Command` spawn 插件，watchdog 任务监控并按指数退避重启。

## 12. 配置（hsmd.toml）

```toml
mode = "agent"                               # "agent" | "coordinator"
mountpoint = "/mnt/lustre"
archive_ids = [1, 2]
handler_count = 8
grace_ms = 600_000

[store]
backend = "sqlite"                           # "memory" | "sqlite" | "redis" | "layered"
path = "/var/lib/hsmd/state.db"

[scheduler]
default = "fifo_per_kind"
[[scheduler.rules]]
match = { hai_data_prefix = "tag=" }
strategy = "host_affinity"
hosts = { "tag=n0" = ["mover0", "mover1"] }

[transport.local]
type = "grpc-uds"
socket_dir = "/var/run/hsmd"

[plugins]
dir = "/usr/libexec/hsmd"

[[plugins.instance]]
name = "archive-1"
binary = "hsm-plugin-terrasync"
archive_id = 1
[plugins.instance.config]
template = "s3+https://${AWS_ENDPOINT}/${BUCKET}/${PREFIX}"
credentials.source = "env"
credentials.access_key_var = "S3_AK"
credentials.secret_key_var = "S3_SK"
qos.bandwidth = "200MiB/s"
qos.iops = 5000
chunk_size = "4MiB"
keymap = "fid_v2"

[[plugins.instance]]
name = "archive-2"
binary = "hsm-plugin-terrasync"
archive_id = 2
[plugins.instance.config]
template = "nfs://${NFS_HOST}:2049/export"
keymap = "fid_v2"

[observability]
prometheus_listen = "0.0.0.0:9300"
otlp_endpoint = "http://otel-collector:4317"
log_level = "info"
log_format = "json"
```

**热加载**：SIGHUP → 重读 → 仅 `scheduler.rules`、`plugins.instance` 支持热替换；其余打日志后忽略。

## 13. 可观测性

**Metrics**（Prometheus 命名空间 `hsm_`）：

| 指标 | 类型 | 标签 | 说明 |
|---|---|---|---|
| `hsm_actions_received_total` | counter | kind, archive_id | KUC 收到 |
| `hsm_actions_completed_total` | counter | kind, archive_id, rc | end 调用 |
| `hsm_action_duration_seconds` | histogram | kind, archive_id | begin→end |
| `hsm_action_bytes_total` | counter | kind, archive_id | 实际搬运字节 |
| `hsm_inflight` | gauge | agent | 当前在跑 |
| `hsm_queue_depth` | gauge | kind | 调度等待 |
| `hsm_agent_up` | gauge | agent | 1=connected |
| `hsm_store_lag_seconds` | histogram | op | 持久化延迟 |
| `hsm_qos_bandwidth_mibps` | gauge | archive_id | 来自 terrasync `QosStats` |

**Tracing**：每个 Action 一个 span（trace_id = cookie hash），插件传播 → 全链路。
**Logs**：`tracing` + `tracing-subscriber` JSON 输出，cookie/fid/archive_id 字段，stderr 走 systemd-journald。
**In-band reporting**（沿用 coordinatool 的 `.reporting/<hint>` 文件）作为可选 sink。

## 14. CLI（hsmctl）

```
hsmctl status [--verbose]
hsmctl agents
hsmctl actions [--state waiting|running] [--archive ID]
hsmctl lock | unlock | lock-quit
hsmctl drain
hsmctl requeue < /sys/.../active_requests
hsmctl request {archive|restore|remove|cancel} <path>...
hsmctl xattr show <path>
hsmctl import --archive 1 --src-uri s3://... --dst /mnt/lustre/x
```

底层走 daemon 暴露的同一 TCP/UDS 控制通道（`STATUS`/`LOCK`/`QUEUE` 命令），与 coordinatool-client 行为一致。

## 15. 测试策略

| 层 | 手段 | 覆盖 |
|---|---|---|
| 单元 | `cargo test` | hsm-core / scheduler / store 三件可纯逻辑测 |
| 协议 | golden file | hsm_action_list 二进制解析对照 C 结构 |
| 模拟 | `mock-mdt` 假 KUC | 起本地 pipe 充当 MDT，不依赖 Lustre |
| spike | `spikes/terrasync-spike` | 验证 terrasync API 真的能驱动起来 |
| 集成 | docker compose + redis + minio | daemon + plugin + 模拟搬运 |
| 兼容 | 真 Lustre VM | 跑 `sanity-hsm.sh` 子集（archive/restore/remove/cancel/import）|
| 互通 | 跨实现 | C 版 lhsmtool 通过 TCP 接 Rust coordinator；反向亦然 |
| 故障注入 | `turmoil` / 手动 | redis 挂、agent 挂、KUC 断、磁盘满 |

## 16. 安全 / 可靠性

- **零信任 hai_data**：作为 hint 用，但 path/url 拼接前必须校验，防止越权写出 mountpoint
- **xattr 写入幂等**：先写 xattr 再 `end()`，crash 后 RESTORE 仍能找到对象
- **group lock RAII** + `HsmActionHandle::end(self)` 消费所有权，编译期保证不重复 end
- **进度单调**：`store.update_progress` 拒绝 offset 回退
- **凭据**：S3/对象存储凭据走 `secrecy::Secret<String>`，禁止 `Display`；支持 env / IAM / Vault
- **限速 / 配额**：terrasync `QosManager` 每 archive_id；plugin 侧再用 `tokio::sync::Semaphore` 控并发

## 17. 里程碑

| M | 范围 | 验收 |
|---|---|---|
| **M0** | terrasync API spike（本仓库 `spikes/`） | spike 二进制能跑通 file→file copy + cancel + 进度 |
| **M1** | Skeleton + UAPI | workspace / lustre-sys (bindgen) / hsm-core 类型 / hsm-proto / mock-mdt；`cargo test` 全绿 |
| **M2** | 单 plugin 端到端（file://） | hsmd agent 模式 / SqliteStore / FifoPerKind / `hsm-plugin-terrasync` 用 file:// / hsmctl status；真 Lustre 上跑通 archive→release→restore→remove，sanity-hsm 子集通过 |
| **M3** | 多后端（同一 plugin 切 URL） | 同 plugin 切 S3 / NFS / CIFS 后端，几乎零代码（`hsm-credentials` 是主要工作量），验证 BLAKE3 + QoS |
| **M3.5** | terrasync 上游 PR | 细粒度 multipart 接口、per-transfer CancellationToken；合不上则 fork |
| **M4** | Coordinator 模式 + Redis | TCP/JSON 协议兼容 coordinatool / RedisStore / 调度策略；C 版 lhsmtool 通过 LD_PRELOAD shim 接 Rust coordinator 跑通 |
| **M5** | 生产化 | grace 重连 / 故障注入回归 / hsmctl 全功能 / systemd unit / 文档 / Helm chart；7×24 小时 soak test，OOM/leak 0 缺陷 |

## 18. 关键决策回顾

1. **不重写内核 coordinator**：风险/收益失衡；ABI 兼容才是用户价值
2. **进程隔离插件 vs 动态库**：lemur 实践证明崩溃隔离比性能损失更重要；gRPC IPC 开销在 MB 级搬运里可忽略
3. **协议二选一并存**：本地 gRPC（强类型/流控好），远程 JSON/TCP（兼容 coordinatool 既有客户端，零迁移成本）
4. **Sqlite 默认 + Redis 可选**：避免强依赖，单机也能开箱即用；多 daemon HA 才上 Redis
5. **scheduler trait + 规则链**：coordinatool 的策略其实就是这种叠加链，Rust 用 trait object 表达更清晰
6. **`HsmActionHandle::end(self)`**：用类型系统消除"忘了 end / end 两次"
7. **terrasync-rs 替代自写 backend**：M2~M3 plugin 数据路径代码从 ~3000 行降到 ~800 行，多支持 NFS/CIFS"白送"；只在 plugin 进程内引入，daemon 干净
