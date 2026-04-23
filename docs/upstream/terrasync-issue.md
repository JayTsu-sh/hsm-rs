# Upstream issue draft for terrasync-rs

> 给 https://github.com/JayTsu-sh/terrasync-rs 提的合并改进建议。
> 这份是给 maintainer 的"meta-issue"，列三项改进；落实施时建议拆成 3 个独立 issue + 3 个独立 PR。

---

## Title

`storage_v2: support fine-grained, cancellable, rate-accurate transfers for embedding in long-running daemons`

## Context

We are building **hsm-rs**（Rust 重写的 Lustre HSM 用户态栈），并把 terrasync-rs 的 `storage_v2` 当成多后端搬运引擎。整体设计参见 hsm-rs `docs/DESIGN.md` 第 10 节。我们已用 `spikes/terrasync-spike` 验证了 `create_storage` / `copy_file` / `compute_hash` / `delete_file` 这四个核心 API 能驱动起来，BLAKE3 校验、`bytes_counter` 实时进度都符合预期。

但要真正跑在 7×24 的 daemon 里，并满足 Lustre HSM 的 SLA，还差三块上游能力。下面分别说。

---

## Proposal 1 — Per-transfer `CancellationToken` in `copy_file`

### Why

Lustre HSM 的 `HSMA_CANCEL` 要求 copytool 在收到 cancel 后 **尽快** 终止当前 in-flight transfer，并通过 `llapi_hsm_action_end(rc=ECANCELED)` 上报。当前 `StorageEnum::copy_file` 没有任何取消入口——一旦开始就只能等它跑完（或者依赖底层 reqwest/aws-sdk 的连接超时）。这在大文件场景下会让取消延迟达到分钟级，违反 HSM 协议。

### Proposed API

```rust
// crates/storage_v2/src/storage_enum.rs
impl StorageEnum {
    pub async fn copy_file(
        from: &StorageEnum,
        to: &StorageEnum,
        entry: &EntryEnum,
        qos: Option<QosManager>,
        enable_integrity_check: bool,
        is_source_reserved: bool,
        bytes_counter: Option<Arc<AtomicU64>>,
        cancel: Option<tokio_util::sync::CancellationToken>,  // ← NEW
    ) -> Result<()>;
}
```

实现侧只需在每个 chunk 边界（已有的 chunk loop 内）`token.is_cancelled()` 时返回 `StorageError::Cancelled`。S3 multipart 路径在每个 part 完成后查询。

### Backwards compat

新增参数。如果不想破坏现有调用方，先加 `copy_file_ext(...)` 带 cancel，旧接口 `copy_file(...)` 内部传 `None`。

---

## Proposal 2 — Expose fine-grained S3 multipart primitives

### Why

HSM 大文件 archive（>1 GiB）需要：

- 精确的 byte-level 进度回报（每 N MiB 上报一次 `llapi_hsm_action_progress`）
- chunk 边界响应 cancel
- 失败时只重传单个 part，不是整个对象

当前 `S3Storage::write_multipart_data` 是黑盒一次性吞整流，进度只能从外层 `bytes_counter` 看到大颗粒，且无法在 part 失败时只重试该 part。

### Proposed API

```rust
// crates/storage_v2/src/s3.rs (re-export through StorageEnum)
pub struct MultipartUpload<'s> { /* opaque, holds upload_id */ }

impl S3Storage {
    pub async fn multipart_begin(
        &self,
        relative_path: &Path,
        content_type: Option<&str>,
        tags: Option<Vec<Tag>>,
    ) -> Result<MultipartUpload<'_>>;
}

impl<'s> MultipartUpload<'s> {
    /// part_number: 1-based, S3 要求每段 ≥ 5 MiB（最后一段除外）
    pub async fn upload_part(&mut self, part_number: i32, data: Bytes) -> Result<()>;

    pub async fn complete(self) -> Result<()>;

    pub async fn abort(self) -> Result<()>;   // cancel 时调用
}
```

实现层这只是把 aws-sdk-s3 的 `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload` / `AbortMultipartUpload` 透出，逻辑都在 SDK 里——~150 行代码。

### Use case in hsm-plugin-terrasync

```rust
let mut up = s3.multipart_begin(&key, None, None).await?;
let mut part_no = 1;
let mut buf = BytesMut::with_capacity(5 * MiB);
while let Some(chunk) = read_next_chunk().await? {
    cancel.check()?;                            // Proposal 1
    buf.extend_from_slice(&chunk);
    if buf.len() >= 5 * MiB || is_last_chunk {
        qos.acquire_bandwidth(buf.len() as u64).await;
        up.upload_part(part_no, buf.split().freeze()).await?;
        progress.advance(buf_len_just_uploaded).await;
        part_no += 1;
    }
}
up.complete().await?;
```

---

## Proposal 3 — `QosManager::acquire_bandwidth` accuracy on bursty workloads

### Repro

我们的 spike（[`spikes/terrasync-spike/src/main.rs`](https://github.com/.../hsm-rs/spikes/terrasync-spike)）配置：

```rust
let qos = QosManager::try_new(Some("8MiB/s"), 1.0, None)?;
StorageEnum::copy_file(&src, &dst, &entry, Some(qos), true, true, Some(counter)).await?;
```

源/目标都是 local，16 MiB 文件。期望 ≈ 2 秒完成（8 MiB/s），实测 **1.02 秒**（15.74 MiB/s），约为限速值 2 倍。

### 可能原因

`crates/storage_v2/src/qos.rs:184 acquire_bandwidth`：

```rust
let mut remaining = cells_u32;
while remaining > 0 {
    let batch = remaining.min(4096);
    if let Some(n) = NonZeroU32::new(batch) {
        let _ = limiter.until_n_ready(n).await;
    }
    remaining -= batch;
}
```

当 `cells > 4096`（即单次 acquire > 4 MiB）时被分批，但批与批之间没有 sleep。governor `until_n_ready` 是基于令牌桶，burst 容量（peak_rate=1.0 时 = base_quota）一旦释放就立即可申请下一批，导致瞬时速率远超目标。

### 建议

- 选项 A：让 `acquire_bandwidth` 在分批时显式按 quota 计算 sleep 间隔，确保平均速率收敛
- 选项 B：暴露 `QosManager::acquire_bandwidth_strict(bytes)`，文档明确 "每调用一次至少消耗 bytes / quota 秒"
- 选项 C：调用方手动把大块切小（≤ 4096 cells），文档里写清楚

我们倾向选项 A——daemon 嵌入场景对限速精度敏感（避免压垮共享带宽）。

### Test case

```rust
#[tokio::test]
async fn qos_8mibps_caps_8mib_in_one_second() {
    let qos = QosManager::try_new(Some("8MiB/s"), 1.0, None).unwrap();
    let start = Instant::now();
    qos.acquire_bandwidth(8 * 1024 * 1024).await;
    qos.acquire_bandwidth(8 * 1024 * 1024).await;
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(900),
        "expected ≥ 0.9s for 16 MiB at 8 MiB/s, got {elapsed:?}");
}
```

---

## What we'll do regardless

无论上游接不接，hsm-rs 都会在 `crates/plugins/terrasync` 里保留 fork 路径（`Cargo.toml` 指 fork 分支）作为兜底。但接进 mainline 对生态有更广收益（任何要嵌入 terrasync 的 daemon 都会撞上这三件事）。

愿意：

- 拆三个 issue 提
- 各发一个 PR（`hsm-rs` 这边出工时）
- 加配套的集成测试（spike 已经是雏形）

只想先听一下大方向的意见——这三项改动的口径您是否认可？是否有更喜欢的 API 形状？
