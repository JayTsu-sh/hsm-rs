# terrasync-spike

M0 验证：确认 [terrasync-rs](https://github.com/JayTsu-sh/terrasync-rs) 的 `storage_v2`
crate 能驱动 hsm-rs 的数据搬运层。

不进主 workspace —— 一次性脚本，验证完即弃，但保留代码作为 `hsm-plugin-terrasync`
的种子参考。

## 跑

```bash
cd spikes/terrasync-spike
cargo run --release
```

需要写 `/tmp/hsm-spike-{src,dst}`，每次运行前清理。

## 验证项 / 发现

| # | 验证项 | 结果 | 备注 |
|---|---|---|---|
| 1 | `create_storage(path)` 工作 | ✓ | local 后端，自动检测类型 |
| 2 | `get_metadata` 返回 `EntryEnum::NAS` | ✓ | size / mtime 等字段齐全 |
| 3 | `StorageEnum::copy_file` 跨 storage 拷贝 | ✓ | 16 MiB 文件 1s 完成 |
| 4 | `compute_hash` BLAKE3 校验一致 | ✓ | 直接落 `trusted.lhsm_hash` |
| 5 | `delete_file` remove 路径 | ✓ | get_metadata 验证消失 |
| 6 | `CancellationToken` + `acquire_bandwidth` chunk 边界取消 | ✓ | 250 ms 内取消，最坏延迟 ≈ 一个 chunk QoS 等待时间 |
| 7 | QoS 带宽限速精度 | ⚠️ | 配 8 MiB/s 实测 15.74 MiB/s，**已知偏差** —— 见下方 |

## ⚠️ 已知问题：QoS 限速不准

实测 `QosManager::try_new(Some("8MiB/s"), 1.0, None)` 后，16 MiB 文件在 1.02 s 完成
（≈ 15.74 MiB/s），约为限速值的 2 倍。

推测原因（见 `terrasync-rs/crates/storage_v2/src/qos.rs:184` `acquire_bandwidth`）：

- `copy_file` 单 chunk 路径走整文件一次 acquire（chunks 较少时令牌桶 burst 占优）
- `until_n_ready(NonZeroU32)` 在分批 acquire 时，每批 ≤ 4096 cells，但批与批之间
  没有强制 sleep，governor 的令牌再生跟不上即时调用频率

这是要给 terrasync-rs 提的第三项上游问题（详见
`docs/upstream/terrasync-issue.md`）。在 `hsm-plugin-terrasync` 里我们的 chunk loop
是 4 MiB 粒度、循环 acquire，应当能一定程度缓解；但限速精度问题需要上游配合修复
才能严格满足 SLA。

## 不验证什么

- S3 / NFS / CIFS 后端 —— 留 M3 集成时再测，需要 minio / NFS server。
- 真 Lustre fd 写入 —— restore 路径在 M2 接 llapi 时验证。
- gRPC 插件协议 —— 与 terrasync 无关，M2 自测。
