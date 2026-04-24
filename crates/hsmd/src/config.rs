//! `hsmd.toml` configuration loader.
//!
//! Minimal in M2d.3 — covers what the binary actually drives. Ranges
//! / defaults map 1:1 to [`crate::DaemonConfig`] for runtime use.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use crate::DaemonConfig;

/// Top-level config struct read from `hsmd.toml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HsmdConfig {
    /// Recv source mode. `mock` runs without a Lustre kernel module
    /// (the binary periodically scans `mock_actions_file` if provided).
    /// `live` will use `LiveCopytool` once M2d.2c lands; the binary
    /// rejects this with a clear error today.
    #[serde(default = "default_mode")]
    pub mode: Mode,

    /// Lustre mount point (used to resolve FIDs to filesystem paths
    /// in dispatched actions). Stub-prefixed in M2d, real resolution
    /// in M2d.2c.
    #[serde(default = "default_mountpoint")]
    pub mountpoint: PathBuf,

    /// Optional JSONL file of actions to feed into mock mode. Each
    /// line is one [`MockAction`]; the binary polls the file every
    /// 200ms and enqueues new lines. Useful for local dev /
    /// integration tests without a Lustre cluster.
    #[serde(default)]
    pub mock_actions_file: Option<PathBuf>,

    /// Transport block.
    #[serde(default)]
    pub transport: Transport,

    /// Scheduler tick + dispatch limits.
    #[serde(default)]
    pub scheduler: SchedulerCfg,

    /// Logging block.
    #[serde(default)]
    pub log: LogCfg,

    /// xattr namespace for daemon-managed `lhsm_*` attrs. Defaults to
    /// `trusted` (production Lustre); set to `user` for unprivileged
    /// dev / CI runs on ext4 / xfs / tmpfs.
    #[serde(default)]
    pub xattr: XattrCfg,
}

/// xattr namespace selector — wraps [`crate::xattr_store::XattrNamespace`].
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum XattrNs {
    /// `trusted.lhsm_*` (production Lustre, requires `CAP_SYS_ADMIN`).
    #[default]
    Trusted,
    /// `user.lhsm_*` (dev / CI without root).
    User,
}

impl From<XattrNs> for crate::xattr_store::XattrNamespace {
    fn from(value: XattrNs) -> Self {
        match value {
            XattrNs::Trusted => crate::xattr_store::XattrNamespace::Trusted,
            XattrNs::User => crate::xattr_store::XattrNamespace::User,
        }
    }
}

/// Top-level config block grouping xattr knobs.
#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct XattrCfg {
    /// Namespace for daemon-managed xattrs.
    #[serde(default)]
    pub namespace: XattrNs,
}

/// Recv source mode.
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// In-memory mock; no Lustre kernel module needed.
    Mock,
    /// Real Lustre via liblustreapi. M2d.2c.
    Live,
}

fn default_mode() -> Mode {
    Mode::Mock
}

fn default_mountpoint() -> PathBuf {
    PathBuf::from("/mnt/lustre")
}

/// Transport configuration. UDS for local plugins; the M4
/// coordinatool-compatible TCP/JSON layer is separate.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transport {
    /// Path of the Unix Domain Socket the daemon binds to.
    /// Plugins connect here via `HSM_AGENT_SOCKET`.
    pub socket_path: PathBuf,
}

impl Default for Transport {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from("/var/run/hsmd/agent.sock"),
        }
    }
}

/// Scheduler tick + dispatch tunables — surface of [`DaemonConfig`].
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerCfg {
    /// Tick interval in milliseconds.
    pub tick_interval_ms: u64,
    /// Max dispatched assignments per tick.
    pub max_per_tick: usize,
}

impl Default for SchedulerCfg {
    fn default() -> Self {
        Self {
            tick_interval_ms: 50,
            max_per_tick: 32,
        }
    }
}

/// Logging knobs.
#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LogCfg {
    /// `RUST_LOG`-style env filter. `None` = inherit from environment
    /// (or default to `info`).
    pub filter: Option<String>,
    /// `json` produces structured logs for ingestion; `pretty` is
    /// human-readable. Default `pretty`.
    #[serde(default = "default_log_format")]
    pub format: LogFormat,
}

/// Log output format selector.
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Coloured, human-readable output suitable for development.
    #[default]
    Pretty,
    /// One JSON object per line — production-friendly.
    Json,
}

fn default_log_format() -> LogFormat {
    LogFormat::Pretty
}

/// One pre-canned action consumed by mock mode's action file watcher.
/// Mirror of [`lustre_llapi::ReceivedAction`] with simpler types so the
/// JSONL surface stays human-friendly.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MockAction {
    /// Cookie (decimal or 0x… hex string).
    pub cookie: u64,
    /// FID seq.
    pub fid_seq: u64,
    /// FID oid.
    pub fid_oid: u32,
    /// FID ver. Defaults to 0.
    #[serde(default)]
    pub fid_ver: u32,
    /// Archive id.
    pub archive_id: u32,
    /// `archive | restore | remove | cancel`.
    pub kind: String,
    /// Bytes the (mock) plugin should pretend to transfer. Defaults
    /// to whole-file sentinel.
    #[serde(default)]
    pub length: Option<u64>,
}

/// Errors loading or validating a config.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// IO failure reading the config file.
    #[error("read config {path}: {source}")]
    Read {
        /// Config file path.
        path: PathBuf,
        /// Underlying IO error.
        #[source]
        source: std::io::Error,
    },
    /// TOML parse failure.
    #[error("parse config {path}: {source}")]
    Parse {
        /// Config file path.
        path: PathBuf,
        /// Underlying parse error.
        #[source]
        source: toml::de::Error,
    },
}

impl HsmdConfig {
    /// Load + parse a TOML config from `path`.
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Read {
            path: path.into(),
            source: e,
        })?;
        toml::from_str(&text).map_err(|e| ConfigError::Parse {
            path: path.into(),
            source: e,
        })
    }

    /// Bridge to the runtime [`DaemonConfig`].
    pub fn to_daemon_config(&self) -> DaemonConfig {
        DaemonConfig {
            tick_interval: Duration::from_millis(self.scheduler.tick_interval_ms),
            max_per_tick: self.scheduler.max_per_tick,
            mountpoint: self.mountpoint.clone(),
            xattr_namespace: Some(self.xattr.namespace.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config_with_defaults() {
        let toml = r#"
            [transport]
            socket_path = "/tmp/hsmd.sock"
        "#;
        let cfg: HsmdConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.mode, Mode::Mock);
        assert_eq!(cfg.mountpoint, PathBuf::from("/mnt/lustre"));
        assert_eq!(cfg.transport.socket_path, PathBuf::from("/tmp/hsmd.sock"));
        assert_eq!(cfg.scheduler.tick_interval_ms, 50);
        assert_eq!(cfg.scheduler.max_per_tick, 32);
        assert_eq!(cfg.log.format, LogFormat::Pretty);
    }

    #[test]
    fn parses_full_config() {
        let toml = r#"
            mode = "live"
            mountpoint = "/srv/lustre"

            [transport]
            socket_path = "/run/hsmd/agent.sock"

            [scheduler]
            tick_interval_ms = 100
            max_per_tick = 64

            [log]
            filter = "hsmd=debug,hsm_plugin_sdk=info"
            format = "json"
        "#;
        let cfg: HsmdConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.mode, Mode::Live);
        assert_eq!(cfg.mountpoint, PathBuf::from("/srv/lustre"));
        assert_eq!(cfg.scheduler.tick_interval_ms, 100);
        assert_eq!(cfg.scheduler.max_per_tick, 64);
        assert_eq!(cfg.log.filter.as_deref(), Some("hsmd=debug,hsm_plugin_sdk=info"));
        assert_eq!(cfg.log.format, LogFormat::Json);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let toml = r#"
            mode = "mock"
            mistyped_key = "oops"

            [transport]
            socket_path = "/tmp/x.sock"
        "#;
        let res: Result<HsmdConfig, _> = toml::from_str(toml);
        assert!(res.is_err());
    }

    #[test]
    fn mock_action_jsonl_round_trip() {
        let line = r#"{"cookie":171,"fid_seq":8589934593,"fid_oid":18,"archive_id":1,"kind":"archive"}"#;
        let a: MockAction = serde_json::from_str(line).unwrap();
        assert_eq!(a.cookie, 171);
        assert_eq!(a.fid_seq, 0x2_0000_0001);
        assert_eq!(a.fid_oid, 18);
        assert_eq!(a.fid_ver, 0); // default
        assert_eq!(a.kind, "archive");
        assert!(a.length.is_none());
    }
}
