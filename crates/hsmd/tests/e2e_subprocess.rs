//! M2d.3 subprocess e2e: real `hsmd` + real `hsm-plugin-noop` binaries
//! talking over a real Unix Domain Socket.
//!
//! Pipeline:
//!   1. Build (idempotently) both bins.
//!   2. Spawn `hsmd` with mode = mock + a JSONL `mock_actions_file`.
//!   3. Spawn `hsm-plugin-noop` pointing at the same UDS.
//!   4. Append N actions to the JSONL.
//!   5. Poll daemon stderr until it emits N `hsmd.status … completed`
//!      log lines.
//!   6. SIGTERM both — assert clean exit and that the plugin logged
//!      `invocations = N`.
//!
//! This is the M2d.3 acceptance gate: end-to-end binary integration
//! without any in-process glue, validating the CLI / config / signal
//! plumbing alongside the gRPC + scheduler path.

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ---------- helpers ---------------------------------------------------------

fn workspace_target_dir() -> PathBuf {
    // crates/hsmd/Cargo.toml sits two levels under the workspace root.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().join("target")
}

fn binary_path(name: &str) -> PathBuf {
    // Prefer CARGO_BIN_EXE_<name> when it's set (works for hsmd because the
    // test is in the hsmd crate). For the plugin we have to look it up by
    // path under target/{profile}/.
    let key = format!("CARGO_BIN_EXE_{name}");
    if let Ok(p) = std::env::var(&key) {
        return PathBuf::from(p);
    }
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    workspace_target_dir().join(profile).join(name)
}

fn ensure_built(name: &str) -> PathBuf {
    let p = binary_path(name);
    if p.exists() {
        return p;
    }
    // Fall back: ask cargo to build it. Slow on first run but
    // self-healing for `cargo test -p hsmd` invocations that didn't
    // pre-build the plugin crate.
    let status = Command::new(env!("CARGO"))
        .args(["build", "--bin", name])
        .status()
        .expect("spawn cargo build");
    assert!(status.success(), "cargo build --bin {name} failed");
    let p = binary_path(name);
    assert!(p.exists(), "binary {name} not produced at {}", p.display());
    p
}

fn temp_dir(label: &str) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("hsm-rs-e2e-{label}-{pid}-{nanos}"));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Cleans up a child + its temp dir on drop.
struct ChildGuard {
    label: &'static str,
    child: Option<Child>,
    log: Arc<Mutex<Vec<String>>>,
}

impl ChildGuard {
    fn new(label: &'static str, mut child: Child) -> Self {
        let stderr = child.stderr.take().expect("piped stderr");
        let log: Arc<Mutex<Vec<String>>> = Arc::default();
        let log_clone = log.clone();
        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().map_while(Result::ok) {
                eprintln!("[{label}] {line}");
                log_clone.lock().unwrap().push(line);
            }
        });
        ChildGuard {
            label,
            child: Some(child),
            log,
        }
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().expect("alive").id()
    }

    fn snapshot_log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// Sends SIGTERM and waits up to `timeout`; falls back to SIGKILL.
    fn shutdown(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let pid = self.pid() as i32;
        // SAFETY: pid is from a child we own; libc::kill is signal-safe.
        #[allow(unsafe_code)]
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let mut child = self.child.take().unwrap();
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(s)) => return s,
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        return child.wait().expect("wait after kill");
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("[{}] try_wait: {e}", self.label),
            }
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Strip ANSI SGR escapes (`ESC[…m`) so log-line matching survives the
/// pretty colour codes tracing-subscriber injects by default.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1b && i + 1 < bytes.len() && bytes[i + 1] == b'[' {
            // ESC[ … <final byte in 0x40..=0x7e>
            i += 2;
            while i < bytes.len() {
                let b = bytes[i];
                i += 1;
                if (0x40..=0x7e).contains(&b) {
                    break;
                }
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

fn poll_until<F: FnMut() -> bool>(timeout: Duration, mut f: F) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

// ---------- the test --------------------------------------------------------

#[test]
fn subprocess_archive_three_actions_end_to_end() {
    let hsmd_bin = ensure_built("hsmd");
    let plugin_bin = ensure_built("hsm-plugin-noop");

    let tmp = temp_dir("subproc-archive");
    let socket_path = tmp.join("agent.sock");
    let actions_path = tmp.join("actions.jsonl");
    let hsmd_cfg = tmp.join("hsmd.toml");
    let plug_cfg = tmp.join("plugin.toml");

    // Pre-create the (empty) JSONL so the daemon's watcher doesn't
    // bounce on ENOENT for the first tick.
    std::fs::write(&actions_path, "").unwrap();

    std::fs::write(
        &hsmd_cfg,
        format!(
            r#"
                mode = "mock"
                mountpoint = "/mnt/lustre"
                mock_actions_file = "{actions}"

                [transport]
                socket_path = "{socket}"

                [scheduler]
                tick_interval_ms = 50
                max_per_tick = 32

                [log]
                filter = "hsmd=debug,info"
            "#,
            actions = actions_path.display(),
            socket = socket_path.display(),
        ),
    )
    .unwrap();

    std::fs::write(
        &plug_cfg,
        format!(
            r#"
                socket_path = "{socket}"
                agent_id = "noop-subproc-1"
                archive_ids = [1]
                chunk_size_mib = 1
                whole_file_mib = 1
                chunk_delay_ms = 0
                log_filter = "info"
            "#,
            socket = socket_path.display(),
        ),
    )
    .unwrap();

    // --- spawn hsmd ---------------------------------------------------------
    let hsmd = Command::new(&hsmd_bin)
        .args(["--config", hsmd_cfg.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn hsmd");
    let mut hsmd = ChildGuard::new("hsmd", hsmd);

    // Wait for the UDS socket to appear (= server bound).
    assert!(
        poll_until(Duration::from_secs(5), || socket_path.exists()),
        "hsmd never bound socket at {}; logs:\n{}",
        socket_path.display(),
        hsmd.snapshot_log().join("\n"),
    );

    // --- spawn plugin -------------------------------------------------------
    let plugin = Command::new(&plugin_bin)
        .args(["--config", plug_cfg.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn plugin");
    let mut plugin = ChildGuard::new("hsm-plugin-noop", plugin);

    // Wait for the plugin to register.
    assert!(
        poll_until(Duration::from_secs(5), || {
            plugin
                .snapshot_log()
                .iter()
                .any(|l| strip_ansi(l).contains("registered with daemon"))
        }),
        "plugin never registered; plugin logs:\n{}\nhsmd logs:\n{}",
        plugin.snapshot_log().join("\n"),
        hsmd.snapshot_log().join("\n"),
    );

    // --- append actions -----------------------------------------------------
    let n = 3usize;
    let mut f = OpenOptions::new()
        .append(true)
        .open(&actions_path)
        .expect("open actions file");
    for i in 0..n {
        let cookie = 100 + i as u64;
        let oid = 0x1000 + i as u32;
        let line = format!(
            r#"{{"cookie":{cookie},"fid_seq":2,"fid_oid":{oid},"archive_id":1,"kind":"archive","length":4096}}"#
        );
        writeln!(f, "{line}").unwrap();
    }
    drop(f);

    // --- wait for hsmd to log N completions --------------------------------
    let completed = poll_until(Duration::from_secs(15), || {
        hsmd.snapshot_log()
            .iter()
            .map(|l| strip_ansi(l))
            .filter(|l| l.contains("hsmd.status") && l.contains("completed"))
            .count()
            >= n
    });
    assert!(
        completed,
        "hsmd did not log {n} completions in time; hsmd logs:\n{}\nplugin logs:\n{}",
        hsmd.snapshot_log().join("\n"),
        plugin.snapshot_log().join("\n"),
    );

    // --- shutdown plugin and inspect its terminal log ----------------------
    let plug_status = plugin.shutdown(Duration::from_secs(5));
    assert!(
        plug_status.success(),
        "plugin exited non-zero: {plug_status:?}"
    );
    let plug_log = plugin.snapshot_log();
    let needle_a = format!("invocations={n}");
    let needle_b = format!("invocations = {n}");
    assert!(
        plug_log
            .iter()
            .map(|l| strip_ansi(l))
            .any(|l| l.contains(&needle_a) || l.contains(&needle_b)),
        "plugin did not report invocations={n}; logs:\n{}",
        plug_log.join("\n"),
    );

    // --- shutdown hsmd ------------------------------------------------------
    let hsmd_status = hsmd.shutdown(Duration::from_secs(5));
    assert!(
        hsmd_status.success(),
        "hsmd exited non-zero: {hsmd_status:?}"
    );

    // Best-effort cleanup: the socket should be gone (hsmd removes it on
    // exit), and the temp dir gets garbage collected by the OS / next run.
    assert!(
        !socket_path.exists(),
        "hsmd left socket behind at {}",
        socket_path.display()
    );

    // Tear down the temp dir last so any failure path keeps logs/config
    // around for inspection.
    let _ = std::fs::remove_dir_all(&tmp);
}

// libc minimal shim — we only need SIGTERM + kill.
#[allow(unsafe_code)]
mod libc {
    pub const SIGTERM: i32 = 15;
    unsafe extern "C" {
        pub fn kill(pid: i32, sig: i32) -> i32;
    }
}
