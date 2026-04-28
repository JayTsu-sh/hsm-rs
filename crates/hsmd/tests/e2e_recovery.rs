//! End-to-end recovery test.
//!
//! Scenario: daemon crashes (SIGKILL) after receiving an action but before a
//! plugin has connected. On restart the daemon reads the persisted Waiting
//! record from SqliteStore, re-queues it via `recover()`, and when the
//! plugin finally connects the action completes normally.
//!
//! This exercises the full crash-restart-recover path without any mocking
//! of the store internals.

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ── helpers (mirror of e2e_terrasync_restore) ──────────────────────────────

fn workspace_target_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().join("target")
}

fn binary_path(name: &str) -> PathBuf {
    if let Ok(p) = std::env::var(format!("CARGO_BIN_EXE_{name}")) {
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
    let status = Command::new(env!("CARGO"))
        .args(["build", "--bin", name])
        .status()
        .expect("spawn cargo build");
    assert!(status.success(), "cargo build --bin {name} failed");
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

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1b && i + 1 < bytes.len() && bytes[i + 1] == b'[' {
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
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                eprintln!("[{label}] {line}");
                log_clone.lock().unwrap().push(line);
            }
        });
        Self {
            label,
            child: Some(child),
            log,
        }
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    fn snapshot_log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    fn log_contains(&self, needle: &str) -> bool {
        self.snapshot_log()
            .iter()
            .any(|l| strip_ansi(l).contains(needle))
    }

    /// Send SIGKILL — simulates an unclean crash.
    fn kill_now(&mut self) {
        if let Some(ref mut c) = self.child {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.child = None;
    }

    fn shutdown(&mut self, timeout: Duration) {
        let pid = self.pid() as i32;
        // SAFETY: `pid` is a positive child PID obtained from `Child::id()`.
        // `self.child` is `Some` here (we checked `pid()` above) so the
        // child process is still alive. SIGTERM is a well-defined POSIX signal.
        #[allow(unsafe_code)]
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let mut child = self.child.take().unwrap();
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return;
                }
                Ok(None) => thread::sleep(Duration::from_millis(50)),
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

fn poll_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

#[allow(unsafe_code)]
mod libc {
    pub const SIGTERM: i32 = 15;
    unsafe extern "C" {
        pub fn kill(pid: i32, sig: i32) -> i32;
    }
}

// ── the test ───────────────────────────────────────────────────────────────

/// Two-phase recovery E2E:
///
/// **Phase 1** — daemon receives an action, stores it (Waiting), is killed
/// before any plugin connects. The SQLite DB now has the record.
///
/// **Phase 2** — daemon restarts with the same DB. `recover()` re-queues the
/// Waiting record. The plugin connects, the action is dispatched and
/// completes. Backend object must be present on disk.
#[test]
fn daemon_restart_recovers_waiting_action() {
    let hsmd_bin = ensure_built("hsmd");
    let plugin_bin = ensure_built("hsm-plugin-terrasync");

    let tmp = temp_dir("recovery");
    let lustre = tmp.join("lustre");
    let backend = tmp.join("backend");
    let socket_path = tmp.join("agent.sock");
    let store_db = tmp.join("state.db");
    let actions_path = tmp.join("actions.jsonl");

    std::fs::create_dir_all(&lustre).unwrap();
    std::fs::create_dir_all(&backend).unwrap();
    std::fs::write(&actions_path, "").unwrap();

    // Deterministic FID used across both phases.
    let fid_seq: u64 = 0x200000401;
    let fid_oid: u32 = 0xb1;
    let fid_ver: u32 = 0;
    let primary_path = lustre.join(format!("__fid__[{fid_seq:#x}:{fid_oid:#x}:{fid_ver:#x}]"));
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&primary_path, &payload).unwrap();

    // Shared hsmd config used in both phases.
    let hsmd_cfg = tmp.join("hsmd.toml");
    std::fs::write(
        &hsmd_cfg,
        format!(
            r#"
                mode = "mock"
                mountpoint = "{lustre}"
                mock_actions_file = "{actions}"
                store_path = "{db}"

                [transport]
                socket_path = "{socket}"

                [scheduler]
                tick_interval_ms = 50
                max_per_tick = 32
                grace_ms = 30000

                [xattr]
                namespace = "user"

                [log]
                filter = "hsmd=debug,info"
            "#,
            lustre = lustre.display(),
            actions = actions_path.display(),
            db = store_db.display(),
            socket = socket_path.display(),
        ),
    )
    .unwrap();

    let plug_cfg = tmp.join("plugin.toml");
    std::fs::write(
        &plug_cfg,
        format!(
            r#"
                socket_path = "{socket}"
                agent_id    = "terra-recovery-1"
                archive_ids = [1]
                archive_root_url = "file://{backend}"
                log_filter = "info,hsm.plugin.terrasync=debug"
            "#,
            socket = socket_path.display(),
            backend = backend.display(),
        ),
    )
    .unwrap();

    // ── Phase 1: inject action, then SIGKILL daemon before plugin connects ──

    let hsmd1 = Command::new(&hsmd_bin)
        .args(["--config", hsmd_cfg.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn hsmd (phase 1)");
    let mut hsmd1 = ChildGuard::new("hsmd-p1", hsmd1);

    assert!(
        poll_until(Duration::from_secs(5), || socket_path.exists()),
        "phase 1: hsmd never bound socket"
    );

    // Inject the archive action.
    {
        let mut f = OpenOptions::new().append(true).open(&actions_path).unwrap();
        writeln!(
            f,
            r#"{{"cookie":1,"fid_seq":{fid_seq},"fid_oid":{fid_oid},"fid_ver":{fid_ver},"archive_id":1,"kind":"archive","length":{len}}}"#,
            len = payload.len()
        )
        .unwrap();
    }

    // Wait until the daemon has persisted the record (log "store insert" or
    // "hsmd.recv" confirms the action went through recv→store).
    assert!(
        poll_until(Duration::from_secs(5), || {
            hsmd1.log_contains("hsmd.recv") || hsmd1.log_contains("store")
        }),
        "phase 1: action never reached recv loop"
    );

    // Give the recv loop one more tick to persist.
    thread::sleep(Duration::from_millis(200));

    // SIGKILL — simulate unclean crash. No plugin was running, so the
    // action stays Waiting in the DB.
    hsmd1.kill_now();

    // Sanity: DB file must exist.
    assert!(store_db.exists(), "sqlite DB missing after phase 1 crash");

    // ── Phase 2: restart daemon (same DB) + start plugin ───────────────────

    // Clean up socket leftover from the killed daemon.
    let _ = std::fs::remove_file(&socket_path);

    let hsmd2 = Command::new(&hsmd_bin)
        .args(["--config", hsmd_cfg.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn hsmd (phase 2)");
    let mut hsmd2 = ChildGuard::new("hsmd-p2", hsmd2);

    assert!(
        poll_until(Duration::from_secs(5), || socket_path.exists()),
        "phase 2: hsmd never bound socket"
    );

    // Verify recovery ran and found the record.
    assert!(
        poll_until(Duration::from_secs(5), || {
            hsmd2.log_contains("recovery complete") || hsmd2.log_contains("requeued")
        }),
        "phase 2: recovery log never appeared;\n{}",
        hsmd2.snapshot_log().join("\n")
    );

    // Now start the plugin — it connects and receives the recovered action.
    let plugin = Command::new(&plugin_bin)
        .args(["--config", plug_cfg.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn plugin (phase 2)");
    let mut plugin = ChildGuard::new("plugin-p2", plugin);

    assert!(
        poll_until(Duration::from_secs(5), || {
            plugin.log_contains("registered with daemon")
        }),
        "phase 2: plugin never registered"
    );

    // Wait for the recovered action to complete.
    // strip_ansi first: structured-log fields like `cookie=0x1` are surrounded
    // by ANSI codes in the raw log line, so raw contains() would miss them.
    let marker = "cookie=0x1";
    assert!(
        poll_until(Duration::from_secs(15), || {
            hsmd2
                .snapshot_log()
                .iter()
                .map(|l| strip_ansi(l))
                .any(|l| l.contains("hsmd.status") && l.contains("completed") && l.contains(marker))
        }),
        "phase 2: action never completed;\nhsmd:\n{}\nplugin:\n{}",
        hsmd2.snapshot_log().join("\n"),
        plugin.snapshot_log().join("\n"),
    );

    // Backend object must exist on disk.
    let backend_obj = backend
        .join("1")
        .join(format!("{fid_seq:#x}:{fid_oid:#x}:{fid_ver:#x}"));
    assert!(
        backend_obj.exists(),
        "backend object missing after recovery: {}",
        backend_obj.display()
    );
    assert_eq!(std::fs::read(&backend_obj).unwrap(), payload);

    // Clean shutdown.
    plugin.shutdown(Duration::from_secs(5));
    hsmd2.shutdown(Duration::from_secs(5));
    let _ = std::fs::remove_dir_all(&tmp);
}
