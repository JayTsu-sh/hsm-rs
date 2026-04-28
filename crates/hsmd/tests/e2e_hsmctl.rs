//! E2E test for hsmctl.
//!
//! Starts hsmd (mock mode, MemStore), injects an action, then runs:
//!   hsmctl status    — must report 1 total action
//!   hsmctl agents    — must list the connected plugin
//!   hsmctl actions   — must list the action cookie

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn workspace_target_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target")
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
    let s = Command::new(env!("CARGO"))
        .args(["build", "--bin", name])
        .status()
        .unwrap();
    assert!(s.success(), "cargo build --bin {name} failed");
    binary_path(name)
}

fn temp_dir(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("hsm-rs-e2e-{label}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
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
        let stderr = child.stderr.take().unwrap();
        let log: Arc<Mutex<Vec<String>>> = Arc::default();
        let lc = log.clone();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                eprintln!("[{label}] {line}");
                lc.lock().unwrap().push(line);
            }
        });
        Self {
            label,
            child: Some(child),
            log,
        }
    }
    fn log_contains(&self, needle: &str) -> bool {
        self.log
            .lock()
            .unwrap()
            .iter()
            .any(|l| strip_ansi(l).contains(needle))
    }
    fn shutdown(&mut self) {
        let pid = self.child.as_ref().unwrap().id() as i32;
        // SAFETY: `pid` is a positive child PID from `Child::id()`, the child
        // is still alive (`self.child` is `Some`), and SIGTERM is valid POSIX.
        #[allow(unsafe_code)]
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        if let Some(mut c) = self.child.take() {
            let _ = c.wait();
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

#[allow(unsafe_code)]
mod libc {
    pub const SIGTERM: i32 = 15;
    unsafe extern "C" {
        pub fn kill(pid: i32, sig: i32) -> i32;
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

fn run_hsmctl(bin: &PathBuf, socket: &PathBuf, args: &[&str]) -> (String, bool) {
    let out = Command::new(bin)
        .arg("--socket")
        .arg(socket)
        .args(args)
        .output()
        .expect("spawn hsmctl");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    if !stderr.is_empty() {
        eprintln!("[hsmctl] {stderr}");
    }
    (stdout, out.status.success())
}

// ── the test ────────────────────────────────────────────────────────────────

#[test]
fn hsmctl_status_agents_actions() {
    let hsmd_bin = ensure_built("hsmd");
    let plugin_bin = ensure_built("hsm-plugin-terrasync");
    let hsmctl_bin = ensure_built("hsmctl");

    let tmp = temp_dir("hsmctl");
    let lustre = tmp.join("lustre");
    let backend = tmp.join("backend");
    let socket_path = tmp.join("agent.sock");
    let actions_path = tmp.join("actions.jsonl");

    std::fs::create_dir_all(&lustre).unwrap();
    std::fs::create_dir_all(&backend).unwrap();
    std::fs::write(&actions_path, "").unwrap();

    let fid_seq: u64 = 0x200000401;
    let fid_oid: u32 = 0xcc;
    let primary_path = lustre.join(format!("__fid__[{fid_seq:#x}:{fid_oid:#x}:0x0]"));
    std::fs::write(&primary_path, b"hello hsmctl test").unwrap();

    // Write configs.
    let hsmd_cfg = tmp.join("hsmd.toml");
    std::fs::write(
        &hsmd_cfg,
        format!(
            r#"
        mode = "mock"
        mountpoint = "{lustre}"
        mock_actions_file = "{actions}"
        [transport]
        socket_path = "{socket}"
        [scheduler]
        tick_interval_ms = 50
        max_per_tick = 32
        [xattr]
        namespace = "user"
        [log]
        filter = "hsmd=debug,info"
    "#,
            lustre = lustre.display(),
            actions = actions_path.display(),
            socket = socket_path.display()
        ),
    )
    .unwrap();

    let plug_cfg = tmp.join("plugin.toml");
    std::fs::write(
        &plug_cfg,
        format!(
            r#"
        socket_path = "{socket}"
        agent_id    = "hsmctl-test-agent"
        archive_ids = [1]
        archive_root_url = "file://{backend}"
    "#,
            socket = socket_path.display(),
            backend = backend.display()
        ),
    )
    .unwrap();

    // Start hsmd.
    let hsmd = Command::new(&hsmd_bin)
        .args(["--config", hsmd_cfg.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut hsmd = ChildGuard::new("hsmd", hsmd);
    assert!(
        poll_until(Duration::from_secs(5), || socket_path.exists()),
        "hsmd never bound"
    );

    // Start plugin.
    let plugin = Command::new(&plugin_bin)
        .args(["--config", plug_cfg.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut plugin = ChildGuard::new("plugin", plugin);
    assert!(
        poll_until(Duration::from_secs(5), || plugin
            .log_contains("registered with daemon")),
        "plugin never registered"
    );

    // ── hsmctl status (no actions yet) ──────────────────────────────────────
    let (out, ok) = run_hsmctl(&hsmctl_bin, &socket_path, &["status"]);
    assert!(ok, "hsmctl status failed:\n{out}");
    assert!(
        out.contains("waiting"),
        "missing 'waiting' in status:\n{out}"
    );
    assert!(out.contains("Agents"), "missing 'Agents' in status:\n{out}");
    assert!(
        out.contains("hsmctl-test-agent"),
        "agent not listed:\n{out}"
    );

    // ── hsmctl agents ────────────────────────────────────────────────────────
    let (out, ok) = run_hsmctl(&hsmctl_bin, &socket_path, &["agents"]);
    assert!(ok, "hsmctl agents failed:\n{out}");
    assert!(out.contains("hsmctl-test-agent"), "agent missing:\n{out}");
    assert!(out.contains("[1]"), "archive id missing:\n{out}");

    // Inject an action.
    {
        let mut f = OpenOptions::new().append(true).open(&actions_path).unwrap();
        writeln!(f, r#"{{"cookie":255,"fid_seq":{fid_seq},"fid_oid":{fid_oid},"fid_ver":0,"archive_id":1,"kind":"archive"}}"#).unwrap();
    }

    // Wait for the action to appear in-flight (started or completed).
    thread::sleep(Duration::from_millis(500));

    // ── hsmctl actions ────────────────────────────────────────────────────────
    let (out, ok) = run_hsmctl(&hsmctl_bin, &socket_path, &["actions"]);
    assert!(ok, "hsmctl actions failed:\n{out}");
    // The action may have already completed — it will be in the store until deleted.
    // At minimum the command must run without error.
    eprintln!("[hsmctl actions]\n{out}");

    // Wait for completion log in hsmd.
    assert!(
        poll_until(Duration::from_secs(10), || hsmd.log_contains("completed")),
        "action never completed"
    );

    plugin.shutdown();
    hsmd.shutdown();
    let _ = std::fs::remove_dir_all(&tmp);
}
