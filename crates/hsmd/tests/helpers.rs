//! Shared helpers for subprocess end-to-end tests.
//!
//! Include in each test file with:
//! ```ignore
//! #[path = "helpers.rs"]
//! mod helpers;
//! use helpers::*;
//! ```

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub fn workspace_target_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().join("target")
}

pub fn binary_path(name: &str) -> PathBuf {
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

pub fn ensure_built(name: &str) -> PathBuf {
    let p = binary_path(name);
    if p.exists() {
        return p;
    }
    let status = Command::new(env!("CARGO"))
        .args(["build", "--bin", name])
        .status()
        .expect("spawn cargo build");
    assert!(status.success(), "cargo build --bin {name} failed");
    let p = binary_path(name);
    assert!(p.exists(), "binary {name} not produced at {}", p.display());
    p
}

pub fn temp_dir(label: &str) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("hsm-rs-e2e-{label}-{pid}-{nanos}"));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

pub fn strip_ansi(s: &str) -> String {
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

pub struct ChildGuard {
    pub label: &'static str,
    child: Option<Child>,
    log: Arc<Mutex<Vec<String>>>,
}

impl ChildGuard {
    pub fn new(label: &'static str, mut child: Child) -> Self {
        let stderr = child.stderr.take().expect("piped stderr");
        let log: Arc<Mutex<Vec<String>>> = Arc::default();
        let log_clone = log.clone();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
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

    pub fn pid(&self) -> u32 {
        self.child.as_ref().expect("child alive").id()
    }

    pub fn snapshot_log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    pub fn log_contains(&self, needle: &str) -> bool {
        self.snapshot_log()
            .iter()
            .any(|l| strip_ansi(l).contains(needle))
    }

    /// Send SIGKILL — simulates an unclean crash.
    pub fn kill_now(&mut self) {
        if let Some(ref mut c) = self.child {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.child = None;
    }

    /// Send SIGTERM and wait up to `timeout`; SIGKILL if it doesn't exit.
    pub fn shutdown(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let pid = self.pid() as i32;
        // SAFETY: pid is a live child PID from Child::id(); SIGTERM is valid POSIX.
        #[allow(unsafe_code)]
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let mut child = self.child.take().unwrap();
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(s)) => return s,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    return child.wait().expect("wait after kill");
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

pub fn poll_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
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
pub mod libc {
    pub const SIGTERM: i32 = 15;
    unsafe extern "C" {
        pub fn kill(pid: i32, sig: i32) -> i32;
    }
}
