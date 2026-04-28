//! M2e subprocess e2e: real `hsmd` + real `hsm-plugin-terrasync` move
//! actual bytes from a "lustre" tempdir to an "archive" tempdir.
//!
//! Pipeline:
//!   1. Build (idempotently) both bins.
//!   2. Spawn `hsmd` with mode = mock + a JSONL `mock_actions_file`,
//!      mountpoint = lustre tempdir.
//!   3. Pre-create `<lustre>/__fid__[<fid>]` with deterministic
//!      content. (The daemon's M2d primary_path stub is
//!      `<mount>/__fid__<fid_display>` which we shadow with the
//!      same naming convention.)
//!   4. Spawn `hsm-plugin-terrasync` pointing at the same UDS, with
//!      archive_root = backend tempdir.
//!   5. Append an archive action to the JSONL.
//!   6. Poll daemon stderr until `hsmd.status … completed` for the
//!      cookie shows up.
//!   7. Assert `<backend>/1/<fid>` exists and matches the source
//!      bytes.
//!   8. SIGTERM both — assert both exit 0.

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ---------- helpers ---------------------------------------------------------

fn workspace_target_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().join("target")
}

fn binary_path(name: &str) -> PathBuf {
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

    fn shutdown(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let pid = self.pid() as i32;
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

#[allow(unsafe_code)]
mod libc {
    pub const SIGTERM: i32 = 15;
    unsafe extern "C" {
        pub fn kill(pid: i32, sig: i32) -> i32;
    }
}

// ---------- the test --------------------------------------------------------

#[test]
fn subprocess_archive_moves_bytes_via_terrasync() {
    let hsmd_bin = ensure_built("hsmd");
    let plugin_bin = ensure_built("hsm-plugin-terrasync");

    let tmp = temp_dir("terra-archive");
    let lustre = tmp.join("lustre");
    let backend = tmp.join("backend");
    let socket_path = tmp.join("agent.sock");
    let actions_path = tmp.join("actions.jsonl");
    let hsmd_cfg = tmp.join("hsmd.toml");
    let plug_cfg = tmp.join("plugin.toml");

    std::fs::create_dir_all(&lustre).unwrap();
    std::fs::create_dir_all(&backend).unwrap();
    std::fs::write(&actions_path, "").unwrap();

    // Pre-create the "lustre" file the daemon will resolve to. The
    // daemon's M2d primary_path stub is `<mount>/__fid__<fid_display>`
    // — we mirror that naming exactly.
    let fid_seq: u64 = 0x200000401;
    let fid_oid: u32 = 0x12;
    let fid_ver: u32 = 0;
    let fid_display = format!("[{fid_seq:#x}:{fid_oid:#x}:{fid_ver:#x}]");
    let primary_path = lustre.join(format!("__fid__{fid_display}"));
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&primary_path, &payload).unwrap();

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
            socket = socket_path.display(),
        ),
    )
    .unwrap();

    std::fs::write(
        &plug_cfg,
        format!(
            r#"
                socket_path = "{socket}"
                agent_id = "terra-subproc-1"
                archive_ids = [1]
                archive_root_url = "file://{backend}"
                log_filter = "info,hsm.plugin.terrasync=debug"
            "#,
            socket = socket_path.display(),
            backend = backend.display(),
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

    assert!(
        poll_until(Duration::from_secs(5), || socket_path.exists()),
        "hsmd never bound socket; logs:\n{}",
        hsmd.snapshot_log().join("\n"),
    );

    // --- spawn plugin -------------------------------------------------------
    let plugin = Command::new(&plugin_bin)
        .args(["--config", plug_cfg.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn plugin");
    let mut plugin = ChildGuard::new("hsm-plugin-terrasync", plugin);

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

    // --- inject archive action ---------------------------------------------
    let cookie = 555u64;
    let mut f = OpenOptions::new()
        .append(true)
        .open(&actions_path)
        .expect("open actions file");
    let line = format!(
        r#"{{"cookie":{cookie},"fid_seq":{fid_seq},"fid_oid":{fid_oid},"fid_ver":{fid_ver},"archive_id":1,"kind":"archive","length":{len}}}"#,
        len = payload.len()
    );
    writeln!(f, "{line}").unwrap();
    drop(f);

    // --- wait for completion -----------------------------------------------
    let cookie_marker = format!("cookie={cookie:#x}");
    assert!(
        poll_until(Duration::from_secs(15), || {
            hsmd.snapshot_log().iter().map(|l| strip_ansi(l)).any(|l| {
                l.contains("hsmd.status") && l.contains("completed") && l.contains(&cookie_marker)
            })
        }),
        "no completion log for cookie {cookie} ({cookie_marker}); hsmd logs:\n{}\nplugin logs:\n{}",
        hsmd.snapshot_log().join("\n"),
        plugin.snapshot_log().join("\n"),
    );

    // --- assert backend object content ------------------------------------
    let fid_uuid = format!("{fid_seq:#x}:{fid_oid:#x}:{fid_ver:#x}");
    let backend_path = backend.join("1").join(&fid_uuid);
    assert!(
        backend_path.exists(),
        "backend object missing at {}; hsmd logs:\n{}\nplugin logs:\n{}",
        backend_path.display(),
        hsmd.snapshot_log().join("\n"),
        plugin.snapshot_log().join("\n"),
    );
    let bytes = std::fs::read(&backend_path).expect("read backend object");
    assert_eq!(bytes, payload, "backend bytes != source bytes");

    // --- shutdown ----------------------------------------------------------
    let plug_status = plugin.shutdown(Duration::from_secs(5));
    assert!(plug_status.success(), "plugin exit {plug_status:?}");
    let hsmd_status = hsmd.shutdown(Duration::from_secs(5));
    assert!(hsmd_status.success(), "hsmd exit {hsmd_status:?}");
    assert!(
        !socket_path.exists(),
        "hsmd left socket behind at {}",
        socket_path.display()
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
