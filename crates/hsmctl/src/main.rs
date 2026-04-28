//! `hsmctl` — hsm-rs management CLI.
//!
//! Usage:
//!   hsmctl [--socket <path>] [--namespace trusted|user] <command> [args]
//!
//! Commands:
//!   status                        Daemon health: action counts + connected agents
//!   agents                        List connected plugin agents
//!   actions [--state <filter>]    List queued / in-flight actions

//!   xattr show <path>             Display lhsm_* xattrs on a Lustre file
//!   import --uuid <string>         Register a file as archived (write xattrs)

//!          --hash <hex64>
//!          --url <url>
//!          <path>
//!   lock                          (not yet implemented)
//!   drain                         (not yet implemented)
//!   request                       (not yet implemented)
//!   requeue                       (not yet implemented)

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use hsm_proto::v1::hsm_control_client::HsmControlClient;
use hsm_proto::v1::{CtlListActionsRequest, CtlListAgentsRequest, CtlStatusRequest};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::transport::Endpoint;
use tower::service_fn;

// ── arg types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Namespace {
    Trusted,
    User,
}

impl Namespace {
    fn to_xattr_ns(self) -> hsmd::xattr_store::XattrNamespace {
        match self {
            Namespace::Trusted => hsmd::xattr_store::XattrNamespace::Trusted,
            Namespace::User => hsmd::xattr_store::XattrNamespace::User,
        }
    }
}

#[derive(Debug)]
struct ActionsFilter {
    state: String,
    archive: u32,
}

#[derive(Debug)]
struct ImportArgs {
    // archive_id is parsed but not stored in xattrs (BackendObject carries
    // uuid/hash/url; the archive_id is implicit in the uuid path).
    uuid: String,
    hash_hex: String,
    url: String,
    path: PathBuf,
}

#[derive(Debug)]
enum Cmd {
    Status,
    Agents,
    Actions(ActionsFilter),
    XattrShow { path: PathBuf },
    Import(ImportArgs),
    // Stubs — not yet implemented
    Lock,
    Drain,
    Request,
    Requeue,
}

struct Args {
    socket: PathBuf,
    namespace: Namespace,
    cmd: Cmd,
}

// ── arg parsing ────────────────────────────────────────────────────────────

fn usage() -> ! {
    eprintln!(
        "usage: hsmctl [--socket <path>] [--namespace trusted|user] <command> [options]

commands:
  status                               daemon health (action counts + agents)
  agents                               list connected agents
  actions [--state waiting|started]    list actions
          [--archive <id>]
  xattr show <path>                    display lhsm_* xattrs
  import --uuid <s> --hash <hex64> --url <url>   write lhsm_* xattrs (register archived file)
         --hash <hex64> --url <url>
         <path>
  lock | drain | request | requeue     (not yet implemented)

options:
  --socket <path>      daemon UDS socket  [default: /var/run/hsmd/agent.sock]
  --namespace <ns>     xattr namespace: trusted | user  [default: trusted]"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    let mut socket = PathBuf::from("/var/run/hsmd/agent.sock");
    let mut namespace = Namespace::Trusted;

    macro_rules! next_val {
        ($flag:expr) => {{
            i += 1;
            raw.get(i).unwrap_or_else(|| {
                eprintln!("{} requires a value", $flag);
                std::process::exit(2)
            })
        }};
    }

    // Global flags (before the subcommand).
    while let Some(arg) = raw.get(i) {
        match arg.as_str() {
            "--socket" | "-s" => {
                socket = PathBuf::from(next_val!("--socket"));
            }
            "--namespace" | "-n" => {
                namespace = match next_val!("--namespace").as_str() {
                    "trusted" => Namespace::Trusted,
                    "user" => Namespace::User,
                    other => {
                        eprintln!("unknown namespace {other:?}; expected trusted or user");
                        std::process::exit(2);
                    }
                };
            }
            "--help" | "-h" => usage(),
            _ => break,
        }
        i += 1;
    }

    let cmd = match raw.get(i).map(String::as_str) {
        Some("status") => Cmd::Status,
        Some("agents") => Cmd::Agents,
        Some("actions") => {
            i += 1;
            let mut state = String::new();
            let mut archive: u32 = 0;
            while let Some(arg) = raw.get(i) {
                match arg.as_str() {
                    "--state" => {
                        state = next_val!("--state").clone();
                    }
                    "--archive" => {
                        archive = next_val!("--archive").parse().unwrap_or_else(|_| {
                            eprintln!("--archive must be a number");
                            std::process::exit(2);
                        });
                    }
                    _ => break,
                }
                i += 1;
            }
            Cmd::Actions(ActionsFilter { state, archive })
        }
        Some("xattr") => {
            i += 1;
            match raw.get(i).map(String::as_str) {
                Some("show") => {
                    i += 1;
                    let path = raw.get(i).map(PathBuf::from).unwrap_or_else(|| {
                        eprintln!("xattr show requires a <path>");
                        std::process::exit(2);
                    });
                    Cmd::XattrShow { path }
                }
                other => {
                    eprintln!("unknown xattr subcommand {:?}; expected 'show'", other);
                    std::process::exit(2);
                }
            }
        }
        Some("import") => {
            i += 1;
            let mut uuid: Option<String> = None;
            let mut hash_hex: Option<String> = None;
            let mut url: Option<String> = None;
            let mut path: Option<PathBuf> = None;
            while let Some(arg) = raw.get(i) {
                match arg.as_str() {
                    "--uuid" => uuid = Some(next_val!("--uuid").clone()),
                    "--hash" => hash_hex = Some(next_val!("--hash").clone()),
                    "--url" => url = Some(next_val!("--url").clone()),
                    p if !p.starts_with('-') => {
                        path = Some(PathBuf::from(p));
                    }
                    other => {
                        eprintln!("unknown import flag {other:?}");
                        std::process::exit(2);
                    }
                }
                i += 1;
            }
            Cmd::Import(ImportArgs {
                uuid: uuid.unwrap_or_else(|| {
                    eprintln!("import requires --uuid");
                    std::process::exit(2);
                }),
                hash_hex: hash_hex.unwrap_or_else(|| {
                    eprintln!("import requires --hash");
                    std::process::exit(2);
                }),
                url: url.unwrap_or_else(|| {
                    eprintln!("import requires --url");
                    std::process::exit(2);
                }),
                path: path.unwrap_or_else(|| {
                    eprintln!("import requires <path>");
                    std::process::exit(2);
                }),
            })
        }
        Some("lock") => Cmd::Lock,
        Some("drain") => Cmd::Drain,
        Some("request") => Cmd::Request,
        Some("requeue") => Cmd::Requeue,
        None => usage(),
        Some(other) => {
            eprintln!("unknown command {other:?}");
            std::process::exit(2);
        }
    };

    Args {
        socket,
        namespace,
        cmd,
    }
}

// ── gRPC connection ────────────────────────────────────────────────────────

async fn connect(socket: &Path) -> Result<HsmControlClient<tonic::transport::Channel>, String> {
    let path = socket.to_path_buf();
    let channel = Endpoint::try_from("http://hsmd.uds.local")
        .expect("static endpoint")
        .connect_with_connector(service_fn(move |_| {
            let p = path.clone();
            async move {
                let stream = UnixStream::connect(&p).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|e| format!("connect to {}: {e}", socket.display()))?;
    Ok(HsmControlClient::new(channel))
}

// ── command implementations ────────────────────────────────────────────────

async fn cmd_status(socket: &Path) -> Result<(), String> {
    let mut client = connect(socket).await?;
    let resp = client
        .status(CtlStatusRequest {})
        .await
        .map_err(|e| format!("RPC status: {e}"))?
        .into_inner();

    println!("Actions");
    println!("  waiting : {}", resp.actions_waiting);
    println!("  started : {}", resp.actions_started);
    println!("  total   : {}", resp.actions_total);
    println!();
    println!("Agents ({})", resp.agents.len());
    for a in &resp.agents {
        let mut ids: Vec<u32> = a.archive_ids.clone();
        ids.sort_unstable();
        println!("  {}  archives={:?}", a.agent_id, ids);
    }
    Ok(())
}

async fn cmd_agents(socket: &Path) -> Result<(), String> {
    let mut client = connect(socket).await?;
    let resp = client
        .list_agents(CtlListAgentsRequest {})
        .await
        .map_err(|e| format!("RPC list_agents: {e}"))?
        .into_inner();

    if resp.agents.is_empty() {
        println!("No agents connected.");
        return Ok(());
    }
    println!("{:<30} {:<20} {}", "AGENT", "ARCHIVES", "SINCE");
    for a in &resp.agents {
        let mut ids: Vec<u32> = a.archive_ids.clone();
        ids.sort_unstable();
        let since = if a.registered_at_secs > 0 {
            format_unix_secs(a.registered_at_secs)
        } else {
            "-".to_string()
        };
        println!("{:<30} {:<20} {}", a.agent_id, format!("{ids:?}"), since);
    }
    Ok(())
}

async fn cmd_actions(socket: &Path, filter: &ActionsFilter) -> Result<(), String> {
    let mut client = connect(socket).await?;
    let resp = client
        .list_actions(CtlListActionsRequest {
            state_filter: filter.state.clone(),
            archive_id: filter.archive,
        })
        .await
        .map_err(|e| format!("RPC list_actions: {e}"))?
        .into_inner();

    if resp.actions.is_empty() {
        println!("No actions.");
        return Ok(());
    }
    println!(
        "{:<18} {:<30} {:>3} {:<8} {:<9} {:<25} {}",
        "COOKIE", "FID", "AID", "KIND", "STATE", "AGENT", "PROGRESS"
    );
    for a in &resp.actions {
        println!(
            "{:<18} {:<30} {:>3} {:<8} {:<9} {:<25} {} bytes",
            format!("{:#x}", a.cookie),
            a.fid,
            a.archive_id,
            a.kind,
            a.state,
            if a.agent_id.is_empty() {
                "-"
            } else {
                &a.agent_id
            },
            a.progress,
        );
    }
    Ok(())
}

fn cmd_xattr_show(path: &Path, ns: Namespace) -> Result<(), String> {
    match hsmd::xattr_store::read_obj(path, ns.to_xattr_ns()) {
        Ok(Some(obj)) => {
            println!("uuid : {}", obj.uuid);
            println!("hash : {}", obj.hash_hex());
            println!("url  : {}", obj.url);
        }
        Ok(None) => {
            println!("{}: no lhsm_* xattrs (file not archived)", path.display());
        }
        Err(e) => return Err(format!("xattr read {}: {e}", path.display())),
    }
    Ok(())
}

fn cmd_import(args: &ImportArgs, ns: Namespace) -> Result<(), String> {
    // Decode hex hash → [u8; 32].
    let hash =
        decode_hash32(&args.hash_hex).map_err(|e| format!("--hash {}: {e}", args.hash_hex))?;

    let obj = hsm_core::BackendObject {
        uuid: args.uuid.clone(),
        hash,
        url: args.url.clone(),
    };
    hsmd::xattr_store::write_obj(&args.path, ns.to_xattr_ns(), &obj)
        .map_err(|e| format!("xattr write {}: {e}", args.path.display()))?;

    println!("imported: {}", args.path.display());
    println!("  uuid={}", obj.uuid);
    println!("  hash={}", obj.hash_hex());
    println!("  url={}", obj.url);
    Ok(())
}

fn decode_hash32(s: &str) -> Result<[u8; 32], String> {
    hex::decode(s)
        .map_err(|e| format!("invalid hex: {e}"))?
        .try_into()
        .map_err(|_| {
            format!(
                "expected 32 bytes (64 hex chars), got {} bytes",
                s.len() / 2
            )
        })
}

fn format_unix_secs(secs: i64) -> String {
    // Simple RFC-3339-ish display without pulling in chrono.
    // Saturate negative values (pre-epoch or unset) to 0 rather than wrapping.
    let d = std::time::Duration::from_secs(u64::try_from(secs).unwrap_or(0));
    let t = std::time::UNIX_EPOCH + d;
    match t.elapsed() {
        Ok(ago) => {
            let s = ago.as_secs();
            if s < 60 {
                format!("{s}s ago")
            } else if s < 3600 {
                format!("{}m ago", s / 60)
            } else if s < 86400 {
                format!("{}h ago", s / 3600)
            } else {
                format!("{}d ago", s / 86400)
            }
        }
        Err(_) => format!("unix={secs}"),
    }
}

// ── main ───────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = parse_args();

    let result = match &args.cmd {
        Cmd::Status => cmd_status(&args.socket).await,
        Cmd::Agents => cmd_agents(&args.socket).await,
        Cmd::Actions(f) => cmd_actions(&args.socket, f).await,
        Cmd::XattrShow { path } => cmd_xattr_show(path, args.namespace),
        Cmd::Import(a) => cmd_import(a, args.namespace),
        Cmd::Lock | Cmd::Drain | Cmd::Request | Cmd::Requeue => {
            eprintln!("command not yet implemented");
            return ExitCode::from(2);
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
