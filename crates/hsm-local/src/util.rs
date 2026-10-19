use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use hsm_core::managed::{ContentDigest, DataEndpoint, HsmError, HsmResult};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const ADAPTER: &str = "local-managed-v1";

#[derive(Clone, Serialize, Deserialize)]
pub struct LocalEndpoint {
    pub root: PathBuf,
    pub path: String,
}

pub fn endpoint(root: &Path) -> HsmResult<DataEndpoint> {
    Ok(DataEndpoint {
        adapter: ADAPTER.into(),
        capability: encode(&LocalEndpoint {
            root: root.to_owned(),
            path: "data".into(),
        })?,
    })
}

pub fn decode_endpoint(value: &DataEndpoint) -> HsmResult<LocalEndpoint> {
    if value.adapter != ADAPTER {
        return Err(HsmError::Unsupported(value.adapter.clone()));
    }
    let endpoint: LocalEndpoint = decode(&value.capability)?;
    if !endpoint.root.is_absolute() || endpoint.path != "data" {
        return Err(HsmError::Invalid("invalid local capability".into()));
    }
    Ok(endpoint)
}

pub fn encode<T: Serialize>(value: &T) -> HsmResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| HsmError::Invalid(e.to_string()))
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> HsmResult<T> {
    serde_json::from_slice(bytes).map_err(|e| HsmError::Integrity(e.to_string()))
}

pub fn key<T: Serialize>(value: &T) -> HsmResult<String> {
    Ok(blake3::hash(&encode(value)?).to_hex().to_string())
}

pub fn lock_root(root: &Path) -> HsmResult<(PathBuf, File)> {
    let requested = if root.is_absolute() {
        root.to_owned()
    } else {
        std::env::current_dir()?.join(root)
    };
    let mut missing = Vec::new();
    let mut cursor = requested.as_path();
    while !cursor.exists() {
        missing.push(cursor.to_owned());
        cursor = cursor
            .parent()
            .ok_or_else(|| HsmError::Invalid("root ancestor".into()))?;
    }
    fs::create_dir_all(&requested)?;
    for created in missing.iter().rev() {
        if let Some(parent) = created.parent() {
            sync_dir(parent)?;
        }
    }
    let root = requested.canonicalize()?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(".owner.lock"))?;
    file.try_lock_exclusive()
        .map_err(|e| HsmError::Busy(format!("local Adapter root is already owned: {e}")))?;
    Ok((root, file))
}

pub fn transfer_lock(root: &Path) -> HsmResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(".transfer.lock"))?;
    file.try_lock_exclusive()
        .map_err(|e| HsmError::Busy(format!("local data session active: {e}")))?;
    Ok(file)
}

pub fn sync_dir(path: &Path) -> HsmResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

pub fn new_layout(parent: &Path) -> HsmResult<PathBuf> {
    let path = parent.join(uuid::Uuid::new_v4().to_string());
    fs::create_dir(&path)?;
    sync_dir(parent)?;
    Ok(path)
}

pub fn digest_file(path: &Path) -> HsmResult<(u64, ContentDigest)> {
    let mut file = File::open(path)?;
    let mut hash = blake3::Hasher::new();
    let mut size = 0u64;
    let mut buffer = vec![0; 128 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        size += count as u64;
    }
    Ok((
        size,
        ContentDigest {
            algorithm: "blake3".into(),
            value: hash.finalize().as_bytes().to_vec(),
        },
    ))
}

/// Atomically replaces native metadata, then syncs the containing directory.
/// A failure after rename is Unknown; callers must poison/reopen their cache.
pub fn write_json<T: Serialize>(path: &Path, value: &T) -> HsmResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| HsmError::Invalid("metadata parent".into()))?;
    let temp = parent.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(&encode(value)?)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    sync_dir(parent)
        .map_err(|e| HsmError::Unknown(format!("metadata renamed but directory sync failed: {e}")))
}

/// Publish a create-only receipt from an already synced temp inode.
/// `hard_link` is atomic and fails if a different writer won the final identity.
pub fn publish_receipt<T: Serialize>(path: &Path, value: &T) -> HsmResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| HsmError::Invalid("receipt parent".into()))?;
    let temp = parent.join(format!(".receipt-{}", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(&encode(value)?)?;
    file.sync_all()?;
    fs::hard_link(&temp, path)?;
    sync_dir(parent).map_err(|e| {
        HsmError::Unknown(format!("receipt published but directory sync failed: {e}"))
    })?;
    let _ = fs::remove_file(temp);
    Ok(())
}
