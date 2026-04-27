//! Live copytool — talks to the kernel via liblustreapi.
//!
//! Linux only. Outside this crate, this is the only place hsm-rs touches
//! `lustre-sys` (raw FFI). Everything here is `unsafe { … }`-disciplined:
//!
//! - Each FFI call is wrapped in a single `unsafe` block with a comment
//!   stating the precondition we're upholding.
//! - All raw pointers from llapi are stored in `NonNull<T>` and freed in
//!   `Drop` so neither the copytool nor in-flight action handles leak on
//!   panic.
//! - The variable-length `hsm_action_list` walker uses
//!   `read_unaligned` exclusively — the parent struct is
//!   `__attribute__((packed))`, so taking `&` references to fields would be
//!   UB even if we never deref them.

// FFI module — every operation in here is by definition unsafe.
#![allow(unsafe_code)]

use std::collections::HashMap;
use std::ffi::CString;
use std::os::fd::RawFd;
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};

use bytes::Bytes;
use hsm_core::{ArchiveId, Cookie, Extent};
use lustre_hsm_uapi::conv;
use lustre_sys as sys;
use tracing::{debug, warn};

use crate::action::{ActionHandle, EndStatus, HasCookieLifecycle, ReceivedAction};
use crate::error::{HsmError, Result};

/// Owns a `struct hsm_copytool_private *` from liblustreapi plus a map of
/// in-flight `struct hsm_copyaction_private *` per cookie.
pub struct LiveCopytool {
    raw: NonNull<sys::hsm_copytool_private>,
    /// Mount point we registered on (kept for diagnostics + Drop logging).
    mount: PathBuf,
    /// Active actions: cookie → opaque per-action handle.
    handles: HashMap<Cookie, NonNull<sys::hsm_copyaction_private>>,
}

// liblustreapi state is per-process, not thread-safe in the strict sense,
// but the daemon only ever reads from `recv` on a single tokio task; once
// we hand out an `ActionHandle<'_>`, the borrow checker enforces single-
// threaded access. Marking `Send` lets us hand the LiveCopytool to a
// dedicated tokio task. We do **not** mark `Sync` — concurrent llapi
// calls through the same `hsm_copytool_private` are not safe.
unsafe impl Send for LiveCopytool {}

impl LiveCopytool {
    /// Register as a copytool for `mount`, serving the given archive ids.
    ///
    /// `archives` must not be empty — Lustre treats an empty list as
    /// "all archives", which is rarely what callers want; we surface that
    /// as an explicit error to force a conscious choice.
    pub fn register(mount: &Path, archives: &[ArchiveId]) -> Result<Self> {
        if archives.is_empty() {
            return Err(HsmError::EmptyArchiveList);
        }
        let c_mnt =
            CString::new(mount.as_os_str().as_encoded_bytes()).map_err(|_| HsmError::Register {
                mount: mount.to_path_buf(),
                errno: 22, // EINVAL
            })?;
        // llapi takes `int *` and treats it as an array of archive ids.
        // It expects the platform-native `int` width; cast safely.
        let mut archive_ints: Vec<c_int> = archives.iter().map(|a| a.get() as c_int).collect();
        let mut raw: *mut sys::hsm_copytool_private = ptr::null_mut();

        // SAFETY: `c_mnt` is a valid NUL-terminated C string for the duration
        // of the call. `archive_ints` is a contiguous Vec of c_int valid for
        // the call. `raw` is a writable out-pointer; on success llapi fills
        // it with a heap-allocated handle we then own (freed in Drop).
        let rc = unsafe {
            sys::llapi_hsm_copytool_register(
                &mut raw,
                c_mnt.as_ptr() as *const c_char,
                archive_ints.len() as c_int,
                archive_ints.as_mut_ptr(),
                /* rfd_flags */ 0,
            )
        };
        if rc < 0 {
            return Err(HsmError::Register {
                mount: mount.to_path_buf(),
                errno: -rc,
            });
        }
        let nn = NonNull::new(raw).ok_or(HsmError::Register {
            mount: mount.to_path_buf(),
            errno: 22, // EINVAL: llapi returned 0 but null pointer (shouldn't happen).
        })?;
        debug!(target: "hsm.live", mount = %mount.display(), n_archives = archives.len(), "copytool registered");
        Ok(Self {
            raw: nn,
            mount: mount.to_path_buf(),
            handles: HashMap::new(),
        })
    }

    /// File descriptor the kernel writes new action lists to. Wrap in
    /// `tokio::io::unix::AsyncFd` to drive an async main loop.
    pub fn raw_fd(&self) -> RawFd {
        // SAFETY: `self.raw` is a valid registered copytool handle for the
        // lifetime of `self`. llapi_hsm_copytool_get_fd is documented as
        // never failing for a registered handle — it just reads a struct
        // field — but we still let llapi do it instead of poking internals.
        unsafe { sys::llapi_hsm_copytool_get_fd(self.raw.as_ptr()) }
    }

    /// Block (or return immediately if data is buffered) on the kernel
    /// pipe; return the parsed actions.
    ///
    /// Returns an empty vec if the kernel sent an empty list, which is a
    /// no-op heartbeat from the coordinator (see DESIGN §10 of
    /// `lustre-release` survey).
    pub fn recv(&mut self) -> Result<Vec<ReceivedAction>> {
        let mut hal_ptr: *mut sys::hsm_action_list = ptr::null_mut();
        let mut msgsize: c_int = 0;
        // SAFETY: copytool handle is valid; both out-pointers are writable.
        let rc =
            unsafe { sys::llapi_hsm_copytool_recv(self.raw.as_ptr(), &mut hal_ptr, &mut msgsize) };
        if rc < 0 {
            return Err(HsmError::Recv(-rc));
        }
        let hal = NonNull::new(hal_ptr).ok_or_else(|| HsmError::MalformedActionList {
            reason: "llapi_hsm_copytool_recv returned NULL hsm_action_list".to_string(),
        })?;
        // SAFETY: `hal` points into a kernel-supplied buffer that remains
        // valid until the next `recv` call on the same copytool. We borrow
        // it briefly, copy out the fields we need, and never store the ptr.
        unsafe { parse_action_list(hal, msgsize as usize) }
    }

    /// Begin processing an action — RAII wrapper enforces end-or-abandon.
    ///
    /// Internally allocates a new `hsm_copyaction_private` via llapi and
    /// stores it under `cookie` so progress/end can find it later.
    pub fn begin<'c>(&'c mut self, action: &ReceivedAction) -> Result<ActionHandle<'c, Self>> {
        // We need to round-trip back to a `hsm_action_item` for llapi.
        // Build one on the stack (the trailing `hai_data` is variable, but
        // for `begin` llapi only reads the fixed prefix — it's stored once
        // and progress/end refer to it via `hai_cookie`).
        let mut hai = build_hsm_action_item(action);

        let mut phcp: *mut sys::hsm_copyaction_private = ptr::null_mut();
        // SAFETY: `self.raw` is valid; `&hai` is a valid `hsm_action_item`
        // (we own it on this stack frame); llapi copies what it needs.
        let rc = unsafe {
            sys::llapi_hsm_action_begin(
                &mut phcp,
                self.raw.as_ptr(),
                &mut hai as *mut _ as *const _,
                /* restore_mdt_index */ -1,
                /* restore_open_flags */ 0,
                /* is_error */ false,
            )
        };
        if rc < 0 {
            return Err(HsmError::Begin(-rc));
        }
        let nn = NonNull::new(phcp).ok_or(HsmError::Begin(22))?;
        self.handles.insert(action.cookie, nn);
        Ok(ActionHandle::new(self, action.cookie))
    }

    /// Like [`begin`](Self::begin) but does not return an RAII `ActionHandle`.
    /// The caller takes responsibility for calling [`HasCookieLifecycle::cookie_end`]
    /// (or `cookie_abandon`) later — no Drop-time safety net exists.
    ///
    /// Used by `LiveRecvSource` to register the begin before forwarding
    /// the action to the daemon; completion arrives asynchronously via a
    /// separate channel, at which point `cookie_end` is called directly.
    pub fn begin_manual(&mut self, action: &ReceivedAction) -> Result<()> {
        let mut hai = build_hsm_action_item(action);
        let mut phcp: *mut sys::hsm_copyaction_private = ptr::null_mut();
        let rc = unsafe {
            sys::llapi_hsm_action_begin(
                &mut phcp,
                self.raw.as_ptr(),
                &mut hai as *mut _ as *const _,
                -1,
                0,
                false,
            )
        };
        if rc < 0 {
            return Err(HsmError::Begin(-rc));
        }
        let nn = NonNull::new(phcp).ok_or(HsmError::Begin(22))?;
        self.handles.insert(action.cookie, nn);
        Ok(())
    }

    /// Return the file descriptor associated with an active action handle.
    ///
    /// Used for Restore in live mode: after `begin_manual`, the caller reads
    /// the archived bytes from the backend and writes them to this FD — the
    /// kernel then materialises the OST objects without triggering a second
    /// automatic restore cycle.
    ///
    /// The returned FD is owned by `phcp` inside Lustre and must **not** be
    /// closed by the caller; it becomes invalid after `cookie_end`.
    pub fn get_action_fd(&self, cookie: Cookie) -> Result<std::os::unix::io::RawFd> {
        let h = *self
            .handles
            .get(&cookie)
            .ok_or_else(|| HsmError::UnknownCookie(cookie.get()))?;
        // SAFETY: `h` is a valid per-action handle stored by begin_manual/begin.
        let fd = unsafe { sys::llapi_hsm_action_get_fd(h.as_ptr()) };
        if fd < 0 {
            Err(HsmError::Begin(-fd))
        } else {
            Ok(fd)
        }
    }
}

impl HasCookieLifecycle for LiveCopytool {
    fn cookie_progress(&mut self, cookie: Cookie, e: Extent) -> Result<()> {
        let h = *self
            .handles
            .get(&cookie)
            .ok_or_else(|| HsmError::UnknownCookie(cookie.get()))?;
        let extent = conv::extent_to_sys(e);
        // SAFETY: `h` is the per-cookie handle returned by a successful
        // `llapi_hsm_action_begin` call; still valid because `end` /
        // `abandon` are the only paths that remove it from the map.
        let rc = unsafe {
            sys::llapi_hsm_action_progress(
                h.as_ptr(),
                &extent as *const _,
                /* total */ e.length,
                /* hp_flags */ 0,
            )
        };
        if rc < 0 {
            return Err(HsmError::Progress(-rc));
        }
        Ok(())
    }

    fn cookie_end(&mut self, cookie: Cookie, e: Extent, status: EndStatus) -> Result<()> {
        let mut h = self
            .handles
            .remove(&cookie)
            .ok_or_else(|| HsmError::UnknownCookie(cookie.get()))?;
        let extent = conv::extent_to_sys(e);
        let mut h_ptr = h.as_ptr();
        // SAFETY: `h_ptr` points to a valid copyaction_private; llapi frees
        // it and writes back NULL through the in/out param.
        let rc = unsafe {
            sys::llapi_hsm_action_end(
                &mut h_ptr,
                &extent as *const _,
                /* hp_flags */ 0,
                status.errno(),
            )
        };
        // After the call, llapi has freed the per-action handle either way.
        let _ = &mut h; // silence unused-mut warnings on some toolchains
        if rc < 0 {
            return Err(HsmError::End(-rc));
        }
        Ok(())
    }

    fn cookie_abandon(&mut self, cookie: Cookie) {
        // RAII path: a handle was dropped without `end`. Treat it as a
        // cancelled action so the coordinator re-dispatches.
        if let Some(h) = self.handles.remove(&cookie) {
            let extent = conv::extent_to_sys(Extent::new(0, 0));
            let mut h_ptr = h.as_ptr();
            // SAFETY: see cookie_end. We do *not* surface the rc — abandon
            // is best-effort cleanup on a path where the caller has
            // already given up.
            let rc = unsafe {
                sys::llapi_hsm_action_end(
                    &mut h_ptr,
                    &extent as *const _,
                    0,
                    EndStatus::Cancelled.errno(),
                )
            };
            if rc < 0 {
                warn!(target: "hsm.live", cookie = %cookie, errno = -rc, "abandon-end failed");
            }
        }
    }
}

impl Drop for LiveCopytool {
    fn drop(&mut self) {
        // End any handles still in flight before unregistering, otherwise
        // liblustreapi will leak them (and the kernel will eventually time
        // them out, re-dispatching to another copytool).
        let leftover: Vec<Cookie> = self.handles.keys().copied().collect();
        for c in leftover {
            self.cookie_abandon(c);
        }
        let mut p = self.raw.as_ptr();
        // SAFETY: `p` is the registered copytool handle; llapi frees it
        // and writes NULL back through the in/out param.
        let rc = unsafe { sys::llapi_hsm_copytool_unregister(&mut p) };
        if rc < 0 {
            warn!(target: "hsm.live", mount = %self.mount.display(), errno = -rc, "copytool unregister failed");
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const FSNAME_ALIGN: usize = 8;

const fn align_up(value: usize, to: usize) -> usize {
    (value + to - 1) & !(to - 1)
}

/// Walk a `hsm_action_list` buffer of `total_bytes` and copy out
/// `ReceivedAction`s.
///
/// # Safety
///
/// `hal` must point to a valid, kernel-supplied `hsm_action_list` followed
/// by `hal_count` `hsm_action_item`s, the whole structure occupying at
/// least `total_bytes` from `hal.as_ptr()`.
unsafe fn parse_action_list(
    hal: NonNull<sys::hsm_action_list>,
    total_bytes: usize,
) -> Result<Vec<ReceivedAction>> {
    let base = hal.as_ptr() as *const u8;

    // hsm_action_list is __attribute__((packed)) — read fields unaligned.
    let count_ptr = unsafe { ptr::addr_of!((*hal.as_ptr()).hal_count) };
    let archive_ptr = unsafe { ptr::addr_of!((*hal.as_ptr()).hal_archive_id) };
    let count: u32 = unsafe { count_ptr.read_unaligned() };
    let archive_id = ArchiveId::new(unsafe { archive_ptr.read_unaligned() });

    // hal_fsname is a flexible array member starting after the fixed prefix.
    // bindgen usually emits a zero-sized __IncompleteArrayField<i8>; compute
    // the start offset from the struct layout.
    let fsname_off = std::mem::size_of::<sys::hsm_action_list>();
    if fsname_off > total_bytes {
        return Err(HsmError::MalformedActionList {
            reason: format!("buffer ({total_bytes} B) shorter than fixed hsm_action_list prefix"),
        });
    }
    let fsname_ptr = unsafe { base.add(fsname_off) };
    let (fsname, fsname_len) = unsafe { read_cstr(fsname_ptr, total_bytes - fsname_off) }?;

    let mut cursor = fsname_off + align_up(fsname_len + 1, FSNAME_ALIGN);
    let mut out = Vec::with_capacity(count as usize);

    for i in 0..count {
        if cursor + std::mem::size_of::<sys::hsm_action_item>() > total_bytes {
            return Err(HsmError::MalformedActionList {
                reason: format!("hai #{i} prefix exceeds buffer ({cursor} > {total_bytes})"),
            });
        }
        let hai_ptr = unsafe { base.add(cursor) } as *const sys::hsm_action_item;

        // Every hai field accessed via read_unaligned (parent is packed).
        let hai_len: u32 = unsafe { ptr::addr_of!((*hai_ptr).hai_len).read_unaligned() };
        let hai_action: u32 = unsafe { ptr::addr_of!((*hai_ptr).hai_action).read_unaligned() };
        let hai_fid: sys::lu_fid = unsafe { ptr::addr_of!((*hai_ptr).hai_fid).read_unaligned() };
        let hai_dfid: sys::lu_fid = unsafe { ptr::addr_of!((*hai_ptr).hai_dfid).read_unaligned() };
        let hai_extent: sys::hsm_extent =
            unsafe { ptr::addr_of!((*hai_ptr).hai_extent).read_unaligned() };
        let hai_cookie: u64 = unsafe { ptr::addr_of!((*hai_ptr).hai_cookie).read_unaligned() };
        let hai_gid: u64 = unsafe { ptr::addr_of!((*hai_ptr).hai_gid).read_unaligned() };

        let hai_len_usize = hai_len as usize;
        if hai_len_usize < std::mem::size_of::<sys::hsm_action_item>()
            || cursor + hai_len_usize > total_bytes
        {
            return Err(HsmError::MalformedActionList {
                reason: format!("hai #{i} hai_len={hai_len} invalid"),
            });
        }

        // Copy out the trailing hai_data[] payload.
        let data_off = std::mem::size_of::<sys::hsm_action_item>();
        let data_len = hai_len_usize - data_off;
        let data = if data_len == 0 {
            Bytes::new()
        } else {
            // SAFETY: validated above that cursor + hai_len fits in buffer.
            let data_ptr = unsafe { (hai_ptr as *const u8).add(data_off) };
            let slice = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
            Bytes::copy_from_slice(slice)
        };

        out.push(ReceivedAction {
            cookie: Cookie::new(hai_cookie),
            fid: conv::fid_from_sys(hai_fid),
            dfid: conv::fid_from_sys(hai_dfid),
            archive_id,
            kind: conv::action_kind_from_wire(hai_action)?,
            extent: conv::extent_from_sys(hai_extent),
            gid: hai_gid,
            data,
            fsname: fsname.clone(),
        });

        cursor += align_up(hai_len_usize, FSNAME_ALIGN);
    }

    Ok(out)
}

/// Read a NUL-terminated string from `ptr`, scanning at most `max` bytes.
/// Returns `(string, length excluding NUL)`.
///
/// # Safety
///
/// `ptr` must be valid for reads of at most `max` bytes.
unsafe fn read_cstr(ptr: *const u8, max: usize) -> Result<(String, usize)> {
    for i in 0..max {
        // SAFETY: i < max which is the caller's promised valid extent.
        let b = unsafe { *ptr.add(i) };
        if b == 0 {
            // SAFETY: same — slice is fully within the valid range.
            let bytes = unsafe { std::slice::from_raw_parts(ptr, i) };
            let s = std::str::from_utf8(bytes)
                .map_err(|e| HsmError::MalformedActionList {
                    reason: format!("hal_fsname is not utf-8: {e}"),
                })?
                .to_string();
            return Ok((s, i));
        }
    }
    Err(HsmError::MalformedActionList {
        reason: format!("hal_fsname has no NUL terminator within {max} bytes"),
    })
}

/// Build a `hsm_action_item` from our domain `ReceivedAction` for use with
/// `llapi_hsm_action_begin`. Trailing `hai_data` is **not** included; llapi
/// only reads the fixed prefix during begin.
fn build_hsm_action_item(a: &ReceivedAction) -> sys::hsm_action_item {
    let mut hai: sys::hsm_action_item = unsafe { std::mem::zeroed() };
    hai.hai_len = std::mem::size_of::<sys::hsm_action_item>() as u32;
    hai.hai_action = conv::action_kind_to_wire(a.kind);
    hai.hai_fid = conv::fid_to_sys(a.fid);
    hai.hai_dfid = conv::fid_to_sys(a.dfid);
    hai.hai_extent = conv::extent_to_sys(a.extent);
    hai.hai_cookie = a.cookie.get();
    hai.hai_gid = a.gid;
    hai
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Registering on a non-Lustre path must fail — and must fail
    /// *cleanly*, with our Register error variant rather than a panic /
    /// segfault. This is the only path we can exercise without a real
    /// Lustre cluster; the happy paths require sanity-hsm.sh.
    #[test]
    fn register_on_non_lustre_path_returns_register_error() {
        let res = LiveCopytool::register(Path::new("/tmp"), &[ArchiveId::new(1)]);
        match res {
            Err(HsmError::Register { mount, errno }) => {
                assert_eq!(mount, PathBuf::from("/tmp"));
                assert!(errno > 0, "errno should be positive, got {errno}");
            }
            // LiveCopytool intentionally lacks Debug (raw pointers); use is_ok().
            Ok(_) => panic!("expected HsmError::Register on a non-Lustre path, got Ok"),
            Err(other) => panic!("expected HsmError::Register, got {other:?}"),
        }
    }

    #[test]
    fn empty_archive_list_is_rejected_before_ffi() {
        let res = LiveCopytool::register(Path::new("/tmp"), &[]);
        assert!(matches!(res, Err(HsmError::EmptyArchiveList)));
    }

    #[test]
    fn align_up_is_correct() {
        assert_eq!(align_up(0, 8), 0);
        assert_eq!(align_up(1, 8), 8);
        assert_eq!(align_up(8, 8), 8);
        assert_eq!(align_up(9, 8), 16);
        assert_eq!(align_up(15, 8), 16);
    }
}
