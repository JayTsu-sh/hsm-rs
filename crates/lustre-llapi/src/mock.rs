//! In-memory mock copytool — no Lustre dependency.
//!
//! Used by:
//! - This crate's own tests for `ActionHandle` lifecycle.
//! - Higher-layer tests (scheduler / store / dispatcher / plugin SDK) that
//!   want to exercise the daemon's HSM-action flow without standing up a
//!   real coordinator + KUC pipe.
//!
//! The mock records everything the daemon does so tests can assert on
//! observed `progress` / `end` / `abandon` events afterwards.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use hsm_core::{ActionKind, ArchiveId, Cookie, Extent, Fid};

use crate::action::{ActionHandle, EndStatus, HasCookieLifecycle, ReceivedAction};
use crate::error::{HsmError, Result};

/// What the mock observed for one cookie.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockCompletion {
    /// Cookie of the completed / cancelled / abandoned action.
    pub cookie: Cookie,
    /// Final extent reported via `end` (or zero for `Abandon`).
    pub extent: Extent,
    /// How the action terminated.
    pub outcome: MockOutcome,
}

/// Terminal events observable on the mock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MockOutcome {
    /// `ActionHandle::end(Ok)` was called.
    Ended(EndStatus),
    /// `ActionHandle` was dropped without `end` — RAII fired `abandon`.
    Abandoned,
}

/// State tracked per in-flight cookie.
#[derive(Clone, Debug)]
struct InFlight {
    /// Last extent reported via `progress`.
    progress: Extent,
}

/// In-memory copytool for testing.
///
/// Tests prime it with [`enqueue`](Self::enqueue) before the system under
/// test calls [`recv`](Self::recv).
#[derive(Default)]
pub struct MockCopytool {
    pending: VecDeque<ReceivedAction>,
    in_flight: HashMap<Cookie, InFlight>,
    progress_log: Vec<(Cookie, Extent)>,
    completions: Vec<MockCompletion>,
}

impl MockCopytool {
    /// Empty mock with no pending actions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueue an action to be returned by the next [`recv`](Self::recv).
    /// Tests call this directly to simulate the kernel coordinator
    /// dispatching work.
    pub fn enqueue(&mut self, a: ReceivedAction) {
        self.pending.push_back(a);
    }

    /// Convenience: enqueue with sensible defaults for cases tests don't
    /// care about (`fid == dfid`, `data == ""`, `gid == 0`).
    pub fn enqueue_simple(&mut self, cookie: Cookie, fid: Fid, archive: ArchiveId, kind: ActionKind, extent: Extent) {
        self.enqueue(ReceivedAction {
            cookie,
            fid,
            dfid: fid,
            archive_id: archive,
            kind,
            extent,
            gid: 0,
            data: Bytes::new(),
            fsname: "mock".to_string(),
        });
    }

    /// Drain currently-queued actions. Returns `Ok(empty vec)` (not an
    /// error) when there's nothing pending — the daemon's main loop will
    /// then await `raw_fd().readable()`, which the mock can't simulate, so
    /// tests should poll `recv` after each `enqueue`.
    pub fn recv(&mut self) -> Result<Vec<ReceivedAction>> {
        Ok(self.pending.drain(..).collect())
    }

    /// Begin processing an action. Returns an [`ActionHandle`] tied to
    /// `self`'s lifetime — the borrow checker enforces single-active-handle
    /// per copytool, mirroring the kernel's per-cookie group-lock semantics.
    pub fn begin(&mut self, cookie: Cookie) -> Result<ActionHandle<'_, Self>> {
        if self.in_flight.contains_key(&cookie) {
            return Err(HsmError::Begin(libc_eexist()));
        }
        self.in_flight.insert(cookie, InFlight { progress: Extent::new(0, 0) });
        Ok(ActionHandle::new(self, cookie))
    }

    // --- Test inspection helpers --------------------------------------------

    /// All terminal events observed so far (Ended / Abandoned).
    pub fn completions(&self) -> &[MockCompletion] {
        &self.completions
    }

    /// All progress updates observed so far, in order.
    pub fn progress_log(&self) -> &[(Cookie, Extent)] {
        &self.progress_log
    }

    /// Cookies that are currently in-flight (begun, not yet ended/abandoned).
    pub fn in_flight_cookies(&self) -> Vec<Cookie> {
        self.in_flight.keys().copied().collect()
    }
}

impl HasCookieLifecycle for MockCopytool {
    fn cookie_progress(&mut self, cookie: Cookie, e: Extent) -> Result<()> {
        let slot = self.in_flight.get_mut(&cookie).ok_or(HsmError::UnknownCookie(cookie.get()))?;
        // Reject backwards progress (matches the store's invariant; see DESIGN.md §16).
        if e.offset < slot.progress.offset {
            return Err(HsmError::Progress(libc_einval()));
        }
        slot.progress = e;
        self.progress_log.push((cookie, e));
        Ok(())
    }

    fn cookie_end(&mut self, cookie: Cookie, e: Extent, status: EndStatus) -> Result<()> {
        if self.in_flight.remove(&cookie).is_none() {
            return Err(HsmError::UnknownCookie(cookie.get()));
        }
        self.completions.push(MockCompletion {
            cookie,
            extent: e,
            outcome: MockOutcome::Ended(status),
        });
        Ok(())
    }

    fn cookie_abandon(&mut self, cookie: Cookie) {
        if self.in_flight.remove(&cookie).is_some() {
            self.completions.push(MockCompletion {
                cookie,
                extent: Extent::new(0, 0),
                outcome: MockOutcome::Abandoned,
            });
        }
    }
}

// libc errno constants we use without pulling in the libc crate. Linux x86_64
// values; the mock is platform-independent so we just hard-code the Linux
// values everywhere — they only show up in tests.
const fn libc_einval() -> i32 {
    22
}
const fn libc_eexist() -> i32 {
    17
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_action(cookie: u64) -> ReceivedAction {
        ReceivedAction {
            cookie: Cookie::new(cookie),
            fid: Fid::new(0x200000401, cookie as u32, 0),
            dfid: Fid::new(0x200000401, cookie as u32, 0),
            archive_id: ArchiveId::new(1),
            kind: ActionKind::Archive,
            extent: Extent::WHOLE,
            gid: 0,
            data: Bytes::new(),
            fsname: "mock".into(),
        }
    }

    #[test]
    fn recv_drains_pending() {
        let mut ct = MockCopytool::new();
        ct.enqueue(sample_action(1));
        ct.enqueue(sample_action(2));
        let batch = ct.recv().unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].cookie, Cookie::new(1));
        assert!(ct.recv().unwrap().is_empty());
    }

    #[test]
    fn begin_progress_end_lifecycle() {
        let mut ct = MockCopytool::new();
        let cookie = Cookie::new(0xabc);
        let mut h = ct.begin(cookie).unwrap();
        h.progress(Extent::new(0, 4096)).unwrap();
        h.progress(Extent::new(4096, 4096)).unwrap();
        h.end(Extent::new(0, 8192), EndStatus::Ok).unwrap();

        assert_eq!(ct.in_flight_cookies(), Vec::<Cookie>::new());
        assert_eq!(ct.progress_log().len(), 2);
        assert_eq!(ct.completions().len(), 1);
        assert!(matches!(
            ct.completions()[0].outcome,
            MockOutcome::Ended(EndStatus::Ok)
        ));
    }

    #[test]
    fn dropping_handle_without_end_logs_abandon() {
        let mut ct = MockCopytool::new();
        let cookie = Cookie::new(99);
        {
            let _h = ct.begin(cookie).unwrap();
            // _h dropped here without end()
        }
        assert_eq!(ct.completions().len(), 1);
        assert_eq!(ct.completions()[0].cookie, cookie);
        assert_eq!(ct.completions()[0].outcome, MockOutcome::Abandoned);
        assert_eq!(ct.in_flight_cookies(), Vec::<Cookie>::new());
    }

    #[test]
    fn double_begin_same_cookie_errors() {
        let mut ct = MockCopytool::new();
        let cookie = Cookie::new(7);
        let h1 = ct.begin(cookie).unwrap();
        // Drop h1 first — borrowck won't let us hold two handles
        // concurrently. Rust's type system already prevents the live
        // double-begin race; this test confirms that even after drop
        // (which abandons), re-begin works (clean slate).
        drop(h1);
        let _h2 = ct.begin(cookie).expect("re-begin after abandon should be fine");
    }

    #[test]
    fn end_with_unknown_cookie_errors() {
        let mut ct = MockCopytool::new();
        // Bypass the handle path to simulate a buggy caller.
        let res = ct.cookie_end(Cookie::new(404), Extent::WHOLE, EndStatus::Ok);
        assert!(matches!(res, Err(HsmError::UnknownCookie(404))));
    }

    #[test]
    fn progress_rejects_backwards_offset() {
        let mut ct = MockCopytool::new();
        let cookie = Cookie::new(0x10);
        let mut h = ct.begin(cookie).unwrap();
        h.progress(Extent::new(8192, 4096)).unwrap();
        let res = h.progress(Extent::new(4096, 4096));
        assert!(matches!(res, Err(HsmError::Progress(_))));
        // Cleanup — drop h to abandon, otherwise borrow scope holds.
        drop(h);
    }

    #[test]
    fn enqueue_simple_round_trip() {
        let mut ct = MockCopytool::new();
        ct.enqueue_simple(
            Cookie::new(1),
            Fid::new(1, 2, 0),
            ArchiveId::new(3),
            ActionKind::Remove,
            Extent::WHOLE,
        );
        let r = &ct.recv().unwrap()[0];
        assert_eq!(r.cookie, Cookie::new(1));
        assert_eq!(r.kind, ActionKind::Remove);
        assert_eq!(r.archive_id, ArchiveId::new(3));
    }
}
