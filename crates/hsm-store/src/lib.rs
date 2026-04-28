//! Persistent state for in-flight HSM actions.
//!
//! Every action that crosses `hsmd`'s boundary (from KUC `recv` through
//! plugin completion) is mirrored here so the daemon can recover after a
//! crash: load the store, reconcile with what each plugin reports it has
//! in flight, restart whatever's left.
//!
//! M2b ships only [`MemStore`] (no on-disk durability) — enough to drive
//! the scheduler / dispatcher tests and the M2c agent-mode happy path.
//! [`SqliteStore`] follows in M2b' once the daemon's main loop exists to
//! exercise the recovery path under realistic load.
//!
//! Three invariants the store enforces:
//!
//! 1. **Cookie uniqueness** — `insert` of a known cookie is an error,
//!    not silent overwrite. The kernel doesn't reuse cookies for
//!    in-flight actions; if we see one twice it's a bug.
//! 2. **Monotonic progress** — `update_progress` rejects backwards
//!    offsets. Buggy movers reporting "I just did 0..1MiB, now 0..512KiB"
//!    would silently roll back the daemon's view; we surface it instead.
//! 3. **State machine direction** — terminal states
//!    (`Succeed` / `Failed` / `Canceled`) are sticky. Once entered, no
//!    transition back to `Waiting` / `Started`.
//!
//! Higher layers can rely on these without having to write defensive code
//! around their store calls.

#![warn(missing_docs)]

pub mod any;
pub mod error;
pub mod mem;
pub mod sqlite;
pub mod store;

pub use any::AnyStore;
pub use error::{StoreError, StoreResult};
pub use mem::MemStore;
pub use sqlite::SqliteStore;
pub use store::ActionStore;
