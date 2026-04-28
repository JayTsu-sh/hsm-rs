//! Contract test suite for [`ActionStore`] implementations.
//!
//! Every impl must pass the same behavioural assertions. Add new impls by
//! calling `run_contract_suite` in a new `#[tokio::test]`.

use std::time::SystemTime;

use bytes::Bytes;
use hsm_core::{
    Action, ActionKind, ActionRecord, AgentId, ArState, ArchiveId, Cookie, Extent, Fid,
};
use hsm_store::{ActionStore, MemStore, StoreError};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn record(cookie: u64) -> ActionRecord {
    ActionRecord::new(
        Action {
            cookie: Cookie::new(cookie),
            fid: Fid::new(0x200000401, cookie as u32, 0),
            dfid: Fid::new(0x200000401, cookie as u32, 0),
            archive_id: ArchiveId::new(1),
            kind: ActionKind::Archive,
            extent: Extent::WHOLE,
            gid: 0,
            data: Bytes::from_static(b"hint"),
        },
        SystemTime::UNIX_EPOCH,
    )
}

fn agent(name: &str) -> AgentId {
    AgentId::new(name)
}

fn started(agent_name: &str) -> ArState {
    ArState::Started {
        agent: agent(agent_name),
        since_unix_ms: 0,
    }
}

// ---------------------------------------------------------------------------
// The shared suite
// ---------------------------------------------------------------------------

async fn run_contract_suite(store: &impl ActionStore) {
    // insert + get round-trip
    store.insert(record(1)).await.unwrap();
    let r = store.get(Cookie::new(1)).await.unwrap().unwrap();
    assert_eq!(r.action.cookie, Cookie::new(1));
    assert!(matches!(r.state, ArState::Waiting));
    assert_eq!(r.action.data, Bytes::from_static(b"hint"));

    // duplicate insert → DuplicateCookie
    let err = store.insert(record(1)).await.unwrap_err();
    assert!(matches!(err, StoreError::DuplicateCookie(c) if c == Cookie::new(1)));

    // transition Waiting → Started
    store
        .transition(Cookie::new(1), started("mover-0"))
        .await
        .unwrap();
    let r = store.get(Cookie::new(1)).await.unwrap().unwrap();
    assert!(matches!(r.state, ArState::Started { .. }));

    // transition Started → Succeed
    store
        .transition(Cookie::new(1), ArState::Succeed { rc: 0 })
        .await
        .unwrap();

    // transition out of terminal → TerminalTransition
    let err = store
        .transition(Cookie::new(1), ArState::Waiting)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::TerminalTransition {
            cookie,
            from: ArState::Succeed { rc: 0 },
        } if cookie == Cookie::new(1)
    ));

    // transition unknown cookie → UnknownCookie
    let err = store
        .transition(Cookie::new(999), ArState::Waiting)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownCookie(c) if c == Cookie::new(999)));

    // insert more records for progress + agent tests
    store.insert(record(2)).await.unwrap();
    store.insert(record(3)).await.unwrap();

    // update_progress advances
    store
        .update_progress(Cookie::new(2), Extent::new(0, 4096))
        .await
        .unwrap();
    store
        .update_progress(Cookie::new(2), Extent::new(4096, 4096))
        .await
        .unwrap();
    let r = store.get(Cookie::new(2)).await.unwrap().unwrap();
    assert_eq!(r.progress.offset, 4096);

    // update_progress regression → ProgressRegression
    let err = store
        .update_progress(Cookie::new(2), Extent::new(0, 4096))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::ProgressRegression { cookie, .. } if cookie == Cookie::new(2))
    );

    // update_progress unknown cookie → UnknownCookie
    let err = store
        .update_progress(Cookie::new(888), Extent::new(0, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownCookie(c) if c == Cookie::new(888)));

    // list_by_agent
    store
        .transition(Cookie::new(2), started("mover-1"))
        .await
        .unwrap();
    store
        .transition(Cookie::new(3), started("mover-0"))
        .await
        .unwrap();
    let m0 = store.list_by_agent(&agent("mover-0")).await.unwrap();
    assert_eq!(m0.len(), 1);
    assert_eq!(m0[0].action.cookie, Cookie::new(3));
    let m1 = store.list_by_agent(&agent("mover-1")).await.unwrap();
    assert_eq!(m1.len(), 1);

    // len
    let n = store.len().await.unwrap();
    assert_eq!(n, 3); // cookies 1, 2, 3

    // load_all
    let all = store.load_all().await.unwrap();
    assert_eq!(all.len(), 3);

    // delete is idempotent
    store.delete(Cookie::new(404)).await.unwrap();
    store.delete(Cookie::new(1)).await.unwrap();
    store.delete(Cookie::new(1)).await.unwrap();
    assert!(store.get(Cookie::new(1)).await.unwrap().is_none());
    assert_eq!(store.len().await.unwrap(), 2);

    // get on missing → None (not error)
    assert!(store.get(Cookie::new(999)).await.unwrap().is_none());
}

// ---------------------------------------------------------------------------
// MemStore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mem_store_contract() {
    run_contract_suite(&MemStore::new()).await;
}

// ---------------------------------------------------------------------------
// SqliteStore (always exercised in tests, regardless of the `sqlite` feature)
// ---------------------------------------------------------------------------

async fn open_sqlite() -> hsm_store::sqlite::SqliteStore {
    hsm_store::sqlite::SqliteStore::open("sqlite::memory:")
        .await
        .expect("open in-memory sqlite")
}

#[tokio::test]
async fn sqlite_store_contract() {
    run_contract_suite(&open_sqlite().await).await;
}

#[tokio::test]
async fn sqlite_store_clone_shares_pool() {
    let a = open_sqlite().await;
    let b = a.clone();
    a.insert(record(1)).await.unwrap();
    assert!(b.get(Cookie::new(1)).await.unwrap().is_some());
}

#[tokio::test]
async fn sqlite_store_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let url = format!("sqlite://{}", path.display());

    {
        let store = hsm_store::sqlite::SqliteStore::open(&url).await.unwrap();
        store.insert(record(42)).await.unwrap();
        store
            .transition(Cookie::new(42), started("mover-0"))
            .await
            .unwrap();
    }

    // Reopen — simulates daemon restart.
    {
        let store = hsm_store::sqlite::SqliteStore::open(&url).await.unwrap();
        let r = store.get(Cookie::new(42)).await.unwrap().unwrap();
        assert_eq!(r.action.cookie, Cookie::new(42));
        assert!(matches!(r.state, ArState::Started { .. }));
    }
}
