//! SQLite-backed [`ActionStore`].
//!
//! Provides crash-safe persistence so the daemon can recover in-flight
//! actions after a restart. The schema is a single `actions` table;
//! [`SqliteStore::open`] creates it on first run via `CREATE TABLE IF NOT
//! EXISTS`.
//!
//! ## Design choices
//!
//! - **WAL + `synchronous=NORMAL`**: concurrent reads never block writes;
//!   crash safety is preserved without `FULL` fsync overhead.
//! - **No `sqlx::query!` macros**: avoids the `DATABASE_URL` build-time
//!   requirement. Runtime query strings are validated by the integration
//!   tests in `tests/contract.rs`.
//! - **Cookie as `i64`**: SQLite's `INTEGER` is signed 64-bit. We
//!   reinterpret `u64` bits as `i64` on write and back on read — the bit
//!   pattern is preserved.
//! - **`ArState` as TEXT discriminant + nullable helper columns**: keeps
//!   the table human-readable and avoids a JSON blob that would defeat
//!   SQLite's `STRICT` type enforcement.
//! - **Transition / progress atomicity**: `UPDATE … WHERE cookie=? AND
//!   <guard>` is atomic in SQLite. Zero rows_affected triggers a follow-up
//!   `SELECT` to distinguish "unknown cookie" from "guard failed".

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use hsm_core::{
    Action, ActionKind, ActionRecord, AgentId, ArState, ArchiveId, Cookie, Extent, Fid,
};
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions};

use crate::{ActionStore, StoreError, StoreResult};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS actions (
    cookie        INTEGER PRIMARY KEY NOT NULL,

    fid_seq       INTEGER NOT NULL,
    fid_oid       INTEGER NOT NULL,
    fid_ver       INTEGER NOT NULL,
    dfid_seq      INTEGER NOT NULL,
    dfid_oid      INTEGER NOT NULL,
    dfid_ver      INTEGER NOT NULL,
    archive_id    INTEGER NOT NULL,
    kind          TEXT    NOT NULL,
    extent_off    INTEGER NOT NULL,
    extent_len    INTEGER NOT NULL,
    gid           INTEGER NOT NULL,
    data          BLOB    NOT NULL,

    state         TEXT    NOT NULL,
    agent_id      TEXT,
    since_unix_ms INTEGER,
    rc            INTEGER,

    prog_off      INTEGER NOT NULL DEFAULT 0,
    prog_len      INTEGER NOT NULL DEFAULT 0,

    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
) STRICT;
-- Partial index speeds up list_by_agent (agent disconnect path).
CREATE INDEX IF NOT EXISTS idx_actions_started_agent
    ON actions (agent_id) WHERE state = 'Started';
-- Composite index speeds up query_actions (hsmctl actions --state --archive).
CREATE INDEX IF NOT EXISTS idx_actions_state_archive
    ON actions (state, archive_id);
";

/// SQLite-backed persistent action store.
///
/// `Clone` is cheap — the underlying [`SqlitePool`] is `Arc`-wrapped.
#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Open (or create) a SQLite database at `url`.
    ///
    /// `url` is any SQLite connection string accepted by sqlx, e.g.:
    /// - `"sqlite:/var/lib/hsmd/state.db"` — file on disk
    /// - `"sqlite::memory:"` — in-process in-memory (tests)
    ///
    /// Applies WAL journal mode and creates the schema on first run.
    pub async fn open(url: &str) -> Result<Self, sqlx::Error> {
        use std::str::FromStr;
        let opts = SqliteConnectOptions::from_str(url)?.create_if_missing(true);
        let pool = SqlitePool::connect_with(opts).await?;
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA synchronous=NORMAL")
            .execute(&pool)
            .await?;
        // Give SQLite 5 s to retry on write-lock contention before returning
        // SQLITE_BUSY. Required when the pool issues concurrent writes
        // (dispatch tick + status drain both call transition/update_progress).
        sqlx::query("PRAGMA busy_timeout=5000")
            .execute(&pool)
            .await?;
        sqlx::query(SCHEMA).execute(&pool).await?;
        Ok(Self { pool })
    }
}

impl ActionStore for SqliteStore {
    async fn insert(&self, rec: ActionRecord) -> StoreResult<()> {
        let cookie = rec.action.cookie.get() as i64;
        let (state_str, agent_id, since_ms, rc) = encode_state(&rec.state);
        let result = sqlx::query(
            "INSERT INTO actions (
                cookie,
                fid_seq, fid_oid, fid_ver,
                dfid_seq, dfid_oid, dfid_ver,
                archive_id, kind,
                extent_off, extent_len, gid, data,
                state, agent_id, since_unix_ms, rc,
                prog_off, prog_len,
                created_at, updated_at
             ) VALUES (
                ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?
             )",
        )
        .bind(cookie)
        .bind(rec.action.fid.seq as i64)
        .bind(rec.action.fid.oid as i64)
        .bind(rec.action.fid.ver as i64)
        .bind(rec.action.dfid.seq as i64)
        .bind(rec.action.dfid.oid as i64)
        .bind(rec.action.dfid.ver as i64)
        .bind(rec.action.archive_id.get() as i64)
        .bind(encode_kind(rec.action.kind))
        .bind(rec.action.extent.offset as i64)
        .bind(rec.action.extent.length as i64)
        .bind(rec.action.gid as i64)
        .bind(rec.action.data.as_ref())
        .bind(state_str)
        .bind(agent_id)
        .bind(since_ms)
        .bind(rc)
        .bind(rec.progress.offset as i64)
        .bind(rec.progress.length as i64)
        .bind(to_nanos(rec.created_at))
        .bind(to_nanos(rec.updated_at))
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                Err(StoreError::DuplicateCookie(rec.action.cookie))
            }
            Err(e) => Err(StoreError::Backend(e.to_string())),
        }
    }

    async fn transition(&self, cookie: Cookie, new: ArState) -> StoreResult<()> {
        let (state_str, agent_id, since_ms, rc) = encode_state(&new);
        let now = to_nanos(SystemTime::now());
        let cookie_i64 = cookie.get() as i64;

        let affected = sqlx::query(
            "UPDATE actions
             SET state=?, agent_id=?, since_unix_ms=?, rc=?, updated_at=?
             WHERE cookie=? AND state NOT IN ('Succeed','Failed','Canceled')",
        )
        .bind(state_str)
        .bind(agent_id)
        .bind(since_ms)
        .bind(rc)
        .bind(now)
        .bind(cookie_i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?
        .rows_affected();

        if affected == 1 {
            return Ok(());
        }

        // 0 rows: distinguish "unknown cookie" from "already terminal".
        // SELECT * so decode_state_row has all columns available (agent_id, since_unix_ms, rc).
        let row = sqlx::query("SELECT * FROM actions WHERE cookie=?")
            .bind(cookie_i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;

        match row {
            None => Err(StoreError::UnknownCookie(cookie)),
            Some(row) => {
                let from = decode_state_row(&row)?;
                Err(StoreError::TerminalTransition { cookie, from })
            }
        }
    }

    async fn update_progress(&self, cookie: Cookie, e: Extent) -> StoreResult<()> {
        let cookie_i64 = cookie.get() as i64;
        let now = to_nanos(SystemTime::now());

        // The WHERE guard `prog_off <= ?` enforces monotonicity atomically.
        let affected = sqlx::query(
            "UPDATE actions
             SET prog_off=?, prog_len=?, updated_at=?
             WHERE cookie=? AND prog_off <= ?",
        )
        .bind(e.offset as i64)
        .bind(e.length as i64)
        .bind(now)
        .bind(cookie_i64)
        .bind(e.offset as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?
        .rows_affected();

        if affected == 1 {
            return Ok(());
        }

        // 0 rows: distinguish "unknown cookie" from "progress regression".
        let row = sqlx::query("SELECT prog_off, prog_len FROM actions WHERE cookie=?")
            .bind(cookie_i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| StoreError::Backend(err.to_string()))?;

        match row {
            None => Err(StoreError::UnknownCookie(cookie)),
            Some(row) => {
                let have = Extent::new(
                    row.get::<i64, _>("prog_off") as u64,
                    row.get::<i64, _>("prog_len") as u64,
                );
                Err(StoreError::ProgressRegression {
                    cookie,
                    have,
                    got: e,
                })
            }
        }
    }

    async fn delete(&self, cookie: Cookie) -> StoreResult<()> {
        sqlx::query("DELETE FROM actions WHERE cookie=?")
            .bind(cookie.get() as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, cookie: Cookie) -> StoreResult<Option<ActionRecord>> {
        let row = sqlx::query("SELECT * FROM actions WHERE cookie=?")
            .bind(cookie.get() as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        row.map(|r| decode_record(&r)).transpose()
    }

    async fn load_all(&self) -> StoreResult<Vec<ActionRecord>> {
        let rows = sqlx::query("SELECT * FROM actions")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        rows.iter().map(decode_record).collect()
    }

    async fn list_by_agent(&self, agent: &AgentId) -> StoreResult<Vec<ActionRecord>> {
        let rows = sqlx::query("SELECT * FROM actions WHERE state='Started' AND agent_id=?")
            .bind(agent.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        rows.iter().map(decode_record).collect()
    }

    async fn len(&self) -> StoreResult<usize> {
        let row = sqlx::query("SELECT COUNT(*) as n FROM actions")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(row.get::<i64, _>("n") as usize)
    }

    async fn query_actions(
        &self,
        state_filter: Option<&str>,
        archive_id: Option<u32>,
        limit: usize,
    ) -> StoreResult<Vec<ActionRecord>> {
        // Build SQL with optional WHERE clauses to avoid full-table materialisation.
        let mut sql = "SELECT * FROM actions WHERE 1=1".to_string();
        if state_filter.is_some() {
            sql.push_str(" AND state = ?");
        }
        if archive_id.is_some() {
            sql.push_str(" AND archive_id = ?");
        }
        sql.push_str(" LIMIT ?");

        let mut q = sqlx::query(&sql);
        if let Some(state) = state_filter {
            // Stored with PascalCase (encode_kind / encode_state use "Waiting" etc.)
            let stored = match state {
                "waiting" => "Waiting",
                "started" => "Started",
                "succeed" => "Succeed",
                "failed" => "Failed",
                "canceled" => "Canceled",
                other => other, // pass through; caller already validated
            };
            q = q.bind(stored);
        }
        if let Some(aid) = archive_id {
            q = q.bind(aid as i64);
        }
        q = q.bind(limit as i64);

        let rows = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        rows.iter().map(decode_record).collect()
    }

    async fn status_counts(&self) -> StoreResult<(u64, u64, u64)> {
        // Single aggregate query: avoids materialising full records for the status RPC.
        let rows = sqlx::query("SELECT state, COUNT(*) as n FROM actions GROUP BY state")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;

        let mut waiting = 0u64;
        let mut started = 0u64;
        let mut total = 0u64;
        for row in &rows {
            let n = row.get::<i64, _>("n") as u64;
            total += n;
            match row.get::<&str, _>("state") {
                "Waiting" => waiting += n,
                "Started" => started += n,
                _ => {}
            }
        }
        Ok((waiting, started, total))
    }
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn encode_kind(k: ActionKind) -> &'static str {
    match k {
        ActionKind::Archive => "Archive",
        ActionKind::Restore => "Restore",
        ActionKind::Remove => "Remove",
        ActionKind::Cancel => "Cancel",
    }
}

fn decode_kind(s: &str) -> StoreResult<ActionKind> {
    match s {
        "Archive" => Ok(ActionKind::Archive),
        "Restore" => Ok(ActionKind::Restore),
        "Remove" => Ok(ActionKind::Remove),
        "Cancel" => Ok(ActionKind::Cancel),
        other => Err(StoreError::Backend(format!(
            "unknown ActionKind in DB: {other:?}"
        ))),
    }
}

/// Returns `(state_str, agent_id, since_unix_ms, rc)`.
fn encode_state(s: &ArState) -> (&'static str, Option<String>, Option<i64>, Option<i32>) {
    match s {
        ArState::Waiting => ("Waiting", None, None, None),
        ArState::Started {
            agent,
            since_unix_ms,
        } => (
            "Started",
            Some(agent.as_str().to_owned()),
            Some(i64::try_from(*since_unix_ms).unwrap_or(i64::MAX)),
            None,
        ),
        ArState::Succeed { rc } => ("Succeed", None, None, Some(*rc)),
        ArState::Failed { rc } => ("Failed", None, None, Some(*rc)),
        ArState::Canceled => ("Canceled", None, None, None),
    }
}

/// Reconstruct [`ArState`] from a row that has `state`, `agent_id`,
/// `since_unix_ms`, `rc` columns.
fn decode_state_row(row: &sqlx::sqlite::SqliteRow) -> StoreResult<ArState> {
    let discriminant: &str = row.get("state");
    match discriminant {
        "Waiting" => Ok(ArState::Waiting),
        "Started" => {
            // agent_id and since_unix_ms are nullable in the schema but logically
            // required for Started rows. Use Option to avoid a panic on NULL
            // (e.g. corrupted DB, hand-edited row) and surface a clean error.
            let agent = row
                .get::<Option<String>, _>("agent_id")
                .ok_or_else(|| StoreError::Backend("Started row has NULL agent_id".into()))?;
            let since_ms = row
                .get::<Option<i64>, _>("since_unix_ms")
                .ok_or_else(|| StoreError::Backend("Started row has NULL since_unix_ms".into()))?;
            Ok(ArState::Started {
                agent: AgentId::new(agent),
                since_unix_ms: since_ms as u64,
            })
        }
        "Succeed" => {
            let rc = row
                .get::<Option<i32>, _>("rc")
                .ok_or_else(|| StoreError::Backend("Succeed row has NULL rc".into()))?;
            Ok(ArState::Succeed { rc })
        }
        "Failed" => {
            let rc = row
                .get::<Option<i32>, _>("rc")
                .ok_or_else(|| StoreError::Backend("Failed row has NULL rc".into()))?;
            Ok(ArState::Failed { rc })
        }
        "Canceled" => Ok(ArState::Canceled),
        other => Err(StoreError::Backend(format!(
            "unknown ArState discriminant in DB: {other:?}"
        ))),
    }
}

fn decode_record(row: &sqlx::sqlite::SqliteRow) -> StoreResult<ActionRecord> {
    let action = Action {
        cookie: Cookie::new(row.get::<i64, _>("cookie") as u64),
        fid: Fid::new(
            row.get::<i64, _>("fid_seq") as u64,
            row.get::<i64, _>("fid_oid") as u32,
            row.get::<i64, _>("fid_ver") as u32,
        ),
        dfid: Fid::new(
            row.get::<i64, _>("dfid_seq") as u64,
            row.get::<i64, _>("dfid_oid") as u32,
            row.get::<i64, _>("dfid_ver") as u32,
        ),
        archive_id: ArchiveId::new(row.get::<i64, _>("archive_id") as u32),
        kind: decode_kind(row.get("kind"))?,
        extent: Extent::new(
            row.get::<i64, _>("extent_off") as u64,
            row.get::<i64, _>("extent_len") as u64,
        ),
        gid: row.get::<i64, _>("gid") as u64,
        data: Bytes::copy_from_slice(row.get::<&[u8], _>("data")),
    };

    Ok(ActionRecord {
        action,
        state: decode_state_row(row)?,
        progress: Extent::new(
            row.get::<i64, _>("prog_off") as u64,
            row.get::<i64, _>("prog_len") as u64,
        ),
        created_at: from_nanos(row.get::<i64, _>("created_at")),
        updated_at: from_nanos(row.get::<i64, _>("updated_at")),
    })
}

fn to_nanos(t: SystemTime) -> i64 {
    let nanos = t
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos();
    // i64::MAX nanoseconds ≈ year 2262; safe for any plausible wall-clock timestamp.
    // Saturate rather than truncate so corrupted timestamps are obviously wrong.
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

fn from_nanos(nanos: i64) -> SystemTime {
    // nanos is always non-negative (we store UNIX timestamps via to_nanos which
    // saturates at i64::MAX). Treat negative values as epoch to avoid panics on
    // corrupted rows.
    let nanos = u64::try_from(nanos).unwrap_or(0);
    UNIX_EPOCH + Duration::from_nanos(nanos)
}
