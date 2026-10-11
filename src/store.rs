//! Durable history in SQLite.
//!
//! The in-memory model stays the read path for the HTTP API; this is the
//! write-behind sink and the history a restart replays from. Without it the
//! series, the activity ring, and the yield window start empty on every start.
//!
//! Every amount is stored as TEXT. SQLite's INTEGER is a signed 64-bit value, so
//! a `u64` above `2^63 - 1` cannot be stored in it and a `u128` never can; both
//! appear in pool state. The API already sends amounts as decimal strings, and
//! the database keeps that representation end to end, so nothing is silently
//! reinterpreted between the two.
//!
//! Retention is bounded per resolution: a row older than the resolution's own
//! window is deleted on every write cycle. The window defaults to the RAM caps
//! (6 h of minutes, 48 h of five-minutes, 30 days of hours). Set
//! `INDEXER_SNAPSHOT_RETENTION` to `all` to keep everything instead, which is
//! what the storage arithmetic in the README prices.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, params};

use crate::activity::{Activity, ActivityKind};
use crate::decode::TokenMeta;
use crate::state::Snapshot;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS tokens (
    faucet   TEXT PRIMARY KEY,
    symbol   TEXT NOT NULL,
    decimals INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pools (
    pool_id          TEXT PRIMARY KEY,
    token0           TEXT NOT NULL,
    token1           TEXT NOT NULL,
    block            INTEGER NOT NULL,
    first_seen_unix  INTEGER NOT NULL,
    last_change_unix INTEGER NOT NULL,
    lp_supply        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    pool_id       TEXT NOT NULL,
    resolution    INTEGER NOT NULL,
    start_unix    INTEGER NOT NULL,
    block         INTEGER NOT NULL,
    chain_ts      INTEGER NOT NULL,
    reserve0      TEXT NOT NULL,
    reserve1      TEXT NOT NULL,
    open_reserve0 TEXT NOT NULL,
    open_reserve1 TEXT NOT NULL,
    high_reserve0 TEXT NOT NULL,
    high_reserve1 TEXT NOT NULL,
    low_reserve0  TEXT NOT NULL,
    low_reserve1  TEXT NOT NULL,
    lp_supply     TEXT NOT NULL,
    k             TEXT NOT NULL,
    pot0          TEXT NOT NULL,
    pot1          TEXT NOT NULL,
    price0_cum    TEXT NOT NULL,
    price1_cum    TEXT NOT NULL,
    PRIMARY KEY (pool_id, resolution, start_unix)
);

CREATE TABLE IF NOT EXISTS activities (
    pool_id  TEXT NOT NULL,
    at_unix  INTEGER NOT NULL,
    block    INTEGER NOT NULL,
    kind     TEXT NOT NULL,
    volume0  TEXT NOT NULL,
    volume1  TEXT NOT NULL,
    lp_fee0  TEXT NOT NULL,
    lp_fee1  TEXT NOT NULL,
    pot_fee0 TEXT NOT NULL,
    pot_fee1 TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS activities_by_pool ON activities (pool_id, at_unix);
"#;

/// One pool's metadata plus the newest bucket of each resolution.
pub struct PoolWrite {
    pub id: String,
    pub token0: String,
    pub token1: String,
    pub block: u32,
    pub first_seen_unix: u64,
    pub last_change_unix: u64,
    pub lp_supply: u64,
    /// `(resolution seconds, newest bucket)`.
    pub buckets: Vec<(u64, Snapshot)>,
}

/// Everything one write cycle persists. Built under a short write lock, then
/// written with the lock released.
#[derive(Default)]
pub struct Cycle {
    pub tokens: Vec<(String, TokenMeta)>,
    pub pools: Vec<PoolWrite>,
    pub activities: Vec<(String, Activity)>,
}

/// History loaded at startup.
#[derive(Default)]
pub struct Loaded {
    pub tokens: HashMap<String, TokenMeta>,
    pub pools: Vec<LoadedPool>,
}

pub struct LoadedPool {
    pub id: String,
    pub token0: String,
    pub token1: String,
    pub first_seen_unix: u64,
    pub last_change_unix: u64,
    /// `(resolution seconds, buckets in ascending start order)`.
    pub buckets: Vec<(u64, Vec<Snapshot>)>,
}

pub struct Store {
    conn: Connection,
    /// Per-resolution retention in seconds, or None to keep everything.
    retention: Vec<(u64, Option<u64>)>,
    /// Activity retention in seconds.
    activity_retention: u64,
}

impl Store {
    /// Opens the database, applies the schema, and returns the open store.
    pub fn open(
        path: &str,
        retention: Vec<(u64, Option<u64>)>,
        activity_retention: u64,
    ) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory().context("open in-memory sqlite")?
        } else {
            if let Some(parent) = Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            Connection::open(path).with_context(|| format!("open sqlite {path}"))?
        };
        // WAL lets the API read while the poller writes. NORMAL is durable
        // against process death, which is what this store is for; only power
        // loss can lose the last transaction, and the chain is the real source.
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("enable WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("set synchronous")?;
        conn.pragma_update(None, "busy_timeout", 5_000)
            .context("set busy timeout")?;
        conn.execute_batch(SCHEMA).context("apply schema")?;
        Ok(Self {
            conn,
            retention,
            activity_retention,
        })
    }

    /// Opens a second, read-only connection to the same file.
    ///
    /// WAL mode allows one writer and many readers, so the API reads history
    /// from its own connection without touching the poller's or blocking it.
    pub fn open_read_only(path: &str) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("open sqlite {path} read-only"))?;
        conn.pragma_update(None, "query_only", true)
            .context("set query_only")?;
        Ok(Self {
            conn,
            retention: Vec::new(),
            activity_retention: 0,
        })
    }

    /// One pool's history at a resolution, oldest first.
    pub fn history(&self, pool_id: &str, resolution: u64, limit: usize) -> Result<Vec<Snapshot>> {
        // Newest first for the limit, then reversed, so a limit keeps the most
        // recent window rather than the oldest rows in the table.
        let mut statement = self.conn.prepare(
            "SELECT block, chain_ts, start_unix, reserve0, reserve1, open_reserve0,
                    open_reserve1, high_reserve0, high_reserve1, low_reserve0, low_reserve1,
                    lp_supply, k, pot0, pot1, price0_cum, price1_cum
             FROM snapshots
             WHERE pool_id = ?1 AND resolution = ?2
             ORDER BY start_unix DESC
             LIMIT ?3",
        )?;
        let rows = statement.query_map(params![pool_id, seconds(resolution), limit as i64], |row| {
            Ok(Snapshot {
                block: row.get(0)?,
                timestamp: read_u64(row, 1)?,
                polled_at: read_u64(row, 2)?,
                reserve0: parse_u64(&row.get::<_, String>(3)?, "reserve0")?,
                reserve1: parse_u64(&row.get::<_, String>(4)?, "reserve1")?,
                open_reserve0: parse_u64(&row.get::<_, String>(5)?, "open_reserve0")?,
                open_reserve1: parse_u64(&row.get::<_, String>(6)?, "open_reserve1")?,
                high_reserve0: parse_u64(&row.get::<_, String>(7)?, "high_reserve0")?,
                high_reserve1: parse_u64(&row.get::<_, String>(8)?, "high_reserve1")?,
                low_reserve0: parse_u64(&row.get::<_, String>(9)?, "low_reserve0")?,
                low_reserve1: parse_u64(&row.get::<_, String>(10)?, "low_reserve1")?,
                lp_supply: parse_u64(&row.get::<_, String>(11)?, "lp_supply")?,
                k: parse_u128(&row.get::<_, String>(12)?, "k")?,
                pot0: parse_u64(&row.get::<_, String>(13)?, "pot0")?,
                pot1: parse_u64(&row.get::<_, String>(14)?, "pot1")?,
                price0_cumulative: parse_u128(&row.get::<_, String>(15)?, "price0_cum")?,
                price1_cumulative: parse_u128(&row.get::<_, String>(16)?, "price1_cum")?,
            })
        })?;
        let mut points: Vec<Snapshot> = rows.collect::<rusqlite::Result<_>>()?;
        points.reverse();
        Ok(points)
    }

    /// Reads the pool set, token metadata, and every retained snapshot.
    pub fn load(&self) -> Result<Loaded> {
        let mut loaded = Loaded::default();

        let mut statement = self
            .conn
            .prepare("SELECT faucet, symbol, decimals FROM tokens")?;
        let tokens = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                TokenMeta {
                    symbol: row.get(1)?,
                    decimals: u8::try_from(row.get::<_, i64>(2)?).unwrap_or(0),
                },
            ))
        })?;
        for token in tokens {
            let (faucet, meta) = token?;
            loaded.tokens.insert(faucet, meta);
        }

        let mut statement = self.conn.prepare(
            "SELECT pool_id, token0, token1, first_seen_unix, last_change_unix
             FROM pools ORDER BY pool_id",
        )?;
        let pools = statement.query_map([], |row| {
            Ok(LoadedPool {
                id: row.get(0)?,
                token0: row.get(1)?,
                token1: row.get(2)?,
                first_seen_unix: read_u64(row, 3)?,
                last_change_unix: read_u64(row, 4)?,
                buckets: Vec::new(),
            })
        })?;
        for pool in pools {
            loaded.pools.push(pool?);
        }

        let mut statement = self.conn.prepare(
            "SELECT pool_id, resolution, start_unix, block, chain_ts, reserve0, reserve1,
                    open_reserve0, open_reserve1, high_reserve0, high_reserve1,
                    low_reserve0, low_reserve1, lp_supply, k, pot0, pot1, price0_cum, price1_cum
             FROM snapshots ORDER BY pool_id, resolution, start_unix",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                read_u64(row, 1)?,
                Snapshot {
                    block: row.get(3)?,
                    timestamp: read_u64(row, 4)?,
                    polled_at: read_u64(row, 2)?,
                    reserve0: parse_u64(&row.get::<_, String>(5)?, "reserve0")?,
                    reserve1: parse_u64(&row.get::<_, String>(6)?, "reserve1")?,
                    open_reserve0: parse_u64(&row.get::<_, String>(7)?, "open_reserve0")?,
                    open_reserve1: parse_u64(&row.get::<_, String>(8)?, "open_reserve1")?,
                    high_reserve0: parse_u64(&row.get::<_, String>(9)?, "high_reserve0")?,
                    high_reserve1: parse_u64(&row.get::<_, String>(10)?, "high_reserve1")?,
                    low_reserve0: parse_u64(&row.get::<_, String>(11)?, "low_reserve0")?,
                    low_reserve1: parse_u64(&row.get::<_, String>(12)?, "low_reserve1")?,
                    lp_supply: parse_u64(&row.get::<_, String>(13)?, "lp_supply")?,
                    k: parse_u128(&row.get::<_, String>(14)?, "k")?,
                    pot0: parse_u64(&row.get::<_, String>(15)?, "pot0")?,
                    pot1: parse_u64(&row.get::<_, String>(16)?, "pot1")?,
                    price0_cumulative: parse_u128(&row.get::<_, String>(17)?, "price0_cum")?,
                    price1_cumulative: parse_u128(&row.get::<_, String>(18)?, "price1_cum")?,
                },
            ))
        })?;
        for row in rows {
            let (pool_id, resolution, snapshot) = row?;
            let Some(pool) = loaded.pools.iter_mut().find(|pool| pool.id == pool_id) else {
                continue;
            };
            match pool
                .buckets
                .iter_mut()
                .find(|(period, _)| *period == resolution)
            {
                Some((_, buckets)) => buckets.push(snapshot),
                None => pool.buckets.push((resolution, vec![snapshot])),
            }
        }

        Ok(loaded)
    }

    /// Writes one cycle in a single transaction, then enforces retention.
    pub fn write(&mut self, cycle: &Cycle) -> Result<usize> {
        let transaction = self.conn.transaction()?;
        let mut written = 0usize;

        {
            let mut token = transaction.prepare(
                "INSERT INTO tokens (faucet, symbol, decimals) VALUES (?1, ?2, ?3)
                 ON CONFLICT (faucet) DO UPDATE SET symbol = ?2, decimals = ?3",
            )?;
            for (faucet, meta) in &cycle.tokens {
                token.execute(params![faucet, meta.symbol, i64::from(meta.decimals)])?;
                written += 1;
            }
        }

        {
            let mut pool = transaction.prepare(
                "INSERT INTO pools (pool_id, token0, token1, block, first_seen_unix,
                                    last_change_unix, lp_supply)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT (pool_id) DO UPDATE SET
                     block = ?4, last_change_unix = ?6, lp_supply = ?7",
            )?;
            let mut snapshot = transaction.prepare(
                "INSERT INTO snapshots (pool_id, resolution, start_unix, block, chain_ts,
                     reserve0, reserve1, open_reserve0, open_reserve1, high_reserve0,
                     high_reserve1, low_reserve0, low_reserve1, lp_supply, k, pot0, pot1,
                     price0_cum, price1_cum)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                         ?16, ?17, ?18, ?19)
                 ON CONFLICT (pool_id, resolution, start_unix) DO UPDATE SET
                     block = ?4, reserve0 = ?6, reserve1 = ?7, high_reserve0 = ?10,
                     high_reserve1 = ?11, low_reserve0 = ?12, low_reserve1 = ?13,
                     lp_supply = ?14, k = ?15, pot0 = ?16, pot1 = ?17,
                     price0_cum = ?18, price1_cum = ?19",
            )?;
            for pool_write in &cycle.pools {
                pool.execute(params![
                    pool_write.id,
                    pool_write.token0,
                    pool_write.token1,
                    i64::from(pool_write.block),
                    seconds(pool_write.first_seen_unix),
                    seconds(pool_write.last_change_unix),
                    pool_write.lp_supply.to_string(),
                ])?;
                written += 1;
                for (resolution, bucket) in &pool_write.buckets {
                    snapshot.execute(params![
                        pool_write.id,
                        seconds(*resolution),
                        seconds(bucket.polled_at),
                        i64::from(bucket.block),
                        seconds(bucket.timestamp),
                        bucket.reserve0.to_string(),
                        bucket.reserve1.to_string(),
                        bucket.open_reserve0.to_string(),
                        bucket.open_reserve1.to_string(),
                        bucket.high_reserve0.to_string(),
                        bucket.high_reserve1.to_string(),
                        bucket.low_reserve0.to_string(),
                        bucket.low_reserve1.to_string(),
                        bucket.lp_supply.to_string(),
                        bucket.k.to_string(),
                        bucket.pot0.to_string(),
                        bucket.pot1.to_string(),
                        bucket.price0_cumulative.to_string(),
                        bucket.price1_cumulative.to_string(),
                    ])?;
                    written += 1;
                }
            }
        }

        {
            let mut activity = transaction.prepare(
                "INSERT INTO activities (pool_id, at_unix, block, kind, volume0, volume1,
                     lp_fee0, lp_fee1, pot_fee0, pot_fee1)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )?;
            for (pool_id, entry) in &cycle.activities {
                activity.execute(params![
                    pool_id,
                    seconds(entry.timestamp),
                    i64::from(entry.block),
                    kind_name(entry.kind),
                    entry.volume0.to_string(),
                    entry.volume1.to_string(),
                    entry.lp_fee0.to_string(),
                    entry.lp_fee1.to_string(),
                    entry.protocol_fee0.to_string(),
                    entry.protocol_fee1.to_string(),
                ])?;
                written += 1;
            }
        }

        transaction.commit()?;
        self.prune()?;
        Ok(written)
    }

    /// Deletes snapshots past their resolution's window and old activities.
    fn prune(&self) -> Result<()> {
        let now = crate::state::unix_now();
        for (resolution, retention) in &self.retention {
            let Some(window) = retention else { continue };
            self.conn.execute(
                "DELETE FROM snapshots WHERE resolution = ?1 AND start_unix < ?2",
                params![seconds(*resolution), seconds(now.saturating_sub(*window))],
            )?;
        }
        self.conn.execute(
            "DELETE FROM activities WHERE at_unix < ?1",
            params![seconds(now.saturating_sub(self.activity_retention))],
        )?;
        Ok(())
    }

    /// Per-table row counts, for the startup log and the storage arithmetic.
    pub fn counts(&self) -> Result<Vec<(&'static str, u64)>> {
        let mut out = Vec::new();
        for table in ["pools", "tokens", "snapshots", "activities"] {
            let count: i64 =
                self.conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })?;
            out.push((table, count as u64));
        }
        Ok(out)
    }

    /// Bytes on disk, main file plus WAL.
    pub fn bytes_on_disk(&self) -> Result<u64> {
        let pages: i64 = self
            .conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))?;
        let page_size: i64 = self
            .conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok(pages as u64 * page_size as u64)
    }
}

/// SQLite has no unsigned 64-bit integer. Wall-clock seconds, block numbers,
/// and resolution periods fit in `i64` with room to spare; amounts do not, and
/// those travel as TEXT.
fn seconds(value: u64) -> i64 {
    i64::try_from(value).expect("unix seconds fit in i64")
}

fn read_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    u64::try_from(row.get::<_, i64>(index)?)
        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, 0))
}

fn kind_name(kind: ActivityKind) -> &'static str {
    match kind {
        ActivityKind::Swap => "swap",
        ActivityKind::AddLiquidity => "add",
        ActivityKind::RemoveLiquidity => "remove",
        ActivityKind::Collect => "collect",
        ActivityKind::Other => "other",
    }
}

fn parse_u64(value: &str, field: &str) -> rusqlite::Result<u64> {
    value.parse().map_err(|_| {
        rusqlite::Error::InvalidColumnType(0, field.to_string(), rusqlite::types::Type::Text)
    })
}

fn parse_u128(value: &str, field: &str) -> rusqlite::Result<u128> {
    value.parse().map_err(|_| {
        rusqlite::Error::InvalidColumnType(0, field.to_string(), rusqlite::types::Type::Text)
    })
}
