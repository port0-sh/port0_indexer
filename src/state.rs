//! In-memory index state.
//!
//! RAM only. Each pool keeps its latest decoded state, a bounded ring of
//! classified activities, and one time series per resolution. A point is
//! appended only when a tracked field changes; within a bucket the last point
//! is replaced, so an idle pool costs one record per bucket.

use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use miden_protocol::account::AccountId;
use serde::Serialize;

use crate::activity::{Activity, Aggregates, aggregates_since, classify};
use crate::decode::{NetworkFees, PoolState, TokenMeta, decode_network_fees, decode_pool_state};
use crate::registry::RegistryEntry;

/// Resolutions retained per pool: `(period seconds, point cap)`.
/// History tiers: resolution in seconds, and how many buckets the RAM mirror
/// keeps. One bucket each: the RAM mirror only has to remember the open bucket
/// so a poll can extend it, because history is read from the store. A tier's
/// retention on disk is separate and lives in `RETENTION`.
const SERIES: [(u64, usize); 5] = [(60, 1), (300, 1), (3600, 1), (21_600, 1), (86_400, 1)];

/// How much history each tier keeps on disk: 12 h of minutes, 7 d of five
/// minutes, 30 d of hours, 180 d of six-hours, and every day forever.
///
/// The windows are the ladder from the README: at 10,000 pools the four bounded
/// tiers hold 15.05 GB, and the daily tier adds 1.32 GB a year.
pub const RETENTION: [(u64, Option<u64>); 5] = [
    (60, Some(12 * 60 * 60)),
    (300, Some(7 * 24 * 60 * 60)),
    (3600, Some(30 * 24 * 60 * 60)),
    (21_600, Some(180 * 24 * 60 * 60)),
    (86_400, None),
];

/// Poll cadences by time since the last state change, in seconds: a pool that
/// just traded is polled every 15 s, one idle for an hour every minute, one idle
/// for a day every 10 minutes, and anything older hourly. The point is to stop
/// paying an RPC call per pool per tick once a pool stops moving.
const POLL_TIERS: [(u64, u64); 4] = [(300, 15), (3_600, 60), (86_400, 600), (u64::MAX, 3_600)];

/// How often a pool idle for `idle` seconds is polled.
fn cadence_for(idle: u64) -> u64 {
    POLL_TIERS
        .iter()
        .find(|(limit, _)| idle < *limit)
        .map(|(_, cadence)| *cadence)
        .unwrap_or(3_600)
}

/// Maximum classified activities retained per pool.
///
/// The trailing-window aggregates read this ring. A pool with more than this
/// many actions in 24 hours under-counts; raise the cap for such a pool.
pub const MAX_ACTIVITIES: usize = 4096;

/// One point in a pool's history.
///
/// A point is the state at the end of its bucket, plus the bucket's opening and
/// extreme states, which are what a candlestick needs. The extremes are stored
/// as the reserve pair that produced them, not as a price: the price is a ratio
/// of two reserves, and only the pair reconstructs it exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Snapshot {
    pub block: u32,
    /// Chain timestamp of the last state change.
    pub timestamp: u64,
    /// Start of the bucket, on the indexer's wall clock. It does not advance
    /// as the bucket collects samples: the bucket is identified by when it
    /// opened, which is also its position on a chart's time axis.
    pub polled_at: u64,
    pub reserve0: u64,
    pub reserve1: u64,
    /// Reserves at the bucket's first sample.
    pub open_reserve0: u64,
    pub open_reserve1: u64,
    /// Reserves at the bucket's highest price.
    pub high_reserve0: u64,
    pub high_reserve1: u64,
    /// Reserves at the bucket's lowest price.
    pub low_reserve0: u64,
    pub low_reserve1: u64,
    pub lp_supply: u64,
    pub k: u128,
    pub pot0: u64,
    pub pot1: u64,
    pub price0_cumulative: u128,
    pub price1_cumulative: u128,
}

/// Whether `a`'s price (`a1 / a0`) exists and is above `b`'s. A pair with a zero
/// reserve has no price: it never wins an extreme, and it loses to any pair that
/// has one. The comparison is cross-multiplied, so no division rounds it.
fn is_above(a0: u64, a1: u64, b0: u64, b1: u64) -> bool {
    if a0 == 0 || a1 == 0 {
        return false;
    }
    if b0 == 0 || b1 == 0 {
        return true;
    }
    a1 as u128 * b0 as u128 > b1 as u128 * a0 as u128
}

/// Whether `a`'s price is below `b`'s; see `is_above` for the zero-reserve rule.
fn is_below(a0: u64, a1: u64, b0: u64, b1: u64) -> bool {
    if a0 == 0 || a1 == 0 {
        return false;
    }
    if b0 == 0 || b1 == 0 {
        return true;
    }
    (a1 as u128 * b0 as u128) < (b1 as u128 * a0 as u128)
}

/// A fixed-resolution time series.
struct Series {
    period_secs: u64,
    cap: usize,
    points: VecDeque<Snapshot>,
}

impl Series {
    fn new(period_secs: u64, cap: usize) -> Self {
        Self {
            period_secs,
            cap,
            points: VecDeque::with_capacity(cap),
        }
    }

    /// Appends a point, replacing the bucket's last point when the period has
    /// not elapsed since it.
    fn push(&mut self, snapshot: Snapshot) {
        if let Some(last) = self.points.back_mut()
            && snapshot.polled_at.saturating_sub(last.polled_at) < self.period_secs
        {
            // Same bucket. The bucket is keyed by when it opened, so its
            // `polled_at` stays put: refreshing it here would push the next
            // boundary forward on every poll and the series would never open
            // a second bucket. The new state becomes the close, and the
            // bucket's open and price extremes carry over. A zero reserve has
            // no price, so it cannot win an extreme.
            let start = last.polled_at;
            let open = (last.open_reserve0, last.open_reserve1);
            let (mut high0, mut high1) = (last.high_reserve0, last.high_reserve1);
            let (mut low0, mut low1) = (last.low_reserve0, last.low_reserve1);
            let (close0, close1) = (snapshot.reserve0, snapshot.reserve1);
            if is_above(close0, close1, high0, high1) {
                (high0, high1) = (close0, close1);
            }
            if is_below(close0, close1, low0, low1) {
                (low0, low1) = (close0, close1);
            }
            *last = snapshot;
            last.polled_at = start;
            last.open_reserve0 = open.0;
            last.open_reserve1 = open.1;
            last.high_reserve0 = high0;
            last.high_reserve1 = high1;
            last.low_reserve0 = low0;
            last.low_reserve1 = low1;
            return;
        }
        if self.points.len() == self.cap {
            self.points.pop_front();
        }
        self.points.push_back(snapshot);
    }
}

/// A registered pool with its latest state, history, and activity.
pub struct PoolRecord {
    pub state: PoolState,
    pub block: u32,
    pub first_seen_unix: u64,
    pub last_change_unix: u64,
    /// Network-account fee configuration, read from the pool's auth slots.
    pub network_fees: NetworkFees,
    series: Vec<Series>,
    activities: VecDeque<(Activity, u64)>,
    /// Activities classified since the last persistence cycle.
    pending: Vec<Activity>,
    /// Unix second at which this pool is next due for an RPC poll.
    next_poll_at: u64,
}

impl PoolRecord {
    fn new(state: PoolState, block: u32, network_fees: NetworkFees) -> Self {
        let now = unix_now();
        let mut record = Self {
            state,
            block,
            first_seen_unix: now,
            last_change_unix: now,
            network_fees,
            series: SERIES
                .iter()
                .map(|(period, cap)| Series::new(*period, *cap))
                .collect(),
            activities: VecDeque::with_capacity(MAX_ACTIVITIES),
            pending: Vec::new(),
            // Due immediately: a pool discovered by the registry has never been
            // fetched, so the first poll must not wait for a cadence.
            next_poll_at: 0,
        };
        record.push_snapshot(now);
        record
    }

    /// Loads persisted buckets into the series. Buckets newer than what the
    /// series already holds are appended; older ones are dropped, because the
    /// live series is the truth for the window it covers.
    pub fn restore(&mut self, buckets: Vec<(u64, Vec<Snapshot>)>) {
        for (period, points) in buckets {
            let Some(series) = self
                .series
                .iter_mut()
                .find(|series| series.period_secs == period)
            else {
                continue;
            };
            for point in points {
                let newer = series
                    .points
                    .back()
                    .map(|last| point.polled_at > last.polled_at)
                    .unwrap_or(true);
                if newer {
                    if series.points.len() == series.cap {
                        series.points.pop_front();
                    }
                    series.points.push_back(point);
                }
            }
        }
    }

    /// The newest bucket of each resolution, with its period in seconds.
    pub fn persist(&self, id: &str) -> crate::store::PoolWrite {
        let buckets = self
            .series
            .iter()
            .filter_map(|series| {
                series
                    .points
                    .back()
                    .cloned()
                    .map(|snapshot| (series.period_secs, snapshot))
            })
            .collect();
        crate::store::PoolWrite {
            id: id.to_string(),
            token0: self.state.token0_faucet.to_hex(),
            token1: self.state.token1_faucet.to_hex(),
            block: self.block,
            first_seen_unix: self.first_seen_unix,
            last_change_unix: self.last_change_unix,
            lp_supply: self.state.lp_supply,
            buckets,
        }
    }

    /// True when this pool is due for an RPC poll.
    pub fn due(&self, now: u64) -> bool {
        now >= self.next_poll_at
    }

    /// Schedules the next poll from how long the pool has been idle.
    pub fn reschedule(&mut self, now: u64) {
        let idle = now.saturating_sub(self.last_change_unix);
        self.next_poll_at = now.saturating_add(cadence_for(idle));
    }

    /// Makes the pool due at the next tick, for a manual refresh.
    pub fn mark_due(&mut self) {
        self.next_poll_at = 0;
    }

    /// Drains the activities classified since the last cycle.
    pub fn take_pending(&mut self) -> Vec<Activity> {
        std::mem::take(&mut self.pending)
    }

    fn snapshot(&self, polled_at: u64) -> Snapshot {
        let reserve0 = self.state.reserve0;
        let reserve1 = self.state.reserve1;
        Snapshot {
            block: self.block,
            timestamp: self.state.block_timestamp_last,
            polled_at,
            reserve0,
            reserve1,
            open_reserve0: reserve0,
            open_reserve1: reserve1,
            high_reserve0: reserve0,
            high_reserve1: reserve1,
            low_reserve0: reserve0,
            low_reserve1: reserve1,
            lp_supply: self.state.lp_supply,
            k: self.state.k(),
            pot0: self.state.pot0,
            pot1: self.state.pot1,
            price0_cumulative: self.state.price0_cumulative_last,
            price1_cumulative: self.state.price1_cumulative_last,
        }
    }

    fn push_snapshot(&mut self, polled_at: u64) {
        let snapshot = self.snapshot(polled_at);
        for series in &mut self.series {
            series.push(snapshot.clone());
        }
    }

    /// Seeds the activity ring from the store.
    ///
    /// Not `push_activity`: that also queues the activity for the next write,
    /// and the row it came from is already in the table. The ring is stamped
    /// with the activity's own time, which is what the trailing aggregates
    /// measure against.
    fn seed_activities(&mut self, activities: Vec<Activity>) {
        for activity in activities {
            if self.activities.len() == MAX_ACTIVITIES {
                self.activities.pop_front();
            }
            self.activities.push_back((activity, activity.timestamp));
        }
    }

    fn push_activity(&mut self, activity: Activity, polled_at: u64) {
        if self.activities.len() == MAX_ACTIVITIES {
            self.activities.pop_front();
        }
        self.activities.push_back((activity, polled_at));
        self.pending.push(activity);
    }

    /// Applies a newer decoded state. Returns true when a tracked field
    /// changed and the history was updated.
    pub fn apply(&mut self, state: PoolState, block: u32) -> bool {
        let now = unix_now();
        let Some(activity) = classify(&self.state, &state, block) else {
            self.block = block;
            // An unchanged pool still gets a point. The series is a price
            // series, and a chart needs one candle per interval: without a
            // point for an idle interval the last bucket never advances and
            // the chart shows a single candle forever. The bucket's
            // replacement rule keeps this to one record per period.
            self.push_snapshot(now);
            return false;
        };
        self.state = state;
        self.block = block;
        self.last_change_unix = now;
        self.push_activity(activity, now);
        self.push_snapshot(now);
        true
    }

    /// Trailing aggregates over the activity ring.
    pub fn aggregates(&self, window_secs: u64) -> Aggregates {
        aggregates_since(
            self.activities.iter().map(|(activity, at)| (activity, *at)),
            unix_now(),
            window_secs,
        )
    }
}

/// Whole index state.
#[derive(Default)]
pub struct IndexState {
    pub registry_id: Option<AccountId>,
    pub last_registry_block: Option<u32>,
    /// Highest block the block subscription has processed. Blocks name the
    /// accounts they changed, which is what lets a pool be fetched on touch.
    pub last_processed_block: u32,
    /// Pool ids discovered from the registry, keyed by lowercase hex.
    pub known: HashMap<String, AccountId>,
    /// Fetched pool records, keyed by lowercase hex.
    pub pools: HashMap<String, PoolRecord>,
    pub pairs: HashMap<(String, String), String>,
    /// Token metadata read from faucet accounts, keyed by lowercase hex.
    pub tokens: HashMap<String, TokenMeta>,
    /// Pools read from the store, waiting to be fetched for the first time.
    replay: HashMap<String, crate::store::LoadedPool>,
}

impl IndexState {
    /// Records a discovered pool id from the registry. Returns true when the
    /// id was not previously known.
    pub fn discover(&mut self, entry: RegistryEntry) -> bool {
        self.known
            .insert(entry.pool_id.to_hex(), entry.pool_id)
            .is_none()
    }

    /// Records the ordered asset pair to pool mapping.
    pub fn set_pair(&mut self, asset0: AccountId, asset1: AccountId, pool_id: AccountId) {
        self.pairs
            .insert((asset0.to_hex(), asset1.to_hex()), pool_id.to_hex());
    }

    /// Decodes and stores a pool fetch result.
    pub fn apply_fetch(
        &mut self,
        pool_id: AccountId,
        block: u32,
        storage: &miden_protocol::account::AccountStorageHeader,
        vault: &miden_node_proto::domain::account::AccountVaultDetails,
        map_details: &[miden_node_proto::domain::account::AccountStorageMapDetails],
    ) -> anyhow::Result<bool> {
        let state = decode_pool_state(pool_id, storage, vault)?;
        let network_fees = decode_network_fees(storage, map_details)?;
        let key = pool_id.to_hex();
        match self.pools.get_mut(&key) {
            Some(record) => {
                let changed = record.apply(state, block);
                record.network_fees = network_fees;
                Ok(changed)
            }
            None => {
                let mut record = PoolRecord::new(state, block, network_fees);
                // History from the database belongs to this record from the
                // moment it exists, otherwise the first poll after a restart
                // would start the series empty and the chart would lose the past.
                if let Some(previous) = self.replay.remove(&key) {
                    record.restore(previous.buckets);
                    record.seed_activities(previous.activities);
                    // Keep the yield window continuous: it is measured from the
                    // pool's first sighting, which outlives this process.
                    record.first_seen_unix = previous.first_seen_unix;
                    record.last_change_unix = previous.last_change_unix;
                    // A stored pair that disagrees with the chain means the row
                    // belongs to a different account or the database was
                    // edited. The chain wins; the mismatch is worth a line.
                    let chain0 = record.state.token0_faucet.to_hex();
                    let chain1 = record.state.token1_faucet.to_hex();
                    if previous.token0 != chain0 || previous.token1 != chain1 {
                        tracing::warn!(
                            pool = %key,
                            stored = %format!("{}/{}", previous.token0, previous.token1),
                            chain = %format!("{chain0}/{chain1}"),
                            "stored token pair differs from the chain"
                        );
                    }
                }
                self.pools.insert(key, record);
                Ok(true)
            }
        }
    }

    /// Installs what a store loaded at startup: token metadata, and per pool its
    /// buckets and its first-seen time.
    ///
    /// The buckets and the timestamps are held until the pool is fetched;
    /// `apply_fetch` installs them on the record it creates. The pool set and
    /// the pair index are not seeded from the database: the registry is the
    /// authority for those, and a pool removed from it must fall out of the
    /// index rather than live on in a table.
    pub fn replay_loaded(&mut self, loaded: crate::store::Loaded) {
        self.tokens = loaded.tokens;
        for pool in loaded.pools {
            self.replay.insert(pool.id.clone(), pool);
        }
    }

    /// A fresh, empty index for one registry.
    pub fn new(registry_id: AccountId) -> Self {
        Self {
            registry_id: Some(registry_id),
            ..Default::default()
        }
    }

    /// Builds one persistence cycle and clears the pending activity queue.
    /// Called under a short write lock; the SQLite write happens after release.
    pub fn drain_persist(&mut self) -> crate::store::Cycle {
        let mut cycle = crate::store::Cycle {
            tokens: self
                .tokens
                .iter()
                .map(|(faucet, meta)| (faucet.clone(), meta.clone()))
                .collect(),
            ..Default::default()
        };
        for (key, record) in &mut self.pools {
            cycle.pools.push(record.persist(key));
            for activity in record.take_pending() {
                cycle.activities.push((key.clone(), activity));
            }
        }
        cycle
    }

    /// Seconds of history per resolution, for the store's retention policy.
    pub fn retention(&self) -> Vec<(u64, Option<u64>)> {
        RETENTION.to_vec()
    }
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::cadence_for;

    #[test]
    fn cadence_backs_off_with_idle_time() {
        // Fresh: the fastest cadence, so a just-traded pool is watched closely.
        assert_eq!(cadence_for(0), 15);
        assert_eq!(cadence_for(299), 15);
        // Five minutes idle moves to a minute between polls.
        assert_eq!(cadence_for(300), 60);
        assert_eq!(cadence_for(3_599), 60);
        // An hour idle: ten minutes.
        assert_eq!(cadence_for(3_600), 600);
        assert_eq!(cadence_for(86_399), 600);
        // A day idle: hourly, which is where the 10,000-pool sweep settles.
        assert_eq!(cadence_for(86_400), 3_600);
        assert_eq!(cadence_for(u64::MAX), 3_600);
    }
}
