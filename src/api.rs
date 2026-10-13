//! Read-only HTTP API.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, Request, State};
use axum::http::{
    HeaderValue, Method, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::activity::AGGREGATE_WINDOW_SECS;
use crate::decode::TokenMeta;
use crate::state::{IndexState, PoolRecord, Snapshot, unix_now};

type Shared = Arc<RwLock<IndexState>>;

/// History reads go through their own read-only connection to the same file.
///
/// The RAM mirror keeps only the open bucket of each tier, so history is the
/// store's job. `None` means the indexer runs without a database, in which case
/// the API reports only the open bucket.
type History = Option<Arc<std::sync::Mutex<crate::store::Store>>>;

#[derive(Clone)]
pub struct ApiState {
    pub state: Shared,
    pub history: History,
    pub limiter: Arc<std::sync::Mutex<Limiter>>,
}

/// Limits on the manual refresh endpoint.
///
/// A global token bucket refilled at [`REFRESH_PER_SEC`] bounds every request
/// that reaches the limiter, and one refresh per pool every
/// [`PER_POOL_COOLDOWN_SECS`] bounds repeats. A caller that trips either gets 429
/// with `Retry-After`, because the pool's own cadence will fetch it soon anyway.
const PER_POOL_COOLDOWN_SECS: u64 = 5;
const REFRESH_PER_SEC: u64 = 20;
const REFRESH_BURST: f64 = 20.0;

/// The most keys the cooldown map can hold.
///
/// A caller adds at most [`REFRESH_PER_SEC`] keys a second, and only a key
/// recorded within [`PER_POOL_COOLDOWN_SECS`] can deny anything, so the live set
/// is one key per pool per cooldown window. The map is trimmed to that bound
/// rather than trusted to stay under it.
const PER_POOL_TRACKED_MAX: usize =
    (REFRESH_PER_SEC as usize) * (PER_POOL_COOLDOWN_SECS as usize + 1);

pub struct Limiter {
    per_pool: HashMap<String, u64>,
    tokens: f64,
    last_refill: u64,
}

impl Default for Limiter {
    fn default() -> Self {
        Self {
            per_pool: HashMap::new(),
            tokens: REFRESH_BURST,
            last_refill: unix_now(),
        }
    }
}

impl Limiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes one token from the global bucket.
    pub fn take_token(&mut self) -> bool {
        self.take_token_at(unix_now())
    }

    fn take_token_at(&mut self, now: u64) -> bool {
        let elapsed = now.saturating_sub(self.last_refill) as f64;
        self.tokens = (self.tokens + elapsed * REFRESH_PER_SEC as f64).min(REFRESH_BURST);
        self.last_refill = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }

    /// Records a refresh of `pool_id` if its cooldown has elapsed.
    ///
    /// Callers check that the pool exists first, so the map holds only ids that
    /// a pool record backs.
    pub fn allow_pool(&mut self, pool_id: &str) -> bool {
        self.allow_pool_at(pool_id, unix_now())
    }

    fn allow_pool_at(&mut self, pool_id: &str, now: u64) -> bool {
        if self.per_pool.len() >= PER_POOL_TRACKED_MAX {
            self.per_pool
                .retain(|_, at| now.saturating_sub(*at) < PER_POOL_COOLDOWN_SECS);
            if self.per_pool.len() >= PER_POOL_TRACKED_MAX {
                // Every entry is inside its cooldown, which happens only when the
                // clock does not advance. Dropping them re-admits at most one
                // refresh per pool, which is what the cooldown allows anyway.
                self.per_pool.clear();
            }
        }

        let cooled = self
            .per_pool
            .get(pool_id)
            .map(|at| now.saturating_sub(*at) >= PER_POOL_COOLDOWN_SECS)
            .unwrap_or(true);
        if !cooled {
            return false;
        }
        self.per_pool.insert(pool_id.to_string(), now);
        true
    }

    /// Entries the cooldown map holds. Exposed for the bound test.
    #[cfg(test)]
    fn tracked_pools(&self) -> usize {
        self.per_pool.len()
    }
}

/// Pool summary. Amounts are strings because u64 and u128 exceed the safe
/// integer range of JSON numbers.
#[derive(Debug, Clone, Serialize)]
pub struct PoolSummary {
    pub id: String,
    pub token0: String,
    pub token1: String,
    pub token0_symbol: Option<String>,
    pub token0_decimals: Option<u8>,
    pub token1_symbol: Option<String>,
    pub token1_decimals: Option<u8>,
    pub fee_asset_id: Option<String>,
    /// Present on a pool detail, omitted from the list, where it is the
    /// largest part of every entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_schedule: Option<Vec<FeeScheduleEntry>>,
    pub reserve0: String,
    pub reserve1: String,
    pub lp_supply: String,
    pub k: String,
    pub spot_price: f64,
    pub pot0: String,
    pub pot1: String,
    pub lp_fee_bps: u64,
    pub protocol_fee_share_bps: u64,
    pub block: u32,
    pub first_seen_unix: u64,
    pub last_change_unix: u64,
    pub volume_24h_0: String,
    pub volume_24h_1: String,
    pub lp_fees_24h_0: String,
    pub lp_fees_24h_1: String,
    pub protocol_fees_24h_0: String,
    pub protocol_fees_24h_1: String,
    pub swaps_24h: u64,
    pub adds_24h: u64,
    pub removes_24h: u64,
    pub collects_24h: u64,
    /// Annualised fee yield for liquidity providers, value-weighted across the
    /// two reserves and taken over `fee_apr_window_secs`.
    pub fee_apr: f64,
    /// Seconds of history the yield is annualised from: the pool's age when the
    /// indexer has watched it for less than the aggregate window.
    pub fee_apr_window_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeeScheduleEntry {
    /// Note script root, lowercase hex.
    pub root: String,
    /// Fee amount, decimal string, in the fee asset's smallest unit.
    pub amount: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PoolDetail {
    #[serde(flatten)]
    pub summary: PoolSummary,
    pub history: Vec<SnapshotDto>,
}

/// A history point. Amounts are decimal strings so u64 and u128 survive JSON.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotDto {
    pub block: u32,
    pub timestamp: u64,
    pub polled_at: u64,
    pub reserve0: String,
    pub reserve1: String,
    pub open_reserve0: String,
    pub open_reserve1: String,
    pub high_reserve0: String,
    pub high_reserve1: String,
    pub low_reserve0: String,
    pub low_reserve1: String,
    pub lp_supply: String,
    pub k: String,
    pub pot0: String,
    pub pot1: String,
    pub price0_cumulative: String,
    pub price1_cumulative: String,
}

impl From<&Snapshot> for SnapshotDto {
    fn from(snapshot: &Snapshot) -> Self {
        Self {
            block: snapshot.block,
            timestamp: snapshot.timestamp,
            polled_at: snapshot.polled_at,
            reserve0: snapshot.reserve0.to_string(),
            reserve1: snapshot.reserve1.to_string(),
            open_reserve0: snapshot.open_reserve0.to_string(),
            open_reserve1: snapshot.open_reserve1.to_string(),
            high_reserve0: snapshot.high_reserve0.to_string(),
            high_reserve1: snapshot.high_reserve1.to_string(),
            low_reserve0: snapshot.low_reserve0.to_string(),
            low_reserve1: snapshot.low_reserve1.to_string(),
            lp_supply: snapshot.lp_supply.to_string(),
            k: snapshot.k.to_string(),
            pot0: snapshot.pot0.to_string(),
            pot1: snapshot.pot1.to_string(),
            price0_cumulative: snapshot.price0_cumulative.to_string(),
            price1_cumulative: snapshot.price1_cumulative.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub registry_id: Option<String>,
    pub last_registry_block: Option<u32>,
    pub known_pools: usize,
    pub fetched_pools: usize,
}

fn summary(
    record: &PoolRecord,
    tokens: &HashMap<String, TokenMeta>,
    with_fee_schedule: bool,
) -> PoolSummary {
    let state = &record.state;
    let token_meta = |faucet: &miden_protocol::account::AccountId| {
        tokens
            .get(&faucet.to_hex())
            .map(|meta| (Some(meta.symbol.clone()), Some(meta.decimals)))
            .unwrap_or((None, None))
    };
    let (token0_symbol, token0_decimals) = token_meta(&state.token0_faucet);
    let (token1_symbol, token1_decimals) = token_meta(&state.token1_faucet);
    let aggregates = record.aggregates(AGGREGATE_WINDOW_SECS);
    // Annualise over the history that exists, not over a nominal day. A pool the
    // indexer has watched for three minutes has three minutes of fee income;
    // multiplying it by 480 to reach a year reports noise as a rate. The
    // window travels with the number so a caller can label its basis.
    let fee_apr_window_secs = unix_now()
        .saturating_sub(record.first_seen_unix)
        .min(AGGREGATE_WINDOW_SECS);
    let fee_apr = fee_apr(
        aggregates.lp_fees0,
        aggregates.lp_fees1,
        state.reserve0,
        state.reserve1,
        fee_apr_window_secs,
    );
    let spot_price = if state.reserve0 == 0 {
        0.0
    } else {
        state.reserve1 as f64 / state.reserve0 as f64
    };
    PoolSummary {
        id: state.pool_id.to_hex(),
        token0: state.token0_faucet.to_hex(),
        token1: state.token1_faucet.to_hex(),
        token0_symbol,
        token0_decimals,
        token1_symbol,
        token1_decimals,
        fee_asset_id: record.network_fees.fee_asset_id.map(|id| id.to_hex()),
        fee_schedule: with_fee_schedule.then(|| {
            record
                .network_fees
                .fee_schedule
                .iter()
                .map(|(root, amount)| FeeScheduleEntry {
                    root: root.to_hex(),
                    amount: amount.to_string(),
                })
                .collect()
        }),
        reserve0: state.reserve0.to_string(),
        reserve1: state.reserve1.to_string(),
        lp_supply: state.lp_supply.to_string(),
        k: state.k().to_string(),
        spot_price,
        pot0: state.pot0.to_string(),
        pot1: state.pot1.to_string(),
        lp_fee_bps: state.fees.lp_fee_bps,
        protocol_fee_share_bps: state.fees.protocol_fee_share_bps,
        block: record.block,
        first_seen_unix: record.first_seen_unix,
        last_change_unix: record.last_change_unix,
        volume_24h_0: aggregates.volume0.to_string(),
        volume_24h_1: aggregates.volume1.to_string(),
        lp_fees_24h_0: aggregates.lp_fees0.to_string(),
        lp_fees_24h_1: aggregates.lp_fees1.to_string(),
        protocol_fees_24h_0: aggregates.protocol_fees0.to_string(),
        protocol_fees_24h_1: aggregates.protocol_fees1.to_string(),
        swaps_24h: aggregates.swaps,
        adds_24h: aggregates.adds,
        removes_24h: aggregates.removes,
        collects_24h: aggregates.collects,
        fee_apr,
        fee_apr_window_secs,
    }
}

/// Seconds in a Julian year.
const SECONDS_PER_YEAR: f64 = 31_536_000.0;

/// Annualised fee yield for liquidity providers.
///
/// Fees accrue in whichever token the swapper paid, so both sides are valued in
/// token0 at the pool's own price before the ratio is taken: that price values
/// the two reserves at `2 * reserve0` in token0 terms, which is the capital the
/// yield is taken against. A swap moves the price, which moves the valuation of
/// fees already earned; the pool's price is the only one available without an
/// oracle.
fn fee_apr(fees0: u64, fees1: u64, reserve0: u64, reserve1: u64, window_secs: u64) -> f64 {
    if window_secs == 0 || reserve0 == 0 || reserve1 == 0 {
        return 0.0;
    }
    let fees_in_token0 = fees0 as f64 + fees1 as f64 * reserve0 as f64 / reserve1 as f64;
    let capital_in_token0 = 2.0 * reserve0 as f64;
    fees_in_token0 / capital_in_token0 * SECONDS_PER_YEAR / window_secs as f64
}

async fn list_pools(
    State(api): State<ApiState>,
    Query(query): Query<PoolsQuery>,
) -> Json<PoolPage> {
    let state = api.state.read().await;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let cursor = query.cursor.as_deref().map(normalize);
    let token = query.token.as_deref().map(normalize);

    let mut matched: Vec<&PoolRecord> = state
        .pools
        .values()
        .filter(|record| match &token {
            None => true,
            Some(token) => {
                record.state.token0_faucet.to_hex() == *token
                    || record.state.token1_faucet.to_hex() == *token
            }
        })
        .filter(|record| match &cursor {
            None => true,
            Some(cursor) => record.state.pool_id.to_hex() > *cursor,
        })
        .collect();
    // Keyset pagination needs a total order, so the id is it.
    matched.sort_by_key(|record| record.state.pool_id.to_hex());

    let has_more = matched.len() > limit;
    matched.truncate(limit);
    let next_cursor = has_more.then(|| {
        matched
            .last()
            .map(|record| record.state.pool_id.to_hex())
            .unwrap_or_default()
    });

    Json(PoolPage {
        pools: matched
            .into_iter()
            .map(|record| summary(record, &state.tokens, false))
            .collect(),
        next_cursor,
    })
}

async fn get_pool(
    State(api): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<PoolDetail>, StatusCode> {
    let pool_id = normalize(&id);
    {
        let state = api.state.read().await;
        state.pools.get(&pool_id).ok_or(StatusCode::NOT_FOUND)?;
    }
    let summary = {
        let state = api.state.read().await;
        let record = state.pools.get(&pool_id).ok_or(StatusCode::NOT_FOUND)?;
        summary(record, &state.tokens, true)
    };
    Ok(Json(PoolDetail {
        summary,
        history: history_for(&api.history, &pool_id, 60),
    }))
}

/// Query for the pool list. Keyset pagination: pass the `next_cursor` of the
/// previous page back as `cursor`.
#[derive(Debug, Deserialize)]
pub struct PoolsQuery {
    /// Faucet id, hex or bech32, to keep only pools holding it.
    pub token: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

/// One page of pools.
#[derive(Debug, Serialize)]
pub struct PoolPage {
    pub pools: Vec<PoolSummary>,
    /// Pass back as `cursor` for the next page; absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Default page size, and the largest a caller may ask for.
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 500;

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    /// One of `1m`, `5m`, `1h`. Defaults to `1m`.
    pub resolution: Option<String>,
}

/// Marks a pool due for the next scheduler tick.
///
/// The response is `202` because the fetch happens on the tick, not inline:
/// the endpoint only moves the pool's schedule forward.
async fn refresh_pool(State(api): State<ApiState>, Path(id): Path<String>) -> Response {
    let pool_id = normalize(&id);

    // The pool is checked before the limiter, so an unknown id neither takes a
    // token nor records a key: the cooldown map stays as large as the registry,
    // not as large as the id space a caller can invent.
    if !api.state.read().await.pools.contains_key(&pool_id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let allowed = {
        let Ok(mut limiter) = api.limiter.lock() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        // `allow_pool` is not reached when the bucket is empty, so a throttled
        // request records nothing either.
        limiter.take_token() && limiter.allow_pool(&pool_id)
    };
    if !allowed {
        return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")]).into_response();
    }
    let mut state = api.state.write().await;
    match state.pools.get_mut(&pool_id) {
        Some(record) => {
            record.mark_due();
            StatusCode::ACCEPTED.into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn get_history(
    State(api): State<ApiState>,
    Path(id): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Vec<SnapshotDto>>, StatusCode> {
    let resolution = match query.resolution.as_deref() {
        None | Some("1m") => 60,
        Some("5m") => 300,
        Some("1h") => 3600,
        Some("6h") => 21_600,
        Some("1d") => 86_400,
        Some(_) => return Err(StatusCode::BAD_REQUEST),
    };
    let pool_id = normalize(&id);
    {
        let state = api.state.read().await;
        state.pools.get(&pool_id).ok_or(StatusCode::NOT_FOUND)?;
    }
    Ok(Json(history_for(&api.history, &pool_id, resolution)))
}

/// The newest points the store holds for a pool and resolution.
///
/// Without a database the indexer keeps no history, so the response is empty
/// rather than an error: the pool itself is still served.
fn history_for(history: &History, pool_id: &str, resolution: u64) -> Vec<SnapshotDto> {
    let Some(store) = history else {
        return Vec::new();
    };
    let Ok(store) = store.lock() else {
        tracing::warn!(pool = %pool_id, "history reader lock poisoned");
        return Vec::new();
    };
    match store.history(pool_id, resolution, 2000) {
        Ok(points) => points.iter().map(SnapshotDto::from).collect(),
        Err(err) => {
            tracing::warn!(%err, pool = %pool_id, "history query failed");
            Vec::new()
        }
    }
}

/// Pool ids are keyed by lowercase hex with the `0x` prefix.
fn normalize(id: &str) -> String {
    let id = id.trim();
    if id.starts_with("0x") || id.starts_with("0X") {
        id.to_lowercase()
    } else {
        format!("0x{}", id.to_lowercase())
    }
}

async fn get_status(State(api): State<ApiState>) -> Json<Status> {
    let state = api.state.read().await;
    Json(Status {
        registry_id: state.registry_id.map(|id| id.to_hex()),
        last_registry_block: state.last_registry_block,
        known_pools: state.known.len(),
        fetched_pools: state.pools.len(),
    })
}

/// Cache lifetime for a response, as a `Cache-Control` value.
///
/// The indexer's own view is at most one poll old, so a lifetime longer than a
/// poll adds staleness and nothing else. Nothing a reader acts on is stored:
/// the refresh route, the operational status route, and every non-success. A
/// cached 404 for a pool the registry has not surfaced yet would leave a page
/// saying not found after the pool exists, and a cached 400 would outlive the
/// bad cursor that caused it.
pub fn cache_control(method: &Method, path: &str, query: &str, status: StatusCode) -> &'static str {
    if method == Method::POST || method == Method::OPTIONS || !status.is_success() {
        return "no-store";
    }
    if path == "/status" {
        return "no-store";
    }
    if !path.ends_with("/history") {
        return "public, max-age=5, must-revalidate";
    }
    // A bucket closes within its own period, so the coarse resolutions tolerate a
    // few seconds of staleness where the one minute series does not.
    match resolution(query).as_deref() {
        Some("1h" | "6h" | "1d") => "public, max-age=15, must-revalidate",
        _ => "public, max-age=5, must-revalidate",
    }
}

/// The `resolution` query parameter, if the request carried one.
fn resolution(query: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == "resolution")
        .map(|(_, value)| value.into_owned())
}

/// Applies the cache policy to every response. `Vary` belongs to the CORS layer
/// and is left alone.
async fn cache_headers(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or_default().to_string();
    let mut response = next.run(request).await;
    let value = cache_control(&method, &path, &query, response.status());
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(value));
    response
}

/// Builds the API router.
///
/// CORS is a closed list. The endpoints answer cross-origin only for the origins
/// in `allowed_origins`, and only for GET and POST without credentials. The
/// refresh route moves a pool's poll schedule forward, so a wildcard would let
/// any page a reader visits drive it. An empty list means no cross-origin reads
/// at all, which is the safe default for a service behind a proxy.
pub fn router(api: ApiState, allowed_origins: &[String]) -> Router {
    let origins: Vec<HeaderValue> = allowed_origins
        .iter()
        .filter_map(|origin| match origin.parse::<HeaderValue>() {
            Ok(value) => Some(value),
            Err(err) => {
                tracing::warn!(origin = %origin, %err, "ignoring malformed CORS origin");
                None
            }
        })
        .collect();

    let cors = if origins.is_empty() {
        tracing::info!("no CORS origins configured; the API is same-origin only");
        CorsLayer::new()
    } else {
        tracing::info!(count = origins.len(), "CORS origins allowed");
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins))
            .allow_methods([Method::GET, Method::POST])
            .allow_headers([CONTENT_TYPE])
    };

    Router::new()
        .route("/status", get(get_status))
        .route("/pools", get(list_pools))
        .route("/pools/{id}", get(get_pool))
        .route("/pools/{id}/history", get(get_history))
        .route("/pools/{id}/refresh", post(refresh_pool))
        .layer(cors)
        .layer(middleware::from_fn(cache_headers))
        .with_state(api)
}

/// Serves the API until the process ends.
pub async fn serve(api: ApiState, listen: &str, allowed_origins: &[String]) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(listen = %listen, "HTTP API listening");
    axum::serve(listener, router(api, allowed_origins)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cooldown_denies_a_second_refresh() {
        let mut limiter = Limiter::new();
        assert!(limiter.allow_pool_at("pool-a", 1_000));
        assert!(!limiter.allow_pool_at("pool-a", 1_000));
        assert!(limiter.allow_pool_at("pool-a", 1_005));
    }

    #[test]
    fn token_bucket_bounds_a_burst() {
        let mut limiter = Limiter::new();
        let taken = (0..1000).filter(|_| limiter.take_token_at(1_000)).count();
        assert_eq!(taken, REFRESH_BURST as usize);
        assert!(limiter.take_token_at(1_001));
    }

    /// Twenty keys a second across a five second cooldown is a hundred live
    /// entries, whatever the caller sends.
    #[test]
    fn cooldown_map_is_bounded_by_the_rate_and_the_cooldown() {
        let mut limiter = Limiter::new();
        let mut peak = 0;
        for second in 0..600_u64 {
            let now = 1_000 + second;
            for key in 0..REFRESH_PER_SEC as usize {
                let _ = limiter.allow_pool_at(&format!("pool-{key}"), now);
            }
            peak = peak.max(limiter.tracked_pools());
        }
        assert!(
            peak <= PER_POOL_TRACKED_MAX,
            "cooldown map peaked at {peak}, bound is {PER_POOL_TRACKED_MAX}"
        );
    }

    /// A frozen clock keeps every entry live, so only the guard bounds the map.
    #[test]
    fn cooldown_map_is_bounded_when_the_clock_does_not_advance() {
        let mut limiter = Limiter::new();
        for key in 0..PER_POOL_TRACKED_MAX * 4 {
            let _ = limiter.allow_pool_at(&format!("pool-{key}"), 1_000);
        }
        assert!(limiter.tracked_pools() <= PER_POOL_TRACKED_MAX);
    }

    #[test]
    fn cache_policy_is_bounded_by_the_poll() {
        let (get, ok) = (Method::GET, StatusCode::OK);
        assert_eq!(
            cache_control(&get, "/pools", "", ok),
            "public, max-age=5, must-revalidate"
        );
        assert_eq!(
            cache_control(&get, "/pools/0xabc", "", ok),
            "public, max-age=5, must-revalidate"
        );
        assert_eq!(
            cache_control(&get, "/pools/0xabc/history", "resolution=1m", ok),
            "public, max-age=5, must-revalidate"
        );
        assert_eq!(
            cache_control(&get, "/pools/0xabc/history", "resolution=1d", ok),
            "public, max-age=15, must-revalidate"
        );
    }

    #[test]
    fn nothing_a_reader_acts_on_is_stored() {
        assert_eq!(
            cache_control(
                &Method::POST,
                "/pools/0xabc/refresh",
                "",
                StatusCode::ACCEPTED
            ),
            "no-store"
        );
        assert_eq!(
            cache_control(&Method::GET, "/pools/0xabc", "", StatusCode::NOT_FOUND),
            "no-store"
        );
        assert_eq!(
            cache_control(
                &Method::GET,
                "/pools",
                "cursor=bad",
                StatusCode::BAD_REQUEST
            ),
            "no-store"
        );
        assert_eq!(
            cache_control(&Method::GET, "/status", "", StatusCode::OK),
            "no-store"
        );
        assert_eq!(
            cache_control(&Method::OPTIONS, "/pools", "", StatusCode::OK),
            "no-store"
        );
        assert_eq!(
            cache_control(
                &Method::GET,
                "/pools",
                "",
                StatusCode::INTERNAL_SERVER_ERROR
            ),
            "no-store"
        );
    }
}
