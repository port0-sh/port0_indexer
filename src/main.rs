//! port0.sh pool indexer.
//!
//! Reads the chain through the node RPC only. No Miden client and no store.
//! Polls the registry for pool ids and each pool for state, keeps a bounded
//! in-memory history, and serves the result over HTTP.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use miden_node_proto::clients::RpcClient;
use miden_node_proto::generated::miden::node::v1::BlockSubscriptionRequest;
use miden_protocol::account::AccountId;
use tokio::sync::RwLock;

use port0_indexer::state::IndexState;

struct Config {
    rpc_url: String,
    registry_id: AccountId,
    listen: String,
    pool_poll: Duration,
    registry_poll: Duration,
    /// SQLite path, or empty for an index that forgets everything on restart.
    db: String,
    /// Keep snapshots forever instead of pruning them to the RAM window.
    keep_all_snapshots: bool,
    /// Origins the API answers cross-origin, from `INDEXER_ALLOWED_ORIGINS`.
    allowed_origins: Vec<String>,
}

impl Config {
    fn from_env() -> Result<Self> {
        let rpc_url = std::env::var("MIDEN_RPC_URL")
            .unwrap_or_else(|_| "https://rpc.testnet.miden.io".into());
        let registry_hex =
            std::env::var("PORT0_REGISTRY_ID").context("PORT0_REGISTRY_ID is required")?;
        let registry_id =
            AccountId::from_hex(&registry_hex).context("invalid PORT0_REGISTRY_ID")?;
        // Loopback by default: the API is reached through a reverse proxy, and a
        // host that forgets to set this should not expose it on every interface.
        let listen = std::env::var("INDEXER_LISTEN").unwrap_or_else(|_| "127.0.0.1:8080".into());
        let pool_poll = Duration::from_secs(env_secs("POOL_POLL_SECS", 15));
        let registry_poll = Duration::from_secs(env_secs("REGISTRY_POLL_SECS", 60));
        let db = std::env::var("INDEXER_DB").unwrap_or_else(|_| "port0_indexer.sqlite".into());
        let keep_all_snapshots = std::env::var("INDEXER_SNAPSHOT_RETENTION")
            .map(|value| value.eq_ignore_ascii_case("all"))
            .unwrap_or(false);
        let allowed_origins = env_list("INDEXER_ALLOWED_ORIGINS");
        Ok(Self {
            rpc_url,
            registry_id,
            listen,
            pool_poll,
            registry_poll,
            db,
            keep_all_snapshots,
            allowed_origins,
        })
    }
}

/// Reads a comma-separated list, dropping empty entries.
fn env_list(name: &str) -> Vec<String> {
    std::env::var(name)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn env_secs(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load `.env` from the current directory when present. Real environment
    // variables take precedence; a missing file is not an error.
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "port0_indexer=info".into()),
        )
        .init();

    let config = Config::from_env()?;
    let client = port0_indexer::rpc::connect(&config.rpc_url, Duration::from_secs(10)).await?;
    tracing::info!(
        rpc = %config.rpc_url,
        registry = %config.registry_id.to_hex(),
        pool_poll_secs = config.pool_poll.as_secs(),
        registry_poll_secs = config.registry_poll.as_secs(),
        "indexer starting"
    );

    let mut state = IndexState::new(config.registry_id);
    // Start the block stream at the present. Replaying from genesis would walk
    // every block once for no benefit: pools are discovered by the registry,
    // and only blocks after this point can move a known pool.
    if let Ok(tip) = port0_indexer::rpc::chain_tip(&mut client.clone()).await {
        state.last_processed_block = tip;
    }
    let state = Arc::new(RwLock::new(state));

    // Open the store and replay its history before the first poll, so a restart
    // continues the series instead of starting it empty.
    let mut store = if config.db.is_empty() || config.db == "off" {
        tracing::info!("INDEXER_DB is empty; history will not survive a restart");
        None
    } else {
        let retention = {
            let guard = state.read().await;
            let capped = guard.retention();
            if config.keep_all_snapshots {
                capped
                    .into_iter()
                    .map(|(period, _)| (period, None))
                    .collect()
            } else {
                capped
            }
        };
        let opened =
            port0_indexer::store::Store::open(&config.db, retention, ACTIVITY_RETENTION_SECS)?;
        let loaded = opened.load()?;
        let pools = loaded.pools.len();
        let snapshots: usize = loaded
            .pools
            .iter()
            .map(|pool| {
                pool.buckets
                    .iter()
                    .map(|(_, points)| points.len())
                    .sum::<usize>()
            })
            .sum();
        {
            let mut guard = state.write().await;
            guard.replay_loaded(loaded);
        }
        tracing::info!(
            db = %config.db,
            pools,
            snapshots,
            snapshots_kept = if config.keep_all_snapshots {
                "every bucket, forever"
            } else {
                "the ladder: 12h of 1m, 7d of 5m, 30d of 1h, 180d of 6h, 1d forever"
            },
            "sqlite store open"
        );
        Some(opened)
    };

    // The API reads history over its own connection to the same file. WAL
    // allows one writer and many readers, so neither side waits on the other.
    let history = match &store {
        Some(_) if !config.db.is_empty() && config.db != "off" => {
            let reader = port0_indexer::store::Store::open_read_only(&config.db)?;
            Some(Arc::new(std::sync::Mutex::new(reader)))
        }
        _ => None,
    };
    let api_state = port0_indexer::api::ApiState {
        state: Arc::clone(&state),
        history,
        limiter: Arc::new(std::sync::Mutex::new(port0_indexer::api::Limiter::new())),
    };
    let listen = config.listen.clone();
    let allowed_origins = config.allowed_origins.clone();
    tokio::spawn(async move {
        if let Err(err) = port0_indexer::api::serve(api_state, &listen, &allowed_origins).await {
            tracing::error!(%err, "HTTP API exited");
        }
    });

    let registry_state = Arc::clone(&state);
    let registry_id = config.registry_id;
    let registry_poll = config.registry_poll;
    let mut registry_client = client.clone();
    tokio::spawn(async move {
        loop {
            match poll_registry(&mut registry_client, registry_id, &registry_state).await {
                Ok(new_pools) => tracing::info!(new_pools, "registry polled"),
                Err(err) => tracing::warn!(%err, "registry poll failed"),
            }
            tokio::time::sleep(registry_poll).await;
        }
    });

    // Block-driven refresh. The stream makes the common case cheap: a block
    // names the accounts it touched, so only those pools are fetched. The
    // adaptive sweep stays as the fallback for when the stream is down.
    let block_state = Arc::clone(&state);
    let mut block_client = client.clone();
    tokio::spawn(async move {
        follow_blocks(&mut block_client, block_state).await;
    });

    let mut pool_client = client.clone();
    // Scheduler tick. Per-pool cadence comes from `POLL_TIERS`, so this only
    // bounds how late a due pool can be, not how often the network is used.
    let pool_poll = Duration::from_secs(config.pool_poll.as_secs().clamp(1, 5));
    let mut cycles: u64 = 0;
    loop {
        match poll_pools(&mut pool_client, &state).await {
            Ok(updated) => tracing::info!(updated, "pools polled"),
            Err(err) => tracing::warn!(%err, "pool poll failed"),
        }

        if let Some(store) = store.as_mut() {
            // Take the write lock only long enough to snapshot the model and
            // clear the activity queue; the SQLite write runs with it released.
            let cycle = {
                let mut guard = state.write().await;
                guard.drain_persist()
            };
            match store.write(&cycle) {
                Ok(rows) => tracing::debug!(rows, activities = cycle.activities.len(), "persisted"),
                Err(err) => tracing::warn!(%err, "persist failed"),
            }
            cycles += 1;
            if cycles.is_multiple_of(20)
                && let (Ok(bytes), Ok(counts)) = (store.bytes_on_disk(), store.counts())
            {
                tracing::info!(bytes, ?counts, "store size");
            }
        }

        tokio::time::sleep(pool_poll).await;
    }
}

/// Streams committed blocks, marking every pool a block touched as due.
///
/// A dropped stream is not fatal: the loop reconnects from the last processed
/// block, and the adaptive sweep keeps polls flowing while it is down.
async fn follow_blocks(client: &mut RpcClient, state: Arc<RwLock<IndexState>>) {
    loop {
        let from = {
            let guard = state.read().await;
            guard.last_processed_block.saturating_add(1)
        };
        match client
            .block_subscription(BlockSubscriptionRequest { block_from: from })
            .await
        {
            Ok(response) => {
                let mut stream = response.into_inner();
                while let Ok(Some(message)) = stream.message().await {
                    if let Some(block) = message.block.as_ref() {
                        mark_touched(block, &state).await;
                    }
                    let mut guard = state.write().await;
                    guard.last_processed_block = message.committed_chain_tip;
                }
                tracing::info!("block stream ended; reconnecting");
            }
            Err(err) => tracing::warn!(%err, "block subscription failed"),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Marks every known pool whose account the block changed as due for a poll.
async fn mark_touched(
    block: &miden_objects::proto::blockchain::SignedBlock,
    state: &Arc<RwLock<IndexState>>,
) {
    let Some(body) = block.body.as_ref() else {
        return;
    };
    let touched: Vec<(u64, u64)> = body
        .updated_accounts
        .iter()
        .filter_map(|update| update.account_id.as_ref())
        .filter_map(account_id_felts)
        .collect();
    if touched.is_empty() {
        return;
    }
    let mut guard = state.write().await;
    for record in guard.pools.values_mut() {
        let id = record.state.pool_id;
        // `prefix()` wraps the versioned prefix; `as_u64` is its raw value,
        // which is the felt the proto id carries.
        let key = (id.prefix().as_u64(), id.suffix().as_canonical_u64());
        if touched.contains(&key) {
            record.mark_due();
        }
    }
}

/// `(prefix, suffix)` of a proto account id, which is how a decoded account id
/// compares against one: the two share these felts, whatever their hex form.
fn account_id_felts(id: &miden_objects::proto::account::AccountId) -> Option<(u64, u64)> {
    match id.version.as_ref()? {
        miden_objects::proto::account::account_id::Version::V1(v1) => {
            Some((v1.prefix.as_ref()?.value, v1.suffix.as_ref()?.value))
        }
    }
}

/// Activities are kept this long. They feed the 24 h aggregates; seven days
/// leaves room to recompute them after a restart without unbounded growth.
const ACTIVITY_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

/// Polls the registry and records newly discovered pool ids and asset pairs.
async fn poll_registry(
    client: &mut RpcClient,
    registry_id: AccountId,
    state: &Arc<RwLock<IndexState>>,
) -> Result<usize> {
    match read_registry(client, registry_id, state).await {
        Ok(discovered) => Ok(discovered),
        Err(err) => {
            let over = err
                .downcast_ref::<port0_indexer::registry::OverCapacity>()
                .map(|over| over.slot);
            match over {
                Some(slot) => {
                    tracing::info!(
                        slot,
                        "registry map is over the inline read cap; syncing by block range"
                    );
                    sync_registry(client, registry_id, state).await
                }
                None => Err(err),
            }
        }
    }
}

/// One account read of the registry, for a registry inside the node's inline cap.
async fn read_registry(
    client: &mut RpcClient,
    registry_id: AccountId,
    state: &Arc<RwLock<IndexState>>,
) -> Result<usize> {
    let account = port0_indexer::rpc::fetch_account_with_maps(client, registry_id).await?;
    let details = account.details.context("registry account has no details")?;

    let entries = port0_indexer::registry::decode_registry(&details.storage_details.map_details)?;
    let pairs = port0_indexer::registry::decode_asset_pairs(&details.storage_details.map_details)?;

    let mut guard = state.write().await;
    guard.last_registry_block = Some(account.block_num.as_u32());
    Ok(apply_registry(&mut guard, entries, pairs))
}

/// Discovers pools from the registry's map updates, which carry no entry cap.
///
/// The range starts at the latest block a registry read covered, or at the
/// earliest block a known pool was seen in, which the store holds. A restart
/// therefore resumes without a second cursor, and discovery is idempotent, so an
/// overlapping range costs one read and nothing else.
async fn sync_registry(
    client: &mut RpcClient,
    registry_id: AccountId,
    state: &Arc<RwLock<IndexState>>,
) -> Result<usize> {
    let from = {
        let guard = state.read().await;
        guard
            .last_registry_block
            .or_else(|| guard.pools.values().map(|record| record.block).min())
            .unwrap_or_default()
    };

    let (updates, covered) =
        port0_indexer::rpc::sync_account_storage_maps(client, registry_id, from).await?;
    let entries = port0_indexer::registry::decode_pool_updates(&updates)?;
    let pairs = port0_indexer::registry::decode_asset_pair_updates(&updates)?;

    let mut guard = state.write().await;
    guard.last_registry_block = Some(covered);
    let discovered = apply_registry(&mut guard, entries, pairs);
    tracing::info!(
        from,
        covered,
        updates = updates.len(),
        discovered,
        "registry synced"
    );
    Ok(discovered)
}

/// Applies a registry read or a registry sync to the state.
fn apply_registry(
    guard: &mut IndexState,
    entries: Vec<port0_indexer::registry::RegistryEntry>,
    pairs: Vec<((AccountId, AccountId), AccountId)>,
) -> usize {
    let mut discovered = 0;
    for entry in entries {
        if guard.discover(entry) {
            discovered += 1;
        }
    }
    for ((asset0, asset1), pool_id) in pairs {
        guard.set_pair(asset0, asset1, pool_id);
    }
    discovered
}

/// Fetches every known pool that is due, applying its state.
///
/// The sweep runs on a short tick, but a pool is only fetched when its own
/// cadence says so, so an idle pool costs nothing between polls.
async fn poll_pools(client: &mut RpcClient, state: &Arc<RwLock<IndexState>>) -> Result<usize> {
    let now = port0_indexer::state::unix_now();
    let ids: Vec<AccountId> = {
        let guard = state.read().await;
        guard
            .known
            .values()
            .filter(|id| {
                guard
                    .pools
                    .get(&id.to_hex())
                    .map(|record| record.due(now))
                    .unwrap_or(true)
            })
            .copied()
            .collect()
    };
    if ids.is_empty() {
        return Ok(0);
    }
    tracing::debug!(
        due = ids.len(),
        pools = state.read().await.pools.len(),
        "polling due pools"
    );

    let mut updated = 0;
    for id in ids {
        let account = match port0_indexer::rpc::fetch_account_with_maps(client, id).await {
            Ok(account) => account,
            Err(err) => {
                tracing::warn!(pool = %id.to_hex(), %err, "pool fetch failed");
                continue;
            }
        };
        let Some(details) = account.details else {
            continue;
        };

        let faucets: Option<[miden_protocol::account::AccountId; 2]> = {
            let mut guard = state.write().await;
            if guard.apply_fetch(
                id,
                account.block_num.as_u32(),
                &details.storage_details.header,
                &details.vault_details,
                &details.storage_details.map_details,
            )? {
                updated += 1;
            }
            // After the apply, so a change resets the cadence rather than
            // rescheduling from the idle time the change just invalidated.
            guard.pools.get_mut(&id.to_hex()).map(|record| {
                record.reschedule(port0_indexer::state::unix_now());
                [record.state.token0_faucet, record.state.token1_faucet]
            })
        };

        for faucet in faucets.into_iter().flatten() {
            if state.read().await.tokens.contains_key(&faucet.to_hex()) {
                continue;
            }
            match port0_indexer::rpc::fetch_account(client, faucet).await {
                Ok(faucet_account) => {
                    let Some(faucet_details) = faucet_account.details else {
                        continue;
                    };
                    match port0_indexer::decode::decode_token_meta(
                        &faucet_details.storage_details.header,
                    ) {
                        Ok(meta) => {
                            state.write().await.tokens.insert(faucet.to_hex(), meta);
                        }
                        Err(err) => {
                            tracing::warn!(faucet = %faucet.to_hex(), %err, "token metadata decode failed");
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(faucet = %faucet.to_hex(), %err, "faucet fetch failed");
                }
            }
        }
    }
    Ok(updated)
}
