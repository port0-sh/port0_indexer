//! Probes the registry storage-map sync endpoint against a live node.
//!
//! Checks the two things the indexer depends on when a registry outgrows the
//! inline map read cap: that the node accepts a block range and that
//! `pagination_info` reports the block the page ended at. Reads only, writes
//! nothing, and contacts no other endpoint.
//!
//! Run with: `cargo run --bin probe_registry_sync [-- <registry id hex>]`
//! The registry id is read from the first argument, then from
//! `MIDEN_XYC_REGISTRY_ID` (a `.env` file in this directory is loaded).

use std::time::Duration;

use anyhow::{Context, Result};
use miden_protocol::account::AccountId;

/// Blocks before the tip to ask about by default. A window rather than the whole
/// chain, so the probe stays cheap; the endpoint's paging is exercised either
/// way. A second argument overrides it, which is how a window containing a
/// registration is reached.
const DEFAULT_WINDOW: u32 = 200;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    // An empty first argument means "take the id from the environment", so a
    // window can be given without repeating the id.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let registry_hex = args
        .first()
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .or_else(|| std::env::var("MIDEN_XYC_REGISTRY_ID").ok())
        .context("registry id: pass one as an argument or set MIDEN_XYC_REGISTRY_ID")?;
    let registry = AccountId::from_hex(&registry_hex).context("invalid registry account id")?;
    let rpc_url =
        std::env::var("MIDEN_RPC_URL").unwrap_or_else(|_| "https://rpc.testnet.miden.io".into());

    let mut client = miden_xyk_indexer::rpc::connect(&rpc_url, Duration::from_secs(30)).await?;
    let window: u32 = args
        .get(1)
        .map(|value| value.parse().context("window is a block count"))
        .transpose()?
        .unwrap_or(DEFAULT_WINDOW);

    let tip = miden_xyk_indexer::rpc::chain_tip(&mut client).await?;
    let from = tip.saturating_sub(window);

    let (updates, covered) =
        miden_xyk_indexer::rpc::sync_account_storage_maps(&mut client, registry, from).await?;

    println!("registry {registry_hex}");
    println!(
        "chain tip {tip}, window {from}..={tip}, updates {}, covered {covered}",
        updates.len()
    );
    for update in updates.iter().take(8) {
        println!("  block {} slot {}", update.block_num, update.slot_name);
    }

    let pools = miden_xyk_indexer::registry::decode_pool_updates(&updates)?;
    let pairs = miden_xyk_indexer::registry::decode_asset_pair_updates(&updates)?;
    println!(
        "decoded: {} pool updates, {} pair updates",
        pools.len(),
        pairs.len()
    );
    for entry in pools.iter().take(3) {
        println!("  pool {}", entry.pool_id.to_hex());
    }
    Ok(())
}
