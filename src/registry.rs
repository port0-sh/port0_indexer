//! Registry decode.
//!
//! The registry account holds three storage maps. The `pools` map yields the
//! canonical pool set; `assets_to_pool` yields the ordered asset pair index.

use anyhow::{Context, Result, bail};
use miden_node_proto::domain::account::{AccountStorageMapDetails, StorageMapEntries};
use miden_protocol::Word;
use miden_protocol::account::AccountId;

pub const POOLS_SLOT: &str = "pool_registry::pool_registry::pools";
pub const ASSETS_TO_POOL_SLOT: &str = "pool_registry::pool_registry::assets_to_pool";

/// One registered pool and its recorded code commitment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryEntry {
    pub pool_id: AccountId,
    pub code_commitment: Word,
}

fn all_entries<'a>(
    details: &'a [AccountStorageMapDetails],
    slot: &str,
) -> Result<&'a [(miden_protocol::account::StorageMapKey, Word)]> {
    let name = miden_protocol::account::StorageSlotName::new(slot.to_string())
        .with_context(|| format!("invalid registry slot name: {slot}"))?;
    let detail = details
        .iter()
        .find(|detail| detail.slot_name == name)
        .ok_or_else(|| anyhow::anyhow!("registry response is missing slot {slot}"))?;

    match &detail.entries {
        StorageMapEntries::AllEntries(entries) => Ok(entries),
        StorageMapEntries::LimitExceeded => {
            bail!("{slot} exceeds the response limit; page it with SyncAccountStorageMaps")
        }
        StorageMapEntries::PartialMap { .. } => {
            bail!("{slot} returned a partial map; all entries are required")
        }
    }
}

fn pool_id_from_key(key: Word) -> Result<AccountId> {
    AccountId::try_from_elements(key[1], key[0]).context("registry key is not a valid account id")
}

/// Decodes the `pools` map into pool ids and code commitments.
pub fn decode_registry(details: &[AccountStorageMapDetails]) -> Result<Vec<RegistryEntry>> {
    all_entries(details, POOLS_SLOT)?
        .iter()
        .map(|(key, commitment)| {
            let key_word: Word = (*key).into();
            Ok(RegistryEntry {
                pool_id: pool_id_from_key(key_word)?,
                code_commitment: *commitment,
            })
        })
        .collect()
}

/// Decodes the `assets_to_pool` map into ordered asset pairs and pool ids.
pub fn decode_asset_pairs(
    details: &[AccountStorageMapDetails],
) -> Result<Vec<((AccountId, AccountId), AccountId)>> {
    all_entries(details, ASSETS_TO_POOL_SLOT)?
        .iter()
        .map(|(key, value)| {
            let key_word: Word = (*key).into();
            let value_word = *value;
            let asset0 = AccountId::try_from_elements(key_word[1], key_word[0])
                .context("asset pair key is not a valid account id")?;
            let asset1 = AccountId::try_from_elements(key_word[3], key_word[2])
                .context("asset pair key is not a valid account id")?;
            let pool_id = pool_id_from_key(value_word)?;
            Ok(((asset0, asset1), pool_id))
        })
        .collect()
}
