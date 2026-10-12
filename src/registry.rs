//! Registry decode.
//!
//! The registry account holds three storage maps. The `pools` map yields the
//! canonical pool set; `assets_to_pool` yields the ordered asset pair index.

use anyhow::{Context, Result};
use miden_node_proto::domain::account::{AccountStorageMapDetails, StorageMapEntries};
use miden_node_proto::generated::miden::node::v1::StorageMapUpdate;
use miden_protocol::Word;
use miden_protocol::account::AccountId;

pub const POOLS_SLOT: &str = "pool_registry::pool_registry::pools";
pub const ASSETS_TO_POOL_SLOT: &str = "pool_registry::pool_registry::assets_to_pool";

/// A registry storage map holding more entries than one account read returns.
///
/// The node caps an inline map read at a thousand entries and reports the cap
/// rather than a partial map. The cap is what tells the caller to read the map
/// through the paginated `SyncAccountStorageMaps` endpoint instead, which has no
/// entry limit.
#[derive(Debug)]
pub struct OverCapacity {
    pub slot: &'static str,
}

impl std::fmt::Display for OverCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} holds more entries than one account read returns",
            self.slot
        )
    }
}

impl std::error::Error for OverCapacity {}

/// One registered pool and its recorded code commitment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryEntry {
    pub pool_id: AccountId,
    pub code_commitment: Word,
}

fn all_entries<'a>(
    details: &'a [AccountStorageMapDetails],
    slot: &'static str,
) -> Result<&'a [(miden_protocol::account::StorageMapKey, Word)]> {
    let name = miden_protocol::account::StorageSlotName::new(slot.to_string())
        .with_context(|| format!("invalid registry slot name: {slot}"))?;
    let detail = details
        .iter()
        .find(|detail| detail.slot_name == name)
        .ok_or_else(|| anyhow::anyhow!("registry response is missing slot {slot}"))?;

    match &detail.entries {
        StorageMapEntries::AllEntries(entries) => Ok(entries),
        StorageMapEntries::LimitExceeded => Err(OverCapacity { slot }.into()),
        StorageMapEntries::PartialMap { .. } => {
            anyhow::bail!("{slot} returned a partial map; all entries are required")
        }
    }
}

/// Converts one update's key and value, skipping anything that is not a word.
fn update_words(update: &StorageMapUpdate) -> Option<(Word, Word)> {
    let key = Word::try_from(update.key.as_ref()?).ok()?;
    let value = Word::try_from(update.value.as_ref()?).ok()?;
    if value == Word::empty() {
        // A removal carries the empty word. The registry removes nothing, and a
        // removal must not be read as a code commitment.
        return None;
    }
    Some((key, value))
}

/// Decodes the `pools` map from `SyncAccountStorageMaps` updates.
///
/// The updates arrive in block order and repeat across an overlapping range,
/// which discovery absorbs: a pool already known is not discovered twice.
pub fn decode_pool_updates(updates: &[StorageMapUpdate]) -> Result<Vec<RegistryEntry>> {
    updates
        .iter()
        .filter(|update| update.slot_name == POOLS_SLOT)
        .filter_map(update_words)
        .map(|(key, commitment)| {
            Ok(RegistryEntry {
                pool_id: pool_id_from_key(key)?,
                code_commitment: commitment,
            })
        })
        .collect()
}

/// Decodes the `assets_to_pool` map from `SyncAccountStorageMaps` updates.
pub fn decode_asset_pair_updates(
    updates: &[StorageMapUpdate],
) -> Result<Vec<((AccountId, AccountId), AccountId)>> {
    updates
        .iter()
        .filter(|update| update.slot_name == ASSETS_TO_POOL_SLOT)
        .filter_map(update_words)
        .map(|(key, value)| {
            Ok((
                (
                    AccountId::try_from_elements(key[1], key[0])
                        .context("asset pair key is not a valid account id")?,
                    AccountId::try_from_elements(key[3], key[2])
                        .context("asset pair key is not a valid account id")?,
                ),
                pool_id_from_key(value)?,
            ))
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use miden_objects::proto::primitives;
    use miden_protocol::Felt;

    /// A well-formed public account id, used only as one.
    const POOL: &str = "0xc6c6d66253c8a351702f9871c62c8a";
    const SECOND: &str = "0x1c4a686390cb8a5134ee15d1fae627";

    fn felt(value: u64) -> Felt {
        Felt::new(value).expect("a value under the modulus")
    }

    fn account(hex: &str) -> AccountId {
        AccountId::from_hex(hex).expect("well-formed account id")
    }

    /// The key the registry uses for a pool id: its two id felts, prefix first,
    /// then two zeros. `AccountId::try_from_elements(suffix, prefix)` reads them
    /// back in that order.
    fn pool_key(id: AccountId) -> Word {
        Word::new([
            felt(id.prefix().as_u64()),
            id.suffix(),
            Felt::ZERO,
            Felt::ZERO,
        ])
    }

    fn commitment() -> Word {
        Word::new([felt(1), felt(2), felt(3), felt(4)])
    }

    fn update(slot: &str, key: Word, value: Word) -> StorageMapUpdate {
        StorageMapUpdate {
            block_num: 11,
            slot_name: slot.to_string(),
            key: Some(primitives::Word::from(key)),
            value: Some(primitives::Word::from(value)),
        }
    }

    #[test]
    fn pool_updates_decode_to_entries() {
        let id = account(POOL);
        let updates = [update(POOLS_SLOT, pool_key(id), commitment())];
        let entries = decode_pool_updates(&updates).expect("decode");
        assert_eq!(
            entries,
            vec![RegistryEntry {
                pool_id: id,
                code_commitment: commitment(),
            }]
        );
    }

    #[test]
    fn pair_updates_decode_to_pairs() {
        let token0 = account(POOL);
        let token1 = account(SECOND);
        let pool = account(SECOND);
        let key = Word::new([
            felt(token0.prefix().as_u64()),
            token0.suffix(),
            felt(token1.prefix().as_u64()),
            token1.suffix(),
        ]);
        let updates = [update(ASSETS_TO_POOL_SLOT, key, pool_key(pool))];
        let pairs = decode_asset_pair_updates(&updates).expect("decode");
        assert_eq!(pairs, vec![((token0, token1), pool)]);
    }

    #[test]
    fn other_slots_and_removals_are_skipped() {
        let id = account(POOL);
        let updates = [
            update(
                "other_package::other_package::pools",
                pool_key(id),
                commitment(),
            ),
            update(POOLS_SLOT, pool_key(id), Word::empty()),
        ];
        assert!(decode_pool_updates(&updates).expect("decode").is_empty());
    }

    #[test]
    fn an_over_capacity_map_asks_for_the_sync_endpoint() {
        let details = [AccountStorageMapDetails {
            slot_name: miden_protocol::account::StorageSlotName::new(POOLS_SLOT.to_string())
                .expect("slot name"),
            entries: StorageMapEntries::LimitExceeded,
        }];
        let err = decode_registry(&details).expect_err("the cap must be reported");
        let over = err
            .downcast_ref::<OverCapacity>()
            .expect("typed cap error, so the caller can sync instead of failing");
        assert_eq!(over.slot, POOLS_SLOT);
    }
}
