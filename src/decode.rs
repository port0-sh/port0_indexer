//! Pool state decode from the RPC account response.
//!
//! The Rust port of the SDK's `pool/decoding.ts` and `pool/reads.ts`. The pool
//! stores eleven value slots; reserves equal the vault balances of token0 and
//! token1 minus the protocol pot.

use anyhow::{Context, Result, anyhow};
use miden_node_proto::domain::account::{
    AccountStorageMapDetails, AccountVaultDetails, StorageMapEntries,
};
use miden_protocol::Felt;
use miden_protocol::Word;
use miden_protocol::account::{AccountId, AccountStorageHeader, StorageSlotName};
use miden_protocol::asset::TokenSymbol;

/// Storage slot name prefix shared by every XYK pool slot.
pub const SLOT_PREFIX: &str = "miden_xyk::miden_xyk";

fn slot_name(field: &str) -> Result<StorageSlotName> {
    StorageSlotName::new(format!("{SLOT_PREFIX}::{field}"))
        .with_context(|| format!("invalid pool slot name: {field}"))
}

fn zero_word() -> Word {
    Word::new([Felt::ZERO, Felt::ZERO, Felt::ZERO, Felt::ZERO])
}

fn slot_word(header: &AccountStorageHeader, field: &str) -> Result<Option<Word>> {
    let name = slot_name(field)?;
    Ok(header
        .find_slot_header_by_name(&name)
        .map(|slot| slot.value()))
}

fn require_word(header: &AccountStorageHeader, field: &str) -> Result<Word> {
    slot_word(header, field)?.ok_or_else(|| anyhow!("pool is missing slot {field}"))
}

/// Fee configuration read from the `fees` slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolFees {
    pub lp_fee_bps: u64,
    pub protocol_fee_share_bps: u64,
}

/// Derived pool state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolState {
    pub pool_id: AccountId,
    pub token0_key: Word,
    pub token1_key: Word,
    pub token0_faucet: AccountId,
    pub token1_faucet: AccountId,
    pub lp_supply: u64,
    pub fees: PoolFees,
    pub pot0: u64,
    pub pot1: u64,
    pub price0_cumulative_last: u128,
    pub price1_cumulative_last: u128,
    pub block_timestamp_last: u64,
    pub reserve0: u64,
    pub reserve1: u64,
}

impl PoolState {
    /// Constant product of the reserves.
    pub fn k(&self) -> u128 {
        self.reserve0 as u128 * self.reserve1 as u128
    }
}

/// Decodes a pool from its storage header and vault details.
pub fn decode_pool_state(
    pool_id: AccountId,
    header: &AccountStorageHeader,
    vault: &AccountVaultDetails,
) -> Result<PoolState> {
    let token0_key = require_word(header, "token0_asset_key")?;
    let token1_key = require_word(header, "token1_asset_key")?;
    let token0_faucet = faucet_of(&token0_key)?;
    let token1_faucet = faucet_of(&token1_key)?;

    let lp_supply = scalar(header, "lp_supply")?;

    let fees_word = slot_word(header, "fees")?.unwrap_or_else(zero_word);
    // The protocol's fee destination lives on the registry, not in the pool, so
    // only the two rates come from this word.
    let fees = PoolFees {
        lp_fee_bps: fees_word[0].as_canonical_u64(),
        protocol_fee_share_bps: fees_word[1].as_canonical_u64(),
    };

    let pot = slot_word(header, "protocol_pot")?.unwrap_or_else(zero_word);
    let pot0 = pot[0].as_canonical_u64();
    let pot1 = pot[1].as_canonical_u64();

    let price0 = u128_word(slot_word(header, "price0_cumulative_last")?.unwrap_or_else(zero_word));
    let price1 = u128_word(slot_word(header, "price1_cumulative_last")?.unwrap_or_else(zero_word));
    let block_timestamp_last = scalar(header, "block_timestamp_last")?;

    let (vault0, vault1) = vault_balances(vault, token0_faucet, token1_faucet)?;
    let reserve0 = vault0
        .checked_sub(pot0)
        .ok_or_else(|| anyhow!("vault balance of token0 is below the protocol pot"))?;
    let reserve1 = vault1
        .checked_sub(pot1)
        .ok_or_else(|| anyhow!("vault balance of token1 is below the protocol pot"))?;

    Ok(PoolState {
        pool_id,
        token0_key,
        token1_key,
        token0_faucet,
        token1_faucet,
        lp_supply,
        fees,
        pot0,
        pot1,
        price0_cumulative_last: price0,
        price1_cumulative_last: price1,
        block_timestamp_last,
        reserve0,
        reserve1,
    })
}

fn scalar(header: &AccountStorageHeader, field: &str) -> Result<u64> {
    let word = slot_word(header, field)?.unwrap_or_else(zero_word);
    Ok(word[0].as_canonical_u64())
}

/// Reads a `u128` stored as four little-endian 32-bit limbs.
fn u128_word(word: Word) -> u128 {
    (word[0].as_canonical_u64() as u128)
        | ((word[1].as_canonical_u64() as u128) << 32)
        | ((word[2].as_canonical_u64() as u128) << 64)
        | ((word[3].as_canonical_u64() as u128) << 96)
}

/// Recovers the issuing faucet id from a fungible asset key word
/// `[0, 0, faucet_suffix | 0x11, faucet_prefix]`.
fn faucet_of(asset_key: &Word) -> Result<AccountId> {
    let suffix = asset_key[2].as_canonical_u64() & !0xff;
    AccountId::try_from_elements(
        Felt::new(suffix).expect("masked suffix is canonical"),
        asset_key[3],
    )
    .context("asset key does not encode a valid faucet id")
}

/// Reads the two reserve balances from a vault.
///
/// A vault the node refused to enumerate is an error, not an empty vault. Both
/// look like zero balances to a caller that ignores the distinction, and a pool
/// reported with zero reserves reads as a drained pool.
fn vault_balances(
    vault: &AccountVaultDetails,
    token0: AccountId,
    token1: AccountId,
) -> Result<(u64, u64)> {
    let AccountVaultDetails::Assets(assets) = vault else {
        // The node caps the asset list per response; a pool holds exactly
        // token0, token1, and possibly the fee asset.
        anyhow::bail!(
            "vault details exceeded the response limit; refusing to report empty reserves"
        );
    };

    let mut balance0 = 0u64;
    let mut balance1 = 0u64;
    for asset in assets {
        let Some(fungible) = asset.as_fungible() else {
            continue;
        };
        if fungible.faucet_id() == token0 {
            balance0 = fungible.amount().as_u64();
        } else if fungible.faucet_id() == token1 {
            balance1 = fungible.amount().as_u64();
        }
    }
    Ok((balance0, balance1))
}

/// Token metadata read from a faucet's `token_config` slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenMeta {
    pub symbol: String,
    pub decimals: u8,
}

/// Reads symbol and decimals from a faucet account's `token_config` slot.
///
/// The slot word is `[token_supply, max_supply, decimals, token_symbol]`. The
/// symbol is a base-26 packed field element decoded with the protocol's own
/// `TokenSymbol` type.
pub fn decode_token_meta(header: &AccountStorageHeader) -> Result<TokenMeta> {
    let name =
        StorageSlotName::new("miden::standards::faucets::fungible::token_config".to_string())
            .context("invalid faucet token_config slot name")?;
    let word = header
        .find_slot_header_by_name(&name)
        .map(|slot| slot.value())
        .ok_or_else(|| anyhow!("faucet is missing the token_config slot"))?;

    let symbol = TokenSymbol::try_from(word[3])
        .map(|symbol| symbol.to_string())
        .unwrap_or_default();
    Ok(TokenMeta {
        symbol,
        decimals: word[2].as_canonical_u64() as u8,
    })
}

/// Network-account fee configuration read from a pool's auth component.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetworkFees {
    /// Faucet whose asset denominates the note fees.
    pub fee_asset_id: Option<AccountId>,
    /// `(note script root, fee amount)` pairs from the fee schedule map.
    pub fee_schedule: Vec<(Word, u64)>,
}

/// Reads the fee asset id and the fee schedule from a network account.
///
/// The fee asset id is a value slot holding an asset id word, the same layout
/// as a token key. The schedule is a map from note script root to
/// `[fee, 0, 0, 1]`; the trailing `1` marks a set entry, so an unset key reads
/// as the zero word.
pub fn decode_network_fees(
    header: &AccountStorageHeader,
    map_details: &[AccountStorageMapDetails],
) -> Result<NetworkFees> {
    let asset_name =
        StorageSlotName::new("miden::standards::auth::network_account::fee_asset_id".to_string())
            .context("invalid fee asset id slot name")?;
    let fee_asset_id = header
        .find_slot_header_by_name(&asset_name)
        .map(|slot| slot.value())
        .filter(|word| *word != zero_word())
        .map(|word| faucet_of(&word))
        .transpose()?;

    let schedule_name = StorageSlotName::new(
        "miden::standards::fees::policies::basic_constant_fee::fee_schedule".to_string(),
    )
    .context("invalid fee schedule slot name")?;

    let mut fee_schedule = Vec::new();
    if let Some(detail) = map_details
        .iter()
        .find(|detail| detail.slot_name == schedule_name)
        && let StorageMapEntries::AllEntries(entries) = &detail.entries
    {
        for (key, value) in entries {
            let root: Word = (*key).into();
            fee_schedule.push((root, value[0].as_canonical_u64()));
        }
    }

    Ok(NetworkFees {
        fee_asset_id,
        fee_schedule,
    })
}
