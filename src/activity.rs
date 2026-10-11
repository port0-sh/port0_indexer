//! Activity classification and aggregates.
//!
//! A swap, liquidity action, or collect is classified from the deltas between
//! two consecutive decoded states. The swap derivation is exact given the
//! contract arithmetic: the full input enters the vault, the protocol fee is
//! recorded in the pot, and the LP fee stays in the reserve.
//!
//! Multiple blocks can coalesce between polls, so a record is the net effect of
//! the interval, not a single action. Exact per-action events require
//! `sync_transactions` and are a later phase.

use crate::decode::PoolState;

/// Trailing window for the aggregates, in seconds.
pub const AGGREGATE_WINDOW_SECS: u64 = 86_400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    Swap,
    AddLiquidity,
    RemoveLiquidity,
    Collect,
    /// A state change that matches no operation. Kept so a change is never
    /// silently dropped from the classification.
    Other,
}

/// A classified state change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Activity {
    pub block: u32,
    pub timestamp: u64,
    pub kind: ActivityKind,
    /// Full swap input in token0, including the protocol fee.
    pub volume0: u64,
    /// Full swap input in token1, including the protocol fee.
    pub volume1: u64,
    pub lp_fee0: u64,
    pub lp_fee1: u64,
    pub protocol_fee0: u64,
    pub protocol_fee1: u64,
}

/// Classifies the change from `prev` to `next`. Returns None when nothing
/// tracked changed.
pub fn classify(prev: &PoolState, next: &PoolState, block: u32) -> Option<Activity> {
    let d_res0 = next.reserve0 as i128 - prev.reserve0 as i128;
    let d_res1 = next.reserve1 as i128 - prev.reserve1 as i128;
    let d_lp = next.lp_supply as i128 - prev.lp_supply as i128;
    let d_pot0 = next.pot0 as i128 - prev.pot0 as i128;
    let d_pot1 = next.pot1 as i128 - prev.pot1 as i128;

    let unchanged = d_res0 == 0
        && d_res1 == 0
        && d_lp == 0
        && d_pot0 == 0
        && d_pot1 == 0
        && prev.fees == next.fees;
    if unchanged {
        return None;
    }

    // Fees cannot change: a pool is configured at creation and has no
    // procedure that rewrites them, so no branch tests them.
    let kind = if d_lp == 0 && (d_res0 != 0 || d_res1 != 0) {
        ActivityKind::Swap
    } else if d_lp > 0 {
        ActivityKind::AddLiquidity
    } else if d_lp < 0 {
        ActivityKind::RemoveLiquidity
    } else if d_pot0 < 0 || d_pot1 < 0 {
        ActivityKind::Collect
    } else {
        ActivityKind::Other
    };

    let (volume0, volume1, lp_fee0, lp_fee1, protocol_fee0, protocol_fee1) = match kind {
        ActivityKind::Swap => {
            let protocol0 = d_pot0.max(0) as u64;
            let protocol1 = d_pot1.max(0) as u64;
            let reserve_in0 = d_res0.max(0) as u64;
            let reserve_in1 = d_res1.max(0) as u64;
            // The input is reconstructed from the two deltas. The protocol's
            // share comes out of the LP fee, so what the LPs kept is the LP fee
            // minus the share, not the whole fee.
            let input0 = reserve_in0 + protocol0;
            let input1 = reserve_in1 + protocol1;
            (
                input0,
                input1,
                input0 * next.fees.lp_fee_bps / 10_000 - protocol0,
                input1 * next.fees.lp_fee_bps / 10_000 - protocol1,
                protocol0,
                protocol1,
            )
        }
        // Collect moves the pot out of the vault; reserves are unchanged.
        ActivityKind::Collect => {
            let collected0 = (-d_pot0).max(0) as u64;
            let collected1 = (-d_pot1).max(0) as u64;
            (0, 0, 0, 0, collected0, collected1)
        }
        _ => (0, 0, 0, 0, 0, 0),
    };

    Some(Activity {
        block,
        timestamp: next.block_timestamp_last,
        kind,
        volume0,
        volume1,
        lp_fee0,
        lp_fee1,
        protocol_fee0,
        protocol_fee1,
    })
}

/// Aggregates over a trailing window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Aggregates {
    pub volume0: u64,
    pub volume1: u64,
    pub lp_fees0: u64,
    pub lp_fees1: u64,
    pub protocol_fees0: u64,
    pub protocol_fees1: u64,
    pub swaps: u64,
    pub adds: u64,
    pub removes: u64,
    pub collects: u64,
}

/// Sums the activities whose wall-clock time falls inside the window.
///
/// `unix_time` is the activity's wall-clock time, kept alongside the chain
/// timestamp so the window follows the indexer's clock.
pub fn aggregates_since<'a>(
    activities: impl Iterator<Item = (&'a Activity, u64)>,
    now: u64,
    window_secs: u64,
) -> Aggregates {
    let cutoff = now.saturating_sub(window_secs);
    let mut aggregates = Aggregates::default();
    for (activity, unix_time) in activities {
        if unix_time < cutoff {
            continue;
        }
        aggregates.volume0 = aggregates.volume0.saturating_add(activity.volume0);
        aggregates.volume1 = aggregates.volume1.saturating_add(activity.volume1);
        aggregates.lp_fees0 = aggregates.lp_fees0.saturating_add(activity.lp_fee0);
        aggregates.lp_fees1 = aggregates.lp_fees1.saturating_add(activity.lp_fee1);
        aggregates.protocol_fees0 = aggregates
            .protocol_fees0
            .saturating_add(activity.protocol_fee0);
        aggregates.protocol_fees1 = aggregates
            .protocol_fees1
            .saturating_add(activity.protocol_fee1);
        match activity.kind {
            ActivityKind::Swap => aggregates.swaps += 1,
            ActivityKind::AddLiquidity => aggregates.adds += 1,
            ActivityKind::RemoveLiquidity => aggregates.removes += 1,
            ActivityKind::Collect => aggregates.collects += 1,
            ActivityKind::Other => {}
        }
    }
    aggregates
}
