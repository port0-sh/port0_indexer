# miden_xyk_indexer

Pool indexer for the MidenXYK AMM. Reads the chain through the node RPC only:
no Miden client, no keystore. Polls the on-chain registry for pool ids and each
pool for state, keeps a bounded history in SQLite, and serves the result over
HTTP.

For the full stack bring-up and the pool deployment flow, see the `RUNBOOK.md`
in the `miden_xyk_app` repository.

## Configuration

Read from the environment at startup. A `.env` file in the current directory
is loaded first (copy `.env.example`); real environment variables take
precedence, and a missing file is not an error.

| Variable | Default | Meaning |
| --- | --- | --- |
| `MIDEN_RPC_URL` | `https://rpc.testnet.miden.io` | Node RPC endpoint |
| `MIDEN_XYC_REGISTRY_ID` | required | Registry account id, hex |
| `INDEXER_LISTEN` | `0.0.0.0:8080` | HTTP listen address |
| `POOL_POLL_SECS` | `15` | Scheduler tick in seconds, clamped to `1..=5`; per-pool cadence is internal |
| `REGISTRY_POLL_SECS` | `60` | Registry poll interval |
| `INDEXER_DB` | `miden_xyk_indexer.sqlite` | SQLite path; empty or `off` keeps the index in RAM only |
| `INDEXER_SNAPSHOT_RETENTION` | (empty) | Empty prunes each resolution to its window; `all` keeps every bucket |
| `RUST_LOG` | `miden_xyk_indexer=info` | Tracing filter |

## Endpoints

| Route | Result |
| --- | --- |
| `GET /status` | registry id, last registry block, known and fetched pool counts |
| `GET /pools` | one page of pool summaries; `?token=<faucet>`, `?limit=` (default 50, max 500), `?cursor=<pool id>` |
| `GET /pools/{id}` | pool summary with its fee schedule, plus the newest history points |
| `GET /pools/{id}/history` | the pool's history; `?resolution=1m\|5m\|1h\|6h\|1d`, default `1m` |
| `POST /pools/{id}/refresh` | marks the pool due for the next tick; `202`, or `429` with `Retry-After` when limited |

The list is keyset paginated by pool id: pass the `next_cursor` of one page back
as `cursor`. The list omits each pool's fee schedule, which is the largest part of
a summary and only a pool page needs; the detail endpoint includes it.

Amounts are decimal strings because `u64` and `u128` exceed the safe integer
range of JSON numbers. This holds for summaries and for every history snapshot.
`spot_price` and `fee_apr` are display-only `f64`. Each pool summary carries the
trailing 24-hour aggregates: volume per token, LP fees, protocol fees, swap,
add, remove, and collect counts.

`fee_apr` is the annualised fee yield for liquidity providers. Fees accrue in
whichever token the swapper paid, so both sides are valued in token0 at the
pool's own price before the ratio is taken; that price values the reserves at
`2 * reserve0` in token0 terms, which is the capital the yield is taken
against. The rate is annualised over `fee_apr_window_secs`, which is the pool's
age while it is below 24 hours, so a pool watched for ten minutes reports ten
minutes of yield stretched to a year rather than a full day of it.

A snapshot's `timestamp` is the pool's `block_timestamp_last`, so it is zero
for a pool that has never been operated on and unchanged across samples that
saw no state change. `polled_at` is the wall-clock second of the sample.
`reserve0` and `reserve1` are the state at the end of the sample's bucket, and
`open_reserve0/1`, `high_reserve0/1`, and `low_reserve0/1` are the bucket's
opening state and its price extremes, stored as reserve pairs because a price
is a ratio of two reserves and only the pair reconstructs it exactly.

Each summary also resolves display and configuration data from the chain:

| Field | Source |
| --- | --- |
| `token0_symbol`, `token0_decimals`, `token1_symbol`, `token1_decimals` | each faucet's `token_config` slot |
| `fee_asset_id` | pool's `miden::standards::auth::network_account::fee_asset_id` value slot |
| `fee_schedule` | pool's `basic_constant_fee` schedule map, one `{ root, amount }` per note script root |

Token symbols are decoded with the protocol's `TokenSymbol` type. The app maps
the schedule roots to note kinds through the SDK's compiled note scripts.

The API is read-only and sends permissive CORS headers, so the static app can
fetch it cross-origin in development.

## Storage

SQLite holds the pool set, token metadata, one row per history bucket, and one
row per classified activity. History reads do not go through the in-memory model
at all: the API opens its own read-only connection, and WAL lets it read while
the poller writes, so neither waits on the other. The RAM mirror keeps only the
open bucket of each tier, which is all a poll needs to extend it, so memory is
`O(pools)` rather than `O(pools x rows)`.

Every amount is a TEXT column. SQLite's INTEGER is a signed 64-bit value, so a
`u64` above `2^63 - 1` cannot be stored in it and a `u128` never can; both appear
in pool state. `rusqlite` has no `u64` binding at all. Amounts travel as the
decimal strings the API already uses, and block
numbers, timestamps, and resolution periods take an `i64` that cannot overflow.

The file grows by the measured cost of a row: **360.3 bytes per snapshot** and
**97.4 bytes per activity**, including their indexes (200,000 rows of each type
inserted into the shipped schema and the file measured).

Snapshots follow the ladder: five tiers, pruned to their own window on every
write cycle except the last.

| Tier | Resolution | Retained | Rows per pool |
| --- | --- | --- | --- |
| T0 | 1m | 12 h | 720 |
| T1 | 5m | 7 d | 2,016 |
| T2 | 1h | 30 d | 720 |
| T3 | 6h | 180 d | 720 |
| T4 | 1d | forever | 365/year |

A tier is not computed from the one above it. Every poll feeds all five, and each
tier merges the poll into its open bucket (open from the first sample, high and
low from the extremes, close from the newest), which is exact for a candle. At
10,000 pools the four bounded tiers hold 15.05 GB and the daily tier adds 1.32 GB
a year. `INDEXER_SNAPSHOT_RETENTION=all` keeps every bucket instead.

## Capacity

Measured row costs, 10,000 pools, one year. Retention runs five tiers: 1m for
12 h, 5m for 7 d, 1h for 30 d, 6h for 180 d, and 1d forever. The per-pool row
counts are arithmetic on those windows (720 + 2,016 + 720 + 720 bounded, plus
365/year daily); the byte figures are the measured 360.3 and 97.4 bytes per row.
`INDEXER_SNAPSHOT_RETENTION=all` keeps every bucket, whose per-pool yearly count
is 525,600 + 105,120 + 8,760 + 1,460 + 365 = 641,305.

| Policy | Snapshot rows | Snapshot storage |
| --- | --- | --- |
| Keep every bucket | 6.41e9/year | 2.31 TB/year |
| Default (bounded tiers) | 4.18e7 total | 15.05 GB, +1.32 GB/year |

Activity rows are one per state change, so they scale with trading, not with
time alone: at 100 / 1,000 / 10,000 activities per pool per day the year costs
35.6 GB / 356 GB / 3.56 TB. The default activity retention is 7 days, which holds
that to 6.8 GB at 1,000 per day.

What breaks first, in order:

| Wall | Detail |
| --- | --- |
| Registry read | The node does not return a storage map above 1,000 entries inline. The `pools` map is one map, so the pool set stops being readable at 1,001 pools, long before 10,000. Needs `SyncAccountStorageMaps` paging or a sharded registry. |
| RPC rate | Polling 10,000 pools per 15 s is 667 `get_account` calls per second, each also carrying the pool's storage maps and vault. Not serviceable by a public node. Needs event- or block-driven fetching instead of a full sweep. |
| Indexer RAM | The in-memory model keeps one open bucket per tier (5 snapshots) and up to 4,096 activities per pool with no global cap. At 10,000 pools the activity ring dominates at about 3.3 GB; the snapshots are negligible. The process breaks before the disk does. |
| API payload | `GET /pools` returns one page (default 50, maximum 500 entries, order 1 KB each). A client that walks every cursor re-reads 10 MB per sweep. |
| Disk | 2.31 TB/year under `INDEXER_SNAPSHOT_RETENTION=all`. `VACUUM` needs a second copy, so retention has to drop tables or partitions, not rows. `DELETE` never returns space to the filesystem. |
| Browser | 10,000 rows re-rendered every 15 s once the user has loaded every page. The first page is 50 rows; full expansion needs virtualization. |

## Polling

The sweep runs on a short tick, but a pool is fetched only when its own cadence
says so, and a block subscription moves that schedule forward whenever a block
touches the pool:

| Idle for | Polled every |
| --- | --- |
| < 5 min | 15 s |
| < 1 h | 60 s |
| < 1 d | 10 min |
| >= 1 d | 1 h |

A block names the accounts it changed, so the subscription turns activity into
immediate polls and makes a timer unnecessary for a quiet network. It is a
fallback arrangement in both directions: a dropped stream reconnects from the
last processed block and the cadence keeps polls flowing while it is down, and
the cadence keeps working if the stream never connects. At 10,000 dormant pools
the network cost settles at about 3 requests a second instead of 667.

`POST /pools/{id}/refresh` marks one pool due at the next tick. It is limited to
one refresh per pool every 5 s and a global token bucket of 20/s, and a caller
that trips either gets `429` with `Retry-After`.

## Model

- A point is appended on every applied fetch, changed or not, so the last bucket advances and a chart gets one candle per interval; within a bucket the
  last point replaces the previous one, while the bucket's opening state and its
  price extremes carry over, so a snapshot doubles as a candle. History is
  retained at five resolutions per pool; the RAM mirror keeps only the open
  bucket of each tier, because history reads go to SQLite. The disk windows are
  12 h of 1m, 7 d of 5m, 30 d of 1h, 180 d of 6h, and every 1d bucket forever.
- Each change is classified from the state deltas as a swap, add, remove,
  or collect. For a swap the derivation is exact: the full input is
  `delta_reserve_in + delta_pot_in`, the protocol fee is `delta_pot_in`, and
  the LP fee is `input * lp_fee_bps / 10000 - delta_pot_in`, because the
  protocol's share is carved out of the LP fee rather than added to the input.
- Multiple blocks can coalesce between polls, so an activity is the net effect
  of the interval. Exact per-action events need `sync_transactions`.
- Reserves equal the vault balances of token0 and token1 minus the protocol
  pot, matching the contract's `get_reserves`.
- The vault is requested with the empty word as its `asset_vault_commitment`.
  The field is a "send my last known commitment" hint: omitting it means "do
  not include vault data", and the response then carries an empty asset list
  that is indistinguishable from a genuinely empty vault. An indexer that omits
  the field reports every pool as holding zero reserves. A vault the node
  refuses to enumerate is an error, not a zero balance.
- The registry's `pools` map is the pool set. A map above 1000 entries is not
  returned inline by the node; the registry decoder reports the limit instead of
  returning a partial list.
- Token metadata is read once per faucet and cached. The fee schedule is read
  from the pool account on every poll. It is fixed at pool creation, so it does not change.

## Build

```sh
cargo build --release
MIDEN_XYC_REGISTRY_ID=<registry id> ./target/release/miden_xyk_indexer
```

## Roadmap

1. Swap events and exact volume from per-pool `sync_transactions`.
