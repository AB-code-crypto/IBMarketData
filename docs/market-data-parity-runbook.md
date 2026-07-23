# Market-data parity: backfill and comparison

**Status:** shadow validation only  
**Branch:** `agent/architecture-rewrite`  
**Legacy runtime baseline:** `5073dbea3f96740b4390e113d06578a514918f00`

## Scope

This runbook covers only two operations required before the target signal can read target bars:

1. backfill an explicit UTC interval into the target market-data database;
2. compare complete legacy and target BID/ASK bars over the same interval.

It does not add automatic scheduling, a general data-repair platform, or a production cutover.

## Preconditions

The target store must already be migrated:

```bash
python scripts/run_target_migrations.py \
  --manifest migrations/market_data.v1.json \
  --database "$IBMD_DATA_ROOT/market_data/MNQ.sqlite3" \
  --application-version "$IBMD_APPLICATION_VERSION" \
  --apply
```

Stop `apps/run_market_data_v2.py` before backfill. The backfill command takes the same `market_data` process lock and must be the only writer.

## Explicit range backfill

Boundaries are UTC, semi-open and must be aligned to five seconds:

```text
[start_utc, end_utc)
```

Example:

```bash
python scripts/backfill_target_market_data.py \
  --start-utc 2026-07-20T00:00:00Z \
  --end-utc   2026-07-21T00:00:00Z
```

The command:

- intersects the requested interval with registered MNQ contract windows;
- skips declared no-active-contract rollover gaps;
- uses one-hour chunks by default;
- requests BID and ASK under one source generation per chunk;
- writes only through the serialized target writer;
- leaves legacy price data untouched.

The default request spacing is 11 seconds, matching the conservative pacing already used by the legacy historical loader. It can be changed explicitly for a controlled paper/shadow run:

```bash
python scripts/backfill_target_market_data.py \
  --start-utc 2026-07-20T00:00:00Z \
  --end-utc   2026-07-20T06:00:00Z \
  --request-spacing-seconds 11
```

The command refuses ranges outside the current contract-calendar coverage. It also refuses to run concurrently with the target market-data service.

## Strict legacy/target comparison

The comparator opens both databases read-only and compares only complete bars. A legacy row is complete only when all eight BID/ASK OHLC fields are non-null.

```bash
python scripts/compare_market_data_shadow.py \
  --legacy-database /path/to/legacy/data/prices/MNQ.sqlite3 \
  --legacy-table MNQ_5s \
  --target-database "$IBMD_DATA_ROOT/market_data/MNQ.sqlite3" \
  --instrument MNQ \
  --start-utc 2026-07-20T00:00:00Z \
  --end-utc   2026-07-21T00:00:00Z
```

Compared fields:

```text
contract localSymbol
bid_open/high/low/close
ask_open/high/low/close
```

Exit codes:

```text
0  exact parity within tolerance
1  missing bars or value differences found
2  invalid input, schema or read error
```

Optional report file:

```bash
python scripts/compare_market_data_shadow.py \
  ... \
  --output-json market-data-parity.json
```

Default numeric tolerance is `1e-9`. Do not increase it to hide unexplained differences.

## Acceptance rule

A control interval passes only when:

```text
legacy_only_count = 0
target_only_count = 0
value_mismatch_count = 0
```

A mismatch must be explained by a known source difference or fixed. “Approximately equal” is not a parity result.

## Reconnect check

During a shadow drill, interrupt TWS/Gateway connectivity and verify:

1. the current IB source session fails;
2. the next request creates a different `source_session_id`;
3. a BID side from the failed session is not paired with an ASK side from the new session;
4. old complete bars remain readable but their freshness continues aging;
5. no synthetic bar is created during the outage.

## Explicitly deferred

The accepted two-phase pre-warmed futures switch is not implemented by this slice. It is not required to compare the current contract or to run a target signal in shadow mode away from a rollover boundary. It remains mandatory before production cutover.
