# Market-data history parity evidence — 2026-07-23

**Status:** PASS for the measured historical control interval  
**Branch under test:** `agent/architecture-rewrite`  
**Tested branch head before evidence commit:** `1861167006ccd7c904492873758804341704aa36`  
**Legacy runtime baseline:** `5073dbea3f96740b4390e113d06578a514918f00`  
**Operator environment:** Windows, local TWS/Gateway connection  
**Instrument/contract:** MNQ / MNQU6

## Scope

This evidence records one real operator-run comparison between the existing legacy price database and the target shadow market-data database.

It validates only historical complete BID/ASK bars for the explicit interval below. It does not claim realtime, reconnect, history/realtime seam, rollover, full-history or production-cutover readiness.

## Databases

```text
Legacy/control:
C:\IBMarketData-shadow\data\prices\MNQ.sqlite3
Table: MNQ_5s

Target/shadow:
C:\IBMD-shadow-data\account1\market_data\MNQ.sqlite3
Public target bars: public_market_bars_v1
```

The target database was created by the explicit target migration and populated through `scripts/backfill_target_market_data.py`. The legacy database remained the control source.

## Comparison interval

```text
[start, end) =
2026-07-23T10:05:00Z
2026-07-23T11:00:00Z
```

Duration: 55 minutes. At five seconds per bar, the expected complete-bar count is 660.

## Command

```powershell
python scripts/compare_market_data_shadow.py `
  --legacy-database $LegacyDb `
  --legacy-table MNQ_5s `
  --target-database $TargetDb `
  --instrument MNQ `
  --start-utc 2026-07-23T10:05:00Z `
  --end-utc 2026-07-23T11:00:00Z `
  --output-json "C:\IBMarketData-shadow\market-data-parity.json"
```

## Operator-reported result

```json
{
  "end_utc": "2026-07-23T11:00:00Z",
  "instrument_id": "MNQ",
  "is_match": true,
  "legacy_count": 660,
  "legacy_only_count": 0,
  "legacy_only_samples": [],
  "matched_count": 660,
  "start_utc": "2026-07-23T10:05:00Z",
  "target_count": 660,
  "target_only_count": 0,
  "target_only_samples": [],
  "tolerance": 1e-09,
  "value_mismatch_count": 0,
  "value_mismatch_samples": []
}
```

## Acceptance result

```text
legacy_count          = 660
target_count          = 660
matched_count         = 660
legacy_only_count     = 0
target_only_count     = 0
value_mismatch_count  = 0
```

All complete historical bars in the measured interval matched exactly within the configured `1e-9` tolerance across:

```text
contract/localSymbol
bid_open/high/low/close
ask_open/high/low/close
```

**Measured historical parity gate: PASS.**

## Remaining evidence before target signal consumes live shadow data

Only the next necessary checks remain open:

1. concurrent legacy and target realtime collection over an explicit control interval;
2. strict comparison of that realtime interval;
3. explicit history/realtime seam comparison around target startup;
4. a real disconnect/reconnect drill proving session isolation and no synthetic bars.

The accepted pre-warmed rollover switch and production-qualified session calendar remain production-cutover requirements, but are not part of this historical parity result.
