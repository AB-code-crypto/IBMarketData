# Market-data realtime and startup-seam parity evidence — 2026-07-23

**Status:** PASS for the measured current-contract control interval  
**Branch under test:** `agent/architecture-rewrite`  
**Tested branch head before evidence commit:** `a3ab5399b6976af9939d6c40237253abb4b1a6af`  
**Legacy runtime baseline:** `5073dbea3f96740b4390e113d06578a514918f00`  
**Operator environment:** Windows, local TWS/Gateway connection  
**Instrument/contract:** MNQ / MNQU6

## Scope

This evidence records one real operator-run comparison between the legacy and target market-data databases while both collectors ran concurrently with separate IB client IDs.

The measured interval contains:

- five minutes of target historical bootstrap immediately before the first target realtime bar;
- 142 target realtime bars;
- the exact target history-to-realtime startup seam;
- matching legacy bars over the same complete interval.

It does not claim reconnect, rollover, full-history or production-cutover readiness.

## Databases

```text
Legacy/control:
C:\IBMarketData-shadow\data\prices\MNQ.sqlite3
Table: MNQ_5s

Target/shadow:
C:\IBMD-shadow-data\account1\market_data\MNQ.sqlite3
Public target bars: public_market_bars_v1
```

Both services used the same TWS/Gateway account while remaining isolated by separate API client IDs and separate SQLite stores.

## Realtime collection evidence

Initial observation:

```json
{
  "realtime_count": 135,
  "first_realtime_start": "2026-07-23T16:43:45.000000Z",
  "last_realtime_end": "2026-07-23T16:55:00.000000Z"
}
```

The final comparison range was derived after collection had reached 142 target realtime bars:

```text
realtime_count       = 142
comparison_start     = 2026-07-23T16:38:45Z
first_realtime_start = 2026-07-23T16:43:45Z
comparison_end       = 2026-07-23T16:55:35Z
```

The interval therefore contains exactly 60 five-second historical bars before the first realtime bar and 142 realtime bars:

```text
60 + 142 = 202 complete bars
```

## Comparison command

```powershell
python scripts/compare_market_data_shadow.py `
  --legacy-database $LegacyDb `
  --legacy-table MNQ_5s `
  --target-database $TargetDb `
  --instrument MNQ `
  --start-utc 2026-07-23T16:38:45Z `
  --end-utc 2026-07-23T16:55:35Z `
  --output-json "C:\IBMarketData-shadow\market-data-realtime-parity.json"
```

## Operator-reported result

Comparator exit code:

```text
0
```

Report:

```json
{
  "end_utc": "2026-07-23T16:55:35Z",
  "instrument_id": "MNQ",
  "is_match": true,
  "legacy_count": 202,
  "legacy_only_count": 0,
  "legacy_only_samples": [],
  "matched_count": 202,
  "start_utc": "2026-07-23T16:38:45Z",
  "target_count": 202,
  "target_only_count": 0,
  "target_only_samples": [],
  "tolerance": 1e-09,
  "value_mismatch_count": 0,
  "value_mismatch_samples": []
}
```

## Acceptance result

```text
legacy_count          = 202
target_count          = 202
matched_count         = 202
legacy_only_count     = 0
target_only_count     = 0
value_mismatch_count  = 0
comparator exit code  = 0
```

All complete bars matched within `1e-9` across:

```text
contract/localSymbol
bid_open/high/low/close
ask_open/high/low/close
```

**Measured current-contract realtime parity gate: PASS.**

**Measured target startup history-to-realtime seam gate: PASS.**

## Remaining evidence before target signal consumes live shadow data

Only one current-contract market-data validation remains open:

1. a controlled real TWS/Gateway disconnect and reconnect drill proving session replacement, continued freshness aging during the outage, no synthetic bars, and exact legacy/target parity after recovery.

The accepted pre-warmed rollover switch and production-qualified session calendar remain production-cutover requirements, but are not part of this measured current-contract result.
