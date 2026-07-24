# Legacy market-data import evidence — 2026-07-23

**Status:** PASS for the explicit target-signal history interval  
**Branch:** `agent/architecture-rewrite`  
**Instrument:** MNQ  
**Interval:** `[2025-07-23T00:00:00Z, 2026-07-23T17:25:00Z)`

## Scope

This evidence records the one-time migration of complete legacy BID/ASK five-second bars into the target `market_data` store. It does not change the legacy database and does not constitute a production cutover.

## Initial dry-run

```json
{
  "applied": false,
  "complete_source_count": 4269190,
  "end_utc": "2026-07-23T17:25:00.000000Z",
  "existing_exact_count": 1914,
  "imported_count": 4267276,
  "incomplete_source_count": 0,
  "instrument_id": "MNQ",
  "start_utc": "2025-07-23T00:00:00.000000Z"
}
```

The dry-run proved that 1,914 target rows already matched exactly and 4,267,276 complete rows were pending.

## Applied import

```json
{
  "applied": true,
  "complete_source_count": 4269190,
  "end_utc": "2026-07-23T17:25:00.000000Z",
  "existing_exact_count": 1914,
  "imported_count": 4267276,
  "incomplete_source_count": 0,
  "instrument_id": "MNQ",
  "start_utc": "2025-07-23T00:00:00.000000Z"
}
```

## Idempotency and exactness verification

```json
{
  "applied": false,
  "complete_source_count": 4269190,
  "end_utc": "2026-07-23T17:25:00.000000Z",
  "existing_exact_count": 4269190,
  "imported_count": 0,
  "incomplete_source_count": 0,
  "instrument_id": "MNQ",
  "start_utc": "2025-07-23T00:00:00.000000Z"
}
```

Acceptance conditions are satisfied:

```text
existing_exact_count = complete_source_count = 4,269,190
imported_count        = 0
incomplete_source_count = 0
```

**Target signal history gate: PASS.**

The target signal may now be implemented in shadow mode. It must read only the target market-data product, write only its own signal store, and emit no trade commands.
