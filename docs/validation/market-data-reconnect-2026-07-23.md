# Market-data reconnect evidence — 2026-07-23

**Status:** PASS for the measured current-contract reconnect drill  
**Branch under test:** `agent/architecture-rewrite`  
**Tested branch head before evidence commit:** `27d8ccbe95ff8dbd91dda843d4a6ef950f5084ca`  
**Legacy runtime baseline:** `5073dbea3f96740b4390e113d06578a514918f00`  
**Operator environment:** Windows, local TWS/Gateway connection  
**Instrument/contract:** MNQ / MNQU6

## Scope

This evidence records one controlled real disconnect/reconnect drill while the legacy and target market-data collectors were running with separate IB client IDs and separate SQLite stores.

It validates:

- target health becomes fail-closed while TWS/Gateway is unavailable;
- the latest complete bar stops advancing during the outage;
- freshness continues aging instead of being reset;
- reconnect replaces the IB source-session identity;
- collection resumes after TWS/Gateway returns;
- the operator-confirmed corrected comparator reports exact legacy/target parity over the reconnect interval.

It does not claim rollover or production-cutover readiness.

## Pre-outage state

```text
captured_at_utc      = 2026-07-23T17:17:49Z
latest_bar_start_utc = 2026-07-23T17:17:40Z
latest_bar_end_utc   = 2026-07-23T17:17:45Z
source_kind          = REALTIME
source_session_id    = ib_session_74b0bb04d43c40d8a379f7aabd94ae6f
readiness            = READY
freshness_seconds    = 2.454164
```

The pre-outage diagnostic snapshot was taken before the final healthy bar immediately preceding the disconnect. Therefore bar-ID equality between the pre-outage and outage snapshots is not used as the acceptance criterion.

## Outage state

```text
captured_at_utc      = 2026-07-23T17:20:26Z
latest_bar_start_utc = 2026-07-23T17:18:35Z
latest_bar_end_utc   = 2026-07-23T17:18:40Z
source_kind          = REALTIME
source_session_id    = ib_session_74b0bb04d43c40d8a379f7aabd94ae6f
readiness            = BLOCKED
freshness_seconds    = 104.513223
blocking_reason      = ConnectionRefusedError / WinError 1225
```

The service retained the last complete pre-disconnect market bar, stopped advancing the public latest pointer and allowed freshness to age past the configured limit. No synthetic bar was created during the unavailable interval.

## Recovered state

```text
captured_at_utc      = 2026-07-23T17:24:42Z
latest_bar_start_utc = 2026-07-23T17:24:35Z
latest_bar_end_utc   = 2026-07-23T17:24:40Z
source_kind          = REALTIME
source_session_id    = ib_session_099295f317494eaf9e9852e9b62b5a6a
readiness            = READY
freshness_seconds    = 0.500271
```

The source-session identity changed:

```text
before/outage = ib_session_74b0bb04d43c40d8a379f7aabd94ae6f
after         = ib_session_099295f317494eaf9e9852e9b62b5a6a
```

This proves that the target collector did not present the failed connection as the recovered source session.

## Runtime-log observations

The target log recorded:

```text
Error 1100 — connectivity lost
realtime session failed
repeated ConnectionRefusedError while TWS/Gateway was unavailable
successful new API connection
new history bootstrap
new realtime source generation
```

The log also exposed unnecessary default `ib_async` startup synchronization of portfolio, executions and commission reports. That behavior is not required by market-data and is removed separately by connecting read-only with startup fetches disabled.

## Comparator result

The first submitted comparator report was invalid because its calculated start boundary was Unix epoch zero:

```text
start_utc = 1970-01-01T00:00:00Z
```

It is retained only as diagnostic evidence that every target row in that accidental broad range matched a legacy row; it is not the reconnect acceptance report.

The operator reran the comparator with the corrected explicit reconnect interval and confirmed:

```text
comparator exit code  = 0
is_match              = true
legacy_only_count     = 0
target_only_count     = 0
value_mismatch_count  = 0
```

**Measured current-contract disconnect/reconnect gate: PASS.**

## Remaining market-data limits

Current-contract historical, realtime, startup-seam and reconnect parity are now measured and accepted.

The remaining market-data items are production-cutover requirements, not blockers for a current-contract target signal shadow calculation:

1. production-qualified session calendar with verified holidays and early closes;
2. accepted pre-warmed two-phase futures rollover switch;
3. controlled production cutover and rollback procedure.
