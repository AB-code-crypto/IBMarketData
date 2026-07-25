# Execution liquidation trigger producers

## Scope

This component detects conditions that require an existing strategy-owned position episode to become `FLAT`. It does not connect to Interactive Brokers and cannot submit or cancel orders.

The only write is a durable `LiquidationTriggerV1` appended to the single liquidation operation owned by `execution`.

```text
open PositionEpisodeV1
+ protection state
+ daily-risk state
+ versioned session calendar
+ versioned futures calendar
→ zero or more LiquidationTriggerV1 facts
→ one stable liquidation operation per position episode
```

## Producers

### Missing mandatory STOP

The producer waits through a short post-entry installation grace period. After that period, a STOP that is not broker-proven `LIVE` creates:

```text
PLANNED / SUBMITTING / UNKNOWN_OUTCOME / CANCELLED / FAILED / NOT_REQUIRED
→ MISSING_STOP

REJECTED
→ STOP_REJECTED

FILLED while the position episode is still OPEN
→ STOP_BREACHED
```

A STOP already being cancelled by an existing liquidation operation does not create a synthetic missing-STOP trigger.

### Daily PnL halt

A strategy-scoped `DailyRiskStateV1` in any of these states creates `DAILY_HALT`:

```text
TRIGGERED
CLOSING
HALTED with an unexpectedly open position episode
```

The stable source identity is the risk `trading_day`. Incomplete or `NOT_READY` PnL does not fabricate a halt trigger.

### Daily flat

The trigger boundary is derived from the versioned session calendar and the configured daily-flat offset. A custom early close shifts the liquidation boundary by the same offset from the actual daytime session close.

The producer searches for the first daily-flat boundary after the episode opened. Therefore an entry opened in the evening session is not incorrectly liquidated by the boundary that already passed earlier on the same local date.

Production mode requires a production-qualified session calendar. With the current parity-only catalog, the producer reports a blocker instead of inventing holiday/early-close behavior. `--allow-unqualified-session` exists only for development and paper validation.

### Rollover

A rollover trigger is created only when the exact held `conId/localSymbol` is registered and its `active_to_utc` has passed.

```text
held contract still active
→ no trigger

held contract registered and expired
→ ROLLOVER

held contract absent or ambiguous in the calendar
→ blocker; no automatic trigger
```

Liquidation still closes the factually held contract. It never substitutes the newly active contract.

## Idempotency

Trigger identity is stable by liquidation operation, reason and source reference.

Repeated evaluation of the same condition:

```text
same source_ref
→ same trigger_id
→ no second trigger row
→ no second liquidation operation
```

Concurrent reasons such as `DAILY_FLAT`, `MISSING_STOP` and `DAILY_HALT` attach to one operation for the position episode.

## Entrypoint

Validate the target store and catalog:

```powershell
python apps/run_execution_liquidation_triggers_v2.py `
  --validate-store-only
```

Run one broker-free evaluation:

```powershell
python apps/run_execution_liquidation_triggers_v2.py `
  --once
```

Deterministic offline evaluation:

```powershell
python apps/run_execution_liquidation_triggers_v2.py `
  --once `
  --once-at 2026-07-27T19:59:50Z
```

Development-only evaluation with the current parity session calendar:

```powershell
python apps/run_execution_liquidation_triggers_v2.py `
  --once `
  --allow-unqualified-session
```

## Explicit non-goals

This component does not:

```text
placeOrder
cancelOrder
read TWS
calculate PnL
change the active contract calendar
advance liquidation broker actions
run continuously
```

A later execution runtime may schedule this one-shot service internally, but trigger detection remains separate from broker mutation ownership.
