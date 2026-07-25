# Protective fill and OCA lifecycle

**Owner:** target `execution`  
**IB access:** read-only reconciliation only  
**Broker mutation:** none  
**Legacy database compatibility:** not required

## Purpose

This slice completes the durable lifecycle after a protective STOP or TAKE PROFIT has been submitted.

```text
ProtectiveOrderV1
+ complete IB open/completed-order snapshot
+ immutable executions keyed by execId
+ late commission reports
+ fresh COMPLETE broker-position snapshot
→ protective fill/commission ledger
→ OCA sibling proof
→ EXITED or OPERATOR_REQUIRED
→ broker-proven FLAT
→ PositionEpisode CLOSED
```

It does not call `placeOrder` or `cancelOrder`.

## Evidence model

Execution stores protective evidence independently from current-state rows:

```text
internal_protective_fill_evidence
internal_protective_commission_evidence
internal_protective_reconciliation_observations
```

A fill is immutable and keyed by IB `execId`. Commission is a separate fact because it may arrive after the fill.

```text
first reconciliation:
execId exists
commission absent
→ fill persisted
→ commission_complete = false

later reconciliation:
same execId
commission report exists
→ append commission evidence
→ do not rewrite the fill payload
```

The same `execId` cannot be assigned to another position episode or protective order. A materially different payload for an existing `execId` is rejected.

## OCA terminal rules

A protective fill alone is not enough to close the episode.

```text
one STOP/TP FILLED
+ sibling CANCELLED / REJECTED / FAILED / NOT_REQUIRED
→ ProtectionState = EXITED
```

The episode remains open until position feed proves FLAT:

```text
EXITED
+ fresh COMPLETE broker-position snapshot = FLAT
+ no unresolved owned protective order
→ ProtectionState = CLOSED
→ PositionEpisode = CLOSED
→ StrategyPosition = FLAT
```

Unsafe cases fail closed:

```text
one protective order FILLED
+ sibling LIVE / SUBMITTING / CANCEL_REQUESTED / UNKNOWN_OUTCOME
→ OPERATOR_REQUIRED

both protective orders FILLED
→ OPERATOR_REQUIRED

broker position FLAT
+ owned protective order still LIVE/SUBMITTING/UNKNOWN
→ OPERATOR_REQUIRED

broker position differs from the episode contract/side/quantity
→ OPERATOR_REQUIRED
→ StrategyPosition = UNKNOWN
```

No disappearance from a single broker view is treated as OCA cancellation proof.

## Manual flat before protection submission

If the position is broker-proven FLAT while both protective orders remain `PLANNED`, no broker exposure exists.

```text
PLANNED STOP
PLANNED TP
fresh broker position = FLAT
→ both orders NOT_REQUIRED
→ protection CLOSED
→ episode CLOSED
```

This handles manual paper cleanup without manufacturing fake cancellation facts.

## Readiness

```text
EXITED waiting for FLAT
UNPROTECTED
OPERATOR_REQUIRED
unresolved TP outcome
→ command intake BLOCKED

CLOSED
→ command intake may return READY if no independent blocker remains
```

Execution remains the only writer and the only owner of broker actions.

## Schema installation during rewrite

Target development databases are disposable. The base execution schema remains version 3, and this slice adds an explicit development component:

```powershell
python scripts/run_execution_protective_lifecycle_schema.py `
  --database $ExecutionDb `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

The component ledger is:

```text
execution_target_schema_components
```

A checksum mismatch is not upgraded in place. Create a fresh target execution database. Before production cutover, development schema fragments will be squashed into the clean execution bootstrap schema.

The only supported legacy transfer remains the one-time market-price import.

## Entrypoint

Offline validation:

```powershell
python apps/run_execution_protective_lifecycle_v2.py `
  --validate-store-only `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

One read-only lifecycle pass:

```powershell
python apps/run_execution_protective_lifecycle_v2.py `
  --once-position-episode-id position_episode_<id> `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

The entrypoint uses:

```text
IB reconciliation client ID = IB_CLIENT_ID + 100
execution process lock       = shared execution.lock
```

Output explicitly states:

```text
broker_mutations_performed = false
cancel_enabled             = false
automatic_retry_enabled    = false
liquidation_enabled        = false
```

## Deliberate exclusions

This slice does not implement:

```text
cancelOrder
OCA sibling cancellation requests
automatic retry
emergency MARKET liquidation
daily-flat liquidation
rollover liquidation
daily-PnL halt cleanup
continuous execution loop
live-account execution
```

Those belong to the next single-owner liquidation coordinator, not to a second service.
