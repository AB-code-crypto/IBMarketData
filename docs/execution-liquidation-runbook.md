# Unified execution liquidation coordinator

**Status:** implementation complete; real paper gate deferred until the exchange is open  
**Owner:** target `execution`  
**Separate liquidation service:** forbidden  
**Live accounts:** rejected  
**Automatic retry:** disabled  
**Legacy database compatibility:** not required

## Purpose

Liquidation is one serialized execution workflow, not a collection of independent emergency scripts.

```text
MISSING_STOP
STOP_REJECTED
STOP_BREACHED
DAILY_FLAT
DAILY_HALT
ROLLOVER
MANUAL_EMERGENCY
        ↓
one liquidation operation for one position episode
        ↓
terminal protective exits
        ↓
one MARKET close on the actually held contract
        ↓
immutable fill/commission evidence
        ↓
fresh broker-proven FLAT
        ↓
PositionEpisode CLOSED
```

`decision`, `signal`, `position_feed`, daily-risk producers and the operator may request liquidation. Only `execution` owns broker actions.

## Stable identity and trigger merging

The operation identity is derived from:

```text
account
strategy
strategy version
deployment
instrument
position_episode_id
target_state = FLAT
```

The reason is deliberately absent from the uniqueness key.

```text
DAILY_FLAT arrives
→ operation A

MISSING_STOP arrives for the same episode
→ still operation A
→ second immutable trigger
→ reasons are merged
```

A process restart or repeated scheduler tick therefore cannot create a second liquidation operation for the same episode.

## State machines

### Operation

```text
REQUESTED
PREPARING
CANCELING_EXITS
SUBMITTING
LIVE
RECONCILING
SUCCEEDED
FAILED_RETRYABLE
FAILED_OPERATOR_REQUIRED
CANCELLED_AS_ALREADY_FLAT
```

### MARKET-close attempt

```text
PREPARING
SUBMITTING
LIVE
FILLED
CANCELLED
REJECTED
FAILED
UNKNOWN_OUTCOME
```

`UNKNOWN_OUTCOME` is nonterminal broker exposure. It never authorizes another `placeOrder`.

`FAILED_RETRYABLE` is only possible after a terminal broker fact proves the previous attempt is no longer live. A retry is still explicit; it is not scheduled automatically.

## Broker-position proof

Liquidation reads the independent `broker_position_feed` public product.

A usable OPEN proof requires:

```text
snapshot = COMPLETE and fresh
account matches
one nonzero MNQ row only
secType = FUT
conId/localSymbol equal the position episode
integer quantity
broker side equal the position episode side
broker quantity <= owned episode quantity
```

The close route uses the actually held `conId/localSymbol`. It does not substitute the currently active quarterly contract.

The following states are incidents and block automatic mutation:

```text
multiple MNQ contracts
unknown/wrong contract
fractional futures quantity
opposite broker side
broker quantity above the owned episode quantity
stale or incomplete position snapshot
```

## Exit-order sequencing

Protective orders are reconciled before any MARKET close.

The mutation order is strict:

```text
1. TAKE PROFIT cancellation, if LIVE
2. STOP cancellation, if LIVE
3. MARKET close
```

Each invocation performs at most one external broker action.

Before `cancelOrder`:

```text
protective order → CANCEL_REQUESTED
liquidation      → CANCELING_EXITS / RECONCILE_EXITS
state committed to execution DB
```

Before `placeOrder`:

```text
attempt broker order ID allocated
attempt → SUBMITTING
operation → SUBMITTING
state committed to execution DB
```

Any timeout, disconnect or exception after those durable boundaries is possible broker exposure. The next invocation reconciles the existing identity and never repeats the mutation blindly.

## Read-only recovery

Reconciliation remains available when:

```text
broker_actions_enabled = false
```

provided execution reconciliation and clock health are available. This is deliberate: disabling new broker mutations must not disable adoption of already-exposed orders.

Broker mutation itself still requires:

```text
IBMD_ENVIRONMENT = paper
account begins with D
--confirm-paper-account exactly equals IB_ACCOUNT_ID
broker_actions_enabled = true
fresh exact broker position proof
```

## MARKET close quantity

The first attempt requests the fresh broker-proven remaining quantity.

After a partial fill:

```text
operation cumulative fill is updated from absolute attempt evidence
fresh broker position determines the remaining quantity
terminal previous attempt proof is required
attempt N+1 may request only the fresh remaining quantity
```

The coordinator does not assume the original episode quantity is still open.

## Completion

A FILLED MARKET close is not enough to close local state.

Required completion proof:

```text
liquidation attempt terminal or operation already-flat
all protective exits terminal / not required
fresh COMPLETE position snapshot
broker MNQ position = FLAT
```

Then one transaction publishes:

```text
LiquidationOperation → SUCCEEDED or CANCELLED_AS_ALREADY_FLAT
PositionEpisode      → CLOSED
closing_operation_id → liquidation_operation_<id>
ProtectionState      → CLOSED
StrategyPosition     → FLAT
ExecutionReadiness   → READY unless another independent blocker remains
```

If the broker is FLAT while a protective or close order remains LIVE, SUBMITTING, CANCEL_REQUESTED or UNKNOWN_OUTCOME, completion is forbidden.

## Persistence

Fresh target component:

```text
migrations/execution.liquidation.v1.json
scripts/run_execution_liquidation_schema.py
```

Owned tables:

```text
internal_liquidation_operations
internal_liquidation_attempts
internal_liquidation_triggers
internal_liquidation_operation_transitions
internal_liquidation_attempt_transitions
internal_liquidation_reconciliation_observations
internal_liquidation_fill_evidence
internal_liquidation_commission_evidence
```

Public products:

```text
public_liquidation_operations_v1
public_liquidation_attempts_v1
public_liquidation_triggers_v1
public_liquidation_fills_v1
```

Development databases may be recreated. A component checksum mismatch requires a fresh target execution database; compatibility migrations for experimental data are intentionally absent.

## Entrypoint

Validate stores:

```powershell
python apps/run_execution_liquidation_v2.py `
  --validate-store-only `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

Record a durable request without broker mutation:

```powershell
python apps/run_execution_liquidation_v2.py `
  --request-position-episode-id $PositionEpisodeId `
  --reason DAILY_FLAT `
  --source-ref "daily-flat:2026-07-27" `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

Advance the local plan without broker mutation:

```powershell
python apps/run_execution_liquidation_v2.py `
  --advance-position-episode-id $PositionEpisodeId `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

Perform one paper pass:

```powershell
python apps/run_execution_liquidation_v2.py `
  --once-paper-position-episode-id $PositionEpisodeId `
  --confirm-paper-account $env:IB_ACCOUNT_ID `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

Default client IDs:

```text
read-only reconciliation = IB_CLIENT_ID + 100
protective cancellation  = IB_CLIENT_ID + 140
MARKET close submission  = IB_CLIENT_ID + 160
```

The same execution process lock is used by command submission, protection submission, protective lifecycle and liquidation.

## Deliberate exclusions

This slice does not add:

```text
automatic liquidation retry scheduler
live-account execution
multi-account coordinator
separate daily-flat broker owner
separate rollover broker owner
separate missing-STOP broker owner
continuous execution runtime
```

Trigger producers and the continuous execution loop remain later slices. The broker-action owner is already fixed: target `execution` only.
