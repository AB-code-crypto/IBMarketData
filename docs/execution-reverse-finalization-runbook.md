# Execution reverse finalization

## Purpose

A filled `REVERSE` broker operation combines two different economic actions:

```text
close the source position episode
+ open the opposite target position episode
```

For example:

```text
LONG 1 -> SHORT 1
broker order = SELL 2
```

The two contracts cannot both be treated as the entry of the new SHORT episode. Reverse finalization deterministically allocates every immutable broker fill between the source close and the target opening before the new protective prices are calculated.

## Boundary

Entrypoint:

```text
apps/run_execution_reverse_finalization_v2.py
```

The entrypoint:

- never connects to Interactive Brokers;
- never calls `placeOrder`;
- never calls `cancelOrder`;
- never creates a retry;
- uses the single execution process lock;
- reads only durable execution facts and the latest public position-feed snapshot;
- commits the complete episode handoff in one SQLite transaction.

## Required facts

Finalization requires:

```text
ADMITTED REVERSE command
SUCCEEDED/FILLED broker operation
immutable fills keyed by execId
OPEN source PositionEpisodeV1
source protection with no exposed orders
current execution position linked to the source episode
fresh COMPLETE broker-position snapshot proving the opposite target
no liquidation operation for the source episode
```

The broker operation quantity must equal:

```text
source episode quantity + requested target quantity
```

## Fill allocation

Fills are ordered by cumulative quantity, execution timestamp and `execId`. Closing quantity is consumed first. Any quantity left in the same execution belongs to the new episode.

Example:

```text
source LONG 2
requested target SHORT 1
SELL 3

exec A: shares=1, cumulative=1, price=28600
  close=1
  open=0

exec B: shares=2, cumulative=3, price=28590
  close=1
  open=1
```

The new SHORT entry price is `28590`, not the average price of all three sold contracts.

A single execution can also be split:

```text
exec X: shares=2, cumulative=2
  close=1
  open=1
```

Each allocation is persisted as `ReverseFillAllocationV1` with a stable identity.

## Atomic state change

One transaction performs all of the following:

```text
source PositionEpisodeV1 -> CLOSED
source ProtectionStateV1 -> CLOSED
new PositionEpisodeV1    -> OPEN
new ProtectionStateV1    -> PLANNED
StrategyPositionV1       -> opposite target
ExecutionReadinessV1     -> BLOCKED until new STOP is proven
reverse allocations      -> append-only economic evidence
reverse finalization     -> durable audit record
```

The source episode records the reverse `broker_operation_id` as its closing operation.

## Commission semantics

Position ownership and protection planning do not wait for delayed commission reports. A finalization may therefore be stored with:

```text
commission_complete = false
```

A later invocation may only enrich commission completeness from `false` to `true`. It cannot change quantities, prices, execution timestamps, episode identities or any other economic fact. Regression from complete to incomplete is rejected.

Daily-risk calculation must remain `NOT_READY` while required commission evidence is incomplete.

## Schema

Install the fresh target component after execution base schema v3:

```powershell
python scripts/run_execution_reverse_finalization_schema.py `
  --database $ExecutionDb `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

Development databases are disposable. A component checksum mismatch requires a fresh target execution database; no compatibility migration is provided.

## Validation

```powershell
python apps/run_execution_reverse_finalization_v2.py `
  --validate-store-only `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

## Finalize one operation

```powershell
python apps/run_execution_reverse_finalization_v2.py `
  --finalize-operation-id broker_operation_<id> `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

A repeated invocation returns the same durable finalization. If late commission evidence has arrived, only the monotonic commission-completeness fields may be enriched.

## Fail-closed cases

Finalization is rejected when:

- the broker position does not exactly prove the requested opposite target;
- the position snapshot is stale or incomplete;
- fills are missing, duplicated or have non-contiguous cumulative quantity;
- a fill belongs to another account/order/contract/side;
- source STOP or TAKE PROFIT remains live, submitting, cancel-requested, filled or uncertain;
- source position, episode and protection disagree;
- a liquidation operation already owns the source episode;
- a conflicting finalization already exists.
