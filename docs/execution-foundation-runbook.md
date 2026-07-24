# Target execution foundation

**Status:** broker-free foundation plus position projection  
**Command input:** one existing `StrategyCommandRequestV1`  
**Position input:** latest public `BrokerPositionSnapshotV1` plus versioned MNQ contract calendar  
**Output:** command admission ledger and execution public read models  
**IB connection:** none  
**Broker orders:** impossible in this slice

## Scope

The execution foundation now has two broker-free paths.

### Position projection

```text
public BrokerPositionSnapshotV1
+ registered futures contracts
+ active-contract resolution
+ previous proven execution position episode
→ StrategyPositionV1
→ ExecutionReadinessV1 update
```

### Command admission

```text
StrategyCommandRequestV1
+ persisted StrategyPositionV1
+ persisted ExecutionReadinessV1
+ persisted DailyRiskStateV1
→ RECEIVED
→ ADMITTED or REJECTED
```

`ADMITTED` means only that the command passed local validation and is stored for a future broker coordinator. It does not mean submitted, accepted or filled by Interactive Brokers.

The component does not contain:

- an IB client;
- order placement or cancellation;
- broker order IDs;
- fills or commissions;
- TP/SL management;
- liquidation operations;
- daily PnL calculation;
- continuous decision-outbox polling.

## Public products

The execution store publishes:

```text
public_execution_command_states_v1
public_strategy_positions_v1
public_execution_readiness_v1
public_daily_risk_states_v1
```

Command transitions remain an internal append-only audit table.

## Position projection rules

The projector reads only the position-feed public views. It does not import the `position_feed` service package and it never queries IB directly.

### `FLAT`

A fresh COMPLETE account snapshot contains no MNQ futures position.

The absence of MNQ from a proven complete snapshot is the only raw-feed path to `FLAT`.

### `OPEN`

A fresh snapshot contains exactly one registered MNQ contract and its signed quantity is an integer.

`OPEN` also requires durable ownership evidence: the previous execution projection must contain a `position_episode_id` and the exact same contract identity, signed quantity, side and quantity.

The projector preserves the existing `position_episode_id`; it never invents ownership from a broker position alone.

### `OWNERSHIP_UNPROVEN`

A fresh snapshot contains one valid MNQ position, but execution has no matching owned position episode.

This includes manual positions and positions inherited from another runtime unless they are reconciled explicitly later.

Normal strategy command intake is blocked.

### `MULTI_CONTRACT_INCIDENT`

A fresh snapshot contains positions in more than one registered MNQ quarterly contract.

The contracts are published separately. They are not netted or aggregated into a synthetic strategy position.

Normal strategy command intake is blocked.

### `UNKNOWN`

The snapshot cannot be mapped safely, including:

- account mismatch;
- unknown MNQ futures contract;
- `conId` / `localSymbol` identity conflict;
- non-futures row masquerading as MNQ;
- fractional futures quantity;
- duplicate contract row.

No `FLAT` is manufactured from an unresolved snapshot.

### `STALE`

The latest complete snapshot is older than the configured threshold or no snapshot is available.

If an owned position episode was already proven, the stale projection preserves that episode and its last proven contract facts. This allows a later matching fresh snapshot to restore `OPEN` without losing ownership across a transient feed outage.

Stale facts never permit new risk.

### Active contract

The versioned contract calendar marks each held contract as active or non-active at projection time.

A known owned position on a non-active contract remains `OPEN` for close/reconciliation purposes, but normal command admission is blocked with a liquidation-required reason.

If the calendar has no active contract at the observed time, normal command intake is blocked even when the broker snapshot is flat.

## Execution readiness merge

The projector updates only position-related readiness.

It preserves the existing execution control flags:

```text
command_intake_enabled
broker_actions_enabled
reconciliation_complete
clock_healthy
```

Position-related blocking reasons use the prefix:

```text
position_projection:
```

When a fresh usable projection replaces a transient stale/unknown state, old position-projection reasons are removed. Non-position execution-control reasons remain intact.

If no execution control state exists, safe defaults are published:

```text
status                  = NOT_READY or BLOCKED
command_intake_enabled  = false
broker_actions_enabled  = false
reconciliation_complete = false
clock_healthy           = false
```

## Local command states

Only three states exist in this foundation:

```text
RECEIVED
ADMITTED
REJECTED
```

The persisted current state is `ADMITTED` or `REJECTED`. The transition audit always records:

```text
RECEIVED → ADMITTED
```

or:

```text
RECEIVED → REJECTED
```

No broker lifecycle state is invented before a broker order gateway exists.

## Admission rules

A command is admitted only when:

- strategy, version, deployment, instrument and policy hash match;
- the command has not expired;
- command intake is enabled;
- local reconciliation and execution clock are healthy;
- daily PnL is ready and daily risk is `MONITORING`;
- `OPEN` sees a proven `FLAT` projection;
- `REVERSE` sees an opposite, active-contract `OPEN` projection.

A non-active held contract is rejected with a liquidation-required reason. Decision does not own that liquidation.

`broker_actions_enabled=false` is expected in this slice. It does not block local admission, but service health is `DEGRADED` rather than pretending that broker execution is available.

## Storage

```text
<IBMD_DATA_ROOT>/execution/execution.sqlite3
```

The execution process is the only writer.

Read-only inputs:

```text
<IBMD_DATA_ROOT>/decision/decision.sqlite3
  public_strategy_command_requests_v1

<IBMD_DATA_ROOT>/position_feed/broker_positions.sqlite3
  public_broker_position_latest_v1
  public_broker_position_rows_v1
```

## Explicit migration

```powershell
python scripts/run_target_migrations.py `
  --manifest migrations/execution.v1.json `
  --database "$env:IBMD_DATA_ROOT/execution/execution.sqlite3" `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

Runtime never creates or mutates the schema implicitly.

## Offline validation

```powershell
python apps/run_execution_v2.py --validate-store-only
```

## One position projection

```powershell
python apps/run_execution_v2.py --project-position-only
```

Optional overrides:

```text
--position-feed-database <path>
--position-max-age-seconds <seconds>
--catalog-root <path>
--instrument MNQ
```

This command reads one latest COMPLETE snapshot, publishes one position/readiness update and exits. It does not connect to IB.

## Administrative control-state fixture

The old full fixture remains only as a temporary administrative/test seam for readiness and daily-risk initialization:

```powershell
python apps/run_execution_v2.py `
  --publish-fixture-only `
  --foundation-fixture C:\path\execution-foundation-fixture.json
```

Normal command admission no longer accepts a position fixture.

## One command admission

```powershell
python apps/run_execution_v2.py `
  --once-command-id strategy_command_<id>
```

The command is evaluated against the persisted execution position, readiness and current Moscow-day daily-risk state.

Repeated ingestion of the same command returns the stored state without duplicate transitions. A materially conflicting stored fact fails closed.

## Acceptance

Automated checks cover:

- strict position-projection policy and contract identity;
- empty complete snapshot → `FLAT`;
- matching owned contract → `OPEN` with preserved episode;
- unowned broker position → `OWNERSHIP_UNPROVEN`;
- multiple quarterly contracts → `MULTI_CONTRACT_INCIDENT`;
- unknown contract and fractional quantity → `UNKNOWN`;
- stale/missing snapshots without fake freshness;
- ownership preservation and recovery across stale snapshots;
- non-active held contract blocking;
- read-only position-feed and execution-state adapters;
- execution-store persistence through public read models;
- `FLAT + OPEN → ADMITTED`;
- opposite active position + `REVERSE → ADMITTED`;
- expiry, readiness, daily halt and non-active contract rejection;
- idempotent command persistence;
- exactly two local transitions per new command;
- separate single-writer execution store;
- absence of order, fill and broker tables;
- absence of IB imports and calls.

No operator test is required for this broker-free slice.

The next slice may define broker order-attempt and reconciliation contracts for a paper-only coordinator. It must not enable order submission until unknown-outcome, restart and idempotency states are complete.
