# Target execution foundation

**Status:** broker-free foundation only  
**Input:** one existing `StrategyCommandRequestV1` plus one explicit execution fixture  
**Output:** command admission ledger and execution public read models  
**IB connection:** none  
**Broker orders:** impossible in this slice

## Scope

This slice establishes ownership and persistence before any broker gateway exists:

```text
StrategyCommandRequestV1
+ StrategyPositionV1
+ ExecutionReadinessV1
+ DailyRiskStateV1
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

The execution process is the only writer. Decision is read through its public command view using a read-only SQLite connection.

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

## Fixture-only publication

```powershell
python apps/run_execution_v2.py `
  --publish-fixture-only `
  --foundation-fixture C:\path\execution-foundation-fixture.json
```

## One command admission

```powershell
python apps/run_execution_v2.py `
  --once-command-id strategy_command_<id> `
  --foundation-fixture C:\path\execution-foundation-fixture.json
```

Repeated ingestion of the same command and identical fixture returns the stored state without duplicate transitions. A materially different fixture for the same command fails closed.

## Acceptance

Automated checks cover:

- strict public-contract serialization and invariants;
- `FLAT + OPEN → ADMITTED`;
- opposite active position + `REVERSE → ADMITTED`;
- expiry, readiness, daily halt and non-active contract rejection;
- idempotent command persistence;
- exactly two local transitions per new command;
- read-only decision command access;
- separate single-writer execution store;
- absence of order, fill and broker tables;
- absence of IB imports and calls.

No operator test is required for this broker-free slice. The next slice may add the broker-facing coordinator only on a paper deployment, after its attempt and reconciliation contracts are defined.
