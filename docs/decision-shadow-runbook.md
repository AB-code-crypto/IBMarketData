# Target decision — shadow boundary

**Status:** shadow only  
**Input:** one target `SignalEventV1` plus one explicit execution/position fixture  
**Output:** decision audit and optional `StrategyCommandRequestV1`  
**Execution consumer:** disabled  
**Broker access:** none

## Scope

This slice implements only the normal strategy decision already present in the current robot:

```text
FLAT + LONG/SHORT signal
→ OPEN target side with configured fixed quantity

OPEN on the same side
→ NO_ACTION

OPEN on the opposite side and active contract
→ REVERSE target side with configured fixed quantity
```

It also records explicit rejections for:

- expired signal;
- unavailable, stale, unknown or multi-contract position projection;
- unproven position ownership;
- non-active held futures contract;
- execution not ready;
- unhealthy execution clock;
- pending PnL reconciliation;
- daily risk state other than `MONITORING`.

An unresolved command produces `NO_ACTION` rather than a second command.

## Deliberate exclusions

The decision component does not create:

- daily-flat liquidation;
- rollover liquidation;
- daily take-profit liquidation;
- missing-STOP emergency close;
- broker retry orders;
- protective orders;
- ordinary broker orders.

Those operations remain the responsibility of the future execution process and its single liquidation/order coordinator.

## Storage

The component owns:

```text
<IBMD_DATA_ROOT>/decision/decision.sqlite3
```

Public views:

```text
public_decision_records_v1
public_strategy_command_requests_v1
```

The command table is an outbox only. No consumer is enabled in this slice.

## Input fixture

Until target execution publishes its public read models, shadow evaluation requires one strict UTF-8 JSON fixture:

```json
{
  "schema_name": "ExecutionDecisionFixture",
  "schema_version": 1,
  "observed_at_utc": "2026-07-24T10:00:05Z",
  "execution_ready": true,
  "execution_clock_healthy": true,
  "pnl_reconciliation_ready": true,
  "unresolved_command": false,
  "daily_risk_status": "MONITORING",
  "blocking_reason": null,
  "position": {
    "account_id": "U0000000",
    "strategy_id": "IBMarketData.rolling",
    "deployment_id": "shadow-mnq-account1",
    "instrument_id": "MNQ",
    "projection_status": "FLAT",
    "side": "FLAT",
    "quantity": 0,
    "contract_is_active": null
  }
}
```

The fixture is temporary migration input, not a production source of truth.

## Explicit migration

```powershell
python scripts/run_target_migrations.py `
  --manifest migrations/decision.v1.json `
  --database "$env:IBMD_DATA_ROOT/decision/decision.sqlite3" `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

Runtime never creates or mutates the schema implicitly.

## Offline validation

```powershell
python apps/run_decision_v2.py --validate-store-only
```

## One explicit shadow evaluation

```powershell
python apps/run_decision_v2.py `
  --once-event-id signal_event_<id> `
  --execution-fixture C:\path\execution-decision-fixture.json
```

The result contains:

```text
record  → COMMAND / NO_ACTION / REJECTED audit
command → StrategyCommandRequestV1 or null
```

Repeated evaluation of the same signal and identical fixture returns the same decision and command identities. A materially conflicting stored fact fails closed.

## Current acceptance

Automated checks cover:

- strict fixture and public-contract validation;
- `FLAT → OPEN`;
- same-side `NO_ACTION`;
- opposite active-side `REVERSE`;
- non-active contract rejection;
- execution/risk guard rejection;
- signal expiry;
- command identity and store idempotency;
- separate single-writer decision store;
- absence of forced-liquidation command kinds;
- absence of broker imports and calls.

No additional operator test is required for this implementation slice. Target execution is still absent, so no command can reach Interactive Brokers.
