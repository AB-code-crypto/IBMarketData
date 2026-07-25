# Execution daily risk

## Purpose

The target daily-risk subsystem owns one `DailyRiskStateV1` per strategy trading day. It derives PnL only from target-owned execution evidence and target market data.

It does not use:

- legacy trade databases;
- account-wide historical executions without target ownership;
- manually entered positions;
- TWS portfolio PnL as an implicit source of truth;
- missing values converted to zero.

The policy currently uses:

```text
timezone = Europe/Moscow
target   = 500 USD
```

## Realized PnL

The execution DB contains three strategy-owned fill sources:

```text
strategic MARKET operations
protective STOP / TAKE PROFIT orders
liquidation MARKET closes
```

Fills are immutable and keyed by `execId`. Commission reports may arrive later.

For a pure `OPEN` fill:

```text
realized contribution = -commission
```

IB reports zero realized PnL for a pure opening execution, so its commission is recognized explicitly.

For `REVERSE`, protective exit and liquidation fills:

```text
realized contribution = commission_report.realized_pnl
```

The commission report must exist and its realized PnL must be present. Otherwise daily PnL is `NOT_READY`.

A REVERSE execution is still counted once in the strategic fill ledger. `ReverseFillAllocationV1` splits position ownership but does not duplicate the broker fill.

## Unrealized PnL

For a broker-proven open position episode:

```text
LONG:
(mark_mid - entry_average_price) * quantity * multiplier

SHORT:
(entry_average_price - mark_mid) * quantity * multiplier
```

The mark is the latest complete target market-data bar:

```text
mid = (bid_close + ask_close) / 2
```

The mark must match the held `conId/localSymbol` and satisfy the configured freshness limit. A stale or mismatched mark makes the calculation `NOT_READY`.

For a proven `FLAT` strategy position:

```text
unrealized PnL = 0
```

## State machine

```text
NOT_READY
MONITORING
TRIGGERED
CLOSING
HALTED
```

### NOT_READY

Published when owned execution evidence, position ownership or market data is incomplete. PnL fields remain `null`; they are never replaced with zeros.

Normal command intake is blocked.

### MONITORING

Published when realized and unrealized PnL are complete and total PnL remains below target.

The daily-risk blocker is removed. Other execution blockers are preserved.

### TRIGGERED

Published once total PnL reaches or exceeds the target while a position or cleanup obligation remains.

The state is sticky for that trading day. A later price retracement does not return it to `MONITORING`.

The broker-free liquidation trigger producer observes this state and appends a stable `DAILY_HALT` trigger to the single execution-owned liquidation operation.

### CLOSING

Published after liquidation begins or when a prior-day halt cleanup remains unresolved.

The change of the Moscow calendar day does not erase unfinished cleanup.

### HALTED

Published only after the execution position is proven `FLAT` and no non-daily execution blocker remains. Cleanup is `COMPLETE`; command intake stays blocked for the trading day.

## Persistence

Schema component:

```text
migrations/execution.daily_risk.v1.json
scripts/run_execution_daily_risk_schema.py
```

It adds:

```text
internal_daily_risk_calculations
internal_daily_risk_transitions
public_daily_risk_calculations_v1
public_daily_risk_transitions_v1
```

The existing execution-owned `internal_daily_risk_states` remains the current-state product.

A single transaction persists:

```text
DailyRiskCalculationV1
DailyRiskStateV1
state transition audit when status/cleanup changes
ExecutionReadinessV1
```

## Install on a fresh target database

```powershell
python scripts/run_execution_daily_risk_schema.py `
  --database $ExecutionDb `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

Development DB compatibility is intentionally unsupported. A checksum mismatch requires a fresh target execution DB.

## Validate

```powershell
python apps/run_execution_daily_risk_v2.py `
  --validate-store-only `
  --execution-database $ExecutionDb `
  --market-database $MarketDb
```

## Run one calculation

```powershell
python apps/run_execution_daily_risk_v2.py `
  --once `
  --execution-database $ExecutionDb `
  --market-database $MarketDb
```

This command performs no IB connection and no broker mutation.

## Fail-closed conditions

Examples:

```text
missing commission report
missing realized_pnl for an exit execution
unknown/stale/multi-contract position projection
missing OPEN position episode
held contract and market mark mismatch
stale market bar
non-finite PnL
unfinished prior-day halt cleanup
```

Every such condition blocks normal command intake instead of publishing a convincing but fabricated zero PnL.
