# Execution runtime v2

## Purpose

`apps/run_execution_runtime_v2.py` is the single-owner execution control loop.
It holds the execution process lock for the full process lifetime and runs stages
strictly in risk-first order.

This first continuous slice is deliberately **broker-safe**:

- read-only IB reconciliation is enabled;
- broker-free state transitions are enabled;
- every `placeOrder` and `cancelOrder` stage is disabled;
- a pending broker mutation blocks lower-priority work and is surfaced in health;
- no automatic retry exists.

The broker-mutating stages are enabled only after the controlled paper acceptance
gate proves entry, protection, restart adoption and liquidation behavior.

## Canonical stage order

```text
1. STRATEGIC_RECONCILIATION
2. PROTECTIVE_RECONCILIATION
3. LIQUIDATION_ADVANCE
4. POSITION_FINALIZATION
5. POSITION_PROJECTION
6. DAILY_RISK
7. LIQUIDATION_TRIGGERS
8. REVERSE_HANDOFF
9. PROTECTIVE_SUBMISSION
10. COMMAND_ADMISSION
11. STRATEGIC_SUBMISSION
```

`POSITION_FINALIZATION` intentionally precedes generic position projection.
After a filled OPEN or REVERSE operation, the broker position may already have
changed while the target position episode has not yet been created. Projecting
that broker position first could incorrectly publish `OWNERSHIP_UNPROVEN` and
lose the source state needed to split a REVERSE fill into close/open allocations.

## Tick rules

A tick:

- executes stages sequentially;
- never runs stages concurrently;
- continues through broker-free `UPDATED` results;
- stops immediately on `BLOCKED`, `FAILED` or `MUTATED`;
- permits at most one broker mutation;
- fails closed if a stage reports a mutation while mutations are disabled;
- publishes one execution health heartbeat after the tick.

The runtime kernel requires the exact canonical stage set. Missing or reordered
stages are rejected at construction time.

## Enabled stages in this slice

### Strategic reconciliation

Reads existing target operations, then uses the shared read-only IB reconciliation
reader to adopt open/completed orders, executions and late commissions.

### Protective reconciliation

Reconciles STOP/TAKE PROFIT/OCA facts for the single open position episode.
Multiple simultaneously open target episodes are treated as a blocking incident.

### Position finalization

A proven `SUCCEEDED` operation is finalized before generic position projection:

- OPEN creates a new position episode and STOP-first protection plan;
- REVERSE closes the source episode, allocates fills between close/open parts,
  and creates the opposite episode atomically.

### Position projection

Reads the latest COMPLETE independent position-feed snapshot and refreshes the
target `StrategyPositionV1` and execution readiness.

### Daily risk

Calculates strategy-owned realized/unrealized PnL and persists
`DailyRiskStateV1` plus readiness atomically.

### Liquidation trigger production

Evaluates missing/rejected/breached STOP, daily halt, daily flat and rollover
conditions and appends durable triggers to the single liquidation operation.

### Command admission

Reads the oldest decision command not yet present in execution and persists one
local admission result. Admission performs no broker action.

## Disabled mutating stages

The runtime detects but does not execute:

```text
LIQUIDATION_ADVANCE
REVERSE_HANDOFF cancellation
PROTECTIVE_SUBMISSION
STRATEGIC_SUBMISSION
```

When one of these stages has pending work, the tick returns `BLOCKED` and health
becomes `DEGRADED`. Lower-priority work is not executed.

## Store requirements

A fresh target execution database must contain:

```text
execution base schema v1..v3
execution_protective_lifecycle component
execution_liquidation component
execution_reverse_finalization component
execution_daily_risk component
```

The runtime also requires the target decision, position-feed and market-data
public products. Legacy trade/state databases are not read.

## Commands

Validate only:

```powershell
python apps/run_execution_runtime_v2.py --validate-store-only
```

Run one broker-safe tick:

```powershell
python apps/run_execution_runtime_v2.py --once
```

Run continuously:

```powershell
python apps/run_execution_runtime_v2.py `
  --continuous `
  --poll-interval-seconds 1
```

During development with the non-production session calendar:

```powershell
python apps/run_execution_runtime_v2.py `
  --continuous `
  --allow-unqualified-session
```

The override affects only daily-flat trigger evaluation. It does not enable
broker mutations.

## Health behavior

The runtime writes the standard execution health file:

```text
<IBMD_DATA_ROOT>/runtime/health/execution.json
```

Tick mapping:

```text
IDLE / PROGRESSED → READY
BLOCKED           → DEGRADED
FAILED            → BLOCKED
```

Each executed stage is published as a dependency entry. A pending disabled
broker mutation appears as the health blocking reason.

## Paper gate

Continuous broker mutation remains disabled until the following paper evidence
is complete:

```text
DAY MARKET entry → FILLED
repeat/restart without duplicate entry
position episode creation
STOP submission and LIVE proof
TAKE PROFIT submission and OCA proof
REVERSE handoff and fill finalization
DailyRisk trigger
liquidation and broker-proven FLAT
```

After that gate, mutating stage adapters may be connected to the existing
one-shot coordinators. The runtime kernel itself already enforces one broker
action per tick.
