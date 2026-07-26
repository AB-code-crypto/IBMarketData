# Continuous target decision runtime

## Purpose

`apps/run_decision_runtime_v2.py` closes the runtime gap between the continuous
`signal` service and the continuous `execution` owner.

The process:

```text
public_signal_events_v1
+ public StrategyPositionV1
+ public ExecutionReadinessV1
+ public DailyRiskStateV1
+ existing command lifecycle
-> DecisionRecordV1
-> optional StrategyCommandRequestV1
```

It never connects to Interactive Brokers and never places or cancels an order.

## Ownership and boundaries

The runtime writes only:

```text
<IBMD_DATA_ROOT>/decision/decision.sqlite3
```

It reads only public products from:

```text
<IBMD_DATA_ROOT>/signal/signal.sqlite3
<IBMD_DATA_ROOT>/execution/execution.sqlite3
```

It does not import the `signal` or `execution` service packages. Cross-service
facts are reconstructed from public contracts.

## Signal consumption

Only `SignalEventV1` values matching the current:

```text
strategy_id
strategy_version
instrument_id
configuration_hash
```

are eligible.

Events are consumed in deterministic order:

```text
signal_bar_ts
then event_id
```

A source signal is considered processed after any decision record for the same
current policy exists. Restarting the process does not evaluate it again.

The runtime processes at most one pending signal per poll. This prevents one
poll from creating multiple commands before execution has observed the first
one.

## Execution fixture

The old manual fixture is replaced by facts read from public execution views.

Mapping rules:

```text
StrategyPositionV1.projection_status -> decision projection status
StrategyPositionV1.side              -> decision position side
one held contract's active flag      -> contract_is_active
ExecutionReadinessV1                 -> execution/clock/reconciliation guards
DailyRiskStateV1                     -> PnL readiness and daily-risk status
```

A command is permitted only when execution is:

```text
status = READY
command_intake_enabled = true
broker_actions_enabled = true
reconciliation_complete = true
clock_healthy = true
DailyRiskStateV1.pnl_ready = true
DailyRiskStateV1.status = MONITORING
```

If position, readiness or daily-risk state does not yet exist, the runtime does
not invent an `UNKNOWN` fixture and does not consume the signal. Health becomes
`BLOCKED` and the same event remains pending.

Once complete execution state exists, normal decision-domain behavior applies.
A blocked or unsafe execution state produces an explicit rejected or no-action
decision for that ephemeral signal.

## Unresolved command detection

The runtime treats an existing command as unresolved when:

```text
it has not reached execution yet
it is RECEIVED or ADMITTED without a broker operation
its broker operation is not SUCCEEDED
```

An execution-rejected command is resolved. An admitted command with a
`SUCCEEDED` broker operation is also resolved; subsequent protection or
finalization blockers are carried by `ExecutionReadinessV1`.

When an unresolved command exists, a new signal becomes:

```text
outcome = NO_ACTION
reason  = unresolved_command_exists
```

No duplicate command is created.

## Validation

```powershell
python apps/run_decision_runtime_v2.py --validate-store-only
```

This validates the signal, decision and execution public products without
starting the loop.

## One tick

```powershell
python apps/run_decision_runtime_v2.py --once
```

With no pending signal it returns:

```json
{
  "processed": false,
  "event": null,
  "record": null,
  "command": null,
  "broker_access": false
}
```

With a pending signal it writes one decision and prints the event, decision
record and optional command.

## Continuous mode

```powershell
python apps/run_decision_runtime_v2.py `
  --poll-interval-seconds 1
```

The process holds the standard decision process lock and publishes:

```text
<IBMD_DATA_ROOT>/runtime/health/decision.json
```

A transiently incomplete execution state produces `BLOCKED` health and is
retried. Schema or corrupt-data errors fail the process rather than silently
skipping a signal.

## Safety properties

The runtime has:

```text
no IB client
no placeOrder
no cancelOrder
no forced liquidation command
no automatic retry of broker operations
```

Decision remains a producer of intent. Execution remains the only owner of
broker actions.
