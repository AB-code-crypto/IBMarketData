# Target deployment supervisor

## Purpose

`apps/run_target_supervisor.py` is the external launcher for one target
deployment and one IB account.

It launches exactly these required processes:

```text
market_data
broker_position_feed
signal
decision
execution
```

It does not centralize multiple accounts. Four accounts mean four independent
process trees, each with its own `IBMD_DEPLOYMENT_ID`, `IBMD_DATA_ROOT`, account
and client-ID base.

## Boundary

The supervisor reads only:

```text
runtime/health/*.json
child process exit codes
```

It never reads SQLite, never imports trading service packages and never decides:

```text
whether to enter
whether to reverse
whether to place protection
whether to liquidate
```

Execution remains the sole broker-order owner.

## No automatic restart

The first supervisor version intentionally has:

```text
automatic_restart_enabled = false
```

If a required process exits unexpectedly, publishes `FAILED/STOPPED` liveness
or stops heartbeating, the supervisor terminates the complete deployment in
reverse order.

Blindly restarting execution after a possible broker exposure could duplicate
an order. Recovery is explicit and must begin with read-only broker
reconciliation.

A child service may remain `BLOCKED` or `DEGRADED` while still alive and
heartbeating. The supervisor reports the aggregate condition but does not kill
the process merely because execution is waiting for a safety precondition.

## Prerequisite

Create a clean target root first:

```powershell
python scripts/bootstrap_target_deployment.py `
  --target-root $env:IBMD_DATA_ROOT `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

Historical prices are imported separately with:

```text
scripts/import_legacy_market_data.py
```

## Validate and inspect the plan

```powershell
python apps/run_target_supervisor.py --validate-only
```

```powershell
python apps/run_target_supervisor.py --print-plan
```

The plan includes the exact argv, health file and log file for every child.

For paper/development/test deployments the supervisor explicitly passes:

```text
--allow-unqualified-session
```

to the broker-safe execution runtime because the committed parity calendar is
still not production-qualified. Live deployments never receive that override.

## Continuous launch

```powershell
python apps/run_target_supervisor.py --continuous
```

Startup order:

```text
1. market_data
2. broker_position_feed
3. signal
4. decision
5. execution
```

Each process must publish `RUNNING` health with the expected deployment and PID
before the next process starts.

## Files

Logs:

```text
<IBMD_DATA_ROOT>/runtime/logs/market_data.log
<IBMD_DATA_ROOT>/runtime/logs/broker_position_feed.log
<IBMD_DATA_ROOT>/runtime/logs/signal.log
<IBMD_DATA_ROOT>/runtime/logs/decision.log
<IBMD_DATA_ROOT>/runtime/logs/execution.log
```

Supervisor process manifest:

```text
<IBMD_DATA_ROOT>/runtime/supervisor.json
```

Supervisor health:

```text
<IBMD_DATA_ROOT>/runtime/health/supervisor.json
```

The manifest records child PIDs, argv, log paths and startup timestamps. It is
not a trading state store.

## Shutdown

Press `Ctrl+C` in the supervisor console.

Children are terminated in reverse dependency order:

```text
execution
decision
signal
broker_position_feed
market_data
```

After the configured timeout, remaining processes are killed.

## Paper acceptance drill

Do not run the full supervisor during:

```text
scripts/run_paper_acceptance_drill.py
```

The acceptance runner needs exclusive access to the execution process lock and
must control its own one-shot submit/reconciliation sequence. For that drill,
run only the independent market-data and broker-position-feed services required
by the test instructions.

## Current broker limitation

The continuous execution runtime still has broker-mutating stages disabled.
Therefore launching the full stack now exercises continuous data, signal,
decision, reconciliation and state transitions, but does not automatically call
`placeOrder` or `cancelOrder`.

Continuous mutations are enabled only after the controlled paper acceptance
sequence succeeds.
