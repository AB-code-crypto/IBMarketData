# Deterministic paper liquidation restart acceptance

## Purpose

This drill proves restart adoption across the broker mutations used to close one
protected position. The explicit path has three mutations; IB may also remove the
STOP as the OCA sibling while reconciling the TAKE PROFIT cancellation:

```text
explicit: cancel TAKE PROFIT -> cancel STOP -> submit liquidation MARKET close
OCA:      cancel TAKE PROFIT -> submit liquidation MARKET close
```

For each mutation, the child execution process:

```text
persists the durable pre-action state
→ invokes the IB paper gateway exactly once
→ receives a successful receipt
→ writes an atomic restart checkpoint
→ terminates with exit code 86 before reconciliation
```

The parent then invokes the ordinary liquidation entrypoint. That invocation must
report:

```text
broker_mutation_performed = false
```

and reconcile the existing broker action rather than repeating it.

## Prerequisite result

The runner consumes a successful protected-position summary from either:

```text
PaperAcceptanceResult v1
PaperRestartAcceptanceResult v1
```

The summary must prove:

```text
position proof accepted
STOP = LIVE
TAKE PROFIT = LIVE or NOT_REQUIRED
live_position_left_protected = true
```

## Fresh operation only

The restart liquidation drill creates one new `MANUAL_EMERGENCY` liquidation
operation. It refuses an already existing operation. This prevents a partial or
uncertain earlier drill from being mislabeled as a clean restart acceptance.

## Action sequence

For the normal two-order protective policy:

```text
liquidation request, broker-free
→ broker-free advance selects the first protective cancellation
→ cancel TAKE PROFIT and terminate child
→ reconcile TAKE PROFIT cancellation without another cancelOrder
→ if STOP remains LIVE: cancel STOP and terminate child
→ otherwise accept broker-proven OCA sibling cancellation
→ reconcile explicit STOP cancellation without another cancelOrder, when used
→ submit one MARKET close and terminate child
→ reconcile the same liquidation attempt without another placeOrder
→ independent position feed proves FLAT
→ PositionEpisode CLOSED
→ ProtectionState CLOSED
→ StrategyPosition FLAT
→ idempotency invocation with no broker mutation
```

If TAKE PROFIT is `NOT_REQUIRED`, that cancellation checkpoint is omitted.

## Safety boundary

The drill-only flags are accepted only when:

```text
IBMD_ENVIRONMENT = paper
deployment_id contains paper-drill
checkpoint is a new .json file below
  <IBMD_DATA_ROOT>/runtime/paper_restart_acceptance
```

The normal liquidation entrypoint is unchanged unless both flags are present:

```text
--drill-crash-after-broker-action
--drill-crash-checkpoint-file <new path>
```

Do not pass these flags manually in normal operation.

## Run order

First run a successful protected-position acceptance:

```powershell
python scripts/run_paper_acceptance_drill.py `
  --run `
  --target-side LONG
```

or the entry/protection restart acceptance:

```powershell
python scripts/run_paper_restart_acceptance.py `
  --run `
  --target-side LONG
```

Keep the independent position feed running.

## Validate without broker mutation

```powershell
python scripts/run_paper_liquidation_restart_acceptance.py `
  --validate-only
```

## Run

```powershell
$EntrySummary = `
  "C:\IBMD-paper-restart\account1\runtime\paper_restart_acceptance\<drill_id>\<run_id>\summary.json"

python scripts/run_paper_liquidation_restart_acceptance.py `
  --run `
  --entry-summary $EntrySummary
```

There is no `Read-Host`, `input()` or manual confirmation.

## Artifacts

All commands, child outputs and checkpoints are stored below:

```text
<IBMD_DATA_ROOT>/runtime/paper_restart_acceptance/liquidation/<run_id>/
```

A successful `summary.json` proves:

```text
restart_actions =
  CANCEL_TAKE_PROFIT
  [CANCEL_STOP when STOP remains LIVE]
  SUBMIT_MARKET_CLOSE

protective_cancel_mode =
  EXPLICIT_BOTH or OCA_AUTO_CANCELLED_STOP or STOP_ONLY

intentional_process_terminations = 2 or 3
broker_mutation_count             = 2 or 3
initial_advance_broker_free       = true
all_resume_mutations_false        = true
attempt_no                        = 1
restart_adoption_proven           = true
liquidation operation             = SUCCEEDED
liquidation attempt               = FILLED
position episode                  = CLOSED
protection                        = CLOSED
strategy position                 = FLAT
independent flat proof            = accepted
```

## Failure handling

If `failure.json` says:

```json
"broker_exposure_possible": true
```

stop immediately. Inspect TWS and the matching checkpoint. Do not create another
liquidation operation, do not send another cancellation, and do not send another
MARKET close until the persisted operation is reconciled.

A successful broker receipt followed by child termination is intentionally treated
as possible broker exposure. The runner never converts the process exit into
`NOT_SENT`.

## What this proves and does not prove

This drill proves deterministic process termination after a successful gateway
return. It does not yet reproduce a TCP disconnect while an IB request itself is
in flight. That separate test requires controlled TWS or network disruption.
