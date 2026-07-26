# Non-interactive paper REVERSE acceptance

## Purpose

This runner proves the complete REVERSE lifecycle on one already protected paper
position.

For a source `LONG 1` and target `SHORT 1`, the required economic order is:

```text
SELL 2
```

The runner must prove that the resulting fills are split deterministically:

```text
close LONG 1
open SHORT 1
```

The new SHORT episode then receives a new STOP first and a new TAKE PROFIT second.

## Source summary

The runner consumes a successful protected-position summary from either:

```text
PaperAcceptanceResult v1
PaperRestartAcceptanceResult v1
```

The source summary must prove:

```text
broker position accepted
STOP LIVE
TAKE PROFIT LIVE or NOT_REQUIRED
live_position_left_protected = true
```

The target side is derived automatically from the signed broker-position proof:

```text
source LONG  -> target SHORT
source SHORT -> target LONG
```

A conflicting explicit `--target-side` is rejected.

## Strict sequence

```text
fresh broker-proven source position and ownership
→ create one short-lived REVERSE command
→ cancel source TAKE PROFIT once, when LIVE
→ prove terminal TAKE PROFIT state
→ cancel source STOP once
→ prove terminal STOP state
→ prove reverse handoff READY_TO_SUBMIT
→ submit one MARKET reverse order
→ reconcile FILLED / SUCCEEDED
→ invoke same command again and prove no second submission
→ independent position feed proves opposite target position
→ split reverse fills into closing/opening allocations
→ close old PositionEpisode
→ create new opposite PositionEpisode
→ calculate new entry price only from opening allocation
→ create new STOP-first protection plan
→ submit one new STOP
→ prove STOP LIVE
→ submit one new TAKE PROFIT
→ prove TAKE PROFIT LIVE
→ invoke protection again and prove no duplicates
```

## Broker-action budget

For the normal policy with two live source exits and TP enabled on the new episode:

```text
cancel old TAKE PROFIT = 1
cancel old STOP        = 1
MARKET reverse         = 1
new STOP               = 1
new TAKE PROFIT        = 1
broker_mutation_count  = 5
```

One invocation never repeats the same cancellation or submission.

## Prerequisites

Use the same target root and deployment that produced the protected source
position. Keep the independent position feed running.

Do not run:

```text
legacy trader
another target execution writer
target supervisor
paper liquidation runner
```

Required source state:

```text
IBMD_ENVIRONMENT = paper
deployment_id contains paper-drill
source PositionEpisode = OPEN
StrategyPosition = OPEN
source contract is active
STOP = LIVE
TAKE PROFIT = LIVE or NOT_REQUIRED
DailyRiskState = MONITORING and pnl_ready
ExecutionReadiness = READY
no liquidation operation owns the source episode
no unrelated unresolved broker operation
```

## Validate without broker access

```powershell
python scripts/run_paper_reverse_acceptance.py --validate-only
```

## Run

Set the source entry or restart summary:

```powershell
$SourceSummary = `
  "C:\IBMD-paper-acceptance\account1\runtime\paper_acceptance\<drill_id>\<run_id>\summary.json"
```

Then run:

```powershell
python scripts/run_paper_reverse_acceptance.py `
  --run `
  --source-summary $SourceSummary
```

No `Read-Host`, `input()` or manual direction confirmation is used.

## Artifacts

Every command, stdout, stderr, parsed JSON payload and state proof is stored below:

```text
<IBMD_DATA_ROOT>/runtime/paper_reverse_acceptance/<drill_id>/<run_id>/
```

A successful `summary.json` must prove:

```text
handoff_cancel_actions = [TAKE_PROFIT, STOP_LOSS]
reverse_order_quantity = source quantity + target quantity
reverse_submission_count = 1
entry attempt_no = 1
operation = SUCCEEDED
attempt = FILLED
position proof = accepted on opposite side
sum(close_quantity) = source quantity
sum(open_quantity) = target quantity
old episode closed
new episode open
new entry price uses opening allocation only
new STOP LIVE
new TAKE PROFIT LIVE or NOT_REQUIRED
protective idempotency performs no submission
```

## Failure handling

When `failure.json` says:

```json
"position_may_be_open": true
```

stop. Inspect TWS and the saved artifacts. Do not create another REVERSE command.
If the old exits were removed but the reverse order outcome is uncertain, first
reconcile the existing command. If the new opposite position exists without a
proven live STOP, close it manually on the paper account.

## Cleanup

A successful REVERSE acceptance intentionally leaves the opposite position open and
protected. Close it with one of the liquidation acceptance runners, using this
REVERSE summary as the source summary after that runner is enabled for the schema.

## Remaining scenarios

This drill proves one normal REVERSE and its fill allocation. Separate controlled
checks remain necessary for:

```text
restart during reverse handoff cancellation
restart after REVERSE MARKET submission
partial reverse fills
daily-risk trigger after REVERSE
rollover while holding a position
```
