# Non-interactive paper liquidation acceptance

## Purpose

This runner closes the protected MNQ position created by a successful
`PaperAcceptanceResult` and proves the complete liquidation path on an IB paper
account.

It is deliberately separate from the entry/protection runner. A successful entry
acceptance leaves one live position with a live STOP and TAKE PROFIT. This runner
then proves:

```text
one durable MANUAL_EMERGENCY liquidation trigger
→ cancel TAKE PROFIT once
→ prove terminal TAKE PROFIT state
→ cancel STOP once
→ prove terminal STOP state
→ submit one MARKET close
→ reconcile the same attempt
→ independent position feed proves FLAT
→ PositionEpisode CLOSED
→ ProtectionState CLOSED
→ StrategyPosition FLAT
→ repeat the same liquidation with no broker mutation
```

No automatic retry is enabled. An uncertain cancellation or MARKET-close outcome
stops the runner.

## Prerequisites

Use the same target root and deployment that produced the successful entry
acceptance summary.

Required state:

```text
IBMD_ENVIRONMENT=paper
account begins with D
deployment_id contains paper-drill
position feed is running and fresh
entry summary is PaperAcceptanceResult v1
entry position proof was accepted
STOP is LIVE
TAKE PROFIT is LIVE or NOT_REQUIRED
```

Do not run another execution writer or the target supervisor during the drill.
The runner and each child execution entrypoint use the same `execution.lock`.

## Validate without broker mutation

```powershell
python scripts/run_paper_liquidation_acceptance.py `
  --validate-only
```

This validates execution, liquidation, protection and position-feed schemas. It
does not connect to IB and does not create a liquidation trigger.

## Run after successful entry acceptance

Set the path to the entry runner's `summary.json`:

```powershell
$EntrySummary = `
  "C:\IBMD-paper-acceptance\account1\runtime\paper_acceptance\<drill_id>\<run_id>\summary.json"
```

Then run:

```powershell
python scripts/run_paper_liquidation_acceptance.py `
  --run `
  --entry-summary $EntrySummary
```

No `Read-Host`, `input()` or manual direction confirmation is used.

## Broker-action budget

One child invocation performs at most one broker mutation. A fresh protected
position must report exactly:

```text
CANCEL_TAKE_PROFIT      = 1
CANCEL_STOP             = 1
SUBMIT_MARKET_CLOSE     = 1
broker_mutation_count   = 3
liquidation attempt_no  = 1
```

A resumed operation may report fewer mutations because already persisted actions
are reconciled rather than repeated.

## Artifacts

Every command, stdout, stderr and parsed JSON payload is saved below:

```text
<IBMD_DATA_ROOT>/runtime/paper_liquidation_acceptance/<run_id>/
```

The directory contains:

```text
configuration.json
NN-step-command.json
NN-step-stdout.txt
NN-step-stderr.txt
NN-step-result.json
NN-step-payload.json
liquidation-state-NN.json
flat-proof.json
summary.json or failure.json
```

## Success criteria

`summary.json` must prove:

```text
liquidation operation state  = SUCCEEDED
liquidation attempt state    = FILLED
attempt_no                   = 1
PositionEpisode              = CLOSED
ProtectionState              = CLOSED
exposed protective orders    = 0
StrategyPosition             = FLAT
independent position proof   = accepted
idempotency broker mutation  = false
paper account left flat      = true
```

## Failure handling

If `failure.json` contains:

```json
"broker_exposure_possible": true
```

stop. Inspect TWS and the saved child artifacts. Do not create another liquidation
operation and do not submit another close order. Re-run only the same durable
position episode after understanding the existing broker state.

The runner never treats an exception, timeout or missing order row as proof that a
cancel or MARKET close did not reach IB.

## Relationship to cutover

This acceptance proves manual-emergency liquidation of a normally protected
position. Separate controlled scenarios are still required for:

```text
restart adoption
daily-risk halt
daily flat
rollover
REVERSE
```

The runner uses only new target stores. Legacy trade/state databases are not read
or migrated.
