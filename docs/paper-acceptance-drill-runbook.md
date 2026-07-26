# Non-interactive paper acceptance drill

## Status

This runner is the controlled acceptance path for the target execution stack. It is paper-only and intentionally leaves the resulting MNQ position open with a proven live STOP and TAKE PROFIT. Manual cleanup in TWS remains required until the liquidation path passes its own paper drill.

The runner does not preserve or import legacy trade/state data. It uses only target decision, execution and position-feed stores. Historical price import is unrelated to this drill.

## What one run proves

A successful run performs the following strict sequence:

```text
fresh COMPLETE broker position = FLAT
→ prepare one OPEN command (LONG by default)
→ submit at most one MARKET entry
→ bounded read-only reconciliation
→ FILLED / SUCCEEDED
→ invoke the same command again
→ prove submission_performed=false and the same operation/attempt/orderRef
→ wait for the independent position feed to prove the held contract and quantity
→ create PositionEpisodeV1 and the STOP-first protection plan
→ submit at most one STOP
→ prove STOP LIVE
→ submit at most one TAKE PROFIT
→ prove TAKE PROFIT LIVE
→ invoke protective submission again
→ prove submission_performed=false and unchanged live protection
```

No stage automatically retries an uncertain broker mutation. `UNKNOWN_OUTCOME`, an unexpected terminal order state, a stale position snapshot, or an identity change stops the runner immediately.

## Non-interactive operation

There is no `Read-Host`, `input()`, or manual typing of `LONG`. The default side is explicitly `LONG`; `--target-side SHORT` changes it.

The configured account is passed internally as the exact paper-account confirmation. The runner still rejects:

```text
IBMD_ENVIRONMENT != paper
account not beginning with D
deployment_id without paper-drill
stale/incomplete position feed
closed session or daily-flat risk window
```

## Prerequisites

Use a dedicated clean target root created by:

```powershell
python scripts/bootstrap_target_deployment.py `
  --target-root $env:IBMD_DATA_ROOT `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

TWS must be connected to the configured paper account with API order submission enabled. The independent position feed must already be running against the same target root:

```powershell
python apps/run_position_feed_v2.py `
  --database "$env:IBMD_DATA_ROOT\position_feed\broker_positions.sqlite3" `
  --client-id-offset 60 `
  --poll-interval-seconds 2 `
  --snapshot-max-age-seconds 10
```

Before the drill, TWS and the position feed must prove MNQ is flat. Do not run the legacy trader or another target execution writer.

## Validate without broker access

```powershell
python scripts/run_paper_acceptance_drill.py --validate-only
```

This validates target decision/execution/position-feed schemas only. It does not connect to IB or write broker state.

## Run the complete entry and protection drill

```powershell
python scripts/run_paper_acceptance_drill.py `
  --run `
  --target-side LONG
```

A drill ID is generated automatically. To resume the exact same durable command after a process interruption, provide the original ID:

```powershell
python scripts/run_paper_acceptance_drill.py `
  --run `
  --drill-id paper-acceptance-YYYYMMDDTHHMMSSZ `
  --target-side LONG
```

Never invent a new drill ID after an uncertain result. Reuse the existing ID only after inspecting the saved failure artifact and broker state.

## Artifacts

Every child invocation is recorded below:

```text
<IBMD_DATA_ROOT>/runtime/paper_acceptance/<drill_id>/<run_id>/
```

The directory contains:

```text
configuration.json
NN-step-command.json
NN-step-stdout.txt
NN-step-stderr.txt
NN-step-result.json
NN-step-payload.json
position-proof-NN.json
protection-state-NN.json
summary.json or failure.json
```

This preserves TWS warnings and exact JSON even when the runner stops.

## Successful summary

Acceptance requires:

```text
entry_submission_count        = 1 for a fresh run
attempt_no                    = 1
entry operation               = SUCCEEDED
entry attempt                 = FILLED
entry idempotency submission  = false
position proof                = accepted
STOP submission count         = 1 for a fresh run
STOP state                    = LIVE
TAKE PROFIT submission count  = 1 for a fresh run
TAKE PROFIT state             = LIVE
protective idempotency        = no submission
broker_mutation_count         = 3 for a fresh run
```

A resumed run may report fewer mutations because already persisted broker actions are reconciled rather than repeated.

## Failure handling

If `failure.json` says:

```text
position_may_be_open = true
```

inspect TWS immediately. Do not create another command. If STOP is not proven live, close MNQ manually on the paper account. The runner never interprets an exception or missing order row as proof that no broker exposure exists.

## After success

The position remains open and protected. Close it manually in TWS after collecting the entry, STOP, TAKE PROFIT and idempotency evidence. Automated liquidation will be enabled only after its separate controlled paper acceptance drill.
