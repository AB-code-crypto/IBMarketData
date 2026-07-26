# Paper policy-trigger liquidation acceptance

## Purpose

This runner proves that `DAILY_FLAT` and `ROLLOVER` do not bypass the single
execution liquidation owner.

The logical trigger time is explicit, but the broker close is real and uses the
standard paper liquidation coordinator.

```text
policy condition at logical UTC time
→ exact durable trigger reason
→ one liquidation operation for the position episode
→ cancel TAKE PROFIT once
→ cancel STOP once
→ one MARKET close on the actually held contract
→ independent position feed proves FLAT
→ episode/protection/strategy position close
→ idempotency without another broker mutation
```

## Why logical time is explicit

Waiting for a real daily-flat or quarterly rollover boundary wastes days or months
and makes the test difficult to reproduce.

The trigger evaluator already accepts an explicit observation time. The acceptance
runner uses that deterministic time to prove policy selection, while every broker
action occurs at the actual current paper-session time.

This does not fake the broker position or the held contract.

## Supported scenarios

```text
DAILY_FLAT
ROLLOVER
```

The broker-free preparer evaluates all current candidates but persists only the
requested scenario. This prevents a future rollover timestamp from also creating a
historical daily-flat trigger during the same controlled test.

## Source summaries

The runner accepts a protected open position produced by:

```text
PaperAcceptanceResult v1
PaperRestartAcceptanceResult v1
PaperReverseAcceptanceResult v1
```

The summary must prove:

```text
position proof accepted
STOP LIVE
TAKE PROFIT LIVE or NOT_REQUIRED
live_position_left_protected = true
```

## DAILY_FLAT

Choose a logical timestamp at or after the configured daily-flat liquidation
boundary for the source episode.

The committed session calendar is parity-only, so current paper testing requires:

```text
--allow-unqualified-session
```

This override is paper-drill only. Production cutover still requires an official
CME-qualified calendar.

Example:

```powershell
python scripts/run_paper_policy_liquidation_acceptance.py `
  --run `
  --source-summary $SourceSummary `
  --scenario DAILY_FLAT `
  --trigger-at-utc 2026-07-27T19:59:51Z `
  --allow-unqualified-session
```

The selected candidate and persisted trigger must both be exactly:

```text
reason = DAILY_FLAT
source_ref = daily-flat:<session>:<local-date>
```

## ROLLOVER

Choose a logical time after the held contract leaves the active interval and the
next registered contract is active.

Example:

```powershell
python scripts/run_paper_policy_liquidation_acceptance.py `
  --run `
  --source-summary $SourceSummary `
  --scenario ROLLOVER `
  --trigger-at-utc 2026-09-16T22:00:01Z
```

The liquidation coordinator still closes the contract actually held by the broker.
It does not replace that route with the contract active at the logical future time.

## Validate without broker access

```powershell
python apps/prepare_execution_policy_liquidation_paper_drill_v2.py `
  --validate-store-only

python scripts/run_paper_policy_liquidation_acceptance.py `
  --validate-only
```

## Fresh-operation rule

The policy preparer refuses an episode that already has a liquidation operation.
A partially completed or uncertain prior close cannot be relabeled as a clean
policy acceptance.

## Broker-action budget

For a source episode with both exits live:

```text
cancel TAKE PROFIT      = 1
cancel STOP             = 1
submit MARKET close     = 1
broker_mutation_count   = 3
liquidation attempt_no  = 1
```

A source episode with `TAKE_PROFIT=NOT_REQUIRED` has no TP cancellation.

## Artifacts

All trigger and broker-action artifacts are saved below:

```text
<IBMD_DATA_ROOT>/runtime/paper_policy_liquidation_acceptance/<run_id>/
```

`summary.json` records:

```text
scenario
logical_trigger_at_utc
trigger_source_ref
trigger_detail
trigger_id
all candidate reasons
blocked reasons
liquidation operation and attempt identities
broker mutation counts
closed state
independent flat proof
```

## Failure handling

If `failure.json` says:

```json
"broker_exposure_possible": true
```

stop. Inspect TWS and the saved artifacts. Do not create a second liquidation
operation for the same position episode. Continue only through reconciliation of
the existing durable operation.

## What remains

This runner proves policy selection and liquidation integration. Separate tests
remain for:

```text
DAILY_HALT from a real DailyRiskState TRIGGERED
production-qualified holiday/early-close calendar
wall-clock scheduler execution at the real boundary
in-flight network disconnect during policy liquidation
```
