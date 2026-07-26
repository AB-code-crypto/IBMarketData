# Deterministic paper restart acceptance

## Purpose

This drill proves that a broker order is adopted after the submitting child process
terminates between the confirmed broker call and local reconciliation.

The termination is deterministic. The paper-only child entrypoint:

```text
persists SUBMITTING and broker order ID
→ calls the IB paper gateway once
→ receives a successful submission receipt
→ writes an atomic checkpoint
→ terminates with exit code 86 before reconciliation
```

The parent runner then invokes the ordinary entrypoint with the same durable
command or position episode. The ordinary entrypoint must reconcile the existing
order and must report `submission_performed=false`.

## Scope

One fresh run proves the sequence for:

```text
MARKET entry
STOP_LOSS
TAKE_PROFIT, when enabled
```

For each mutation, the runner requires:

```text
one successful broker call checkpoint
same broker order ID after restart
same orderRef after restart
no second submission
terminal FILLED entry or LIVE protective order
attempt_no = 1 for the entry
```

The result leaves the paper position open and protected. Use the separate paper
liquidation acceptance runner to close it.

## Safety boundary

The crash hook is unavailable unless all conditions hold:

```text
IBMD_ENVIRONMENT = paper
deployment_id contains paper-drill
checkpoint path is below
  <IBMD_DATA_ROOT>/runtime/paper_restart_acceptance
checkpoint file does not already exist
```

The normal paper entrypoints do not crash unless both explicit drill arguments are
present:

```text
--drill-crash-after-submit
--drill-crash-checkpoint-file <new .json path>
```

The restart runner supplies those arguments internally. Do not use them for normal
operation.

## Why the child terminates after the broker call

A random process kill is not evidence. It may happen before persistence, before the
broker call, during the broker call, or after reconciliation.

The checkpoint is written only after the wrapped gateway returns a valid receipt.
Therefore the drill proves the exact dangerous interval:

```text
broker may already own the order
local operation is still SUBMITTING
local reconciliation has not started
```

The next process must recover from durable state and broker evidence rather than
calling `placeOrder` again.

## Prerequisites

Run the normal entry/protection acceptance and liquidation acceptance first. The
restart drill uses another completely new target root.

Required conditions:

```text
TWS connected to the intended paper account
API order submission enabled
MNQ position = 0
no live MNQ orders from another drill
independent position feed running
legacy trader stopped
target supervisor stopped
no other target execution writer
```

## Bootstrap a fresh restart root

```powershell
$env:IBMD_ENVIRONMENT = "paper"
$env:IBMD_DEPLOYMENT_ID = "paper-drill-mnq-account1-restart"
$env:IBMD_DATA_ROOT = "C:\IBMD-paper-restart\account1"
$env:IBMD_APPLICATION_VERSION = (git rev-parse HEAD).Trim()

$env:IB_HOST = "127.0.0.1"
$env:IB_PORT = "7497"
$env:IB_CLIENT_ID = "200"
$env:IB_ACCOUNT_ID = "DUQ895165"

python scripts/bootstrap_target_deployment.py `
  --target-root $env:IBMD_DATA_ROOT `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

The root must not exist before bootstrap.

## Start the independent position feed

In a separate PowerShell:

```powershell
python apps/run_position_feed_v2.py `
  --database "$env:IBMD_DATA_ROOT\position_feed\broker_positions.sqlite3" `
  --client-id-offset 60 `
  --poll-interval-seconds 2 `
  --snapshot-max-age-seconds 10
```

## Validate without broker mutation

```powershell
python scripts/run_paper_restart_acceptance.py --validate-only
```

## Run

```powershell
python scripts/run_paper_restart_acceptance.py `
  --run `
  --target-side LONG
```

There is no `Read-Host`, `input()` or manual direction confirmation.

## Artifacts

Every normal invocation, intentional child termination and atomic checkpoint is
stored under:

```text
<IBMD_DATA_ROOT>/runtime/paper_restart_acceptance/<drill_id>/<run_id>/
```

A successful `summary.json` proves:

```text
intentional_process_terminations = 3
broker_mutation_count             = 3
all_resume_submissions_false      = true
restart_adoption_proven           = true
attempt_no                        = 1
entry                              = FILLED / SUCCEEDED
STOP                               = LIVE
TAKE PROFIT                        = LIVE or NOT_REQUIRED
```

## Failure handling

When `failure.json` reports `position_may_be_open=true`:

```text
stop
inspect TWS
inspect the matching checkpoint and child stderr
never create a new drill ID
never send another order manually through the runner
```

If the position is open and STOP is not proven live, close the paper position
manually. The runner never treats a child exit, timeout or missing order row as
proof that the broker did not receive the order.

## Remaining restart scenarios

This drill covers successful submit-return followed by process termination for
entry and protective orders. Separate controlled drills remain necessary for:

```text
cancelOrder restart adoption
liquidation MARKET-close restart adoption
TWS disconnect during an in-flight request
full supervisor/process restart
```
