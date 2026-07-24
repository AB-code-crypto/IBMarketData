# Controlled paper execution drill

**Status:** operator-controlled paper test only  
**Broker mutation during preparation:** none  
**Broker mutation during submit:** one explicit MARKET order at most  
**Live accounts:** structurally rejected

## Purpose

This runbook proves the first target broker mutation path without mixing the drill with the long-running shadow stores.

```text
fresh COMPLETE broker-position snapshot proving MNQ FLAT
→ dedicated paper-drill decision/execution databases
→ one short-lived operator-staged OPEN command
→ second preparation pass refreshes the position proof
→ one explicit submit invocation
→ immediate read-only reconciliation
→ repeat invocation proves no second placeOrder
```

The preparation tool does not connect to Interactive Brokers. It only reads the public position-feed product and writes the dedicated decision/execution drill stores.

## Hard boundaries

Preparation is rejected unless:

```text
IBMD_ENVIRONMENT = paper
IB_ACCOUNT_ID begins with D
--confirm-paper-account exactly equals IB_ACCOUNT_ID
IBMD_DEPLOYMENT_ID contains paper-drill
one active registered MNQ FUT contract exists
latest position-feed snapshot is COMPLETE and fresh
MNQ position projection is FLAT
no unresolved broker operation owns the drill scope
```

The staged decision is explicitly marked `paper_execution_drill`. It is not represented as a real rolling-strategy signal.

The drill uses the versioned strategy target quantity. There is no quantity override.

## Separate drill store

Use a dedicated root, for example:

```text
C:\IBMD-paper-drill\account1
```

Do not stage the drill into the normal shadow decision/execution databases.

The raw position feed may remain in the existing shadow root and is passed by explicit path.

## Step 1 — update the checkout

```powershell
cd C:\IBMarketData-shadow
.\.venv\Scripts\Activate.ps1

git pull --ff-only
git rev-parse HEAD
```

## Step 2 — environment for the dedicated drill

```powershell
$env:IBMD_ENVIRONMENT = "paper"
$env:IBMD_DEPLOYMENT_ID = "paper-drill-mnq-account1"
$env:IBMD_DATA_ROOT = "C:\IBMD-paper-drill\account1"
$env:IBMD_APPLICATION_VERSION = "paper-drill"

$env:IB_HOST = "127.0.0.1"
$env:IB_PORT = "7497"
$env:IB_CLIENT_ID = "200"
$env:IB_ACCOUNT_ID = "YOUR_PAPER_ACCOUNT"

$DecisionDb = "C:\IBMD-paper-drill\account1\decision\decision.sqlite3"
$ExecutionDb = "C:\IBMD-paper-drill\account1\execution\execution.sqlite3"
$PositionFeedDb = "C:\IBMD-shadow-data\account1\position_feed\broker_positions.sqlite3"
```

The account must be the actual IB paper account and must start with `D`.

## Step 3 — explicit migrations

```powershell
python scripts/run_target_migrations.py `
  --manifest migrations/decision.v1.json `
  --database $DecisionDb `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply

python scripts/run_target_migrations.py `
  --manifest migrations/execution.v1.json `
  --database $ExecutionDb `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

The position-feed database must already have its own migration applied.

## Step 4 — keep the position feed fresh

Run the raw position feed in another PowerShell using the shadow position-feed root and the same paper account.

```powershell
cd C:\IBMarketData-shadow
.\.venv\Scripts\Activate.ps1

$env:IBMD_ENVIRONMENT = "paper"
$env:IBMD_DEPLOYMENT_ID = "shadow-position-feed-account1"
$env:IBMD_DATA_ROOT = "C:\IBMD-shadow-data\account1"
$env:IBMD_APPLICATION_VERSION = "paper-drill-position-feed"

$env:IB_HOST = "127.0.0.1"
$env:IB_PORT = "7497"
$env:IB_CLIENT_ID = "200"
$env:IB_ACCOUNT_ID = "YOUR_PAPER_ACCOUNT"

python apps/run_position_feed_v2.py `
  --database "C:\IBMD-shadow-data\account1\position_feed\broker_positions.sqlite3" `
  --client-id-offset 60
```

Leave this process running through preparation and submission.

If the paper account contains any MNQ position, preparation stops. The tool does not close, adopt or reinterpret that position.

## Step 5 — validate stores without broker mutation

```powershell
python apps/prepare_execution_paper_drill_v2.py `
  --validate-store-only `
  --decision-database $DecisionDb `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

## Step 6 — prepare and review

Choose the direction explicitly. Example only:

```powershell
$DrillId = "mnq-paper-drill-001"
$TargetSide = "LONG"

python apps/prepare_execution_paper_drill_v2.py `
  --prepare `
  --drill-id $DrillId `
  --target-side $TargetSide `
  --confirm-paper-account $env:IB_ACCOUNT_ID `
  --command-ttl-seconds 600 `
  --position-max-age-seconds 30 `
  --decision-database $DecisionDb `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb `
  | Tee-Object ".\paper-drill-prepared.json"
```

Review at minimum:

```text
ready_for_submit = true
broker_mutations_performed = false
command.command_kind = OPEN
command.desired_target_side = the side you typed
command.desired_target_quantity = 1 for the current profile
execution_fixture.position.projection_status = FLAT
execution_fixture.readiness.broker_actions_enabled = true
active_contract.local_symbol = current catalog contract
```

Preparation creates no broker operation and calls no IB API.

## Step 7 — refresh the proof and submit immediately

The generic submit coordinator reads persisted execution state. Therefore the approved drill procedure reruns preparation immediately before submission. Reusing the same `drill_id` does not create another command; it refreshes the broker-proven FLAT fixture.

```powershell
$PreparedJson = python apps/prepare_execution_paper_drill_v2.py `
  --prepare `
  --drill-id $DrillId `
  --target-side $TargetSide `
  --confirm-paper-account $env:IB_ACCOUNT_ID `
  --command-ttl-seconds 600 `
  --position-max-age-seconds 30 `
  --decision-database $DecisionDb `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb `
  | Out-String

$Prepared = $PreparedJson | ConvertFrom-Json

if (-not $Prepared.ready_for_submit) {
    throw "Paper drill is not ready for submit"
}

$SubmitBefore = [DateTimeOffset]::Parse(
    $Prepared.submit_before_utc
).UtcDateTime

if ((Get-Date).ToUniversalTime() -ge $SubmitBefore) {
    throw "Fresh position proof expired; rerun preparation"
}

$CommandId = $Prepared.command.command_id

python apps/run_execution_submit_v2.py `
  --once-command-id $CommandId `
  --confirm-paper-account $env:IB_ACCOUNT_ID `
  --decision-database $DecisionDb `
  --execution-database $ExecutionDb `
  --submit-client-id-offset 120 `
  --reconciliation-client-id-offset 100 `
  | Tee-Object ".\paper-drill-submit.json"

$SubmitExitCode = $LASTEXITCODE
Write-Host "Paper submit exit code: $SubmitExitCode"
```

This is the first command in the runbook that may send an order.

## Step 8 — expected result

A normal fast MARKET fill should produce approximately:

```text
submission_performed = true
attempt_no = 1
reconciliation_outcome = FILLED
operation_state = SUCCEEDED
attempt_state = FILLED
filled_qty = 1
remaining_qty = 0
```

A live order may temporarily produce `LIVE`.

`UNKNOWN_OUTCOME` is a fail-closed result, not permission to retry.

## Step 9 — repeat the exact command

```powershell
python apps/run_execution_submit_v2.py `
  --once-command-id $CommandId `
  --confirm-paper-account $env:IB_ACCOUNT_ID `
  --decision-database $DecisionDb `
  --execution-database $ExecutionDb `
  --submit-client-id-offset 120 `
  --reconciliation-client-id-offset 100 `
  | Tee-Object ".\paper-drill-repeat.json"
```

Acceptance:

```text
submission_performed = false
same command_id
same operation_id
same attempt_id
same attempt_no = 1
no second broker order
```

## Step 10 — evidence to retain

Keep:

```text
paper-drill-prepared.json
paper-drill-submit.json
paper-drill-repeat.json
TWS order/execution screenshot or exported record
execution.sqlite3 backup after the drill
```

## Stop conditions

Do not submit when any of these is true:

```text
account is not the intended paper account
MNQ is not broker-proven FLAT
position feed is stale or unavailable
active contract is unresolved
another target execution writer is running
another unresolved operation exists
prepared target side is not the intended side
submit_before_utc has passed
```

Do not manually delete an `UNKNOWN_OUTCOME` row and do not rerun with a new drill id to work around it. Reconcile or resolve the existing operation first.

## Deliberate exclusions

This drill does not test or provide:

```text
protective STOP/TP
cancelOrder
automatic retry
liquidation
daily flat
continuous execution scheduling
live-account execution
```

Those remain blocked until the one-order paper drill and the subsequent disconnect/restart drill are accepted.
