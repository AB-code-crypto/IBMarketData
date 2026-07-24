# Target rolling signal — shadow runbook

**Status:** shadow only  
**Input:** target `market_data` public views  
**Output:** target `signal` SQLite store  
**Trading side effects:** none

## Scope

This slice implements only the existing MNQ rolling calculation:

```text
complete target BID/ASK bars
→ 90-minute current pattern
→ historical candidates with the same hourly phase
→ centered Pearson filter
→ minmax hard filter
→ weighted candidate score
→ top-candidate potential
→ SignalCalculationV1
→ optional SignalEventV1
```

It does not:

- read broker positions;
- create decision or execution commands;
- send orders;
- write legacy `state.sqlite3` or `trade.sqlite3`;
- generate plots;
- send Telegram notifications;
- change strategy parameters.

Plots remain deliberately outside this first parity slice. A plot failure must never determine whether a signal exists.

## Files

For the measured Windows deployment:

```text
Target market-data:
C:\IBMD-shadow-data\account1\market_data\MNQ.sqlite3

Target signal store:
C:\IBMD-shadow-data\account1\signal\signal.sqlite3

Signal health:
C:\IBMD-shadow-data\account1\runtime\health\signal.json

Signal lock:
C:\IBMD-shadow-data\account1\runtime\locks\signal.lock
```

## Update the checkout

```powershell
cd C:\IBMarketData-shadow
.\.venv\Scripts\Activate.ps1

git pull --ff-only
git rev-parse HEAD
```

## Environment

The signal process does not connect to IB, but it uses the shared typed deployment settings. Set the deployment values in the current PowerShell process:

```powershell
$env:IBMD_DEPLOYMENT_ID = "shadow-mnq-account1"
$env:IBMD_DATA_ROOT = "C:\IBMD-shadow-data\account1"
$env:IBMD_APPLICATION_VERSION = "signal-shadow"

$env:IB_HOST = "127.0.0.1"
$env:IB_PORT = "7497"
$env:IB_CLIENT_ID = "200"
$env:IB_ACCOUNT_ID = "YOUR_ACCOUNT_ID"
```

No IB connection is opened by `run_signal_v2.py`.

## Explicit signal-store migration

```powershell
$TargetDb = "C:\IBMD-shadow-data\account1\market_data\MNQ.sqlite3"
$SignalDb = "C:\IBMD-shadow-data\account1\signal\signal.sqlite3"

python scripts/run_target_migrations.py `
  --manifest migrations/signal.v1.json `
  --database $SignalDb `
  --application-version $env:IBMD_APPLICATION_VERSION `
  --apply
```

Runtime never creates or mutates its schema implicitly.

## Offline dependency validation

```powershell
python apps/run_signal_v2.py `
  --market-database $TargetDb `
  --signal-database $SignalDb `
  --validate-store-only
```

Expected output:

```text
signal dependencies are compatible: market=..., signal=...
```

## Deterministic one-shot calculation

The imported target history covers the following test point:

```text
2026-07-23T17:24:00Z
```

Run:

```powershell
python apps/run_signal_v2.py `
  --market-database $TargetDb `
  --signal-database $SignalDb `
  --once-at 2026-07-23T17:24:00Z |
  Tee-Object ".\target-signal-once.json"
```

The result is either:

```text
status = SIGNAL
and event contains LONG or SHORT
```

or:

```text
status = NO_SIGNAL
and reason explains why the current potential did not cross the frozen threshold
```

Both are valid calculation outcomes. The parity question is whether the metrics match the current rolling algorithm, not whether this particular historical minute happens to produce a trade signal.

Run the same command a second time. It must return the already stored calculation rather than create another calculation/event for the same strategy configuration and source bar.

## Explicit legacy-vs-target calculation parity

After the repeated one-shot has returned the same stored target fact, compare it with the current rolling implementation on the same timestamp:

```powershell
python scripts/compare_signal_shadow.py `
  --legacy-price-dir "C:\IBMarketData-shadow\data\prices" `
  --target-json ".\target-signal-once.json" `
  --instrument MNQ `
  --output-json ".\target-signal-parity.json"

$ParityExitCode = $LASTEXITCODE
Write-Host "Signal comparator exit code: $ParityExitCode"
Get-Content ".\target-signal-parity.json"
```

The comparator:

- derives the timestamp from the stored target JSON;
- points the current rolling implementation at the explicit legacy price directory;
- opens the legacy price database with SQLite `mode=ro`;
- calls the current Pearson, minmax, score and potential functions directly;
- does not generate a plot;
- does not write a legacy signal event;
- compares candidate counts, entry price, filter counts, scores, direction, potential and final signal status.

The current legacy imports still require the repository `.env` to be valid. No IB connection is opened.

Acceptance requires:

```text
exit code = 0
is_match  = true
mismatches = []
```

Do not increase tolerance to hide a mismatch. A difference must be explained or fixed before continuous shadow mode.

## Public read-only products

```text
public_signal_calculations_v1
public_signal_events_v1
public_signal_latest_v1
```

The signal store contains no `trade_intents`, command outbox or broker-order table.

## Continuous shadow mode

Continuous mode is used only after the explicit one-shot and legacy parity result are reviewed. Run target market-data first, then:

```powershell
python apps/run_signal_v2.py `
  --market-database $TargetDb `
  --signal-database $SignalDb
```

Startup intentionally accepts the current due point without recalculating it. Calculation begins at the next 60-second due point, matching the current robot.

## Acceptance before decision work

The signal gate requires:

1. explicit migration and store validation;
2. one real historical `--once-at` calculation;
3. repeated `--once-at` proving no duplicate calculation/event;
4. metric parity with the existing rolling calculation on the same timestamp;
5. a short continuous shadow run proving one calculation attempt per due point;
6. confirmation that no trading command or broker call is possible from the signal process.

Only after these checks may the target `decision` service consume `SignalEventV1`.
