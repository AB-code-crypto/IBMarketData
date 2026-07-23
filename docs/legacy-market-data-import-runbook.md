# Explicit legacy market-data import

**Status:** migration support for target shadow validation  
**Source:** complete BID/ASK bars already stored by the legacy robot  
**Destination:** target `market_data` SQLite store

## Purpose

The target rolling signal needs approximately one year of complete five-second MNQ history. That history already exists in the verified legacy price database.

Reloading the same year from Interactive Brokers would add pacing cost and another opportunity for avoidable source differences. This command performs a one-time, explicit and validated copy of complete legacy bars into the target market-data store.

It is not:

- an automatic scheduler;
- a general repair platform;
- a recurring synchronization process;
- a production cutover;
- a replacement for target realtime collection.

## Safety properties

The importer:

- opens the legacy SQLite database read-only;
- requires distinct legacy and target files;
- defaults to dry-run;
- requires an explicit aligned UTC interval;
- imports only rows with all eight BID/ASK OHLC values present;
- resolves every legacy `localSymbol` through the target contract calendar;
- rejects an unknown contract;
- rejects an existing target bar whose values differ;
- preserves exact BID/ASK values;
- skips target bars that already match exactly;
- takes the existing target `market_data` process lock before `--apply`;
- records one target audit event for the import;
- never modifies the legacy database.

Stop `apps/run_market_data_v2.py` before applying the import. The lock prevents a second target writer, but an orderly stop makes the operation explicit.

## Example paths for the measured Windows deployment

```text
Legacy/control:
C:\IBMarketData-shadow\data\prices\MNQ.sqlite3
Table: MNQ_5s

Target/shadow:
C:\IBMD-shadow-data\account1\market_data\MNQ.sqlite3
```

## Environment

The dry-run does not require deployment environment values.

`--apply` uses the target service lock and therefore requires the deployment identity and target data root in the current PowerShell process:

```powershell
$env:IBMD_DEPLOYMENT_ID = "shadow-mnq-account1"
$env:IBMD_DATA_ROOT = "C:\IBMD-shadow-data\account1"
$env:IBMD_APPLICATION_VERSION = "architecture-rewrite-shadow"
```

IB connection variables are not used by the importer.

## Dry-run

The current rolling profile uses a 365-day candidate lookback and a 90-minute pattern window. The example starts early enough to include the pattern preceding the earliest candidate and ends immediately after the measured reconnect interval.

```powershell
cd C:\IBMarketData-shadow
.\.venv\Scripts\Activate.ps1

$LegacyDb = "C:\IBMarketData-shadow\data\prices\MNQ.sqlite3"
$TargetDb = "C:\IBMD-shadow-data\account1\market_data\MNQ.sqlite3"

python scripts/import_legacy_market_data.py `
  --legacy-database $LegacyDb `
  --legacy-table MNQ_5s `
  --target-database $TargetDb `
  --instrument MNQ `
  --start-utc 2025-07-23T00:00:00Z `
  --end-utc 2026-07-23T17:25:00Z
```

Dry-run output includes:

```text
complete_source_count
existing_exact_count
imported_count
incomplete_source_count
applied = false
```

No target rows are inserted without `--apply`.

## Apply

Run only after reviewing the dry-run counts:

```powershell
python scripts/import_legacy_market_data.py `
  --legacy-database $LegacyDb `
  --legacy-table MNQ_5s `
  --target-database $TargetDb `
  --instrument MNQ `
  --start-utc 2025-07-23T00:00:00Z `
  --end-utc 2026-07-23T17:25:00Z `
  --apply
```

A successful apply returns:

```text
applied = true
imported_count >= 0
```

Running the same command a second time is expected to produce:

```text
existing_exact_count = complete_source_count
imported_count        = 0
```

That is the idempotency check.

## Post-import verification

Compare the same interval read-only:

```powershell
python scripts/compare_market_data_shadow.py `
  --legacy-database $LegacyDb `
  --legacy-table MNQ_5s `
  --target-database $TargetDb `
  --instrument MNQ `
  --start-utc 2025-07-23T00:00:00Z `
  --end-utc 2026-07-23T17:25:00Z `
  --output-json "C:\IBMarketData-shadow\market-data-import-parity.json"

$LASTEXITCODE
Get-Content "C:\IBMarketData-shadow\market-data-import-parity.json"
```

Acceptance requires:

```text
exit code             = 0
is_match              = true
legacy_only_count     = 0
target_only_count     = 0
value_mismatch_count  = 0
```

Incomplete legacy half-bars are deliberately excluded from both the import and parity comparison.

## Gate to target signal

Target signal work may begin only after:

1. dry-run counts are reviewed;
2. import completes successfully;
3. the full imported interval passes the strict comparator.

The signal remains shadow-only and does not create trade commands during its first parity stage.
