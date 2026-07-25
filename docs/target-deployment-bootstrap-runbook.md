# Clean target deployment bootstrap

## Purpose

`scripts/bootstrap_target_deployment.py` creates one completely new target data
root for one account/deployment.

The bootstrap does **not** preserve or migrate:

```text
legacy trading state
legacy orders/fills database
legacy signal database
legacy job_data state
legacy ownership/protection state
```

The only supported legacy bridge remains the separate one-time historical price
import.

## Output layout

For a target root such as:

```text
C:\IBMD-data\account1
```

the bootstrap creates:

```text
catalog\
market_data\MNQ.sqlite3
position_feed\broker_positions.sqlite3
signal\signal.sqlite3
decision\decision.sqlite3
execution\execution.sqlite3
runtime\bootstrap.json
```

The execution database includes the current target components:

```text
protective lifecycle
liquidation
reverse finalization
daily risk
```

## Safety model

The bootstrap:

- requires a target root that does not exist;
- never deletes or reuses an existing directory;
- creates all stores under a sibling staging directory;
- validates every migration and execution component ledger;
- copies and validates the versioned target catalog;
- writes bootstrap metadata only after every store succeeds;
- atomically renames the staging directory into place;
- removes the staging directory on any failure;
- performs no IB connection or broker mutation;
- performs no historical-price import.

A partially initialized deployment root is therefore never published as the
final target path.

## Bootstrap manifest

The versioned bundle is:

```text
bootstrap/target.v1.json
```

It defines only the five target-owned stores and their current schema artifacts.
The bootstrap metadata records SHA-256 hashes of:

```text
bootstrap manifest
all base migration manifests
all execution component manifests
all catalog JSON artifacts
```

`--validate-target` fails when the application artifacts no longer match the
ones used to create the deployment.

## Plan

```powershell
cd C:\IBMarketData-shadow
.\.venv\Scripts\Activate.ps1

$TargetRoot = "C:\IBMD-data\account1"
$Version = (git rev-parse HEAD).Trim()

python scripts/bootstrap_target_deployment.py `
  --target-root $TargetRoot `
  --application-version $Version `
  --plan
```

Planning does not create the target root.

## Apply

```powershell
python scripts/bootstrap_target_deployment.py `
  --target-root $TargetRoot `
  --application-version $Version `
  --apply
```

If `$TargetRoot` already exists, the command stops. Do not add a destructive
`--force` mode. Choose a new root or explicitly archive/delete the old test root
outside the bootstrap tool.

## Validate

```powershell
python scripts/bootstrap_target_deployment.py `
  --target-root $TargetRoot `
  --application-version $Version `
  --validate-target
```

Validation checks:

```text
bootstrap bundle hash
source artifact hashes
catalog bundle hash
all store migration ledgers
all execution component ledgers
```

## Production calendar gate

The committed repository calendar remains parity-only until an official CME
Trading Schedules export is transformed and reviewed.

A production bootstrap must use a catalog copy containing that qualified
artifact and pass:

```powershell
python scripts/bootstrap_target_deployment.py `
  --target-root $TargetRoot `
  --application-version $Version `
  --require-production-sessions `
  --apply
```

With the current parity calendar this command must fail before creating the
final root.

## One-time historical price import

After bootstrap, the target market-data DB is structurally ready but empty.
Run the independent importer first as a dry-run, then with `--apply`:

```powershell
$LegacyDb = "C:\IBMarketData-shadow\data\prices\MNQ.sqlite3"
$TargetDb = Join-Path $TargetRoot "market_data\MNQ.sqlite3"

python scripts/import_legacy_market_data.py `
  --legacy-database $LegacyDb `
  --legacy-table MNQ_5s `
  --target-database $TargetDb `
  --instrument MNQ `
  --start-utc <START_UTC> `
  --end-utc <END_UTC>

python scripts/import_legacy_market_data.py `
  --legacy-database $LegacyDb `
  --legacy-table MNQ_5s `
  --target-database $TargetDb `
  --instrument MNQ `
  --start-utc <START_UTC> `
  --end-utc <END_UTC> `
  --apply
```

Rerun the dry-run and require:

```text
existing_exact_count = complete_source_count
imported_count = 0
```

No other legacy database is copied.

## Deployment rule

Use one separately bootstrapped root per account:

```text
C:\IBMD-data\account1
C:\IBMD-data\account2
C:\IBMD-data\account3
C:\IBMD-data\account4
```

There is no shared execution database and no centralized multi-account writer.
