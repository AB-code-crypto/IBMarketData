# Target data compatibility policy

## Decision

The architecture rewrite does **not** preserve runtime or database compatibility with the legacy robot.

Legacy code and legacy SQLite files remain immutable reference material only. The target system may use new schemas, new identities, new public views and new service boundaries without adapters that imitate the old layout.

## Only supported legacy transfer

The only required one-time data transfer is historical price data:

```text
legacy price SQLite
→ scripts/import_legacy_market_data.py
→ target market_data SQLite
```

No other legacy database is migrated:

```text
no legacy trade DB migration
no legacy order-state migration
no legacy signal-state migration
no legacy position ownership migration
no legacy protection/order adoption by schema conversion
```

Any broker position or broker order that exists at cutover must be handled through current broker reconciliation and explicit ownership rules, not by copying old database rows.

## Target development stores

Target databases created during the rewrite are disposable development artifacts until the cutover schema is frozen.

Consequences:

- schema design is optimized for the target architecture, not old column names;
- no compatibility views are added for legacy readers;
- no in-place migration is promised for experimental target databases;
- when a component schema changes materially, creating a fresh target database is acceptable;
- before production cutover, development schema fragments will be squashed into a clean bootstrap schema;
- the explicit price importer remains a separate, repeatable one-time operation.

## Current lifecycle component

The protective fill/OCA lifecycle uses an explicit execution-owned schema component while the rewrite is still in development. It records immutable fills, late commissions and reconciliation observations without changing legacy databases.

A checksum mismatch in this component is fail-closed and instructs the operator to create a fresh target execution database. This is intentional; backward migration code for disposable development stores is out of scope.

## Cutover rule

Production cutover starts from:

```text
fresh target databases
+ target migrations/bootstrap
+ one-time historical price import
+ live broker reconciliation
```

It does not start by upgrading or mutating the legacy robot databases.
