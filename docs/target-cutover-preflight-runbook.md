# Target cutover preflight

## Status

The preflight is broker-free. It starts no service, opens no IB connection and sends no order.

It currently authorizes only:

```text
PAPER_SOAK
```

`LIVE_CUTOVER` is deliberately blocked because live-account enablement has not been implemented or accepted.

## Acceptance manifest

A paper-soak authorization requires eight successful runner summaries:

```text
ENTRY_PROTECTION
LIQUIDATION
RESTART
LIQUIDATION_RESTART
REVERSE
DAILY_HALT
DAILY_FLAT
ROLLOVER
```

The manifest builder validates every summary against its exact schema and success conditions. It then copies the summaries into:

```text
IBMD_DATA_ROOT/runtime/acceptance/evidence/
```

and records their SHA-256 hashes in:

```text
IBMD_DATA_ROOT/runtime/acceptance/manifest.json
```

The manifest is immutable. Rebuilding over an existing manifest or evidence directory is refused.

Example:

```powershell
python scripts/build_target_acceptance_manifest.py `
  --build `
  --entry-summary "<entry-summary.json>" `
  --liquidation-summary "<liquidation-summary.json>" `
  --restart-summary "<restart-summary.json>" `
  --liquidation-restart-summary "<liquidation-restart-summary.json>" `
  --reverse-summary "<reverse-summary.json>" `
  --daily-halt-summary "<daily-halt-summary.json>" `
  --daily-flat-summary "<daily-flat-summary.json>" `
  --rollover-summary "<rollover-summary.json>"
```

Revalidation:

```powershell
python scripts/build_target_acceptance_manifest.py --validate
```

Changing, replacing or deleting any staged summary invalidates the manifest.

## Preflight checks

`run_target_cutover_preflight.py` verifies:

```text
paper environment and D* paper account
clean target bootstrap and schema ledgers
bootstrap artifact hashes
all eight acceptance gates and summary hashes
current target catalog hash
session-calendar qualification for the local CME date
no live legacy wt_run service PID
all target service process locks are free
```

For a controlled paper soak while the official CME exception calendar is still unavailable, an explicit override is required:

```powershell
python scripts/run_target_cutover_preflight.py `
  --mode PAPER_SOAK `
  --check `
  --allow-unqualified-session
```

The override is forbidden for `LIVE_CUTOVER` and is recorded in the authorization.

## Authorization

After every check passes:

```powershell
python scripts/run_target_cutover_preflight.py `
  --mode PAPER_SOAK `
  --issue-authorization `
  --allow-unqualified-session
```

The default output is:

```text
IBMD_DATA_ROOT/runtime/authorization.json
```

It is short-lived, immutable and bound to:

```text
mode
environment
account
deployment
application version
data root
acceptance manifest hash
bootstrap hash
catalog bundle hash
issue/expiry times
session qualification override
```

The authorization explicitly keeps:

```text
live_account_enablement = false
automatic_retry_enabled = false
```

A changed catalog, changed application version, changed data root, changed acceptance summary or expired timestamp invalidates it.

## Process ownership

Preflight must run while both stacks are stopped.

Legacy ownership is detected from live `data/runtime/wt_run/*.json` PIDs. Target ownership is detected by attempting to acquire all target service locks:

```text
supervisor
market_data
broker_position_feed
signal
decision
execution
```

A held lock is a hard blocker. Preflight never kills a process.

## Current boundary

Issuing `authorization.json` does not by itself make the runtime trade. Continuous broker-mutation stages remain disabled until execution is changed to consume and verify this authorization on every startup.

That wiring is the next independent safety slice.
