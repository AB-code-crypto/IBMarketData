# Runtime authorization startup gate

## Purpose

The cutover preflight produces a short-lived `PAPER_SOAK` authorization only after the target bootstrap, catalog, process ownership and all eight acceptance summaries pass.

This slice connects that authorization to supervisor and execution startup.

It does **not** enable continuous broker-mutation adapters yet.

## Fail-closed chain

```text
supervisor receives --runtime-authorization
→ verifies authorization scope and expiry
→ revalidates target bootstrap hashes
→ revalidates all acceptance summary hashes and semantics
→ reloads target catalog
→ verifies current CME session qualification policy
→ selects authorized execution wrapper
→ execution wrapper repeats the same verification
→ writes runtime/authorization-active.json
→ invokes canonical execution runtime in the same process
```

The supervisor and execution wrapper independently verify the evidence. Passing a path through supervisor argv is not treated as authorization.

## Read-only default

Without `--runtime-authorization`, supervisor continues to launch:

```text
apps/run_execution_runtime_v2.py --continuous
```

The canonical runtime remains read-only with all mutating stages disabled.

With verified authorization, supervisor launches:

```text
apps/run_execution_authorized_runtime_v2.py
  --runtime-authorization <authorization.json>
  --acceptance-manifest <manifest.json>
  --bootstrap-manifest <target.v1.json>
  --catalog-root <target catalog>
  --continuous
```

The wrapper then invokes the same canonical runtime. It does not create a second execution process and does not bypass `execution.lock`.

## Verification-only command

Before supervisor startup:

```powershell
python apps/run_execution_authorized_runtime_v2.py `
  --runtime-authorization `
    "$env:IBMD_DATA_ROOT\runtime\authorization.json" `
  --validate-authorization-only
```

Successful output includes:

```text
continuous_broker_mutations_authorized = true
continuous_broker_mutation_adapters_enabled = false
execution_runtime_started = false
live_account_enablement = false
automatic_retry_enabled = false
```

## Supervisor plan

```powershell
python apps/run_target_supervisor.py `
  --print-plan `
  --runtime-authorization `
    "$env:IBMD_DATA_ROOT\runtime\authorization.json"
```

The plan records the authorization hash and the exact execution argv.

## Startup

After paper acceptance and preflight authorization:

```powershell
python apps/run_target_supervisor.py `
  --continuous `
  --runtime-authorization `
    "$env:IBMD_DATA_ROOT\runtime\authorization.json"
```

At the current project stage this still runs the canonical execution loop without continuous broker mutations. This is deliberate. The next slice installs the mutating stage adapters behind the already-verified authorization.

## Hard failures

Startup is rejected when any of these change:

```text
environment or account
deployment_id
application version
data root
bootstrap hash
catalog hash
acceptance manifest hash
any staged summary hash or success facts
authorization expiry
current session qualification without an authorized paper override
```

A missing file, malformed JSON or unknown schema is also a hard failure.

## Live account boundary

The authorization contract accepts `PAPER_SOAK` only.

```text
LIVE_CUTOVER is blocked
live_account_enablement = false
```

No command-line flag can turn a live account on in this slice.
