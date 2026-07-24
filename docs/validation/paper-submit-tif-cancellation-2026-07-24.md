# Paper submit TIF cancellation — 2026-07-24

## Status

The first real paper-account submit drill did **not** fill. The broker accepted the API call far enough to create a local trade identity, then TWS cancelled the order during validation.

This drill is not accepted as a successful broker-mutation gate.

## Prepared facts

```text
instrument              = MNQ
contract                = MNQU6
conId                    = 793356225
command kind             = OPEN
target side              = LONG
target quantity          = 1
position projection      = FLAT
execution readiness      = READY
broker actions enabled   = true
session phase            = TRADING
```

Prepared command:

```text
strategy_command_9cc723f3ec5e7125b7d6b13f86a4fd9f
```

## Broker call

Execution persisted one operation and one attempt before the external call:

```text
operation_id = broker_operation_6b4a46771e07f56a2acc687be7bee8d2
attempt_id   = broker_attempt_3d246eab3f8980a8d23c4acd9771ef58
attempt_no   = 1
order_id     = 1
order_ref    = IBMD:broker_operation_6b4a46771e07f56a2acc687be7bee8d2:1
```

`placeOrder` was invoked exactly once.

TWS immediately reported:

```text
Error 10349: Order TIF was set to DAY based on order preset
status = Cancelled
filled = 0
remaining reported by TWS = 0
```

The paper position remained FLAT.

## Safety result

The initial run stored:

```text
operation_state = UNKNOWN_OUTCOME
attempt_state   = UNKNOWN_OUTCOME
filled_qty      = 0
remaining_qty   = 1
```

A repeated invocation preserved the same command, operation, attempt and attempt number and reported:

```text
submission_performed = false
```

Therefore the duplicate-submission guard worked: the cancelled/uncertain first attempt did not cause a second MARKET order.

## Root causes

### Missing explicit TIF

The target MARKET order left `tif` empty and relied on the TWS preset. TWS treated the preset substitution as validation error 10349 and cancelled the order.

The gateway now sets:

```text
tif = DAY
```

explicitly on the submitted order.

### Terminal quantity normalization

For this validation cancellation, TWS exposed:

```text
requested = 1
filled    = 0
remaining = 0
```

The read-only mapper previously rejected that terminal broker representation because it did not satisfy the internal economic invariant:

```text
filled + remaining = requested
```

For terminal `Cancelled`, `ApiCancelled`, `Inactive`, `Rejected` and `Failed` facts, the mapper now normalizes the internal unfilled quantity to:

```text
requested - filled
```

Active-order quantity inconsistencies remain rejected.

The mapper also preserves the TWS trade-log warning text when `orderState.warningText` is absent.

## Diagnostic improvement

The submit JSON now includes persisted broker diagnostics:

```text
blocking_reason
broker_status
broker_perm_id
last_broker_proof_at_utc
```

A bounded reconciliation failure will no longer produce an unexplained `UNKNOWN_OUTCOME` payload.

## Next real gate

1. Reconcile the existing attempt read-only after deploying the fix.
2. Preserve the original drill database as incident evidence.
3. Run a fresh drill in a new dedicated paper-drill deployment root.
4. Confirm one `DAY` MARKET order, exact broker reconciliation and no second submission.

No protective order, cancellation automation or live-account path is enabled by this fix.
