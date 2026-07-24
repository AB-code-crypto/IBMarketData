# Execution paper-only submit coordinator

**Status:** one explicit paper MARKET order may be submitted  
**Environment gate:** `IBMD_ENVIRONMENT=paper`  
**Account gate:** configured account must begin with `D` and must be repeated through `--confirm-paper-account`  
**Automatic retry:** disabled  
**Cancellation:** absent  
**Protection and liquidation:** absent

## Purpose

This slice adds the first broker-mutating path only after the durable operation/attempt and read-only reconciliation models already exist.

```text
ADMITTED StrategyCommandRequestV1
+ current persisted execution controls
+ proven strategy position
+ current active registered contract
→ durable PREPARING operation/attempt
→ allocate one broker order id
→ persist SUBMITTING with that order id
→ one MARKET placeOrder call
→ bounded read-only reconciliation
→ exact LIVE/FILLED/terminal fact or UNKNOWN_OUTCOME
```

The coordinator never creates a second attempt automatically.

## Hard gates

A mutating run is rejected unless all of these are true:

```text
IBMD_ENVIRONMENT = paper
configured IB account begins with D
--confirm-paper-account exactly equals IB_ACCOUNT_ID
execution command state = ADMITTED
original strategy command is not expired
strategy/version/deployment/instrument/policy hash match
execution readiness = READY
command intake enabled
broker actions enabled
reconciliation complete
execution clock healthy
daily risk = MONITORING
PnL ready for the current configured trading day
one active registered FUT contract exists
no other unresolved operation owns the same execution scope
```

The exact account confirmation is intentionally required on every explicit invocation. A configuration mistake cannot silently turn this entrypoint into a live-account sender.

## Persistence boundary

The broker order id is allocated while connected to IB. Before `placeOrder` is called, execution commits:

```text
operation.state = SUBMITTING
attempt.state = SUBMITTING
attempt.broker_order_id = allocated order id
attempt.order_ref = stable persisted order ref
```

If persistence fails, `placeOrder` is not called.

If the process crashes after this commit, a restart sees possible broker exposure and performs reconciliation. It does not submit again.

## Broker request

The reusable gateway connects with:

```text
readonly = false
fetchFields = StartupFetchNONE
```

It submits exactly one target-v1 order:

```text
order type = MARKET
side       = persisted BUY/SELL side
quantity   = persisted attempt requested quantity
orderId    = persisted allocated broker order id
orderRef   = persisted stable order ref
account    = exact configured paper account
transmit   = true
```

The IB futures contract is built from the versioned instrument master and futures calendar. No contract is selected from an unversioned runtime constant.

## Handling the `placeOrder` result

A normal return from `placeOrder` is not treated as a fill. The returned order identity must agree with the persisted:

```text
order id
order ref
account
```

A mismatch or exception occurs after the possible-exposure boundary. It therefore does not authorize another submission.

## Immediate reconciliation

After the one submission attempt, the coordinator performs a bounded number of complete read-only snapshots using a separate IB client id:

```text
all open orders
completed orders
executions filtered by account
available commission reports
```

`NOT_FOUND` may be retried only as another read. The MARKET order is never resent.

The first exact or ambiguous result is committed through the existing reconciliation path:

```text
LIVE       → existing attempt becomes LIVE
FILLED     → existing operation becomes SUCCEEDED
CANCELLED  → FAILED_RETRYABLE, but no retry is scheduled
REJECTED   → FAILED_RETRYABLE, but no retry is scheduled
FAILED     → FAILED_RETRYABLE, but no retry is scheduled
AMBIGUOUS  → UNKNOWN_OUTCOME
```

If every bounded reconciliation read fails, or no exact fact is proven, the existing operation and attempt become `UNKNOWN_OUTCOME`.

## Restart behavior

Calling the entrypoint again for the same command behaves according to persisted state:

```text
PREPARING
→ may cross the one SUBMITTING boundary if the command is still valid

SUBMITTING / LIVE / RECONCILING / UNKNOWN_OUTCOME
→ read-only reconciliation only
→ no placeOrder call

SUCCEEDED / FAILED_OPERATOR_REQUIRED / FAILED_RETRYABLE
→ return the persisted state
→ no placeOrder call
```

`FAILED_RETRYABLE` is intentionally not retried by this coordinator. A future retry flow must require the existing terminal broker proof rules and a separately explicit operator action.

## Entry points

Offline validation without IB:

```powershell
python apps/run_execution_submit_v2.py --validate-store-only
```

Explicit paper submission or restart reconciliation:

```powershell
python apps/run_execution_submit_v2.py `
  --once-command-id strategy_command_<id> `
  --confirm-paper-account $env:IB_ACCOUNT_ID
```

Default client ids:

```text
read-only reconciliation = IB_CLIENT_ID + 100
paper submission         = IB_CLIENT_ID + 120
```

The two ids must differ. The entrypoint also acquires the normal execution process lock, so no second target execution writer may run concurrently.

## Deliberate exclusions

This slice does not contain:

- `cancelOrder`;
- automatic order retry;
- a continuous decision/outbox consumer;
- protective STOP or take-profit orders;
- liquidation operations;
- daily-flat or daily-PnL cleanup;
- operator ownership adoption;
- live-account enablement;
- a general multi-instrument order router.

## Automated acceptance

Tests cover:

- live environment rejection;
- mismatched account confirmation rejection;
- non-paper-looking account rejection;
- `readonly=False` and `StartupFetchNONE` on the mutating IB connection;
- exact order id allocation;
- one MARKET `placeOrder` call with persisted account/ref/id/route;
- `SUBMITTING` persistence before the call;
- immediate full-fill reconciliation;
- idempotent second invocation after success;
- submission exception plus `NOT_FOUND` becoming `UNKNOWN_OUTCOME`;
- no second submission after unknown outcome;
- expired command rejection before allocation;
- disabled broker-actions rejection before allocation;
- rejection when another unresolved operation owns the same scope.

## Operational status

Automated tests use fake IB sessions. The first real use must be a controlled paper-account drill with one explicit command and must include disconnect/restart verification. Live submission remains structurally blocked by the environment and account gates.
