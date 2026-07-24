# Execution read-only IB reconciliation

**Status:** read-only reconciliation enabled; broker mutations disabled  
**IB calls:** open orders, completed orders and executions only  
**Order submission:** absent  
**Order cancellation:** absent  
**Automatic retry:** absent

## Purpose

This slice lets target execution prove the state of an already persisted broker attempt after a restart or connection loss without creating another attempt.

```text
persisted BrokerOrderAttemptV1
+ reqAllOpenOrdersAsync
+ reqCompletedOrdersAsync
+ reqExecutionsAsync
→ BrokerReconciliationSnapshotV1
→ exact BrokerOrderObservationV1 or fail-closed uncertainty
→ existing attempt/operation state update
→ immutable fill and commission evidence
```

No code in this slice calls `placeOrder`, `cancelOrder`, `reqIds` or any other broker-mutating method.

## Reusable IB reader

`IBAsyncBrokerReconciliationReader` connects with:

```text
readonly=True
fetchFields=StartupFetchNONE
```

It validates the configured managed account and performs three bounded requests:

```text
all open orders
completed orders (apiOnly=false)
executions filtered by account
```

The result is one strict `BrokerReconciliationSnapshotV1` containing:

```text
source IB session identity
captured_at_utc
open order facts
completed order facts
fill facts keyed by execId
optional commission facts
requests_complete=true
```

A request timeout or account mismatch fails the whole snapshot. Partial request results are not published as complete reconciliation evidence.

## Exact matching rules

The persisted stable `order_ref` is the primary correlation key.

An exact order also has to agree with the persisted attempt on:

```text
account
conId
localSymbol
BUY/SELL side
order type
requested quantity
known broker order id / perm id, when already persisted
```

Two different broker identities with the same `order_ref` produce `AMBIGUOUS`.

A matching execution also has to agree on account, contract, side and the known broker identity. Fills are never reassigned to another attempt by quantity alone.

## Normalized outcomes

```text
active IB order                    → LIVE
terminal full fill                 → FILLED
terminal cancellation              → CANCELLED
broker rejection                   → REJECTED
terminal failure                   → FAILED
no exact fact in complete snapshot → NOT_FOUND
multiple/conflicting facts         → AMBIGUOUS
```

`NOT_FOUND` and `AMBIGUOUS` are fed into the existing attempt state machine, which produces `UNKNOWN_OUTCOME`. They never authorize a retry.

If no order row remains but immutable executions prove the complete requested quantity for one exact broker identity, the outcome may be `FILLED_FROM_EXECUTIONS`. Partial executions without an exact order row remain `AMBIGUOUS` because the remaining order state is not proven.

## Fill and commission evidence

Every fill is keyed by the IB `execId`.

The immutable execution payload and the optional commission payload are persisted as separate append-only reconciliation evidence rows:

```text
outcome = FILL
outcome = COMMISSION
```

This separation matters because IB can deliver the commission report after the execution. A fill can therefore be present with:

```text
commission_complete = false
```

A later read-only reconciliation may add the one immutable commission fact for the same `execId`. It cannot rewrite the execution payload or attach the `execId` to another operation/attempt.

The evidence uses the existing execution migration-v2 append-only reconciliation table; no runtime schema mutation is added.

## Restart flow

For each persisted operation in `SUBMITTING`, `LIVE`, `RECONCILING` or `UNKNOWN_OUTCOME`:

1. execution records/reuses `RECONCILING`;
2. one complete read-only broker snapshot is collected;
3. the current attempt is matched by stable `order_ref` and broker identities;
4. the existing attempt is updated;
5. no attempt number is incremented;
6. fill, commission and normalized observation evidence are committed with the final state update.

A terminal `SUCCEEDED` operation with an incomplete commission may be revisited only to append the missing commission fact. Its terminal operation timestamp is not rewritten.

## One-shot application

Offline schema validation:

```powershell
python apps/run_execution_reconciliation_v2.py --validate-store-only
```

One read-only reconciliation pass:

```powershell
python apps/run_execution_reconciliation_v2.py --once
```

The default API client id is:

```text
IB_CLIENT_ID + 100
```

The application uses the normal execution process lock, so it cannot write the execution DB concurrently with another target execution process.

## Deliberate exclusions

This slice does not contain:

- `placeOrder`;
- `cancelOrder`;
- order-id allocation;
- broker command submission;
- automatic retry scheduling;
- protective STOP/TP management;
- liquidation coordination;
- continuous polling;
- operator ownership adoption.

## Automated acceptance

Tests cover:

- strict snapshot/order/fill/commission contracts;
- read-only connection arguments;
- open, completed and execution requests;
- account filtering;
- exact live-order adoption;
- terminal fill reconciliation;
- duplicate open/completed representation of one broker identity;
- duplicate identities for one `order_ref` becoming `AMBIGUOUS`;
- complete snapshot with no match becoming `NOT_FOUND`;
- full fill proof from executions when the order row is absent;
- immutable fill persistence keyed by `execId`;
- later commission completion without rewriting the fill;
- restart reconciliation without creating attempt 2;
- absence of broker mutation calls.

No operator test is required for this slice. The first broker-mutating step remains blocked until a paper-only submit coordinator persists `SUBMITTING` before the external call and is exercised through unknown-outcome/restart drills.
