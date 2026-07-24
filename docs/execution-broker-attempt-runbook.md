# Execution broker attempt and reconciliation foundation

**Status:** broker-disabled state model only  
**Order submission:** absent  
**IB connection:** absent  
**Store owner:** target `execution` process

## Purpose

This slice defines the durable state that must exist before a future paper-only order gateway is allowed to call `placeOrder`.

```text
ADMITTED StrategyCommandRequestV1
+ proven StrategyPositionV1
+ active registered contract
→ stable BrokerOrderOperationV1
→ stable BrokerOrderAttemptV1
→ broker reconciliation observation
→ durable current state and append-only audit
```

No broker call is implemented here.

## Operation planning

The operation identity is deterministic from the admitted strategy command.

```text
command_id
→ broker_operation_<stable hash>
```

The first attempt identity and `order_ref` are deterministic from:

```text
operation_id + attempt_no
```

The `order_ref` includes the complete operation identity and attempt number and is limited to 64 safe characters.

### Quantity

```text
OPEN from FLAT
→ broker quantity = target quantity

REVERSE LONG 2 → SHORT 1
→ SELL 3

REVERSE SHORT 2 → LONG 1
→ BUY 3
```

The broker operation is always `MARKET` in target v1.

## Operation states

```text
PREPARING
SUBMITTING
LIVE
RECONCILING
SUCCEEDED
FAILED_RETRYABLE
FAILED_OPERATOR_REQUIRED
UNKNOWN_OUTCOME
```

`SUCCEEDED` requires cumulative filled quantity equal to requested quantity.

`FAILED_RETRYABLE` is not permission to retry by itself. The current attempt must also contain a proven terminal broker outcome and a positive remaining quantity.

`UNKNOWN_OUTCOME` blocks every new attempt.

## Attempt states

```text
PREPARING
SUBMITTING
LIVE
FILLED
CANCELLED
REJECTED
FAILED
UNKNOWN_OUTCOME
```

`PREPARING` contains no broker facts.

`SUBMITTING` is persisted immediately before a future external broker call. A crash or exception after this point is treated as possible broker exposure.

`LIVE` requires a durable `order_ref`, broker order id, broker status and broker proof timestamp.

Terminal attempt states require terminal broker proof.

## Reconciliation observations

The normalized broker observation outcomes are:

```text
LIVE
FILLED
CANCELLED
REJECTED
FAILED
NOT_FOUND
AMBIGUOUS
```

An exact observation carries:

```text
order_ref
broker_order_id
broker_perm_id when available
broker_status
requested_qty
filled_qty
remaining_qty
observed_at_utc
```

`NOT_FOUND` and `AMBIGUOUS` do not prove that no broker order exists. After submission has started, both outcomes produce:

```text
attempt   → UNKNOWN_OUTCOME
operation → UNKNOWN_OUTCOME
retry     → forbidden
```

The system does not reinterpret absence from one query as terminal no-fill.

## Retry proof

A new attempt is allowed only when all local facts prove:

1. the operation is `FAILED_RETRYABLE`;
2. the current attempt is `CANCELLED`, `REJECTED` or `FAILED`;
3. terminal broker proof is present;
4. the operation still has positive remaining quantity;
5. the new attempt number is exactly previous attempt number plus one.

The new attempt quantity equals the operation's current remaining quantity.

Example:

```text
operation requested = 3
attempt 1 filled     = 2
attempt 1 remaining  = 1
attempt 1 terminal   = CANCELLED with broker proof

attempt 2 requested  = 1
```

## Restart adoption

On restart execution reads nonterminal operations from its own store.

For the current attempt it reconciles by the persisted stable `order_ref` and durable broker ids when available.

An exact live match updates the existing operation and attempt. It does not create attempt 2.

```text
same operation_id
same attempt_id
same order_ref
new broker proof
state → LIVE
```

If reconciliation cannot produce one exact broker fact, the operation remains blocked in `UNKNOWN_OUTCOME` until explicit operator resolution.

## Persistence

Migration `execution` version 2 adds:

```text
internal_broker_order_operations
internal_broker_order_attempts
internal_broker_operation_transitions
internal_broker_attempt_transitions
internal_broker_reconciliation_observations

public_broker_order_operations_v1
public_broker_order_attempts_v1
```

Current operation and attempt rows are mutable only through the single execution writer.

State transitions and reconciliation observations are append-only audit facts.

The durable mapping is:

```text
command_id
→ operation_id
→ attempt_no / attempt_id
→ order_ref
→ broker_order_id / broker_perm_id when observed
```

## Deliberate exclusions

This slice does not contain:

- `ib_async` execution client;
- `placeOrder` or `cancelOrder`;
- order-id allocation;
- open-order or execution download;
- fill/commission ledger;
- protective STOP/TP;
- liquidation operations;
- continuous command polling;
- automatic retry scheduler.

Those capabilities must use this state model later. They must not bypass it.

## Automated acceptance

Tests cover:

- stable operation, attempt and `order_ref` identity;
- `OPEN` target quantity;
- reverse delta quantity;
- strict contract serialization;
- `NOT_FOUND` and `AMBIGUOUS` becoming `UNKNOWN_OUTCOME`;
- retry suppression after unknown outcome;
- terminal partial cancellation followed by retry of remaining quantity only;
- terminal full fill producing `SUCCEEDED`;
- execution migration upgrade from version 1 to version 2;
- restart adoption of the same live order by exact `order_ref`;
- idempotent repeated reconciliation observation;
- no duplicate attempt after restart;
- append-only operation/attempt transition audit.

No operator test is required because no broker gateway exists in this slice.
