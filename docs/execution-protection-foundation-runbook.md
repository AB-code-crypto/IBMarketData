# Execution protection foundation

**Status:** broker-free foundation only  
**Owner:** target `execution`  
**Broker mutation:** disabled  
**Live-account execution:** absent

## Purpose

This slice establishes durable ownership of one opened strategy position and its protective policy before any STOP or take-profit order is sent to Interactive Brokers.

```text
SUCCEEDED strategic broker operation
+ immutable fill facts keyed by execId
+ fresh COMPLETE broker-position snapshot proving the target position
+ current execution readiness
+ versioned strategy/instrument policy
→ PositionEpisodeV1
→ StrategyPositionV1 with position_episode_id
→ ProtectionStateV1
→ mandatory STOP_LOSS plan first
→ optional TAKE_PROFIT plan second
→ execution command intake BLOCKED until STOP is broker-proven LIVE
```

The planner never connects to IB and never calls `placeOrder` or `cancelOrder`.

## Frozen v1 policy

The position episode snapshots the exact protective configuration that was active when the entry operation completed:

```text
price tick                      = 0.25
STOP required                   = true
STOP distance                   = 150 points
STOP outside RTH                = true
TAKE PROFIT enabled             = true
TAKE PROFIT distance            = 75 points
TAKE PROFIT outside RTH         = false
TIF                              = DAY
price watchdog                  = enabled
stale-feed market close         = disabled
stale-price threshold           = 600 seconds
```

Later catalog changes do not silently rewrite an open episode. The episode stores both the full policy payload and its SHA-256 content hash.

## Position episode activation

An episode is created only when all of these facts are proven:

```text
operation.state = SUCCEEDED
attempt.state = FILLED
operation remaining quantity = 0
attempt remaining quantity = 0
all fills belong to the same account/orderRef/order/contract/side
sum(fill shares) = requested operation quantity
position-feed snapshot is COMPLETE and fresh
broker position matches the desired target side and quantity
no competing MNQ contract is present
source command, operation and target policy scopes match
```

The entry average price is derived from immutable fill prices and quantities. Broker average cost is not used as the protective-price source.

## Stable identities

The following identities are deterministic:

```text
source broker operation
→ one position_episode_id
→ one protection_set_id
→ one STOP protective_order_id/order_ref
→ zero or one TAKE PROFIT protective_order_id/order_ref
```

A restart or repeated planner invocation returns the same persisted facts. It cannot create a second episode or a second protective plan for the same strategic operation.

## STOP-first semantics

The planned order sequence is fixed:

```text
1. STOP_LOSS / STOP order
2. TAKE_PROFIT / LIMIT order
```

Both orders use the same deterministic OCA group when take profit is enabled.

Execution readiness becomes:

```text
status                  = BLOCKED
command_intake_enabled  = false
broker_actions_enabled  = true
blocking reason         = protection:stop_not_proven
```

`broker_actions_enabled=true` is intentional: execution may need to install or recover protection while normal strategy commands remain blocked.

## Protective state semantics

Protection-set states:

```text
PLANNED
STOP_SUBMITTING
STOP_LIVE
PROTECTED
UNPROTECTED
EXITED
CLOSED
OPERATOR_REQUIRED
```

Protective-order states:

```text
PLANNED
SUBMITTING
LIVE
FILLED
CANCEL_REQUESTED
CANCELLED
REJECTED
FAILED
UNKNOWN_OUTCOME
NOT_REQUIRED
```

Important rules:

```text
LIVE STOP                         → broker protection proven
STOP held/rejected with error 399 → UNPROTECTED
TP rejected while STOP is LIVE    → position remains PROTECTED
STOP unknown outcome              → UNPROTECTED / fail closed
one protective fill               → EXITED
flat broker position              → CLOSED
```

A held STOP is not treated as protection merely because an IB order ID exists.

## Execution migration v3

Migration `execution` version 3 adds:

```text
internal_position_episodes
internal_protection_sets
internal_protective_orders
internal_protection_set_transitions
internal_protective_order_transitions

public_position_episodes_v1
public_protection_states_v1
public_protective_orders_v1
```

Current rows are single-writer execution state. Transition rows are append-only audit facts.

## Entrypoint

Offline validation:

```powershell
python apps/run_execution_protection_v2.py `
  --validate-store-only `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

Broker-free planning after a proven entry fill:

```powershell
python apps/run_execution_protection_v2.py `
  --plan-from-operation broker_operation_<id> `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

Output explicitly reports:

```text
broker_mutations_performed          = false
stop_submission_enabled             = false
take_profit_submission_enabled      = false
liquidation_enabled                 = false
```

## Deliberate exclusions

This slice does not implement:

```text
STOP placeOrder
TAKE PROFIT placeOrder
cancelOrder
OCA broker reconciliation
protective order-ID allocation
automatic market close after STOP failure
continuous protection scheduler
liquidation coordinator
live-account enablement
```

## Monday sequence

The deferred paper gate remains ordered:

1. Complete one clean `DAY` MARKET entry drill and prove `FILLED/SUCCEEDED`.
2. Publish a fresh broker-position snapshot proving the resulting position.
3. Run this broker-free protection planner for the filled operation.
4. Verify the immutable episode, STOP-first prices and blocked command intake.
5. Only then enable the next paper-only slice that submits the mandatory STOP.

The MARKET drill is not considered accepted merely because this foundation compiles. The weekend preparation failure was a correct daily-flat/session rejection before command creation and broker mutation. No market-dependent test is being waived permanently.
