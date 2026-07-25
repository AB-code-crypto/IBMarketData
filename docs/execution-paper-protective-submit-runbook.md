# Paper-only protective submission

**Status:** implementation complete; real paper gate deferred until the exchange is open  
**Owner:** target `execution`  
**Live accounts:** rejected  
**Automatic retry:** disabled  
**Cancellation/liquidation:** not implemented in this slice

## Purpose

This slice connects the durable protection plan to Interactive Brokers without creating a second broker-action owner.

```text
OPEN PositionEpisodeV1
+ ProtectionStateV1
+ fresh COMPLETE broker-position snapshot
+ broker-actions/reconciliation/clock readiness
→ one explicit protective broker action at most per invocation
```

The order is strict:

```text
invocation 1:
PLANNED STOP
→ persist STOP SUBMITTING + broker order ID
→ one STP placeOrder
→ bounded read-only reconciliation
→ STOP LIVE / FILLED / terminal failure / UNKNOWN_OUTCOME

later invocation, only after fresh STOP LIVE proof:
PLANNED TAKE PROFIT
→ persist TP SUBMITTING + broker order ID
→ one LMT placeOrder
→ bounded read-only reconciliation
```

A single invocation can never submit both STOP and TAKE PROFIT.

## Hard gates

Broker mutation is rejected unless:

```text
IBMD_ENVIRONMENT = paper
configured account begins with D
--confirm-paper-account exactly equals IB_ACCOUNT_ID
position episode is OPEN
strategy/deployment/instrument scope matches
position episode contract exists exactly once in the versioned calendar
execution strategy position proves the same position_episode_id/side/quantity
execution broker_actions_enabled = true
execution reconciliation_complete = true
execution clock_healthy = true
no non-protection readiness blocker exists
latest position-feed snapshot is COMPLETE and fresh
broker position exactly matches held conId/localSymbol/side/quantity
no competing MNQ contract exists
```

Unlike a new strategic entry, protective submission is not blocked by the daily-flat new-risk window. Protection must remain recoverable outside normal entry hours.

## STOP contract

The mandatory STOP order uses the immutable policy stored on the position episode:

```text
order type = STP
TIF        = DAY for the current MNQ profile
outsideRth = true for the current MNQ profile
quantity   = position episode quantity
contract   = held conId/localSymbol
side       = opposite the position
orderRef   = deterministic protective order reference
OCA group  = deterministic protection-set group when TP is enabled
ocaType    = 1
```

The broker order ID and `SUBMITTING` state are committed before `placeOrder`.

Any exception, timeout or disconnect after that boundary means possible broker exposure. The order is reconciled; it is never submitted again automatically.

## TAKE PROFIT contract

TAKE PROFIT is ineligible until STOP is broker-proven `LIVE` with a recent proof timestamp.

```text
order type = LMT
TIF        = DAY for the current MNQ profile
outsideRth = false
quantity   = position episode quantity
contract   = held conId/localSymbol
side       = opposite the position
OCA group  = the same group as STOP
```

TP failure does not remove proven STOP protection:

```text
STOP LIVE + TP REJECTED/FAILED/CANCELLED
→ ProtectionStateV1 remains PROTECTED
→ alert/operator follow-up is required
```

## Durable outcomes

```text
STOP LIVE
→ STOP_LIVE
→ normal command intake may resume if no other readiness blocker exists

STOP FILLED or TP FILLED
→ EXITED

STOP CANCELLED / REJECTED / FAILED / held error 399
→ UNPROTECTED
→ command intake remains blocked

STOP UNKNOWN_OUTCOME
→ UNPROTECTED
→ no second STOP submission

TP UNKNOWN_OUTCOME while STOP remains LIVE
→ STOP remains the safety fact
→ no second TP submission
```

`NOT_FOUND` in one complete broker snapshot is not proof that a submitted order never existed. Bounded reconciliation may retry reads, but never repeats `placeOrder`.

## Entrypoint

Offline store validation:

```powershell
python apps/run_execution_protective_submit_v2.py `
  --validate-store-only `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

One explicit pass:

```powershell
python apps/run_execution_protective_submit_v2.py `
  --once-position-episode-id position_episode_<id> `
  --confirm-paper-account $env:IB_ACCOUNT_ID `
  --execution-database $ExecutionDb `
  --position-feed-database $PositionFeedDb
```

Default IB client IDs:

```text
read-only reconciliation = IB_CLIENT_ID + 100
protective submission    = IB_CLIENT_ID + 140
```

The execution process lock is shared with the other target execution entrypoints, so two execution writers cannot mutate the execution DB concurrently.

## Current evidence and remaining gate

Automated tests cover:

```text
explicit STOP STP / DAY / outsideRth / OCA fields
explicit TP LMT / DAY / OCA fields
paper-account gate
fresh exact broker-position gate
STOP persisted SUBMITTING before broker call
STOP submitted once and reconciled LIVE
TP unavailable before STOP LIVE
TP submitted only on a later invocation
repeat invocation creates no duplicate order
post-submit exception + no exact fact → UNKNOWN_OUTCOME
UNKNOWN_OUTCOME never resubmits
```

The real paper sequence remains deferred until the exchange is open:

```text
1. Complete a clean DAY MARKET entry: FILLED / SUCCEEDED.
2. Publish fresh broker position with exactly one MNQ contract.
3. Create PositionEpisodeV1 and protection plan.
4. Invoke protective submit once: one STOP only.
5. Prove STOP LIVE.
6. Invoke again: one TAKE PROFIT only.
7. Invoke again: no second STOP/TP.
8. Perform disconnect/restart adoption drills.
```

## Deliberate exclusions

This slice does not provide:

```text
cancelOrder
protective-order automatic retry
automatic emergency MARKET liquidation
OCA sibling terminal cleanup
continuous protection scheduling
protective fill/commission lifecycle completion
live-account execution
```

Those remain separate execution slices. The absence of liquidation means the paper position must still be supervised and manually closed during the first real drill.
