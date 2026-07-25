# Execution reverse protective handoff

## Why this component exists

A strategic `REVERSE` is a MARKET delta order. For example:

```text
LONG 1 → SHORT 1
SELL 2
```

The source position may already own a live STOP and TAKE PROFIT. Sending the reverse order while either exit remains exposed can produce an unintended extra position if an old exit executes during or after the reversal.

Therefore target execution treats protective handoff as a mandatory precondition of reverse submission.

## Safety rule

`run_execution_submit_v2.py` now rejects every `REVERSE` before `allocate_order_id` unless all protective orders of the source `PositionEpisodeV1` are in safe non-exposed states:

```text
PLANNED
CANCELLED
REJECTED
FAILED
NOT_REQUIRED
```

The following states block the reverse:

```text
SUBMITTING
LIVE
CANCEL_REQUESTED
UNKNOWN_OUTCOME
FILLED
```

A position episode already owned by a liquidation operation cannot be reversed.

## One-shot handoff coordinator

Entrypoint:

```text
apps/run_execution_reverse_handoff_v2.py
```

It processes one explicit, unexpired, admitted `REVERSE` command.

Sequence:

```text
1. Prove exact source episode scope and opposite target side.
2. Prove fresh COMPLETE broker position for the exact held conId/localSymbol.
3. Run read-only protective lifecycle reconciliation.
4. If TAKE PROFIT is LIVE, persist CANCEL_REQUESTED and call cancelOrder once.
5. Reconcile the TAKE PROFIT terminal result.
6. On the next invocation, do the same for STOP.
7. Once both exits are non-exposed, publish ready_for_reverse_submit=true.
```

At most one `cancelOrder` is called per invocation.

## Unknown outcomes

A timeout or disconnect after persisted `CANCEL_REQUESTED` does not authorize another cancellation call.

```text
CANCEL_REQUESTED
+ no exact terminal broker fact
→ reconciliation-only path
→ reverse remains blocked
```

A protective fill during the handoff becomes `OPERATOR_REQUIRED`; the reverse MARKET order is not sent because the broker position may already have changed.

## Readiness semantics

During handoff:

```text
command_intake_enabled = false
broker_actions_enabled = preserved
readiness = BLOCKED
```

After both old exits are terminal, execution can remain `BLOCKED` by the old protection reason. The reverse submit coordinator accepts only the narrowly-scoped `protection:` and `reverse_handoff:` blockers after independently proving the protective handoff complete. Any unrelated blocker still rejects submission.

## Commands

Validate stores without connecting to IB:

```powershell
python apps/run_execution_reverse_handoff_v2.py `
  --validate-store-only
```

Advance one handoff step:

```powershell
python apps/run_execution_reverse_handoff_v2.py `
  --once-command-id strategy_command_<id> `
  --confirm-paper-account $env:IB_ACCOUNT_ID
```

Typical operator sequence:

```text
invocation 1 → cancel TAKE PROFIT
invocation 2 → cancel STOP
invocation 3 → ready_for_reverse_submit=true, no mutation
then run_execution_submit_v2.py for the same command id
```

## Still required after the reverse fill

This slice prevents stale protective orders from surviving the reversal. The following separate lifecycle remains required:

```text
reverse fill allocation
close previous PositionEpisodeV1
create new opposite PositionEpisodeV1
calculate new entry price from the opening portion of reverse fills
install new STOP, then TAKE PROFIT
```

Until that finalization exists, reverse submission is guarded but the complete reverse episode transition is not yet accepted for production.

## Non-goals

The handoff does not:

```text
submit the reverse MARKET order
retry cancelOrder automatically
create a liquidation operation
change the active futures contract
run continuously
support live accounts
```
