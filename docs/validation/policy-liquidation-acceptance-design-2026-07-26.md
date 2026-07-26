# Policy liquidation acceptance design — 2026-07-26

## Scope

The paper acceptance layer now covers two deterministic policy reasons without
creating a parallel broker-action owner:

```text
DAILY_FLAT
ROLLOVER
```

The broker-free preparer evaluates the existing production candidate function at
an explicit logical UTC timestamp, selects exactly the requested reason and
persists one durable `LiquidationTriggerV1` through the existing liquidation
repository.

The acceptance runner then delegates all broker actions to the normal paper
liquidation coordinator:

```text
cancel TAKE PROFIT
cancel STOP
submit MARKET close on the actually held contract
reconcile the same liquidation attempt
prove FLAT through the independent position feed
close episode, protection and strategy position
```

## Safety properties

- The source episode must be OPEN and already protected.
- The source summary may come from normal entry, restart entry or REVERSE
  acceptance.
- The episode must not already have a liquidation operation.
- Trigger preparation performs no broker mutation.
- The selected and persisted reasons must exactly match the requested scenario.
- The runner rejects a second cancellation, second close attempt, changed broker
  identity, `UNKNOWN_OUTCOME` or operator-required state.
- One fresh protected source with two live exits has a broker-action budget of
  exactly three mutations.
- The final state requires broker-proven FLAT and an idempotent repeat with no
  mutation.

## Time semantics

The logical trigger timestamp is normalized before comparison. Equivalent UTC
representations such as:

```text
2026-07-27T19:59:51Z
2026-07-27T19:59:51.000000Z
```

represent the same observation and must not cause a false mismatch.

The committed session calendar remains parity-only. DAILY_FLAT paper acceptance
therefore requires an explicit unqualified-session override. Production cutover
still requires a bounded official CME schedule artifact.

## Non-goals

This acceptance does not replace:

```text
production calendar qualification
wall-clock scheduler testing
DAILY_HALT from real PnL evidence
in-flight network disconnect testing
```
