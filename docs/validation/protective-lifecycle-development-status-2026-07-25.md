# Protective lifecycle development gate — 2026-07-25

## Scope

This gate covers broker-read-only lifecycle work after protective order submission:

```text
protective broker facts
→ immutable fills keyed by execId
→ late commissions
→ OCA sibling terminal proof
→ EXITED
→ fresh broker FLAT
→ PositionEpisode CLOSED
```

## Compatibility decision

Legacy runtime/database compatibility is not required. Target development databases may be recreated. Only historical market prices have a supported one-time import path.

## Broker boundary

This slice does not call:

```text
placeOrder
cancelOrder
```

It uses complete read-only broker reconciliation plus the independent position feed.

## Acceptance

Required automated evidence:

- one protective fill is persisted once by `execId`;
- late commission is appended without changing the base fill;
- terminal OCA sibling plus fresh broker FLAT closes the episode;
- live/unknown OCA sibling after a fill becomes `OPERATOR_REQUIRED`;
- manual flat before any protective submission marks plans `NOT_REQUIRED`;
- repeated reconciliation is idempotent;
- existing protection and broker-recovery tests remain green.

Real TWS verification remains deferred until the exchange is open.
