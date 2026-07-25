# Protection foundation weekend build — 2026-07-25

## Status

The broker-free execution protection foundation has been implemented while the exchange is closed. Market-dependent acceptance remains deferred to the next open session.

This record does not accept the earlier MARKET paper drill as successful. The weekend `run2` preparation was correctly rejected by the daily-flat/session guard before command creation and before broker mutation.

## Implemented scope

```text
SUCCEEDED strategic operation
+ immutable fill evidence
+ fresh COMPLETE broker-position snapshot
+ versioned strategy/instrument policy
→ durable PositionEpisodeV1
→ OPEN StrategyPositionV1 with position_episode_id
→ STOP-first ProtectionStateV1
→ execution command intake blocked until STOP is proven LIVE
```

Execution migration version 3 owns position episodes, protection sets, protective orders and append-only transitions.

## Broker boundary

The new entrypoint is broker-free:

```text
apps/run_execution_protection_v2.py
```

It cannot:

```text
connect to IB
placeOrder
cancelOrder
submit STOP
submit TAKE PROFIT
liquidate a position
```

## Deferred open-session gate

At the next open session:

1. complete one clean `DAY` MARKET entry drill;
2. prove `FILLED/SUCCEEDED` and one resulting MNQ position;
3. run the broker-free protection planner for that operation;
4. verify immutable episode identity, STOP/TP prices and blocked command intake;
5. only then test the paper-only mandatory STOP submit path.

The next implementation may proceed during the weekend, but no market-dependent result is silently treated as proven.
