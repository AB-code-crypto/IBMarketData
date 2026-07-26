# Paper DAILY_HALT acceptance

## Purpose

This drill proves the integration between strategy-owned PnL evidence, the sticky daily-risk state machine, the `DAILY_HALT` liquidation trigger, the single execution-owned liquidation coordinator and broker-proven `FLAT`.

It does **not** qualify live market pricing. Only the market mark is synthetic and is labelled as such in every artifact. The execution fills, commissions, current position episode, position-feed snapshots, protective orders, cancellations and liquidation MARKET order remain real paper-account evidence.

## Scope

The accepted flow is:

```text
protected OPEN position
+ real strategy-owned fills and commissions
+ synthetic favourable mark
→ DailyRiskState TRIGGERED / PENDING
→ command intake BLOCKED
→ DAILY_HALT trigger
→ one liquidation operation
→ cancel TAKE PROFIT
→ prove terminal TAKE PROFIT
→ cancel STOP
→ prove terminal STOP
→ one MARKET close
→ broker-proven FLAT
→ PositionEpisode CLOSED
→ ProtectionState CLOSED
→ StrategyPosition FLAT
→ DailyRiskState HALTED / COMPLETE
→ repeated daily-risk calculation remains HALTED
```

The runner performs no automatic retry. A broker-uncertain result stops the drill and requires operator inspection.

## Preconditions

Use a dedicated paper deployment whose `IBMD_DEPLOYMENT_ID` contains `paper-drill`.

Required state:

```text
IBMD_ENVIRONMENT=paper
exact paper account confirmation
one owned MNQ PositionEpisode OPEN
StrategyPosition OPEN for the same episode
DailyRiskState MONITORING
ExecutionReadiness READY
STOP broker-proven LIVE
TAKE PROFIT LIVE or NOT_REQUIRED
no existing liquidation operation for the episode
complete owned fill and commission evidence
fresh independent position-feed snapshots
```

The normal entry/protection acceptance runner or the REVERSE acceptance runner can produce the required protected source position.

## Synthetic evidence boundary

The preparer reads the real execution-owned fill ledger and calculates realized PnL normally. It first evaluates a trial mark equal to the episode entry price. The trial must remain `MONITORING`; otherwise the drill refuses to hide a threshold that was already reached.

The preparer then derives the minimum favourable mark needed to exceed:

```text
target PnL + configured drill cushion
```

The price is rounded in the favourable direction to the instrument tick:

```text
LONG  → round upward
SHORT → round downward
```

The generated market-bar identity is deterministic for:

```text
drill_id
position_episode_id
observed_at_utc
```

The synthetic mark is never written to the market-data database. It exists only inside the explicit paper-drill calculation and its artifacts.

## Broker ownership

The preparer:

```text
never connects to IB
never calls placeOrder
never calls cancelOrder
```

All broker mutations are delegated to the existing liquidation coordinator. Execution remains the only broker-order owner.

One successful fresh protected liquidation is expected to perform exactly:

```text
CANCEL_TAKE_PROFIT  = 1
CANCEL_STOP         = 1
SUBMIT_MARKET_CLOSE = 1
```

Each child invocation performs at most one external broker action.

## Validation without TWS

After bootstrapping a fresh target deployment:

```powershell
python scripts/run_paper_daily_halt_acceptance.py --validate-only
```

The validation output must declare:

```text
paper_daily_halt_dependencies_compatible = true
synthetic_market_mark_only = true
real_owned_fill_evidence_only = true
interactive_confirmation_required = false
automatic_retry_enabled = false
```

## Paper run

Keep the independent position feed running. Provide the summary from a successful protected-position acceptance:

```powershell
python scripts/run_paper_daily_halt_acceptance.py `
  --run `
  --source-summary "<protected-summary.json>"
```

Optional explicit paths:

```powershell
python scripts/run_paper_daily_halt_acceptance.py `
  --run `
  --source-summary "<protected-summary.json>" `
  --execution-database "<execution.sqlite3>" `
  --position-feed-database "<broker_positions.sqlite3>" `
  --market-database "<MNQ.sqlite3>" `
  --catalog-root ".\catalog"
```

There is no `Read-Host` prompt and no interactive side confirmation.

## Success criteria

The final `summary.json` must prove:

```text
scenario = DAILY_HALT
synthetic_market_mark_only = true
real_owned_fill_evidence_only = true
policy trigger reason = DAILY_HALT
policy trigger source = daily-halt:<Moscow trading day>
broker_mutation_count = 3
liquidation fully closed = true
flat proof accepted = true
final daily-risk status = HALTED
final cleanup status = COMPLETE
final command intake enabled = false
repeated daily-risk calculation remains HALTED / COMPLETE
paper account left flat = true
```

## Failure handling

The runner writes `failure.json` on any failure.

When `broker_exposure_possible=true`:

```text
inspect TWS immediately
inspect saved child stdout/stderr and JSON
keep the existing liquidation operation
never invent a new drill ID to force another close
never manually alter the target SQLite state
```

The following are hard failures:

```text
missing or incomplete commission evidence
position/protection scope mismatch
STOP not LIVE
existing liquidation operation before trigger preparation
synthetic trial already TRIGGERED
DAILY_HALT reason mismatch
UNKNOWN_OUTCOME
repeated cancel or MARKET close
attempt_no greater than 1
position feed does not prove FLAT
post-liquidation state returns to MONITORING
HALTED state loses command-intake blocking
```

## Relationship to production acceptance

This drill accepts:

```text
sticky threshold semantics
DAILY_HALT trigger production
liquidation integration
post-FLAT HALTED persistence
```

It does not accept:

```text
live market-data mark accuracy
official CME calendar qualification
live-account operation
continuous runtime broker mutations
```

Those remain separate gates.
