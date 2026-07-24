# Target signal one-shot and idempotency evidence — 2026-07-24

**Status:** PASS for one explicit historical MNQ calculation  
**Branch under test:** `agent/architecture-rewrite`  
**Tested branch head before evidence commit:** `8f0dbec94a2fa128eb010370dd9c1d30eb2b728e`  
**Legacy runtime baseline:** `5073dbea3f96740b4390e113d06578a514918f00`  
**Operator environment:** Windows, local target market-data and signal SQLite stores

## Scope

This evidence records two consecutive target signal calculations for the same explicit historical due point:

```text
instrument     = MNQ
contract       = MNQU6
signal_bar_utc = 2026-07-23T17:24:00Z
```

It validates:

1. successful signal-store migration and dependency validation;
2. one real rolling calculation over the imported target history;
3. immutable publication for the same strategy/configuration/source bar;
4. no duplicate signal event when the result is `NO_SIGNAL`.

It does not yet claim real legacy-vs-target calculation parity or continuous-loop readiness.

## Store setup

The explicit signal migration applied version `1` and reported the store current. Offline validation accepted both:

```text
C:\IBMD-shadow-data\account1\market_data\MNQ.sqlite3
C:\IBMD-shadow-data\account1\signal\signal.sqlite3
```

## First calculation

The first command exited with code `0` and produced:

```text
status                         = NO_SIGNAL
reason                         = potential_below_threshold:18.1062<=30
entry_price                    = 28659.75
raw_candidate_count            = 5912
valid_pattern_count            = 5393
skipped_pattern_count          = 519
pearson_pass_count             = 104
minmax_pass_count              = 28
best_raw_pearson               = 0.8530980901417716
best_signal_pearson            = 0.8107114932943953
best_candidate_score           = 0.8773834163247242
potential_direction            = LONG
potential_end_delta_points     = 18.106193633446342
potential_max_profit_points    = 18.453382611892096
potential_max_drawdown_points  = -2.087049306650171
potential_used                 = 9
event                          = null
```

Published identity:

```text
calculation_id = signal_calculation_893b8e874c7540b49639ed52e7e9be73
source_bar_id  = market_bar_24aaa763c74727518c4422a8d520561d
configuration  = fe22065ee941bf7c942e24febfbe96808b28fd4c54d5384270d45a925b421064
```

The potential direction was `LONG`, but the absolute final potential was below the frozen `30` point threshold. Therefore `NO_SIGNAL` and `event=null` are the expected target outcomes.

## Repeated calculation

The same command was run immediately for the same signal timestamp. It exited with code `0` and returned the already stored calculation:

```text
calculation_id unchanged
calculated_at_utc unchanged
all material metrics unchanged
event remains null
```

The two output files had identical SHA-256 values:

```text
4F4BB9C1E298CA1A491313D8CB2D8D2095C0601BE6A4F91DDED3AAB5578FEB49
```

## Acceptance result

```text
first exit code              = 0
second exit code             = 0
output SHA-256 equal         = true
calculation identity equal   = true
material metrics equal       = true
duplicate event created      = false
```

**Target signal one-shot gate: PASS.**

**Target signal publication idempotency gate: PASS.**

## Remaining signal-shadow gates

Before decision work:

1. compare this real calculation with the current rolling implementation on the same timestamp;
2. run a short continuous target signal session;
3. prove one calculation attempt per due point;
4. confirm the signal process still has no command or broker side effects.
