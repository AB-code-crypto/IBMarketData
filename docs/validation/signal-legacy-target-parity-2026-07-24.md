# Signal legacy/target parity evidence — 2026-07-24

**Status:** PASS for the measured rolling calculation  
**Branch:** `agent/architecture-rewrite`  
**Target calculation timestamp:** `2026-07-23T17:24:00Z`  
**Instrument/contract:** MNQ / MNQU6

## Scope

This evidence compares one stored target `SignalCalculationV1` with the current rolling implementation on the same complete price history and explicit signal timestamp.

The comparison was run from the repository root using module execution:

```powershell
python -m scripts.compare_signal_shadow `
  --legacy-price-dir $LegacyPriceDir `
  --target-json $TargetSignalJson `
  --instrument MNQ `
  --output-json $ParityJson
```

Module execution is required for this measured run because the current legacy packages and root `config.py` must be importable from the repository root.

## Operator-reported result

```json
{
  "is_match": true,
  "signal_bar_utc": "2026-07-23T17:24:00.000000Z",
  "tolerance": 1e-09,
  "mismatches": []
}
```

Legacy and target matched exactly on:

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
```

## Acceptance

```text
is_match   = true
mismatches = []
tolerance  = 1e-9
```

**Measured rolling signal parity gate: PASS.**

The result proves parity for this explicit historical timestamp. It does not yet prove continuous scheduling, freshness gating or one-attempt-per-due-point behavior under live shadow collection. Those remain the next operational checks. The signal service still has no decision, command or broker-order output.
