# Daily-risk late commission enrichment

## Finding

A same-day `HALTED` state was returned unchanged before processing a newly complete `DailyRiskCalculationV1`. That preserved the halt correctly, but it also prevented late commission evidence from enriching previously incomplete PnL fields.

## Required behavior

`HALTED` remains sticky for the trading day, but its calculation fields may be enriched monotonically when missing commission evidence arrives:

```text
status         = HALTED        (unchanged)
cleanup_status = COMPLETE      (unchanged)
pnl_ready      = false -> true
realized_pnl   = null  -> proven value
unrealized_pnl = null  -> proven value
total_pnl      = null  -> proven value
```

The state must never return to `MONITORING`, and incomplete evidence must never be converted to zero.

## Validation

A regression test starts from a `HALTED/COMPLETE` state with incomplete commission evidence, reruns daily risk with the commission attached to the same immutable `execId`, and requires the halt to remain while PnL becomes complete.
