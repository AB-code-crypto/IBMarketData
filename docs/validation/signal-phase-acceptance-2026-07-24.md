# Signal shadow phase acceptance — 2026-07-24

**Status:** ACCEPTED for continued architecture implementation  
**Branch:** `agent/architecture-rewrite`

## Evidence already obtained

The signal phase is accepted on the basis of the following measured and automated evidence:

- target market-data historical parity: 660/660 complete BID/ASK bars;
- target market-data realtime/startup-seam parity: 202/202 complete bars;
- controlled TWS outage: health became `BLOCKED`, freshness aged, latest bar stopped advancing and a new source session appeared after reconnect;
- one-year target history import: 4,269,190 complete rows present, zero pending rows;
- explicit target signal calculation at `2026-07-23T17:24:00Z` completed successfully;
- repeated calculation returned the identical stored result and identical SHA-256;
- legacy/target signal comparator returned `is_match=true`, `mismatches=[]` at tolerance `1e-9`;
- target signal domain, scheduling, persistence and idempotency checks pass in CI;
- live target market-data health was observed as `RUNNING/READY/CONNECTED`, and its latest realtime bar advanced between read-only SQLite observations.

## Deliberately waived operator check

A further multi-minute manual continuous-signal observation was planned but is not required before continuing the rewrite.

Reason:

- the deterministic signal calculation already matches the current implementation exactly;
- scheduling and one-attempt-per-due-point behavior are covered by automated tests;
- the signal process has no command, execution or broker output;
- additional manual observation would add little information relative to the time cost.

This waiver does not make the target system production-ready. Continuous multi-service paper operation remains required later, when decision and execution are integrated.

## Acceptance decision

Phase 5 (`signal shadow`) is closed for implementation sequencing. Phase 6 (`decision shadow`) has begun with a fixture-driven, non-broker implementation whose command outbox has no consumer.
