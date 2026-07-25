# CME session calendar qualification

## Purpose

The target execution system must not open risk or schedule daily-flat liquidation
from a weekly template when CME holiday or early-close data are missing.

`catalog/sessions.v1.json` remains the historical parity baseline and is not
production-qualified. Production qualification is built from an **official CME
Reference Data API Trading Schedules JSON export** by:

```text
scripts/build_cme_session_calendar.py
```

The builder performs no network request. An operator obtains the export with a
valid CME OAuth API ID, stores the raw JSON, and runs the deterministic offline
transformation.

No holiday hours are guessed or copied from secondary calendars.

## Source requirements

The JSON must contain the CME Trading Schedules attributes:

```text
tradingScheduleId
applicableGlobexGroupCodes
scheduleNames
marketEventsByDate
marketEvents
marketEventType
marketEventTime
```

Supported market states:

```text
open                 → TRADING
paused               → MAINTENANCE
preopen              → MAINTENANCE
pcp                  → MAINTENANCE
closed               → CLOSED
```

CME event times are interpreted as GMT/UTC using the documented format:

```text
DDMMYYYY-HH:MM:SS.SSSZ
```

The selected schedule must be unique for the explicit Globex group code. When
one group maps to multiple schedule IDs, `--trading-schedule-id` is mandatory.

## Bounded qualification

A qualified session has both:

```text
exception_coverage_start_date
exception_coverage_end_date
```

Qualification is date-specific:

```text
inside coverage  → production_qualified=true
outside coverage → production_qualified=false
```

The weekly template can still resolve a phase outside coverage, but production
risk gates must reject it. This prevents an expired holiday calendar from
silently becoming authoritative.

The source event stream must establish a market state before coverage starts and
must extend through the end of coverage. Otherwise the build fails.

## Generated exceptions

The official UTC event timeline is projected into `America/Chicago` calendar
days. Adjacent CME events that represent the same target phase are merged.

An exception is stored only when the official day differs from the weekly
template:

```text
normal day     → no exception row
early close    → CUSTOM exception
full closure   → CLOSED exception
special reopen → CUSTOM exception
```

Every exception records:

```text
CME_REFERENCE_DATA_API
trading schedule ID
Globex group code
```

The qualification note records the SHA-256 of the complete source export and the
bounded coverage dates.

## Build command

Example:

```powershell
python scripts/build_cme_session_calendar.py `
  --base-calendar catalog/sessions.v1.json `
  --cme-export C:\IBMD-reference\cme-trading-schedules.json `
  --output C:\IBMD-reference\sessions.cme.2026.json `
  --session-id CME_EQUITY_INDEX `
  --globex-group-code <OFFICIAL_MNQ_GROUP_CODE> `
  --trading-schedule-id <ID_IF_REQUIRED> `
  --coverage-start-date 2026-01-01 `
  --coverage-end-date 2026-12-31 `
  --calendar-version sessions.cme.2026.v1
```

The MNQ Globex group code and optional schedule ID must come from the same CME
export. They are intentionally not guessed in repository code.

The script refuses to overwrite an existing output unless `--force` is given.

## Validation

Validate the generated artifact by replacing a temporary catalog copy and
running:

```powershell
python scripts/validate_target_catalog.py `
  --catalog-root <TEMP_CATALOG_ROOT> `
  --require-production-sessions
```

Also inspect representative dates:

```text
normal maintenance day
holiday closure
early close
Sunday reopen
DST transition week
first coverage date
last coverage date
one date outside coverage
```

## Update policy

CME schedules can change. Before the current coverage expires:

1. obtain a fresh official export;
2. build a new versioned session artifact;
3. compare the exception diff;
4. run catalog and execution tests;
5. deploy the new catalog while execution is stopped;
6. keep the previous artifact for rollback.

Never extend `exception_coverage_end_date` without source events that actually
cover the extended period.

## Current repository state

The repository contains:

- the parity weekly template;
- bounded qualification semantics;
- the official-export transformer;
- deterministic synthetic tests;
- no committed official CME export and no claimed production-qualified 2026
  calendar.

Therefore production broker mutation remains disabled until an official export
is supplied, transformed and reviewed.
