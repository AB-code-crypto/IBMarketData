from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"expected one replacement in {path}, got {count}: {old[:140]!r}"
        )
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/catalog/cme_schedules.py",
    '''    if normalized in {"paused", "pause"}:
        return "paused", CmeMarketPhase.MAINTENANCE
    if normalized in {
        "preopen",
        "preopenhalt",
        "pcp",
        "postclosepreopen",
        "closed",
        "close",
    }:
        return normalized, CmeMarketPhase.CLOSED
''',
    '''    if normalized in {
        "paused",
        "pause",
        "preopen",
        "preopenhalt",
        "pcp",
        "postclosepreopen",
    }:
        return normalized, CmeMarketPhase.MAINTENANCE
    if normalized in {"closed", "close"}:
        return normalized, CmeMarketPhase.CLOSED
''',
)

replace_once(
    "src/ibmd/catalog/cme_schedules.py",
    '''        for key in ("code", "name", "value", "groupCode", "scheduleName"):
''',
    '''        for key in (
            "code",
            "name",
            "value",
            "groupCode",
            "globexGroupCode",
            "applicableGlobexGroupCode",
            "scheduleName",
        ):
''',
)

replace_once(
    "src/ibmd/catalog/cme_schedules.py",
    '''    for event in events:
        local_event = event.occurred_at_utc.astimezone(zone)
        if state == CmeMarketPhase.TRADING:
''',
    '''    for event in events:
        local_event = event.occurred_at_utc.astimezone(zone)
        if event.phase == state:
            continue
        if state == CmeMarketPhase.TRADING:
''',
)

replace_once(
    "src/ibmd/execution/application/liquidation_triggers.py",
    '''    if due_boundary is None:
        return None, None
    if (
        require_production_session
        and not session.is_production_qualified_for(due_local.date())
    ):
''',
    '''    if due_boundary is None:
        return None, None
    due_local = due_boundary.astimezone(session.zone)
    if (
        require_production_session
        and not session.is_production_qualified_for(due_local.date())
    ):
''',
)

replace_once(
    "src/ibmd/execution/application/liquidation_triggers.py",
    '''        )
    due_local = due_boundary.astimezone(session.zone)
    return (
''',
    '''        )
    return (
''',
)

replace_once(
    "src/ibmd/catalog/sessions.py",
    '''        if self.production_qualified and coverage_start is None:
            raise CatalogError(
                "production-qualified session requires bounded exception coverage"
            )
        object.__setattr__(
''',
    '''        if self.production_qualified and coverage_start is None:
            raise CatalogError(
                "production-qualified session requires bounded exception coverage"
            )
        if coverage_start is not None and coverage_end is not None:
            outside = [
                value
                for value in dates
                if not coverage_start
                <= parse_date(value, field_name="exception.local_date")
                <= coverage_end
            ]
            if outside:
                raise CatalogError(
                    "session exceptions fall outside qualification coverage: "
                    f"{outside}"
                )
        object.__setattr__(
''',
)
