from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"expected one replacement in {path}, got {count}: {old[:120]!r}"
        )
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "tester/target_execution_liquidation_trigger_tester.py",
    '''        production_qualified=True,
        exception_coverage_end_date="2026-12-31",
''',
    '''        production_qualified=True,
        exception_coverage_start_date="2026-01-01",
        exception_coverage_end_date="2026-12-31",
''',
)

replace_once(
    "tester/target_cme_session_calendar_tester.py",
    '''from ibmd.catalog.resolver import require_production_qualified_session
''',
    '''from ibmd.catalog.resolver import require_production_qualified_session
from ibmd.catalog.sessions import SessionCalendarV1
''',
)

replace_once(
    "tester/target_cme_session_calendar_tester.py",
    '''        with self.assertRaisesRegex(CatalogError, "outside qualification"):
            from ibmd.catalog.sessions import SessionCalendarV1

            SessionCalendarV1.from_dict(raw)
''',
    '''        with self.assertRaisesRegex(CatalogError, "outside qualification"):
            SessionCalendarV1.from_dict(raw)
''',
)
