from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from ibmd.catalog import CatalogError, SessionPhase, load_catalog_bundle, resolve_session
from ibmd.catalog.cme_schedules import (
    CmeScheduleError,
    build_qualified_cme_session_calendar,
    build_qualified_cme_session_calendar_from_files,
    parse_cme_trading_schedules,
    select_cme_trading_schedule,
)
from ibmd.catalog.common import compute_content_hash
from ibmd.catalog.resolver import require_production_qualified_session
from ibmd.foundation.atomic_json import atomic_write_json, read_json_object

ROOT = Path(__file__).resolve().parents[1]
CATALOG_ROOT = ROOT / "catalog"
FIXTURE = ROOT / "tester" / "fixtures" / "cme_trading_schedules_sample.json"


def source_payload() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def build():
    return build_qualified_cme_session_calendar(
        base_calendar=load_catalog_bundle(CATALOG_ROOT).session_calendar,
        source_payload=source_payload(),
        session_id="CME_EQUITY_INDEX",
        globex_group_code="NQ",
        trading_schedule_id="42",
        coverage_start_date="2026-07-06",
        coverage_end_date="2026-07-08",
        calendar_version="sessions.cme.synthetic.20260706-20260708.v1",
    )


class CmeTradingScheduleParserTest(unittest.TestCase):
    def test_parser_accepts_documented_cme_shape_and_event_format(self) -> None:
        schedules = parse_cme_trading_schedules(source_payload())
        self.assertEqual(len(schedules), 1)
        schedule = schedules[0]
        self.assertEqual(schedule.trading_schedule_id, "42")
        self.assertEqual(schedule.applicable_globex_group_codes, ("NQ",))
        self.assertEqual(schedule.schedule_names, ("Synthetic Equity Index Futures",))
        self.assertEqual(schedule.events[0].event_type, "open")
        self.assertEqual(
            schedule.events[0].occurred_at_utc.isoformat(),
            "2026-07-05T22:00:00+00:00",
        )

    def test_group_code_object_and_embedded_collection_are_supported(self) -> None:
        payload = source_payload()
        schedule = payload["tradingSchedules"][0]
        schedule["applicableGlobexGroupCodes"] = [
            {"globexGroupCode": "NQ"}
        ]
        embedded = {"_embedded": {"tradingSchedules": [schedule]}}
        selected = select_cme_trading_schedule(
            parse_cme_trading_schedules(embedded),
            globex_group_code="NQ",
        )
        self.assertEqual(selected.trading_schedule_id, "42")

    def test_ambiguous_group_requires_explicit_schedule_id(self) -> None:
        payload = source_payload()
        second = copy.deepcopy(payload["tradingSchedules"][0])
        second["tradingScheduleId"] = "43"
        payload["tradingSchedules"].append(second)
        schedules = parse_cme_trading_schedules(payload)
        with self.assertRaisesRegex(CmeScheduleError, "not unique"):
            select_cme_trading_schedule(
                schedules,
                globex_group_code="NQ",
            )
        selected = select_cme_trading_schedule(
            schedules,
            globex_group_code="NQ",
            trading_schedule_id="43",
        )
        self.assertEqual(selected.trading_schedule_id, "43")

    def test_unknown_event_type_is_rejected(self) -> None:
        payload = source_payload()
        payload["tradingSchedules"][0]["marketEventsByDate"][0][
            "marketEvents"
        ][0]["marketEventType"] = "auction"
        with self.assertRaisesRegex(CmeScheduleError, "unsupported"):
            parse_cme_trading_schedules(payload)


class CmeSessionCalendarBuildTest(unittest.TestCase):
    def test_builds_only_days_that_differ_from_weekly_template(self) -> None:
        result = build()
        session = result.calendar.require("CME_EQUITY_INDEX")
        self.assertTrue(session.production_qualified)
        self.assertEqual(
            session.exception_coverage_start_date,
            "2026-07-06",
        )
        self.assertEqual(session.exception_coverage_end_date, "2026-07-08")
        self.assertEqual(result.generated_exception_count, 2)
        self.assertEqual(
            tuple(item.local_date for item in session.exceptions),
            ("2026-07-06", "2026-07-08"),
        )

        early_close = session.exceptions[0]
        self.assertEqual(early_close.status.value, "CUSTOM")
        self.assertEqual(
            tuple(
                (item.start_local, item.end_local)
                for item in early_close.trading_intervals
            ),
            (("00:00:00", "13:00:00"), ("17:00:00", "24:00:00")),
        )
        self.assertEqual(
            tuple(
                (item.start_local, item.end_local)
                for item in early_close.maintenance_intervals
            ),
            (("13:00:00", "17:00:00"),),
        )

        closed = session.exceptions[1]
        self.assertEqual(closed.status.value, "CLOSED")
        self.assertEqual(closed.trading_intervals, ())
        self.assertEqual(closed.maintenance_intervals, ())

    def test_normal_pause_preopen_open_sequence_matches_weekly_template(self) -> None:
        result = build()
        session = result.calendar.require("CME_EQUITY_INDEX")
        self.assertNotIn(
            "2026-07-07",
            {item.local_date for item in session.exceptions},
        )
        before_pause = resolve_session(
            result.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-07T20:59:59Z",
        )
        pause = resolve_session(
            result.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-07T21:30:00Z",
        )
        reopened = resolve_session(
            result.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-07T22:30:00Z",
        )
        self.assertEqual(before_pause.phase, SessionPhase.TRADING)
        self.assertEqual(pause.phase, SessionPhase.MAINTENANCE)
        self.assertEqual(reopened.phase, SessionPhase.TRADING)
        self.assertTrue(before_pause.production_qualified)
        self.assertTrue(pause.production_qualified)
        self.assertTrue(reopened.production_qualified)

    def test_early_close_and_closed_day_resolve_from_exceptions(self) -> None:
        result = build()
        early_trading = resolve_session(
            result.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-06T17:00:00Z",
        )
        early_maintenance = resolve_session(
            result.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-06T19:00:00Z",
        )
        early_reopen = resolve_session(
            result.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-06T22:30:00Z",
        )
        holiday = resolve_session(
            result.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-08T17:00:00Z",
        )
        self.assertEqual(early_trading.phase, SessionPhase.TRADING)
        self.assertEqual(early_maintenance.phase, SessionPhase.MAINTENANCE)
        self.assertEqual(early_reopen.phase, SessionPhase.TRADING)
        self.assertEqual(holiday.phase, SessionPhase.CLOSED)
        self.assertTrue(holiday.reason.startswith("exception:"))

    def test_qualification_is_false_outside_bounded_coverage(self) -> None:
        result = build()
        session = result.calendar.require("CME_EQUITY_INDEX")
        require_production_qualified_session(
            session,
            at_utc="2026-07-07T17:00:00Z",
        )
        outside = resolve_session(
            result.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-09T17:00:00Z",
        )
        self.assertEqual(outside.phase, SessionPhase.TRADING)
        self.assertFalse(outside.production_qualified)
        with self.assertRaisesRegex(CatalogError, "coverage"):
            require_production_qualified_session(
                session,
                at_utc="2026-07-09T17:00:00Z",
            )

    def test_source_must_bracket_requested_coverage(self) -> None:
        missing_start = source_payload()
        del missing_start["tradingSchedules"][0]["marketEventsByDate"][0]
        with self.assertRaisesRegex(CmeScheduleError, "coverage start"):
            build_qualified_cme_session_calendar(
                base_calendar=load_catalog_bundle(CATALOG_ROOT).session_calendar,
                source_payload=missing_start,
                session_id="CME_EQUITY_INDEX",
                globex_group_code="NQ",
                trading_schedule_id="42",
                coverage_start_date="2026-07-06",
                coverage_end_date="2026-07-08",
                calendar_version="sessions.test.missing-start",
            )

        missing_end = source_payload()
        events = missing_end["tradingSchedules"][0]["marketEventsByDate"][-1][
            "marketEvents"
        ]
        events.pop()
        with self.assertRaisesRegex(CmeScheduleError, "coverage end"):
            build_qualified_cme_session_calendar(
                base_calendar=load_catalog_bundle(CATALOG_ROOT).session_calendar,
                source_payload=missing_end,
                session_id="CME_EQUITY_INDEX",
                globex_group_code="NQ",
                trading_schedule_id="42",
                coverage_start_date="2026-07-06",
                coverage_end_date="2026-07-08",
                calendar_version="sessions.test.missing-end",
            )

    def test_generated_artifact_round_trips_and_has_valid_hash(self) -> None:
        result = build()
        raw = result.calendar.to_dict()
        self.assertEqual(raw["content_hash"], compute_content_hash(raw))
        self.assertIn(result.source_sha256, result.calendar.sessions[0].qualification_note)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.json"
            source = root / "source.json"
            atomic_write_json(
                base,
                load_catalog_bundle(CATALOG_ROOT).session_calendar.to_dict(),
            )
            atomic_write_json(source, source_payload())
            rebuilt = build_qualified_cme_session_calendar_from_files(
                base_calendar_path=base,
                source_export_path=source,
                session_id="CME_EQUITY_INDEX",
                globex_group_code="NQ",
                trading_schedule_id="42",
                coverage_start_date="2026-07-06",
                coverage_end_date="2026-07-08",
                calendar_version="sessions.cme.synthetic.file.v1",
            )
            output = root / "sessions.json"
            atomic_write_json(output, rebuilt.calendar.to_dict())
            self.assertEqual(
                read_json_object(output)["content_hash"],
                rebuilt.calendar.content_hash,
            )

    def test_exception_outside_coverage_is_rejected(self) -> None:
        raw = build().calendar.to_dict()
        extra = copy.deepcopy(raw["sessions"][0]["exceptions"][0])
        extra["local_date"] = "2026-07-09"
        raw["sessions"][0]["exceptions"].append(extra)
        raw["content_hash"] = compute_content_hash(raw)
        with self.assertRaisesRegex(CatalogError, "outside qualification"):
            from ibmd.catalog.sessions import SessionCalendarV1

            SessionCalendarV1.from_dict(raw)


if __name__ == "__main__":
    unittest.main()
