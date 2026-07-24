from __future__ import annotations

import copy
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.catalog import (
    ActiveContractStatus,
    CatalogError,
    SessionPhase,
    find_registered_contract,
    load_catalog_bundle,
    resolve_active_contract,
    resolve_session,
)
from ibmd.catalog.models import (
    FuturesContractCalendarV1,
    InstrumentMasterV1,
    SessionCalendarV1,
    StrategyPolicyCatalogV1,
    compute_content_hash,
)
from ibmd.catalog.resolver import require_production_qualified_session

CATALOG_ROOT = ROOT / "catalog"
SOURCE_COMMIT = "5073dbea3f96740b4390e113d06578a514918f00"


class CatalogParityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = load_catalog_bundle(CATALOG_ROOT)

    def test_mnq_instrument_and_strategy_profile_match_baseline(self):
        instrument = self.bundle.instrument_master.require("MNQ")
        self.assertEqual(instrument.sec_type, "FUT")
        self.assertEqual(instrument.trading_class, "MNQ")
        self.assertEqual(instrument.exchange, "CME")
        self.assertEqual(instrument.currency, "USD")
        self.assertEqual(instrument.multiplier, 2.0)
        self.assertEqual(instrument.price_tick, 0.25)
        self.assertEqual(instrument.price_precision, 3)
        self.assertEqual(instrument.default_bar_size_seconds, 5)
        self.assertEqual(instrument.database_name, "MNQ.sqlite3")

        policy = self.bundle.strategy_policy.require("MNQ")
        self.assertTrue(policy.trading_enabled)
        self.assertEqual(policy.target_quantity, 1)
        self.assertEqual(policy.strategic_order_type, "MARKET")
        self.assertEqual(policy.protective.take_profit_points, 75.0)
        self.assertEqual(policy.protective.stop_loss_points, 150.0)
        self.assertEqual(policy.protective.time_in_force, "DAY")
        self.assertTrue(policy.protective.stop_outside_rth)
        self.assertFalse(policy.protective.take_profit_outside_rth)
        self.assertFalse(policy.protective.stale_feed_market_close_enabled)
        self.assertEqual(policy.daily_pnl.target_usd, 500.0)
        self.assertEqual(policy.daily_pnl.timezone, "Europe/Moscow")
        self.assertEqual(policy.daily_flat.liquidation_start_local, "14:59:50")
        self.assertEqual(policy.daily_flat.no_new_risk_local, "15:00:00")
        self.assertEqual(policy.daily_flat.risk_blocked_until_local, "16:00:00")

        signal = policy.signal
        self.assertEqual(signal.source_bar_size_seconds, 5)
        self.assertEqual(signal.max_complete_bar_lag_seconds, 60)
        self.assertEqual(signal.decision_pipeline_max_age_seconds, 30)
        self.assertEqual(signal.rolling_step_seconds, 60)
        self.assertEqual(signal.pattern_lookback_minutes, 90)
        self.assertEqual(signal.potential_horizon_minutes, 30)
        self.assertEqual(signal.historical_lookback_days, 365)
        self.assertEqual(signal.pearson_minimum, 0.7)
        self.assertEqual(signal.minmax_hard_filter_max_ratio, 1.5)
        self.assertEqual(
            (
                signal.score_pearson_weight,
                signal.score_end_delta_weight,
                signal.score_minmax_weight,
            ),
            (1.0, 1.0, 1.0),
        )
        self.assertEqual(
            (
                signal.potential_candidate_min_count,
                signal.potential_candidate_max_count,
            ),
            (7, 9),
        )
        self.assertEqual(signal.minimum_abs_potential_end_delta_points, 30.0)

    def test_mnq_contract_rows_are_exact_baseline_data(self):
        expected = [
            (620730945, "MNQM4", "20240621", "2024-03-13T22:00:00Z", "2024-06-19T17:00:00Z"),
            (637533593, "MNQU4", "20240920", "2024-06-19T22:00:00Z", "2024-09-18T21:00:00Z"),
            (654503320, "MNQZ4", "20241220", "2024-09-18T22:00:00Z", "2024-12-18T22:00:00Z"),
            (672387468, "MNQH5", "20250321", "2024-12-18T23:00:00Z", "2025-03-19T21:00:00Z"),
            (691171685, "MNQM5", "20250620", "2025-03-19T22:00:00Z", "2025-06-18T21:00:00Z"),
            (711280073, "MNQU5", "20250919", "2025-06-18T22:00:00Z", "2025-09-17T21:00:00Z"),
            (730283094, "MNQZ5", "20251219", "2025-09-17T22:00:00Z", "2025-12-17T22:00:00Z"),
            (750150193, "MNQH6", "20260320", "2025-12-17T23:00:00Z", "2026-03-18T21:00:00Z"),
            (770561201, "MNQM6", "20260618", "2026-03-18T22:00:00Z", "2026-06-16T21:00:00Z"),
            (793356225, "MNQU6", "20260918", "2026-06-16T22:00:00Z", "2026-09-16T21:00:00Z"),
            (815824267, "MNQZ6", "20261218", "2026-09-16T22:00:00Z", "2026-12-16T22:00:00Z"),
        ]
        actual = [
            (
                row.con_id,
                row.local_symbol,
                row.last_trade_date,
                row.active_from_utc,
                row.active_to_utc,
            )
            for row in self.bundle.contract_calendar.contracts
        ]
        self.assertEqual(actual, expected)
        self.assertEqual(len(self.bundle.contract_calendar.declared_gaps), 10)
        self.assertTrue(
            all(
                gap.reason == "ROLL_TRANSITION_NO_ACTIVE_CONTRACT"
                for gap in self.bundle.contract_calendar.declared_gaps
            )
        )

    def test_all_artifacts_point_to_the_frozen_runtime_commit(self):
        self.assertEqual(self.bundle.instrument_master.source_runtime_commit, SOURCE_COMMIT)
        self.assertEqual(self.bundle.contract_calendar.source_runtime_commit, SOURCE_COMMIT)
        self.assertEqual(self.bundle.session_calendar.source_runtime_commit, SOURCE_COMMIT)
        self.assertEqual(self.bundle.strategy_policy.source_runtime_commit, SOURCE_COMMIT)
        self.assertEqual(len(self.bundle.bundle_hash), 64)


class ContractCalendarResolutionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.calendar = load_catalog_bundle(CATALOG_ROOT).contract_calendar

    def test_left_inclusive_right_exclusive_boundaries_and_declared_gap(self):
        active = resolve_active_contract(self.calendar, "2026-03-18T22:00:00Z")
        self.assertEqual(active.status, ActiveContractStatus.ACTIVE)
        self.assertEqual(active.contract.local_symbol, "MNQM6")

        last_second = resolve_active_contract(self.calendar, "2026-06-16T20:59:59Z")
        self.assertEqual(last_second.contract.local_symbol, "MNQM6")

        gap = resolve_active_contract(self.calendar, "2026-06-16T21:00:00Z")
        self.assertEqual(gap.status, ActiveContractStatus.DECLARED_GAP)
        self.assertIsNone(gap.contract)
        self.assertEqual(gap.gap.end_utc, "2026-06-16T22:00:00Z")

        next_contract = resolve_active_contract(self.calendar, "2026-06-16T22:00:00Z")
        self.assertEqual(next_contract.status, ActiveContractStatus.ACTIVE)
        self.assertEqual(next_contract.contract.local_symbol, "MNQU6")

    def test_current_baseline_timestamp_resolves_mnqu6_and_old_contract_is_findable(self):
        current = resolve_active_contract(self.calendar, "2026-07-23T00:00:00Z")
        self.assertEqual(current.contract.con_id, 793356225)
        self.assertEqual(current.contract.local_symbol, "MNQU6")
        old = find_registered_contract(self.calendar, con_id=770561201)
        self.assertEqual(old.local_symbol, "MNQM6")
        by_symbol = find_registered_contract(self.calendar, local_symbol="MNQM6")
        self.assertEqual(by_symbol.con_id, 770561201)

    def test_outside_coverage_is_explicit(self):
        before = resolve_active_contract(self.calendar, "2024-03-13T21:59:59Z")
        after = resolve_active_contract(self.calendar, "2026-12-16T22:00:00Z")
        self.assertEqual(before.status, ActiveContractStatus.OUTSIDE_CALENDAR)
        self.assertEqual(after.status, ActiveContractStatus.OUTSIDE_CALENDAR)

    def test_overlap_and_undeclared_gap_are_rejected(self):
        raw = json.loads((CATALOG_ROOT / "contracts.mnq.v1.json").read_text(encoding="utf-8"))
        overlap = copy.deepcopy(raw)
        overlap["contracts"][1]["active_from_utc"] = "2024-06-19T16:00:00Z"
        overlap["content_hash"] = compute_content_hash(overlap)
        with self.assertRaisesRegex(CatalogError, "overlap"):
            FuturesContractCalendarV1.from_dict(overlap)

        undeclared = copy.deepcopy(raw)
        undeclared["declared_gaps"] = undeclared["declared_gaps"][1:]
        undeclared["content_hash"] = compute_content_hash(undeclared)
        with self.assertRaisesRegex(CatalogError, "declared activation gaps"):
            FuturesContractCalendarV1.from_dict(undeclared)


class SessionCalendarTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.calendar = load_catalog_bundle(CATALOG_ROOT).session_calendar
        cls.session = cls.calendar.require("CME_EQUITY_INDEX")

    @staticmethod
    def utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

    def test_normal_weekly_phases_include_dst_conversion(self):
        # July uses CDT (UTC-5): 21:00 UTC = 16:00 CT maintenance start.
        before = resolve_session(
            self.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc=self.utc(2026, 7, 20, 20, 59),
        )
        maintenance = resolve_session(
            self.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc=self.utc(2026, 7, 20, 21, 0),
        )
        reopened = resolve_session(
            self.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc=self.utc(2026, 7, 20, 22, 0),
        )
        self.assertEqual(before.phase, SessionPhase.TRADING)
        self.assertEqual(maintenance.phase, SessionPhase.MAINTENANCE)
        self.assertEqual(reopened.phase, SessionPhase.TRADING)

        # January uses CST (UTC-6): 22:00 UTC = 16:00 CT maintenance start.
        winter_maintenance = resolve_session(
            self.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc=self.utc(2026, 1, 5, 22, 0),
        )
        self.assertEqual(winter_maintenance.phase, SessionPhase.MAINTENANCE)

    def test_weekend_boundary_is_explicit(self):
        friday_open = resolve_session(
            self.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-17T20:59:59Z",
        )
        friday_closed = resolve_session(
            self.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-17T21:00:00Z",
        )
        sunday_closed = resolve_session(
            self.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-19T21:59:59Z",
        )
        sunday_open = resolve_session(
            self.calendar,
            session_id="CME_EQUITY_INDEX",
            at_utc="2026-07-19T22:00:00Z",
        )
        self.assertEqual(friday_open.phase, SessionPhase.TRADING)
        self.assertEqual(friday_closed.phase, SessionPhase.CLOSED)
        self.assertEqual(sunday_closed.phase, SessionPhase.CLOSED)
        self.assertEqual(sunday_open.phase, SessionPhase.TRADING)

    def test_parity_session_is_deliberately_not_live_qualified(self):
        self.assertFalse(self.session.production_qualified)
        self.assertIsNone(self.session.exception_coverage_end_date)
        with self.assertRaisesRegex(CatalogError, "not production-qualified"):
            require_production_qualified_session(self.session)
        with self.assertRaisesRegex(CatalogError, "not production-qualified"):
            load_catalog_bundle(CATALOG_ROOT, require_production_sessions=True)


class StrictArtifactValidationTest(unittest.TestCase):
    def test_unknown_field_and_content_mutation_are_rejected(self):
        instrument_raw = json.loads(
            (CATALOG_ROOT / "instruments.v1.json").read_text(encoding="utf-8")
        )
        unknown = copy.deepcopy(instrument_raw)
        unknown["unexpected"] = True
        unknown["content_hash"] = compute_content_hash(unknown)
        with self.assertRaisesRegex(CatalogError, "unknown"):
            InstrumentMasterV1.from_dict(unknown)

        changed = copy.deepcopy(instrument_raw)
        changed["instruments"][0]["price_tick"] = 0.5
        with self.assertRaisesRegex(CatalogError, "content_hash mismatch"):
            InstrumentMasterV1.from_dict(changed)

    def test_boolean_and_integer_fields_are_strict(self):
        strategy_raw = json.loads(
            (CATALOG_ROOT / "strategy.IBMarketData.rolling.v1.json").read_text(
                encoding="utf-8"
            )
        )
        wrong_bool = copy.deepcopy(strategy_raw)
        wrong_bool["instruments"][0]["trading_enabled"] = "true"
        wrong_bool["content_hash"] = compute_content_hash(wrong_bool)
        with self.assertRaisesRegex(CatalogError, "must be a boolean"):
            StrategyPolicyCatalogV1.from_dict(wrong_bool)

        fractional = copy.deepcopy(strategy_raw)
        fractional["instruments"][0]["target_quantity"] = 1.5
        fractional["content_hash"] = compute_content_hash(fractional)
        with self.assertRaisesRegex(CatalogError, "must be an integer"):
            StrategyPolicyCatalogV1.from_dict(fractional)

    def test_round_trip_preserves_verified_artifacts(self):
        bundle = load_catalog_bundle(CATALOG_ROOT)
        self.assertEqual(
            InstrumentMasterV1.from_dict(bundle.instrument_master.to_dict()),
            bundle.instrument_master,
        )
        self.assertEqual(
            FuturesContractCalendarV1.from_dict(bundle.contract_calendar.to_dict()),
            bundle.contract_calendar,
        )
        self.assertEqual(
            SessionCalendarV1.from_dict(bundle.session_calendar.to_dict()),
            bundle.session_calendar,
        )
        self.assertEqual(
            StrategyPolicyCatalogV1.from_dict(bundle.strategy_policy.to_dict()),
            bundle.strategy_policy,
        )


if __name__ == "__main__":
    unittest.main()
