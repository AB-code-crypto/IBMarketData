from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ibmd.catalog import load_catalog_bundle
from ibmd.catalog.sessions import (
    LocalIntervalV1,
    SessionExceptionStatus,
    SessionExceptionV1,
)
from ibmd.execution.adapters.sqlite_liquidation_triggers import (
    SQLiteLiquidationTriggerReader,
)
from ibmd.execution.adapters.sqlite_protection import SQLiteProtectionStore
from ibmd.execution.application.liquidation_triggers import (
    LiquidationTriggerProducerPolicyV1,
    LiquidationTriggerProducerService,
    evaluate_liquidation_trigger_candidates,
)
from ibmd.execution.domain.protection import PositionEpisodeProtectionPlan
from ibmd.execution.domain.protective_uncertainty import readiness_for_protection
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
)
from ibmd.public_contracts.liquidation import LiquidationReason
from tester.target_execution_liquidation_tester import (
    apply_schema,
    live_protection,
)
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    DEPLOYMENT,
    INSTRUMENT,
    STRATEGY,
    blocked_readiness,
    episode_and_protection,
    strategy_position,
)

ROOT = Path(__file__).resolve().parents[1]


def qualified_bundle(*, early_close: bool = False):
    bundle = load_catalog_bundle(ROOT / "catalog")
    session = bundle.session_calendar.require("CME_EQUITY_INDEX")
    exceptions = session.exceptions
    if early_close:
        exceptions = (
            SessionExceptionV1(
                local_date="2026-07-27",
                status=SessionExceptionStatus.CUSTOM,
                trading_intervals=(
                    LocalIntervalV1(
                        start_local="00:00:00",
                        end_local="12:00:00",
                    ),
                ),
                maintenance_intervals=(),
                reason="test early close",
            ),
        )
    qualified_session = replace(
        session,
        exceptions=exceptions,
        production_qualified=True,
        exception_coverage_start_date="2026-01-01",
        exception_coverage_end_date="2026-12-31",
        qualification_note="test-qualified calendar",
    )
    session_calendar = replace(
        bundle.session_calendar,
        sessions=(qualified_session,),
    )
    return replace(bundle, session_calendar=session_calendar)


def producer_policy(*, require_production_session: bool = True):
    return LiquidationTriggerProducerPolicyV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        missing_stop_grace_seconds=30.0,
        require_production_session=require_production_session,
    )


def daily_risk(status: DailyRiskStatus) -> DailyRiskStateV1:
    return DailyRiskStateV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        trading_day="2026-07-27",
        status=status,
        realized_pnl=550.0,
        unrealized_pnl=25.0,
        total_pnl=575.0,
        target_pnl=500.0,
        pnl_ready=True,
        cleanup_status=(
            DailyRiskCleanupStatus.COMPLETE
            if status == DailyRiskStatus.HALTED
            else DailyRiskCleanupStatus.PENDING
        ),
        updated_at_utc="2026-07-27T19:30:00Z",
    )


class MemoryTriggerState:
    def __init__(
        self,
        *,
        episode,
        protection,
        position,
        readiness,
        risk=None,
    ) -> None:
        self.episode = episode
        self.protection = protection
        self.position = position
        self.readiness = readiness
        self.risk = risk
        self.liquidation = None

    def list_open_episodes(self, **_scope):
        return (self.episode,)

    def read_protection_by_episode(self, position_episode_id):
        return (
            self.protection
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def read_position(self, **_scope):
        return self.position

    def read_readiness(self, **_scope):
        return self.readiness

    def read_latest_daily_risk(self, **_scope):
        return self.risk

    def read_snapshot_by_episode(self, position_episode_id):
        return (
            self.liquidation
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def publish_request(self, *, current, result):
        if current != self.liquidation:
            raise AssertionError("liquidation current state mismatch")
        self.liquidation = result.snapshot
        self.readiness = result.execution_readiness
        return self.liquidation


class LiquidationTriggerEvaluationTest(unittest.TestCase):
    def test_normal_daily_flat_boundary_is_exact(self) -> None:
        episode, protection = live_protection()
        before, blockers = evaluate_liquidation_trigger_candidates(
            bundle=qualified_bundle(),
            producer_policy=producer_policy(),
            episode=episode,
            protection=protection,
            daily_risk=None,
            existing=None,
            observed_at_utc="2026-07-27T19:59:49Z",
        )
        self.assertNotIn(
            LiquidationReason.DAILY_FLAT,
            {item.reason for item in before},
        )
        self.assertEqual(blockers, ())

        due, blockers = evaluate_liquidation_trigger_candidates(
            bundle=qualified_bundle(),
            producer_policy=producer_policy(),
            episode=episode,
            protection=protection,
            daily_risk=None,
            existing=None,
            observed_at_utc="2026-07-27T19:59:50Z",
        )
        daily = [
            item for item in due if item.reason == LiquidationReason.DAILY_FLAT
        ]
        self.assertEqual(len(daily), 1)
        self.assertEqual(
            daily[0].source_ref,
            "daily-flat:CME_EQUITY_INDEX:2026-07-27",
        )
        self.assertEqual(blockers, ())

    def test_early_close_shifts_daily_flat_boundary(self) -> None:
        episode, protection = live_protection()
        candidates, blockers = evaluate_liquidation_trigger_candidates(
            bundle=qualified_bundle(early_close=True),
            producer_policy=producer_policy(),
            episode=episode,
            protection=protection,
            daily_risk=None,
            existing=None,
            observed_at_utc="2026-07-27T15:59:50Z",
        )
        self.assertIn(
            LiquidationReason.DAILY_FLAT,
            {item.reason for item in candidates},
        )
        self.assertEqual(blockers, ())

    def test_unqualified_due_calendar_is_blocked_not_invented(self) -> None:
        episode, protection = live_protection()
        candidates, blockers = evaluate_liquidation_trigger_candidates(
            bundle=load_catalog_bundle(ROOT / "catalog"),
            producer_policy=producer_policy(),
            episode=episode,
            protection=protection,
            daily_risk=None,
            existing=None,
            observed_at_utc="2026-07-27T19:59:50Z",
        )
        self.assertNotIn(
            LiquidationReason.DAILY_FLAT,
            {item.reason for item in candidates},
        )
        self.assertEqual(
            blockers,
            ("daily_flat_session_not_production_qualified:CME_EQUITY_INDEX",),
        )

    def test_missing_stop_uses_grace_then_becomes_durable_reason(self) -> None:
        episode, protection = episode_and_protection()
        within_grace, _ = evaluate_liquidation_trigger_candidates(
            bundle=qualified_bundle(),
            producer_policy=producer_policy(),
            episode=episode,
            protection=protection,
            daily_risk=None,
            existing=None,
            observed_at_utc="2026-07-27T10:00:29Z",
        )
        self.assertNotIn(
            LiquidationReason.MISSING_STOP,
            {item.reason for item in within_grace},
        )
        expired, _ = evaluate_liquidation_trigger_candidates(
            bundle=qualified_bundle(),
            producer_policy=producer_policy(),
            episode=episode,
            protection=protection,
            daily_risk=None,
            existing=None,
            observed_at_utc="2026-07-27T10:00:30Z",
        )
        missing = [
            item for item in expired if item.reason == LiquidationReason.MISSING_STOP
        ]
        self.assertEqual(len(missing), 1)
        self.assertEqual(
            missing[0].source_ref,
            f"missing-stop:{protection.stop_order.protective_order_id}",
        )

    def test_daily_halt_and_rollover_are_independent_candidates(self) -> None:
        episode, protection = live_protection()
        candidates, blockers = evaluate_liquidation_trigger_candidates(
            bundle=qualified_bundle(),
            producer_policy=producer_policy(),
            episode=episode,
            protection=protection,
            daily_risk=daily_risk(DailyRiskStatus.TRIGGERED),
            existing=None,
            observed_at_utc="2026-09-17T00:00:00Z",
        )
        reasons = {item.reason for item in candidates}
        self.assertIn(LiquidationReason.DAILY_HALT, reasons)
        self.assertIn(LiquidationReason.ROLLOVER, reasons)
        self.assertEqual(blockers, ())


class LiquidationTriggerProducerServiceTest(unittest.TestCase):
    def test_multiple_reasons_share_one_operation_and_repeat_is_idempotent(self) -> None:
        episode, protection = live_protection()
        state = MemoryTriggerState(
            episode=episode,
            protection=protection,
            position=strategy_position(episode),
            readiness=readiness_for_protection(
                blocked_readiness(),
                protection=protection,
                observed_at_utc="2026-07-27T10:00:02Z",
            ),
            risk=daily_risk(DailyRiskStatus.TRIGGERED),
        )
        service = LiquidationTriggerProducerService(
            policy=producer_policy(),
            bundle=qualified_bundle(),
            state_source=state,
            repository=state,
        )
        first = service.run_once(observed_at_utc="2026-09-17T00:00:00Z")
        self.assertEqual(first.operation_created_count, 1)
        self.assertEqual(first.trigger_created_count, 3)
        operation_ids = {
            item.operation_id
            for episode_run in first.episodes
            for item in episode_run.persisted
        }
        self.assertEqual(len(operation_ids), 1)

        second = service.run_once(observed_at_utc="2026-09-17T00:00:01Z")
        self.assertEqual(second.operation_created_count, 0)
        self.assertEqual(second.trigger_created_count, 0)
        self.assertEqual(len(state.liquidation.triggers), 3)


class LiquidationTriggerSQLiteReaderTest(unittest.TestCase):
    def test_open_episode_is_read_from_public_target_views(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            episode, protection = episode_and_protection()
            SQLiteProtectionStore(database).publish_plan(
                PositionEpisodeProtectionPlan(
                    episode=episode,
                    strategy_position=strategy_position(episode),
                    execution_readiness=blocked_readiness(),
                    protection=protection,
                )
            )
            reader = SQLiteLiquidationTriggerReader(database)
            reader.validate_schema()
            values = reader.list_open_episodes(
                account_id=ACCOUNT,
                strategy_id=STRATEGY,
                deployment_id=DEPLOYMENT,
                instrument_id=INSTRUMENT,
            )
            self.assertEqual(values, (episode,))
            self.assertEqual(
                reader.read_protection_by_episode(episode.position_episode_id),
                protection,
            )
            stored_position = reader.read_position(
                account_id=ACCOUNT,
                strategy_id=STRATEGY,
                deployment_id=DEPLOYMENT,
                instrument_id=INSTRUMENT,
            )
            self.assertIsNotNone(stored_position)
            self.assertEqual(
                stored_position.position_episode_id,
                episode.position_episode_id,
            )


if __name__ == "__main__":
    unittest.main()
