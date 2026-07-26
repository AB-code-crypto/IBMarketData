from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from ibmd.execution.application.paper_daily_halt_drill import (
    PaperDailyHaltDrillError,
    PaperDailyHaltDrillPolicyV1,
    PaperDailyHaltDrillService,
)
from ibmd.execution.domain.daily_risk import DailyRiskOwnedFillV1
from ibmd.operations.paper_acceptance import PaperAcceptanceArtifactStore
from ibmd.operations.paper_daily_halt_acceptance import (
    PaperDailyHaltAcceptanceRunner,
)
from ibmd.operations.paper_liquidation_acceptance import (
    PaperLiquidationAcceptanceError,
)
from ibmd.public_contracts.broker_execution import BrokerOrderSide
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionSide,
)
from ibmd.public_contracts.protection import ProtectiveOrderKind
from tester.target_execution_daily_risk_tester import commission, owned_fill
from tester.target_execution_liquidation_tester import live_protection
from tester.target_execution_protective_submit_tester import (
    ACCOUNT,
    DEPLOYMENT,
    INSTRUMENT,
    STRATEGY,
    T3,
    strategy_position,
)
from tester.target_paper_liquidation_acceptance_tester import (
    EPISODE_ID,
    OPERATION_ID,
    FakeExecutor,
    FakeStateSource,
    closed_state,
    paper_payload,
    policy as liquidation_policy,
    state as liquidation_state,
)

OBSERVED = "2026-07-27T10:00:05Z"
TRIGGER_ID = "liquidation_trigger_00000000000000000000000000000001"
CLOCK = datetime(2026, 7, 27, 10, 0, 5, tzinfo=timezone.utc)


def ready_readiness() -> ExecutionReadinessV1:
    return ExecutionReadinessV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=ExecutionReadinessStatus.READY,
        command_intake_enabled=True,
        broker_actions_enabled=True,
        reconciliation_complete=True,
        clock_healthy=True,
        blocking_reasons=(),
        updated_at_utc=T3,
    )


def monitoring_state() -> DailyRiskStateV1:
    return DailyRiskStateV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        trading_day="2026-07-27",
        status=DailyRiskStatus.MONITORING,
        realized_pnl=-1.25,
        unrealized_pnl=0.0,
        total_pnl=-1.25,
        target_pnl=500.0,
        pnl_ready=True,
        cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
        updated_at_utc=T3,
    )


def drill_policy() -> PaperDailyHaltDrillPolicyV1:
    return PaperDailyHaltDrillPolicyV1(
        drill_id="paper-daily-halt-test",
        account_id=ACCOUNT,
        environment="paper",
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        timezone_name="Europe/Moscow",
        target_pnl=500.0,
        contract_multiplier=2.0,
        market_max_age_seconds=60.0,
        price_tick=0.25,
        trigger_cushion_usd=1.0,
    )


class MemoryDailyHaltRepository:
    def __init__(self, *, short: bool = False, complete_commission: bool = True):
        episode, protection = live_protection()
        position = strategy_position(episode)
        fill = owned_fill(
            commission_fact=(
                commission("daily-risk-exec-1")
                if complete_commission
                else None
            )
        )
        if short:
            episode = replace(episode, side=StrategyPositionSide.SHORT)
            position = replace(
                position,
                side=StrategyPositionSide.SHORT,
                contracts=tuple(
                    replace(item, signed_quantity=-abs(item.signed_quantity))
                    for item in position.contracts
                ),
            )
            protection = replace(
                protection,
                orders=tuple(
                    replace(item, side=BrokerOrderSide.BUY)
                    for item in protection.orders
                ),
            )
            fill = DailyRiskOwnedFillV1(
                kind=fill.kind,
                fill=replace(fill.fill, side=BrokerOrderSide.SELL),
            )
        self.episode = episode
        self.protection = protection
        self.position = position
        self.readiness = ready_readiness()
        self.state = monitoring_state()
        self.fills = (fill,)
        self.liquidation = None
        self.publish_count = 0

    def read_position(self, **_scope):
        return self.position

    def read_readiness(self, **_scope):
        return self.readiness

    def read_episode(self, position_episode_id):
        return (
            self.episode
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def read_protection_by_episode(self, position_episode_id):
        return (
            self.protection
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def read_owned_fills(self, **_scope):
        return self.fills

    def read_liquidation_operation(self, **_scope):
        return self.liquidation

    def read_latest_state(self, **_scope):
        return self.state

    def publish(self, *, current_state, current_readiness, update):
        if current_state != self.state or current_readiness != self.readiness:
            raise AssertionError("daily-halt source changed concurrently")
        self.state = update.state
        self.readiness = update.execution_readiness
        self.publish_count += 1
        return update


class PaperDailyHaltDrillServiceTest(unittest.TestCase):
    def test_long_trigger_uses_tick_aligned_favourable_mark(self) -> None:
        repository = MemoryDailyHaltRepository()
        result = PaperDailyHaltDrillService(
            policy=drill_policy(),
            execution_state_source=repository,
            episode_source=repository,
            evidence_source=repository,
            repository=repository,
        ).run_once(observed_at_utc=OBSERVED)
        self.assertEqual(result.triggered_update.state.status, DailyRiskStatus.TRIGGERED)
        self.assertEqual(
            result.triggered_update.state.cleanup_status,
            DailyRiskCleanupStatus.PENDING,
        )
        self.assertGreaterEqual(
            result.triggered_update.calculation.total_pnl,
            result.target_total_pnl,
        )
        self.assertGreater(
            result.synthetic_mark.mid_price,
            repository.episode.entry_average_price,
        )
        self.assertAlmostEqual(
            result.synthetic_mark.mid_price / drill_policy().price_tick,
            round(result.synthetic_mark.mid_price / drill_policy().price_tick),
        )
        self.assertEqual(repository.publish_count, 1)
        self.assertEqual(repository.readiness.status, ExecutionReadinessStatus.BLOCKED)
        self.assertFalse(repository.readiness.command_intake_enabled)
        self.assertTrue(repository.readiness.broker_actions_enabled)
        self.assertFalse(result.broker_mutations_performed)

    def test_short_trigger_moves_mark_down_and_still_crosses_target(self) -> None:
        repository = MemoryDailyHaltRepository(short=True)
        result = PaperDailyHaltDrillService(
            policy=drill_policy(),
            execution_state_source=repository,
            episode_source=repository,
            evidence_source=repository,
            repository=repository,
        ).run_once(observed_at_utc=OBSERVED)
        self.assertLess(
            result.synthetic_mark.mid_price,
            repository.episode.entry_average_price,
        )
        self.assertGreaterEqual(
            result.triggered_update.calculation.total_pnl,
            result.target_total_pnl,
        )
        self.assertEqual(result.triggered_update.state.status, DailyRiskStatus.TRIGGERED)

    def test_incomplete_commission_evidence_fails_without_state_write(self) -> None:
        repository = MemoryDailyHaltRepository(complete_commission=False)
        service = PaperDailyHaltDrillService(
            policy=drill_policy(),
            execution_state_source=repository,
            episode_source=repository,
            evidence_source=repository,
            repository=repository,
        )
        with self.assertRaisesRegex(
            PaperDailyHaltDrillError,
            "incomplete owned fill evidence",
        ):
            service.run_once(observed_at_utc=OBSERVED)
        self.assertEqual(repository.publish_count, 0)
        self.assertEqual(repository.state.status, DailyRiskStatus.MONITORING)

    def test_existing_sticky_state_cannot_be_replaced(self) -> None:
        repository = MemoryDailyHaltRepository()
        repository.state = replace(
            repository.state,
            status=DailyRiskStatus.TRIGGERED,
            cleanup_status=DailyRiskCleanupStatus.PENDING,
        )
        service = PaperDailyHaltDrillService(
            policy=drill_policy(),
            execution_state_source=repository,
            episode_source=repository,
            evidence_source=repository,
            repository=repository,
        )
        with self.assertRaisesRegex(
            PaperDailyHaltDrillError,
            "requires current DailyRiskState=MONITORING",
        ):
            service.run_once(observed_at_utc=OBSERVED)
        self.assertEqual(repository.publish_count, 0)

    def test_live_environment_is_rejected(self) -> None:
        values = dict(drill_policy().__dict__)
        values["environment"] = "live"
        with self.assertRaisesRegex(
            PaperDailyHaltDrillError,
            "IBMD_ENVIRONMENT=paper",
        ):
            PaperDailyHaltDrillPolicyV1(**values)


def write_source_summary(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_name": "PaperAcceptanceResult",
                "schema_version": 1,
                "drill_id": "source-daily-halt-test",
                "position_episode_id": EPISODE_ID,
                "position_proof": {"accepted": True},
                "protection": {
                    "fully_live": True,
                    "stop_state": "LIVE",
                    "take_profit_state": "LIVE",
                },
                "live_position_left_protected": True,
            }
        ),
        encoding="utf-8",
    )


def synthetic_trigger_payload() -> dict:
    return {
        "schema_name": "PaperDailyHaltDrillResult",
        "schema_version": 1,
        "drill_id": "paper-daily-halt-runner-test",
        "position_episode_id": EPISODE_ID,
        "owned_fill_count": 1,
        "synthetic_market_mark_only": True,
        "real_owned_fill_evidence_only": True,
        "target_total_pnl": 501.0,
        "triggered_calculation": {
            "open_position_episode_id": EPISODE_ID,
            "total_pnl": 501.25,
        },
        "daily_risk_state": {
            "status": "TRIGGERED",
            "cleanup_status": "PENDING",
            "pnl_ready": True,
            "total_pnl": 501.25,
            "target_pnl": 500.0,
        },
        "execution_readiness": {
            "status": "BLOCKED",
            "command_intake_enabled": False,
            "broker_actions_enabled": True,
        },
        "broker_mutations_performed": False,
    }


def daily_halt_trigger_payload() -> dict:
    source_ref = "daily-halt:2026-07-27"
    return {
        "position_episode_id": EPISODE_ID,
        "observed_at_utc": OBSERVED,
        "selected_reason": "DAILY_HALT",
        "selected_source_ref": source_ref,
        "selected_detail": "daily risk state requires liquidation",
        "all_candidates": [
            {
                "reason": "DAILY_HALT",
                "source_ref": source_ref,
                "detail": "daily risk state requires liquidation",
            }
        ],
        "blocked_reasons": [],
        "liquidation_operation": {
            "liquidation_operation_id": OPERATION_ID,
            "state": "REQUESTED",
        },
        "liquidation_trigger": {
            "trigger_id": TRIGGER_ID,
            "liquidation_operation_id": OPERATION_ID,
            "reason": "DAILY_HALT",
            "source_ref": source_ref,
            "triggered_at_utc": OBSERVED,
        },
        "operation_created": True,
        "trigger_created": True,
        "broker_mutations_performed": False,
        "automatic_retry_enabled": False,
    }


def daily_risk_payload(*, status: str = "HALTED", cleanup: str = "COMPLETE") -> dict:
    return {
        "daily_risk_state": {
            "status": status,
            "cleanup_status": cleanup,
            "pnl_ready": True,
            "total_pnl": 500.0,
            "target_pnl": 500.0,
        },
        "execution_readiness": {
            "status": "BLOCKED",
            "command_intake_enabled": False,
            "broker_actions_enabled": True,
        },
        "broker_mutations_performed": False,
    }


def successful_executor() -> FakeExecutor:
    return FakeExecutor(
        [
            synthetic_trigger_payload(),
            daily_halt_trigger_payload(),
            paper_payload(action="CANCEL_TAKE_PROFIT", mutation=True),
            paper_payload(action="CANCEL_STOP", mutation=True),
            paper_payload(
                action="SUBMIT_MARKET_CLOSE",
                mutation=True,
                operation_state="RECONCILING",
                attempt_state="FILLED",
            ),
            paper_payload(
                action="WAIT_FOR_FLAT",
                mutation=False,
                operation_state="SUCCEEDED",
                attempt_state="FILLED",
                episode_closed=True,
            ),
            paper_payload(
                action="NONE",
                mutation=False,
                operation_state="SUCCEEDED",
                attempt_state="FILLED",
                episode_closed=True,
            ),
            daily_risk_payload(),
            daily_risk_payload(),
        ]
    )


def successful_states():
    return [
        liquidation_state(),
        liquidation_state(next_action="CANCEL_STOP", exposed=1),
        liquidation_state(
            operation_state="PREPARING",
            next_action="SUBMIT_MARKET_CLOSE",
            exposed=0,
        ),
        liquidation_state(
            operation_state="RECONCILING",
            next_action="WAIT_FOR_FLAT",
            attempt_state="FILLED",
            exposed=0,
        ),
        closed_state(),
        closed_state(),
    ]


class PaperDailyHaltAcceptanceRunnerTest(unittest.TestCase):
    def test_trigger_liquidation_and_halted_state_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "source-summary.json"
            write_source_summary(summary)
            artifacts = PaperAcceptanceArtifactStore(root / "artifacts")
            executor = successful_executor()
            runner = PaperDailyHaltAcceptanceRunner(
                policy=liquidation_policy(root, summary),
                drill_id="paper-daily-halt-runner-test",
                market_database=root / "market.sqlite3",
                command_executor=executor,
                state_source=FakeStateSource(successful_states()),
                artifacts=artifacts,
                daily_risk_max_invocations=2,
                daily_risk_poll_seconds=0.0,
                clock=lambda: CLOCK,
                sleeper=lambda _seconds: None,
            )
            result = runner.run()
            payload = result.to_dict()
            self.assertEqual(payload["scenario"], "DAILY_HALT")
            self.assertTrue(payload["daily_halt_sticky"])
            self.assertEqual(payload["broker_mutation_count"], 3)
            self.assertEqual(payload["final_daily_risk_state"]["status"], "HALTED")
            self.assertEqual(
                payload["final_daily_risk_state"]["cleanup_status"],
                "COMPLETE",
            )
            self.assertFalse(
                payload["final_execution_readiness"]["command_intake_enabled"]
            )
            self.assertEqual(executor.calls[0][0], "daily-halt-synthetic-trigger")
            self.assertEqual(executor.calls[1][0], "liquidation-request")
            self.assertEqual(executor.calls[-1][0], "daily-risk-halted-idempotency")
            self.assertTrue((artifacts.directory / "summary.json").is_file())
            self.assertEqual(executor.payloads, [])

    def test_non_sticky_post_liquidation_state_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "source-summary.json"
            write_source_summary(summary)
            executor = successful_executor()
            executor.payloads[-2] = daily_risk_payload(
                status="MONITORING",
                cleanup="NOT_REQUIRED",
            )
            runner = PaperDailyHaltAcceptanceRunner(
                policy=liquidation_policy(root, summary),
                drill_id="paper-daily-halt-runner-test",
                market_database=root / "market.sqlite3",
                command_executor=executor,
                state_source=FakeStateSource(successful_states()),
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                daily_risk_max_invocations=1,
                daily_risk_poll_seconds=0.0,
                clock=lambda: CLOCK,
                sleeper=lambda _seconds: None,
            )
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "sticky halt was lost",
            ):
                runner.run()

    def test_synthetic_trigger_must_declare_real_owned_fill_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary = root / "source-summary.json"
            write_source_summary(summary)
            payload = synthetic_trigger_payload()
            payload["real_owned_fill_evidence_only"] = False
            executor = FakeExecutor([payload])
            runner = PaperDailyHaltAcceptanceRunner(
                policy=liquidation_policy(root, summary),
                drill_id="paper-daily-halt-runner-test",
                market_database=root / "market.sqlite3",
                command_executor=executor,
                state_source=FakeStateSource([liquidation_state()]),
                artifacts=PaperAcceptanceArtifactStore(root / "artifacts"),
                clock=lambda: CLOCK,
                sleeper=lambda _seconds: None,
            )
            with self.assertRaisesRegex(
                PaperLiquidationAcceptanceError,
                "real owned fill evidence",
            ):
                runner.run()
            self.assertEqual(len(executor.calls), 1)


if __name__ == "__main__":
    unittest.main()
