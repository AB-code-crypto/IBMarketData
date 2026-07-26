from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ibmd.decision.adapters.sqlite_runtime import (
    DecisionRuntimeStateIncomplete,
    SQLiteDecisionRuntimeReader,
)
from ibmd.decision.adapters.sqlite_signal import SQLiteDecisionSignalReader
from ibmd.decision.adapters.sqlite_store import SQLiteDecisionStore
from ibmd.decision.application.runtime import (
    ContinuousDecisionService,
    decision_runtime_payload,
)
from ibmd.decision.application.service import DecisionShadowService
from ibmd.execution.adapters.sqlite_store import SQLiteExecutionStore
from ibmd.execution.domain.model import ExecutionFoundationFixtureV1
from ibmd.foundation.identity import new_id
from ibmd.public_contracts.decision import DecisionOutcome
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    PositionContractV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.signal.adapters import SQLiteSignalStore
from tester.target_decision_tester import (
    ACCOUNT_ID,
    DECISION_MANIFEST,
    DEPLOYMENT_ID,
    INSTRUMENT_ID,
    POLICY_HASH,
    SIGNAL_MANIFEST,
    STRATEGY_ID,
    apply_schema,
    make_calculation,
    make_policy,
    make_signal,
)

ROOT = Path(__file__).resolve().parents[1]
EXECUTION_MANIFEST = ROOT / "migrations" / "execution.v1.json"
T0 = "2026-07-23T10:00:05Z"


def ready_fixture(
    *,
    position: StrategyPositionV1 | None = None,
    broker_actions_enabled: bool = True,
) -> ExecutionFoundationFixtureV1:
    observed = T0
    position = position or StrategyPositionV1(
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        deployment_id=DEPLOYMENT_ID,
        instrument_id=INSTRUMENT_ID,
        position_episode_id=None,
        side=StrategyPositionSide.FLAT,
        quantity=0,
        contracts=(),
        projection_status=StrategyPositionStatus.FLAT,
        broker_snapshot_id=None,
        updated_at_utc=observed,
        source_freshness_seconds=1.0,
    )
    return ExecutionFoundationFixtureV1(
        observed_at_utc=observed,
        readiness=ExecutionReadinessV1(
            account_id=ACCOUNT_ID,
            strategy_id=STRATEGY_ID,
            deployment_id=DEPLOYMENT_ID,
            instrument_id=INSTRUMENT_ID,
            status=ExecutionReadinessStatus.READY,
            command_intake_enabled=True,
            broker_actions_enabled=broker_actions_enabled,
            reconciliation_complete=True,
            clock_healthy=True,
            blocking_reasons=(),
            updated_at_utc=observed,
        ),
        position=position,
        daily_risk=DailyRiskStateV1(
            account_id=ACCOUNT_ID,
            strategy_id=STRATEGY_ID,
            deployment_id=DEPLOYMENT_ID,
            trading_day="2026-07-23",
            status=DailyRiskStatus.MONITORING,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            total_pnl=0.0,
            target_pnl=500.0,
            pnl_ready=True,
            cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
            updated_at_utc=observed,
        ),
    )


class DecisionRuntimeHarness:
    def __init__(self, root: Path) -> None:
        self.signal_database = root / "signal.sqlite3"
        self.decision_database = root / "decision.sqlite3"
        self.execution_database = root / "execution.sqlite3"
        apply_schema(self.signal_database, SIGNAL_MANIFEST)
        apply_schema(self.decision_database, DECISION_MANIFEST)
        apply_schema(self.execution_database, EXECUTION_MANIFEST)
        self.signal_store = SQLiteSignalStore(self.signal_database)
        self.decision_store = SQLiteDecisionStore(self.decision_database)
        self.execution_store = SQLiteExecutionStore(self.execution_database)
        self.runtime_reader = SQLiteDecisionRuntimeReader(
            signal_database=self.signal_database,
            decision_database=self.decision_database,
            execution_database=self.execution_database,
        )
        decision_service = DecisionShadowService(
            policy=make_policy(),
            signal_source=SQLiteDecisionSignalReader(self.signal_database),
            repository=self.decision_store,
        )
        self.service = ContinuousDecisionService(
            decision_service=decision_service,
            runtime_source=self.runtime_reader,
            signal_configuration_hash=POLICY_HASH,
        )

    def publish_signal(self, *, created_at_utc: str):
        event = make_signal(created_at_utc=created_at_utc)
        self.signal_store.publish(make_calculation(event))
        return event


class DecisionRuntimeTest(unittest.TestCase):
    def test_runtime_processes_each_signal_once_and_blocks_duplicate_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = DecisionRuntimeHarness(Path(directory))
            harness.execution_store.publish_fixture(ready_fixture())
            harness.service.validate_dependencies()

            first_event = harness.publish_signal(
                created_at_utc="2026-07-23T10:00:01Z"
            )
            first = harness.service.run_once(observed_at_utc=T0)
            self.assertTrue(first.processed)
            self.assertEqual(first.event.event_id, first_event.event_id)
            self.assertEqual(first.evaluation.record.outcome, DecisionOutcome.COMMAND)
            self.assertIsNotNone(first.evaluation.command)

            idle = harness.service.run_once(
                observed_at_utc="2026-07-23T10:00:06Z"
            )
            self.assertFalse(idle.processed)
            self.assertFalse(decision_runtime_payload(idle)["processed"])

            second_event = harness.publish_signal(
                created_at_utc="2026-07-23T10:00:10Z"
            )
            second = harness.service.run_once(
                observed_at_utc="2026-07-23T10:00:15Z"
            )
            self.assertEqual(second.event.event_id, second_event.event_id)
            self.assertEqual(second.evaluation.record.outcome, DecisionOutcome.NO_ACTION)
            self.assertEqual(
                second.evaluation.record.reason_code,
                "unresolved_command_exists",
            )
            self.assertIsNone(second.evaluation.command)

    def test_missing_execution_state_blocks_without_consuming_signal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = DecisionRuntimeHarness(Path(directory))
            event = harness.publish_signal(
                created_at_utc="2026-07-23T10:00:01Z"
            )
            with self.assertRaisesRegex(
                DecisionRuntimeStateIncomplete,
                "execution public state is incomplete",
            ):
                harness.service.run_once(observed_at_utc=T0)
            pending = harness.runtime_reader.read_next_pending_event(
                strategy_id=STRATEGY_ID,
                strategy_version=1,
                deployment_id=DEPLOYMENT_ID,
                instrument_id=INSTRUMENT_ID,
                configuration_hash=POLICY_HASH,
                policy_hash=POLICY_HASH,
            )
            self.assertEqual(pending.event_id, event.event_id)

    def test_fixture_maps_open_contract_and_broker_action_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = DecisionRuntimeHarness(Path(directory))
            position = StrategyPositionV1(
                account_id=ACCOUNT_ID,
                strategy_id=STRATEGY_ID,
                deployment_id=DEPLOYMENT_ID,
                instrument_id=INSTRUMENT_ID,
                position_episode_id=new_id("position_episode"),
                side=StrategyPositionSide.LONG,
                quantity=1,
                contracts=(
                    PositionContractV1(
                        con_id=793356225,
                        local_symbol="MNQU6",
                        signed_quantity=1,
                        contract_is_active=False,
                    ),
                ),
                projection_status=StrategyPositionStatus.OPEN,
                broker_snapshot_id=new_id("position_snapshot"),
                updated_at_utc=T0,
                source_freshness_seconds=1.0,
            )
            harness.execution_store.publish_fixture(
                ready_fixture(
                    position=position,
                    broker_actions_enabled=False,
                )
            )
            fixture = harness.runtime_reader.read_fixture(
                account_id=ACCOUNT_ID,
                strategy_id=STRATEGY_ID,
                strategy_version=1,
                deployment_id=DEPLOYMENT_ID,
                instrument_id=INSTRUMENT_ID,
                observed_at_utc=T0,
            )
            self.assertEqual(fixture.position.side.value, "LONG")
            self.assertFalse(fixture.position.contract_is_active)
            self.assertFalse(fixture.execution_ready)
            self.assertIn("execution_readiness", fixture.blocking_reason)

    def test_signal_from_another_configuration_is_not_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harness = DecisionRuntimeHarness(Path(directory))
            harness.execution_store.publish_fixture(ready_fixture())
            event = replace(
                make_signal(created_at_utc="2026-07-23T10:00:01Z"),
                configuration_hash="b" * 64,
            )
            harness.signal_store.publish(make_calculation(event))
            result = harness.service.run_once(observed_at_utc=T0)
            self.assertFalse(result.processed)


if __name__ == "__main__":
    unittest.main()
