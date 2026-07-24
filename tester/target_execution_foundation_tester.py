from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ibmd.execution.adapters import (
    ExecutionStoreError,
    SQLiteExecutionDecisionReader,
    SQLiteExecutionStore,
)
from ibmd.execution.domain import (
    ExecutionFoundationFixtureV1,
    ExecutionFoundationPolicyV1,
    admit_strategy_command,
)
from ibmd.foundation.identity import new_id
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
    StrategyCommandRequestV1,
)
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionCommandState,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    PositionContractV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_MANIFEST = ROOT / "migrations" / "execution.v1.json"
ACCOUNT = "U0000000"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "shadow-mnq-account1"
INSTRUMENT = "MNQ"
POLICY_HASH = "a" * 64
OBSERVED = "2026-07-24T10:00:05Z"


def apply_schema(path: Path) -> None:
    store_name, migrations = load_migration_manifest(MIGRATION_MANIFEST)
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()


def policy() -> ExecutionFoundationPolicyV1:
    return ExecutionFoundationPolicyV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        policy_hash=POLICY_HASH,
    )


def command(
    *,
    kind: StrategyCommandKind = StrategyCommandKind.OPEN,
    side: DesiredTargetSide = DesiredTargetSide.LONG,
    expires_at: str = "2026-07-24T10:00:30Z",
) -> StrategyCommandRequestV1:
    return StrategyCommandRequestV1(
        command_id=new_id("strategy_command"),
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        source_signal_id=new_id("signal_event"),
        desired_target_side=side,
        desired_target_quantity=1,
        command_kind=kind,
        reason="test",
        created_at_utc="2026-07-24T10:00:00Z",
        expires_at_utc=expires_at,
        policy_hash=POLICY_HASH,
    )


def readiness(
    *,
    status: ExecutionReadinessStatus = ExecutionReadinessStatus.READY,
    command_intake: bool = True,
    broker_actions: bool = False,
    reconciliation: bool = True,
    clock: bool = True,
    reasons: tuple[str, ...] = (),
) -> ExecutionReadinessV1:
    return ExecutionReadinessV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=status,
        command_intake_enabled=command_intake,
        broker_actions_enabled=broker_actions,
        reconciliation_complete=reconciliation,
        clock_healthy=clock,
        blocking_reasons=reasons,
        updated_at_utc=OBSERVED,
    )


def flat_position() -> StrategyPositionV1:
    return StrategyPositionV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_episode_id=None,
        side=StrategyPositionSide.FLAT,
        quantity=0,
        contracts=(),
        projection_status=StrategyPositionStatus.FLAT,
        broker_snapshot_id=new_id("position_snapshot"),
        updated_at_utc=OBSERVED,
        source_freshness_seconds=1.0,
    )


def open_position(
    *,
    side: StrategyPositionSide = StrategyPositionSide.SHORT,
    active: bool = True,
) -> StrategyPositionV1:
    signed = 1 if side == StrategyPositionSide.LONG else -1
    return StrategyPositionV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_episode_id=new_id("position_episode"),
        side=side,
        quantity=1,
        contracts=(
            PositionContractV1(
                con_id=793356225,
                local_symbol="MNQU6",
                signed_quantity=signed,
                contract_is_active=active,
            ),
        ),
        projection_status=StrategyPositionStatus.OPEN,
        broker_snapshot_id=new_id("position_snapshot"),
        updated_at_utc=OBSERVED,
        source_freshness_seconds=1.0,
    )


def risk(
    *,
    status: DailyRiskStatus = DailyRiskStatus.MONITORING,
    pnl_ready: bool = True,
) -> DailyRiskStateV1:
    values = (10.0, 2.0, 12.0) if pnl_ready else (None, None, None)
    cleanup = (
        DailyRiskCleanupStatus.COMPLETE
        if status == DailyRiskStatus.HALTED
        else DailyRiskCleanupStatus.NOT_REQUIRED
    )
    return DailyRiskStateV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        trading_day="2026-07-24",
        status=status,
        realized_pnl=values[0],
        unrealized_pnl=values[1],
        total_pnl=values[2],
        target_pnl=500.0,
        pnl_ready=pnl_ready,
        cleanup_status=cleanup,
        updated_at_utc=OBSERVED,
    )


def fixture(
    *,
    position: StrategyPositionV1 | None = None,
    readiness_value: ExecutionReadinessV1 | None = None,
    risk_value: DailyRiskStateV1 | None = None,
    observed: str = OBSERVED,
) -> ExecutionFoundationFixtureV1:
    return ExecutionFoundationFixtureV1(
        observed_at_utc=observed,
        readiness=readiness_value or readiness(),
        position=position or flat_position(),
        daily_risk=risk_value or risk(),
    )


class ExecutionPublicContractTest(unittest.TestCase):
    def test_fixture_round_trip_and_broker_gateway_may_remain_disabled(self) -> None:
        value = fixture()
        restored = ExecutionFoundationFixtureV1.from_dict(value.to_dict())
        self.assertEqual(restored, value)
        self.assertFalse(restored.readiness.broker_actions_enabled)
        self.assertTrue(restored.readiness.command_intake_enabled)

    def test_open_position_contract_quantity_must_match_projection(self) -> None:
        payload = open_position().to_dict()
        payload["contracts"][0]["signed_quantity"] = 2
        with self.assertRaisesRegex(ValueError, "do not match"):
            StrategyPositionV1.from_dict(payload)


class ExecutionAdmissionTest(unittest.TestCase):
    def test_flat_open_is_admitted_without_enabling_broker_actions(self) -> None:
        result = admit_strategy_command(
            command=command(),
            policy=policy(),
            fixture=fixture(),
        )
        self.assertEqual(
            result.command_state.state,
            ExecutionCommandState.ADMITTED,
        )
        self.assertIsNone(result.command_state.blocking_reason)
        self.assertFalse(result.fixture_payload["readiness"]["broker_actions_enabled"])

    def test_reverse_requires_opposite_active_position(self) -> None:
        reverse = command(
            kind=StrategyCommandKind.REVERSE,
            side=DesiredTargetSide.LONG,
        )
        accepted = admit_strategy_command(
            command=reverse,
            policy=policy(),
            fixture=fixture(position=open_position(side=StrategyPositionSide.SHORT)),
        )
        self.assertEqual(accepted.command_state.state, ExecutionCommandState.ADMITTED)

        rejected = admit_strategy_command(
            command=reverse,
            policy=policy(),
            fixture=fixture(
                position=open_position(
                    side=StrategyPositionSide.SHORT,
                    active=False,
                )
            ),
        )
        self.assertEqual(rejected.command_state.state, ExecutionCommandState.REJECTED)
        self.assertEqual(
            rejected.command_state.blocking_reason,
            "non_active_contract_requires_liquidation",
        )

    def test_expiry_readiness_and_daily_halt_fail_closed(self) -> None:
        expired = admit_strategy_command(
            command=command(expires_at="2026-07-24T10:00:05Z"),
            policy=policy(),
            fixture=fixture(),
        )
        self.assertEqual(expired.command_state.blocking_reason, "command_expired")

        blocked = admit_strategy_command(
            command=command(),
            policy=policy(),
            fixture=fixture(
                readiness_value=readiness(
                    status=ExecutionReadinessStatus.BLOCKED,
                    command_intake=False,
                    reconciliation=False,
                    clock=False,
                    reasons=("reconciliation incomplete",),
                )
            ),
        )
        self.assertEqual(blocked.command_state.blocking_reason, "execution_blocked")

        halted = admit_strategy_command(
            command=command(),
            policy=policy(),
            fixture=fixture(risk_value=risk(status=DailyRiskStatus.HALTED)),
        )
        self.assertEqual(halted.command_state.blocking_reason, "daily_risk_halted")


class ExecutionStoreTest(unittest.TestCase):
    def test_admission_is_idempotent_and_records_two_local_transitions(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            store = SQLiteExecutionStore(database)
            store.validate_schema()
            admission = admit_strategy_command(
                command=command(),
                policy=policy(),
                fixture=fixture(),
            )
            first = store.publish_admission(admission)
            second = store.publish_admission(admission)
            self.assertEqual(first, second)
            self.assertEqual(
                store.read_transition_states(first.command_id),
                ("RECEIVED", "ADMITTED"),
            )

            connection = sqlite3.connect(database)
            try:
                command_count = connection.execute(
                    "SELECT COUNT(*) FROM internal_execution_command_states"
                ).fetchone()[0]
                transition_count = connection.execute(
                    "SELECT COUNT(*) FROM internal_execution_command_transitions"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(command_count, 1)
            self.assertEqual(transition_count, 2)

    def test_conflicting_fixture_for_same_command_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_schema(database)
            store = SQLiteExecutionStore(database)
            value = command()
            first = admit_strategy_command(
                command=value,
                policy=policy(),
                fixture=fixture(),
            )
            store.publish_admission(first)
            conflicting = admit_strategy_command(
                command=value,
                policy=policy(),
                fixture=fixture(
                    risk_value=DailyRiskStateV1(
                        account_id=ACCOUNT,
                        strategy_id=STRATEGY,
                        deployment_id=DEPLOYMENT,
                        trading_day="2026-07-24",
                        status=DailyRiskStatus.MONITORING,
                        realized_pnl=11.0,
                        unrealized_pnl=2.0,
                        total_pnl=13.0,
                        target_pnl=500.0,
                        pnl_ready=True,
                        cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
                        updated_at_utc=OBSERVED,
                    )
                ),
            )
            with self.assertRaisesRegex(ExecutionStoreError, "conflicting"):
                store.publish_admission(conflicting)


class ExecutionDecisionReaderTest(unittest.TestCase):
    def test_reader_uses_public_decision_view_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "decision.sqlite3"
            value = command()
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE source(command_id TEXT PRIMARY KEY, payload_json TEXT)"
                )
                connection.execute(
                    "CREATE VIEW public_strategy_command_requests_v1 AS "
                    "SELECT command_id, payload_json FROM source"
                )
                connection.execute(
                    "INSERT INTO source(command_id, payload_json) VALUES (?, ?)",
                    (value.command_id, json.dumps(value.to_dict())),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteExecutionDecisionReader(database)
            reader.validate_schema()
            self.assertEqual(reader.read_command(value.command_id), value)


if __name__ == "__main__":
    unittest.main()
