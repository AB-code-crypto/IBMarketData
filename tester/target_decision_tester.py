from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.decision import (
    DailyRiskStatus,
    DecisionPolicyV1,
    DecisionShadowService,
    ExecutionDecisionFixtureV1,
    PositionProjectionStatus,
    PositionSide,
    StrategyPositionFixtureV1,
    evaluate_signal,
)
from ibmd.decision.adapters import (
    SQLiteDecisionSignalReader,
    SQLiteDecisionStore,
)
from ibmd.foundation.identity import new_id
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.public_contracts.decision import (
    DecisionOutcome,
    StrategyCommandKind,
)
from ibmd.public_contracts.signal import (
    SignalCalculationStatus,
    SignalCalculationV1,
    SignalDirection,
    SignalEventV1,
)
from ibmd.signal.adapters import SQLiteSignalStore

ACCOUNT_ID = "U0000000"
STRATEGY_ID = "IBMarketData.rolling"
DEPLOYMENT_ID = "shadow-mnq-account1"
INSTRUMENT_ID = "MNQ"
CON_ID = 793356225
LOCAL_SYMBOL = "MNQU6"
POLICY_HASH = "a" * 64
SIGNAL_MANIFEST = ROOT / "migrations" / "signal.v1.json"
DECISION_MANIFEST = ROOT / "migrations" / "decision.v1.json"


def apply_schema(path: Path, manifest: Path) -> None:
    store_name, migrations = load_migration_manifest(manifest)
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="0.1.0-test",
    ).apply()


def make_signal(
    *,
    direction: SignalDirection = SignalDirection.LONG,
    created_at_utc: str = "2026-07-23T10:00:01Z",
) -> SignalEventV1:
    calculation_id = new_id("signal_calculation")
    return SignalEventV1(
        event_id=new_id("signal_event"),
        calculation_id=calculation_id,
        strategy_id=STRATEGY_ID,
        strategy_version=1,
        configuration_hash=POLICY_HASH,
        instrument_id=INSTRUMENT_ID,
        source_bar_id=new_id("market_bar"),
        source_bar_revision=1,
        source_con_id=CON_ID,
        source_local_symbol=LOCAL_SYMBOL,
        signal_bar_utc="2026-07-23T10:00:00Z",
        created_at_utc=created_at_utc,
        direction=direction,
        entry_price=20_000.25,
        best_pearson=0.9,
        candidate_score_best=0.8,
        potential_end_delta_points=(40.0 if direction == SignalDirection.LONG else -40.0),
        potential_max_profit_points=50.0,
        potential_max_drawdown_points=-10.0,
        potential_used=9,
    )


def make_calculation(event: SignalEventV1) -> SignalCalculationV1:
    return SignalCalculationV1(
        calculation_id=event.calculation_id,
        strategy_id=event.strategy_id,
        strategy_version=event.strategy_version,
        configuration_hash=event.configuration_hash,
        instrument_id=event.instrument_id,
        source_bar_id=event.source_bar_id,
        source_bar_revision=event.source_bar_revision,
        source_con_id=event.source_con_id,
        source_local_symbol=event.source_local_symbol,
        signal_bar_utc=event.signal_bar_utc,
        calculated_at_utc=event.created_at_utc,
        status=SignalCalculationStatus.SIGNAL,
        entry_price=event.entry_price,
        raw_candidate_count=10,
        valid_pattern_count=9,
        pearson_pass_count=8,
        minmax_pass_count=7,
        skipped_pattern_count=1,
        best_raw_pearson=0.95,
        best_signal_pearson=event.best_pearson,
        best_candidate_score=event.candidate_score_best,
        potential_direction=event.direction,
        potential_end_delta_points=event.potential_end_delta_points,
        potential_max_profit_points=event.potential_max_profit_points,
        potential_max_drawdown_points=event.potential_max_drawdown_points,
        potential_used=event.potential_used,
        reason=None,
        event=event,
    )


def make_policy() -> DecisionPolicyV1:
    return DecisionPolicyV1(
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        strategy_version=1,
        deployment_id=DEPLOYMENT_ID,
        instrument_id=INSTRUMENT_ID,
        target_quantity=1,
        max_signal_age_seconds=30,
        policy_hash=POLICY_HASH,
    )


def make_position(
    *,
    status: PositionProjectionStatus = PositionProjectionStatus.FLAT,
    side: PositionSide = PositionSide.FLAT,
    quantity: int = 0,
    contract_is_active: bool | None = None,
) -> StrategyPositionFixtureV1:
    return StrategyPositionFixtureV1(
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        deployment_id=DEPLOYMENT_ID,
        instrument_id=INSTRUMENT_ID,
        projection_status=status,
        side=side,
        quantity=quantity,
        contract_is_active=contract_is_active,
    )


def make_fixture(
    *,
    position: StrategyPositionFixtureV1 | None = None,
    observed_at_utc: str = "2026-07-23T10:00:05Z",
    execution_ready: bool = True,
    execution_clock_healthy: bool = True,
    pnl_reconciliation_ready: bool = True,
    unresolved_command: bool = False,
    daily_risk_status: DailyRiskStatus = DailyRiskStatus.MONITORING,
    blocking_reason: str | None = None,
) -> ExecutionDecisionFixtureV1:
    return ExecutionDecisionFixtureV1(
        observed_at_utc=observed_at_utc,
        execution_ready=execution_ready,
        execution_clock_healthy=execution_clock_healthy,
        pnl_reconciliation_ready=pnl_reconciliation_ready,
        unresolved_command=unresolved_command,
        daily_risk_status=daily_risk_status,
        blocking_reason=blocking_reason,
        position=position or make_position(),
    )


class DecisionDomainTest(unittest.TestCase):
    def test_flat_signal_creates_stable_open_command(self) -> None:
        signal = make_signal()
        fixture = make_fixture()
        first = evaluate_signal(
            signal=signal,
            fixture=fixture,
            policy=make_policy(),
        )
        second = evaluate_signal(
            signal=signal,
            fixture=fixture,
            policy=make_policy(),
        )
        self.assertEqual(first, second)
        self.assertEqual(first.record.outcome, DecisionOutcome.COMMAND)
        self.assertEqual(first.command.command_kind, StrategyCommandKind.OPEN)
        self.assertEqual(first.command.desired_target_side.value, "LONG")
        self.assertEqual(first.command.desired_target_quantity, 1)

    def test_same_side_is_no_action_and_opposite_active_reverses(self) -> None:
        signal = make_signal(direction=SignalDirection.LONG)
        same = evaluate_signal(
            signal=signal,
            fixture=make_fixture(
                position=make_position(
                    status=PositionProjectionStatus.OPEN,
                    side=PositionSide.LONG,
                    quantity=1,
                    contract_is_active=True,
                )
            ),
            policy=make_policy(),
        )
        self.assertEqual(same.record.outcome, DecisionOutcome.NO_ACTION)
        self.assertEqual(same.record.reason_code, "already_at_target_side")
        self.assertIsNone(same.command)

        reverse = evaluate_signal(
            signal=signal,
            fixture=make_fixture(
                position=make_position(
                    status=PositionProjectionStatus.OPEN,
                    side=PositionSide.SHORT,
                    quantity=2,
                    contract_is_active=True,
                )
            ),
            policy=make_policy(),
        )
        self.assertEqual(reverse.record.outcome, DecisionOutcome.COMMAND)
        self.assertEqual(reverse.command.command_kind, StrategyCommandKind.REVERSE)
        self.assertEqual(reverse.command.desired_target_quantity, 1)

    def test_non_active_position_and_guards_reject_without_forced_close(self) -> None:
        signal = make_signal()
        old_contract = evaluate_signal(
            signal=signal,
            fixture=make_fixture(
                position=make_position(
                    status=PositionProjectionStatus.OPEN,
                    side=PositionSide.SHORT,
                    quantity=1,
                    contract_is_active=False,
                )
            ),
            policy=make_policy(),
        )
        self.assertEqual(old_contract.record.outcome, DecisionOutcome.REJECTED)
        self.assertEqual(
            old_contract.record.reason_code,
            "non_active_contract_requires_execution_liquidation",
        )
        self.assertIsNone(old_contract.command)

        halted = evaluate_signal(
            signal=signal,
            fixture=make_fixture(daily_risk_status=DailyRiskStatus.HALTED),
            policy=make_policy(),
        )
        self.assertEqual(halted.record.reason_code, "daily_risk_halted")
        self.assertIsNone(halted.command)
        self.assertEqual(
            {item.value for item in StrategyCommandKind},
            {"OPEN", "REVERSE"},
        )

    def test_unknown_projection_unresolved_and_expired_are_explicit(self) -> None:
        signal = make_signal()
        unknown = evaluate_signal(
            signal=signal,
            fixture=make_fixture(
                position=make_position(
                    status=PositionProjectionStatus.UNKNOWN,
                    side=PositionSide.UNKNOWN,
                    quantity=0,
                )
            ),
            policy=make_policy(),
        )
        self.assertEqual(unknown.record.outcome, DecisionOutcome.REJECTED)
        self.assertEqual(unknown.record.reason_code, "position_unknown")

        unresolved = evaluate_signal(
            signal=signal,
            fixture=make_fixture(unresolved_command=True),
            policy=make_policy(),
        )
        self.assertEqual(unresolved.record.outcome, DecisionOutcome.NO_ACTION)
        self.assertEqual(
            unresolved.record.reason_code,
            "unresolved_command_exists",
        )

        expired_at = datetime.fromisoformat(
            signal.created_at_utc.replace("Z", "+00:00")
        ) + timedelta(seconds=30)
        expired = evaluate_signal(
            signal=signal,
            fixture=make_fixture(
                observed_at_utc=expired_at.isoformat().replace("+00:00", "Z")
            ),
            policy=make_policy(),
        )
        self.assertEqual(expired.record.outcome, DecisionOutcome.REJECTED)
        self.assertEqual(expired.record.reason_code, "signal_expired")

    def test_fixture_rejects_string_boolean(self) -> None:
        payload = make_fixture().to_dict()
        payload["execution_ready"] = "true"
        with self.assertRaisesRegex(Exception, "must be a boolean"):
            ExecutionDecisionFixtureV1.from_dict(payload)


class DecisionStoreIntegrationTest(unittest.TestCase):
    def test_signal_reader_service_and_store_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            signal_db = root / "signal.sqlite3"
            decision_db = root / "decision.sqlite3"
            apply_schema(signal_db, SIGNAL_MANIFEST)
            apply_schema(decision_db, DECISION_MANIFEST)

            signal = make_signal()
            SQLiteSignalStore(signal_db).publish(make_calculation(signal))
            service = DecisionShadowService(
                policy=make_policy(),
                signal_source=SQLiteDecisionSignalReader(signal_db),
                repository=SQLiteDecisionStore(decision_db),
            )
            service.validate_dependencies()
            first = service.evaluate_event(
                event_id=signal.event_id,
                fixture=make_fixture(),
            )
            second = service.evaluate_event(
                event_id=signal.event_id,
                fixture=make_fixture(),
            )
            self.assertEqual(first, second)

            connection = sqlite3.connect(decision_db)
            try:
                decision_count = connection.execute(
                    "SELECT COUNT(*) FROM internal_decision_records"
                ).fetchone()[0]
                command_count = connection.execute(
                    "SELECT COUNT(*) FROM internal_strategy_command_requests"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(decision_count, 1)
            self.assertEqual(command_count, 1)

    def test_no_action_persists_without_command(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "decision.sqlite3"
            apply_schema(database, DECISION_MANIFEST)
            evaluation = evaluate_signal(
                signal=make_signal(),
                fixture=make_fixture(
                    position=make_position(
                        status=PositionProjectionStatus.OPEN,
                        side=PositionSide.LONG,
                        quantity=1,
                        contract_is_active=True,
                    )
                ),
                policy=make_policy(),
            )
            stored = SQLiteDecisionStore(database).publish(evaluation)
            self.assertEqual(stored.record.outcome, DecisionOutcome.NO_ACTION)
            connection = sqlite3.connect(database)
            try:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM internal_decision_records"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM internal_strategy_command_requests"
                    ).fetchone()[0],
                    0,
                )
            finally:
                connection.close()


class DecisionEntrypointTest(unittest.TestCase):
    def test_help_does_not_require_environment(self) -> None:
        environment = dict(os.environ)
        for key in tuple(environment):
            if key.startswith("IBMD_") or key.startswith("IB_"):
                environment.pop(key, None)
        result = subprocess.run(
            [sys.executable, str(ROOT / "apps" / "run_decision_v2.py"), "--help"],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("never connects to IB", result.stdout)

    def test_validate_store_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            data_root = Path(temp)
            signal_db = data_root / "signal" / "signal.sqlite3"
            decision_db = data_root / "decision" / "decision.sqlite3"
            apply_schema(signal_db, SIGNAL_MANIFEST)
            apply_schema(decision_db, DECISION_MANIFEST)
            environment = dict(os.environ)
            environment.update(
                {
                    "IBMD_DEPLOYMENT_ID": DEPLOYMENT_ID,
                    "IBMD_DATA_ROOT": str(data_root),
                    "IBMD_APPLICATION_VERSION": "test",
                    "IB_HOST": "127.0.0.1",
                    "IB_PORT": "7497",
                    "IB_CLIENT_ID": "200",
                    "IB_ACCOUNT_ID": ACCOUNT_ID,
                }
            )
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "apps" / "run_decision_v2.py"),
                    "--signal-database",
                    str(signal_db),
                    "--decision-database",
                    str(decision_db),
                    "--validate-store-only",
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("decision dependencies are compatible", result.stdout)


if __name__ == "__main__":
    unittest.main()
