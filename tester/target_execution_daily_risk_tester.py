from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ibmd.execution.adapters.sqlite_broker_attempts import SQLiteBrokerAttemptStore
from ibmd.execution.adapters.sqlite_daily_risk_sources import (
    SQLiteDailyRiskExecutionReader,
    SQLiteDailyRiskMarketDataReader,
)
from ibmd.execution.adapters.sqlite_daily_risk_store import SQLiteDailyRiskStore
from ibmd.execution.adapters.sqlite_protection import SQLiteProtectionStore
from ibmd.execution.application.daily_risk import DailyRiskService
from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.execution.domain.daily_risk import (
    DailyRiskFillKind,
    DailyRiskMarketMarkV1,
    DailyRiskOwnedFillV1,
    DailyRiskPolicyV1,
    calculate_daily_risk,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.identity import new_id
from ibmd.operations.migrations import SQLiteMigrationRunner, load_migration_manifest
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerOperationState,
    BrokerOrderAttemptV1,
    BrokerOrderOperationV1,
    BrokerOrderSide,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerCommissionFactV1,
    BrokerFillFactV1,
)
from ibmd.public_contracts.daily_risk import DailyRiskCalculationV1
from ibmd.public_contracts.decision import DesiredTargetSide, StrategyCommandKind
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStatus,
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
)
from tester.target_execution_protection_tester import (
    ACCOUNT,
    DEPLOYMENT,
    INSTRUMENT,
    STRATEGY,
    T0,
    T2,
    T3,
    apply_schema,
    flat_position,
    plan,
)

ROOT = Path(__file__).resolve().parents[1]
T4 = "2026-07-27T10:00:04Z"
T5 = "2026-07-27T10:00:05Z"
TARGET = 500.0


def policy() -> DailyRiskPolicyV1:
    return DailyRiskPolicyV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        timezone_name="Europe/Moscow",
        target_pnl=TARGET,
        contract_multiplier=2.0,
        market_max_age_seconds=60.0,
    )


def ready_readiness(*, at: str = T3) -> ExecutionReadinessV1:
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
        updated_at_utc=at,
    )


def commission(
    exec_id: str,
    *,
    value: float = 1.25,
    realized_pnl: float | None = 0.0,
) -> BrokerCommissionFactV1:
    return BrokerCommissionFactV1(
        exec_id=exec_id,
        commission=value,
        currency="USD",
        realized_pnl=realized_pnl,
        reported_at_utc=T4,
    )


def owned_fill(
    *,
    kind: DailyRiskFillKind = DailyRiskFillKind.STRATEGIC_OPEN,
    exec_id: str = "daily-risk-exec-1",
    price: float = 28_600.0,
    commission_fact: BrokerCommissionFactV1 | None = None,
) -> DailyRiskOwnedFillV1:
    return DailyRiskOwnedFillV1(
        kind=kind,
        fill=BrokerFillFactV1(
            exec_id=exec_id,
            account_id=ACCOUNT,
            order_ref="IBMD:daily-risk:test",
            broker_order_id=7001,
            broker_perm_id=9001,
            client_id=320,
            con_id=793_356_225,
            local_symbol="MNQU6",
            side=BrokerOrderSide.BUY,
            shares=1,
            price=price,
            cumulative_qty=1,
            average_price=price,
            exchange="CME",
            executed_at_utc=T2,
            observed_at_utc=T4,
            commission=commission_fact,
        ),
    )


def mark(price: float, *, age: float = 1.0) -> DailyRiskMarketMarkV1:
    return DailyRiskMarketMarkV1(
        bar_id=new_id("market_bar"),
        instrument_id=INSTRUMENT,
        con_id=793_356_225,
        local_symbol="MNQU6",
        bar_end_utc=T4,
        mid_price=price,
        age_seconds=age,
    )


def calculate_open(
    *,
    mark_price: float,
    current_state=None,
    current_readiness=None,
    commission_fact=None,
):
    value = plan()
    return calculate_daily_risk(
        policy=policy(),
        owned_fills=(
            owned_fill(
                commission_fact=(
                    commission("daily-risk-exec-1")
                    if commission_fact is None
                    else commission_fact
                )
            ),
        ),
        position=value.strategy_position,
        episode=value.episode,
        market_mark=mark(mark_price),
        current_state=current_state,
        current_readiness=current_readiness or ready_readiness(),
        liquidation=None,
        observed_at_utc=T5,
    )


class DailyRiskDomainTest(unittest.TestCase):
    def test_flat_without_fills_is_monitoring(self) -> None:
        update = calculate_daily_risk(
            policy=policy(),
            owned_fills=(),
            position=flat_position(),
            episode=None,
            market_mark=None,
            current_state=None,
            current_readiness=ready_readiness(),
            liquidation=None,
            observed_at_utc=T5,
        )
        self.assertTrue(update.calculation.pnl_ready)
        self.assertEqual(update.calculation.realized_pnl, 0.0)
        self.assertEqual(update.calculation.unrealized_pnl, 0.0)
        self.assertEqual(update.state.status, DailyRiskStatus.MONITORING)
        self.assertEqual(
            update.state.cleanup_status,
            DailyRiskCleanupStatus.NOT_REQUIRED,
        )
        self.assertEqual(
            update.execution_readiness.status,
            ExecutionReadinessStatus.READY,
        )
        self.assertEqual(
            DailyRiskCalculationV1.from_dict(update.calculation.to_dict()),
            update.calculation,
        )

    def test_open_position_uses_opening_commission_and_mid_mark(self) -> None:
        update = calculate_open(mark_price=28_610.0)
        self.assertAlmostEqual(update.calculation.realized_pnl, -1.25)
        self.assertAlmostEqual(update.calculation.unrealized_pnl, 20.0)
        self.assertAlmostEqual(update.calculation.total_pnl, 18.75)
        self.assertEqual(update.state.status, DailyRiskStatus.MONITORING)
        self.assertEqual(
            update.calculation.strategic_exec_ids,
            ("daily-risk-exec-1",),
        )

    def test_missing_commission_is_not_ready_and_blocks_intake(self) -> None:
        value = plan()
        update = calculate_daily_risk(
            policy=policy(),
            owned_fills=(owned_fill(commission_fact=None),),
            position=value.strategy_position,
            episode=value.episode,
            market_mark=mark(28_610.0),
            current_state=None,
            current_readiness=ready_readiness(),
            liquidation=None,
            observed_at_utc=T5,
        )
        self.assertFalse(update.calculation.pnl_ready)
        self.assertEqual(
            update.calculation.missing_commission_exec_ids,
            ("daily-risk-exec-1",),
        )
        self.assertEqual(update.state.status, DailyRiskStatus.NOT_READY)
        self.assertEqual(
            update.execution_readiness.status,
            ExecutionReadinessStatus.BLOCKED,
        )
        self.assertFalse(update.execution_readiness.command_intake_enabled)

    def test_trigger_is_sticky_even_when_mark_retraces(self) -> None:
        triggered = calculate_open(mark_price=28_900.0)
        self.assertEqual(triggered.state.status, DailyRiskStatus.TRIGGERED)
        lower = calculate_open(
            mark_price=28_610.0,
            current_state=triggered.state,
            current_readiness=triggered.execution_readiness,
        )
        self.assertEqual(lower.state.status, DailyRiskStatus.TRIGGERED)
        self.assertEqual(
            lower.state.cleanup_status,
            DailyRiskCleanupStatus.PENDING,
        )

    def test_flat_after_closing_becomes_halted(self) -> None:
        triggered = calculate_open(mark_price=28_900.0)
        closing = replace(
            triggered.state,
            status=DailyRiskStatus.CLOSING,
            cleanup_status=DailyRiskCleanupStatus.PENDING,
            updated_at_utc=T4,
        )
        update = calculate_daily_risk(
            policy=policy(),
            owned_fills=(
                owned_fill(
                    kind=DailyRiskFillKind.PROTECTIVE_EXIT,
                    commission_fact=commission(
                        "daily-risk-exec-1",
                        realized_pnl=600.0,
                    ),
                ),
            ),
            position=flat_position(),
            episode=None,
            market_mark=None,
            current_state=closing,
            current_readiness=ready_readiness(at=T4),
            liquidation=None,
            observed_at_utc=T5,
        )
        self.assertEqual(update.state.status, DailyRiskStatus.HALTED)
        self.assertEqual(
            update.state.cleanup_status,
            DailyRiskCleanupStatus.COMPLETE,
        )
        self.assertEqual(
            update.execution_readiness.status,
            ExecutionReadinessStatus.BLOCKED,
        )


class MemoryExecutionState:
    def __init__(self, position, readiness) -> None:
        self.position = position
        self.readiness = readiness

    def read_position(self, **_scope):
        return self.position

    def read_readiness(self, **_scope):
        return self.readiness


class MemoryEpisodeSource:
    def __init__(self, episode) -> None:
        self.episode = episode

    def read_episode(self, _episode_id):
        return self.episode


class MemoryEvidenceSource:
    def __init__(self, fills=(), liquidation=None) -> None:
        self.fills = fills
        self.liquidation = liquidation

    def read_owned_fills(self, **_scope):
        return self.fills

    def read_latest_liquidation_operation(self, **_scope):
        return self.liquidation


class MemoryMarkSource:
    def __init__(self, value) -> None:
        self.value = value

    def read_latest_mark(self, **_values):
        return self.value


class MemoryRepository:
    def __init__(self) -> None:
        self.state = None
        self.value = None

    def read_latest_state(self, **_scope):
        return self.state

    def publish(self, *, current_state, current_readiness, update):
        if current_state != self.state:
            raise AssertionError("daily-risk state changed")
        self.state = update.state
        self.value = update
        return update


class DailyRiskServiceTest(unittest.TestCase):
    def test_service_reads_sources_and_publishes(self) -> None:
        value = plan()
        repository = MemoryRepository()
        service = DailyRiskService(
            policy=policy(),
            execution_state_source=MemoryExecutionState(
                value.strategy_position,
                ready_readiness(),
            ),
            episode_source=MemoryEpisodeSource(value.episode),
            owned_fill_source=MemoryEvidenceSource(
                fills=(
                    owned_fill(
                        commission_fact=commission("daily-risk-exec-1")
                    ),
                )
            ),
            market_mark_source=MemoryMarkSource(mark(28_610.0)),
            repository=repository,
        )
        run = service.run_once(observed_at_utc=T5)
        self.assertEqual(run.owned_fill_count, 1)
        self.assertEqual(run.update.state.status, DailyRiskStatus.MONITORING)
        self.assertIs(repository.value, run.update)
        self.assertFalse(run.broker_mutations_performed)


def apply_component(database: Path, manifest_name: str) -> None:
    manifest = json.loads(
        (ROOT / "migrations" / manifest_name).read_text(encoding="utf-8")
    )
    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS execution_target_schema_components ("
            "component_name TEXT PRIMARY KEY, "
            "component_version INTEGER NOT NULL CHECK (component_version > 0), "
            "checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL, "
            "application_version TEXT NOT NULL)"
        )
        for statement in manifest["statements"]:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO execution_target_schema_components ("
            "component_name, component_version, checksum, applied_at_utc, "
            "application_version) VALUES (?, ?, ?, ?, ?)",
            (
                manifest["component_name"],
                manifest["component_version"],
                f"test-{manifest['component_name']}",
                T0,
                "test",
            ),
        )
        connection.commit()
    finally:
        connection.close()


def update_readiness(database: Path, readiness: ExecutionReadinessV1) -> None:
    connection = sqlite3.connect(str(database))
    try:
        connection.execute(
            "UPDATE internal_execution_readiness "
            "SET status=?, command_intake_enabled=?, broker_actions_enabled=?, "
            "updated_at_ts=?, updated_at_utc=?, payload_json=? "
            "WHERE account_id=? AND strategy_id=? AND deployment_id=? "
            "AND instrument_id=?",
            (
                readiness.status.value,
                int(readiness.command_intake_enabled),
                int(readiness.broker_actions_enabled),
                int(0),
                readiness.updated_at_utc,
                canonical_json_text(readiness.to_dict()),
                readiness.account_id,
                readiness.strategy_id,
                readiness.deployment_id,
                readiness.instrument_id,
            ),
        )
        connection.commit()
    finally:
        connection.close()


class DailyRiskStoreTest(unittest.TestCase):
    def test_publish_is_atomic_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "execution.sqlite3"
            apply_schema(database)
            apply_component(database, "execution.daily_risk.v1.json")
            value = plan()
            SQLiteProtectionStore(database).publish_plan(value)
            initial_readiness = ready_readiness()
            update_readiness(database, initial_readiness)
            update = calculate_daily_risk(
                policy=policy(),
                owned_fills=(),
                position=flat_position(),
                episode=None,
                market_mark=None,
                current_state=None,
                current_readiness=initial_readiness,
                liquidation=None,
                observed_at_utc=T5,
            )
            store = SQLiteDailyRiskStore(database)
            store.validate_schema()
            first = store.publish(
                current_state=None,
                current_readiness=initial_readiness,
                update=update,
            )
            second = store.publish(
                current_state=first.state,
                current_readiness=first.execution_readiness,
                update=first,
            )
            self.assertEqual(first, second)
            self.assertEqual(
                store.read_latest_state(
                    account_id=ACCOUNT,
                    strategy_id=STRATEGY,
                    deployment_id=DEPLOYMENT,
                ),
                first.state,
            )
            self.assertEqual(
                store.read_calculation(first.calculation.calculation_id),
                first.calculation,
            )
            connection = sqlite3.connect(str(database))
            try:
                counts = connection.execute(
                    "SELECT "
                    "(SELECT COUNT(*) FROM internal_daily_risk_calculations), "
                    "(SELECT COUNT(*) FROM internal_daily_risk_transitions)"
                ).fetchone()
            finally:
                connection.close()
            self.assertEqual(counts, (1, 1))


def command_state(command_id: str) -> ExecutionCommandStateV1:
    return ExecutionCommandStateV1(
        command_id=command_id,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        command_kind=StrategyCommandKind.OPEN,
        desired_target_side=DesiredTargetSide.LONG,
        desired_target_quantity=1,
        state=ExecutionCommandState.ADMITTED,
        requested_qty=1,
        filled_qty=0,
        remaining_qty=1,
        latest_attempt_id=None,
        blocking_reason=None,
        received_at_utc=T0,
        updated_at_utc=T0,
        terminal_at_utc=None,
    )


def succeeded_operation(command_id: str) -> BrokerOperationSnapshot:
    operation_id = new_id("broker_operation")
    attempt_id = new_id("broker_attempt")
    return BrokerOperationSnapshot(
        operation=BrokerOrderOperationV1(
            operation_id=operation_id,
            command_id=command_id,
            account_id=ACCOUNT,
            strategy_id=STRATEGY,
            strategy_version=1,
            deployment_id=DEPLOYMENT,
            instrument_id=INSTRUMENT,
            side=BrokerOrderSide.BUY,
            order_type="MARKET",
            con_id=793_356_225,
            local_symbol="MNQU6",
            requested_qty=1,
            filled_qty=1,
            remaining_qty=0,
            state=BrokerOperationState.SUCCEEDED,
            current_attempt_id=attempt_id,
            current_attempt_no=1,
            created_at_utc=T0,
            updated_at_utc=T2,
            terminal_at_utc=T2,
            blocking_reason=None,
        ),
        attempt=BrokerOrderAttemptV1(
            attempt_id=attempt_id,
            operation_id=operation_id,
            attempt_no=1,
            order_ref=f"IBMD:{operation_id}:1",
            side=BrokerOrderSide.BUY,
            order_type="MARKET",
            con_id=793_356_225,
            local_symbol="MNQU6",
            requested_qty=1,
            filled_qty=1,
            remaining_qty=0,
            state=BrokerAttemptState.FILLED,
            broker_order_id=7001,
            broker_perm_id=9001,
            broker_status="Filled",
            broker_terminal_proven=True,
            created_at_utc=T0,
            updated_at_utc=T2,
            terminal_at_utc=T2,
            last_broker_proof_at_utc=T2,
            failure_reason=None,
        ),
    )


class DailyRiskSourceTest(unittest.TestCase):
    def test_execution_reader_classifies_strategic_open_fill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "execution.sqlite3"
            apply_schema(database)
            apply_component(database, "execution.protective_lifecycle.v1.json")
            apply_component(database, "execution.liquidation.v1.json")
            command_id = new_id("strategy_command")
            command = command_state(command_id)
            operation = succeeded_operation(command_id)
            connection = sqlite3.connect(str(database))
            try:
                connection.execute(
                    "INSERT INTO internal_execution_command_states ("
                    "command_id, strategy_id, strategy_version, deployment_id, "
                    "instrument_id, command_kind, desired_target_side, "
                    "desired_target_quantity, state, requested_qty, filled_qty, "
                    "remaining_qty, received_at_ts, received_at_utc, updated_at_ts, "
                    "updated_at_utc, terminal_at_ts, terminal_at_utc, blocking_reason, "
                    "fixture_hash, fixture_payload_json, payload_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        command.command_id,
                        command.strategy_id,
                        command.strategy_version,
                        command.deployment_id,
                        command.instrument_id,
                        command.command_kind.value,
                        command.desired_target_side.value,
                        command.desired_target_quantity,
                        command.state.value,
                        command.requested_qty,
                        command.filled_qty,
                        command.remaining_qty,
                        0,
                        command.received_at_utc,
                        0,
                        command.updated_at_utc,
                        None,
                        None,
                        None,
                        "a" * 64,
                        "{}",
                        canonical_json_text(command.to_dict()),
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            SQLiteBrokerAttemptStore(database).publish_initial(operation)
            base_fill = owned_fill(
                commission_fact=None,
            ).fill
            commission_fact = commission(base_fill.exec_id)
            connection = sqlite3.connect(str(database))
            try:
                for sequence, (outcome, payload) in enumerate(
                    (
                        ("FILL", base_fill.to_dict()),
                        ("COMMISSION", commission_fact.to_dict()),
                    ),
                    start=1,
                ):
                    connection.execute(
                        "INSERT INTO internal_broker_reconciliation_observations ("
                        "observation_id, operation_id, attempt_id, sequence_no, "
                        "outcome, observed_at_ts, observed_at_utc, payload_json) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            new_id("broker_observation"),
                            operation.operation.operation_id,
                            operation.attempt.attempt_id,
                            sequence,
                            outcome,
                            0,
                            T4,
                            canonical_json_text(payload),
                        ),
                    )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteDailyRiskExecutionReader(database)
            reader.validate_schema()
            values = reader.read_owned_fills(
                account_id=ACCOUNT,
                strategy_id=STRATEGY,
                deployment_id=DEPLOYMENT,
                instrument_id=INSTRUMENT,
            )
            self.assertEqual(len(values), 1)
            self.assertEqual(values[0].kind, DailyRiskFillKind.STRATEGIC_OPEN)
            self.assertEqual(values[0].fill.commission, commission_fact)

    def test_market_reader_returns_latest_mid_mark(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "MNQ.sqlite3"
            store_name, migrations = load_migration_manifest(
                ROOT / "migrations" / "market_data.v1.json"
            )
            SQLiteMigrationRunner(
                database_path=database,
                store_name=store_name,
                migrations=migrations,
                application_version="test",
            ).apply()
            bar_id = new_id("market_bar")
            connection = sqlite3.connect(str(database))
            try:
                connection.execute(
                    "INSERT INTO internal_market_bars ("
                    "bar_id, instrument_id, con_id, local_symbol, bar_start_ts, "
                    "bar_start_utc, bar_end_utc, bar_duration_seconds, "
                    "bid_open, bid_high, bid_low, bid_close, ask_open, ask_high, "
                    "ask_low, ask_close, source_kind, first_published_at_utc, "
                    "published_at_utc, revision, complete, volume, average, bar_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        bar_id,
                        INSTRUMENT,
                        793_356_225,
                        "MNQU6",
                        0,
                        "1970-01-01T00:00:00Z",
                        T4,
                        5,
                        100.0,
                        101.0,
                        99.0,
                        100.0,
                        100.5,
                        101.5,
                        99.5,
                        100.5,
                        "REALTIME",
                        T4,
                        T4,
                        1,
                        1,
                        None,
                        None,
                        None,
                    ),
                )
                connection.execute(
                    "INSERT INTO internal_market_data_state ("
                    "instrument_id, latest_complete_bar_id, latest_bar_start_ts, "
                    "latest_bar_end_utc, latest_con_id, latest_local_symbol, "
                    "last_ingest_at_utc, last_source_status, last_error_text) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        INSTRUMENT,
                        bar_id,
                        0,
                        T4,
                        793_356_225,
                        "MNQU6",
                        T4,
                        "OK",
                        None,
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            reader = SQLiteDailyRiskMarketDataReader(
                database,
                instrument_id=INSTRUMENT,
                price_precision=3,
            )
            mark_value = reader.read_latest_mark(observed_at_utc=T5)
            self.assertIsNotNone(mark_value)
            self.assertEqual(mark_value.bar_id, bar_id)
            self.assertEqual(mark_value.mid_price, 100.25)


if __name__ == "__main__":
    unittest.main()
