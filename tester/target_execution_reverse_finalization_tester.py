from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ibmd.execution.adapters.sqlite_broker_attempts import (
    SQLiteBrokerAttemptStore,
)
from ibmd.execution.adapters.sqlite_protection import (
    SQLiteProtectionReader,
    SQLiteProtectionStore,
)
from ibmd.execution.adapters.sqlite_reverse_finalization import (
    SQLiteReverseFinalizationStore,
)
from ibmd.execution.adapters.sqlite_state import SQLiteExecutionStateReader
from ibmd.execution.application.reverse_finalization import (
    ReverseFinalizationService,
)
from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.execution.domain.protection import PositionEpisodeProtectionPlan
from ibmd.execution.domain.reverse_finalization import (
    ReverseFinalizationError,
    ReverseFinalizationPolicyV1,
    finalize_reverse_position,
)
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
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
)
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    PositionContractV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodePolicyV1,
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
    ProtectiveOrderType,
    ProtectiveOrderV1,
)

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "DU000000"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "reverse-finalization-test"
INSTRUMENT = "MNQ"
CON_ID = 793_356_225
LOCAL_SYMBOL = "MNQU6"
POLICY_HASH = "a" * 64
T0 = "2026-07-27T10:00:00Z"
T1 = "2026-07-27T10:00:01Z"
T2 = "2026-07-27T10:00:02Z"
T3 = "2026-07-27T10:00:03Z"
T4 = "2026-07-27T10:00:04Z"
T5 = "2026-07-27T10:00:05Z"


def protective_policy() -> PositionEpisodePolicyV1:
    return PositionEpisodePolicyV1(
        price_tick=0.25,
        stop_required=True,
        take_profit_enabled=True,
        stop_loss_points=150.0,
        take_profit_points=75.0,
        time_in_force="DAY",
        stop_outside_rth=True,
        take_profit_outside_rth=False,
        price_watchdog_enabled=True,
        stale_feed_market_close_enabled=False,
        price_stale_max_seconds=600,
    )


def old_plan(*, quantity: int = 1) -> PositionEpisodeProtectionPlan:
    policy = protective_policy()
    episode_id = new_id("position_episode")
    protection_set_id = new_id("protection_set")
    episode = PositionEpisodeV1(
        position_episode_id=episode_id,
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        source_command_id=new_id("strategy_command"),
        source_operation_id=new_id("broker_operation"),
        source_attempt_id=new_id("broker_attempt"),
        source_exec_ids=("old-entry-exec",),
        side=StrategyPositionSide.LONG,
        quantity=quantity,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        entry_average_price=28_600.0,
        broker_snapshot_id=new_id("position_snapshot"),
        opened_at_utc=T0,
        status=PositionEpisodeStatus.OPEN,
        strategy_policy_hash=POLICY_HASH,
        protective_policy_hash=policy.content_hash,
        protective_policy=policy,
    )
    oca_group = f"IBMD_OCA_{protection_set_id.rsplit('_', 1)[-1]}"
    stop = ProtectiveOrderV1(
        protective_order_id=new_id("protective_order"),
        protection_set_id=protection_set_id,
        position_episode_id=episode_id,
        kind=ProtectiveOrderKind.STOP_LOSS,
        state=ProtectiveOrderState.NOT_REQUIRED,
        planned_sequence=1,
        order_ref=f"IBMD:{protection_set_id}:SL",
        side=BrokerOrderSide.SELL,
        order_type=ProtectiveOrderType.STOP,
        quantity=quantity,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        stop_price=28_450.0,
        limit_price=None,
        time_in_force="DAY",
        outside_rth=True,
        oca_group=oca_group,
        filled_qty=0,
        remaining_qty=quantity,
        broker_order_id=None,
        broker_perm_id=None,
        broker_status=None,
        broker_terminal_proven=False,
        created_at_utc=T0,
        updated_at_utc=T1,
        terminal_at_utc=T1,
        last_broker_proof_at_utc=None,
        failure_reason="reverse_handoff_not_required",
    )
    take_profit = ProtectiveOrderV1(
        protective_order_id=new_id("protective_order"),
        protection_set_id=protection_set_id,
        position_episode_id=episode_id,
        kind=ProtectiveOrderKind.TAKE_PROFIT,
        state=ProtectiveOrderState.NOT_REQUIRED,
        planned_sequence=2,
        order_ref=f"IBMD:{protection_set_id}:TP",
        side=BrokerOrderSide.SELL,
        order_type=ProtectiveOrderType.LIMIT,
        quantity=quantity,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        stop_price=None,
        limit_price=28_675.0,
        time_in_force="DAY",
        outside_rth=False,
        oca_group=oca_group,
        filled_qty=0,
        remaining_qty=quantity,
        broker_order_id=None,
        broker_perm_id=None,
        broker_status=None,
        broker_terminal_proven=False,
        created_at_utc=T0,
        updated_at_utc=T1,
        terminal_at_utc=T1,
        last_broker_proof_at_utc=None,
        failure_reason="reverse_handoff_not_required",
    )
    protection = ProtectionStateV1(
        protection_set_id=protection_set_id,
        position_episode_id=episode_id,
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=ProtectionSetStatus.UNPROTECTED,
        orders=(stop, take_profit),
        created_at_utc=T0,
        updated_at_utc=T1,
        terminal_at_utc=None,
        blocking_reason="reverse_handoff_complete",
    )
    position = StrategyPositionV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_episode_id=episode_id,
        side=StrategyPositionSide.LONG,
        quantity=quantity,
        contracts=(
            PositionContractV1(
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                signed_quantity=quantity,
                contract_is_active=True,
            ),
        ),
        projection_status=StrategyPositionStatus.OPEN,
        broker_snapshot_id=new_id("position_snapshot"),
        updated_at_utc=T1,
        source_freshness_seconds=1.0,
    )
    readiness = ExecutionReadinessV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=ExecutionReadinessStatus.BLOCKED,
        command_intake_enabled=False,
        broker_actions_enabled=True,
        reconciliation_complete=True,
        clock_healthy=True,
        blocking_reasons=("reverse_handoff:ready",),
        updated_at_utc=T1,
    )
    return PositionEpisodeProtectionPlan(
        episode=episode,
        strategy_position=position,
        execution_readiness=readiness,
        protection=protection,
    )


def reverse_command(*, target_quantity: int = 1) -> ExecutionCommandStateV1:
    return ExecutionCommandStateV1(
        command_id=new_id("strategy_command"),
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        command_kind=StrategyCommandKind.REVERSE,
        desired_target_side=DesiredTargetSide.SHORT,
        desired_target_quantity=target_quantity,
        state=ExecutionCommandState.ADMITTED,
        requested_qty=target_quantity,
        filled_qty=0,
        remaining_qty=target_quantity,
        latest_attempt_id=None,
        blocking_reason=None,
        received_at_utc=T1,
        updated_at_utc=T1,
        terminal_at_utc=None,
    )


def reverse_operation(
    command: ExecutionCommandStateV1,
    *,
    source_quantity: int,
) -> BrokerOperationSnapshot:
    requested = source_quantity + command.desired_target_quantity
    operation_id = new_id("broker_operation")
    attempt_id = new_id("broker_attempt")
    order_ref = f"IBMD:{operation_id}:1"
    operation = BrokerOrderOperationV1(
        operation_id=operation_id,
        command_id=command.command_id,
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        side=BrokerOrderSide.SELL,
        order_type="MARKET",
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        requested_qty=requested,
        filled_qty=requested,
        remaining_qty=0,
        state=BrokerOperationState.SUCCEEDED,
        current_attempt_id=attempt_id,
        current_attempt_no=1,
        created_at_utc=T1,
        updated_at_utc=T4,
        terminal_at_utc=T4,
        blocking_reason=None,
    )
    attempt = BrokerOrderAttemptV1(
        attempt_id=attempt_id,
        operation_id=operation_id,
        attempt_no=1,
        order_ref=order_ref,
        side=BrokerOrderSide.SELL,
        order_type="MARKET",
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        requested_qty=requested,
        filled_qty=requested,
        remaining_qty=0,
        state=BrokerAttemptState.FILLED,
        broker_order_id=8001,
        broker_perm_id=9001,
        broker_status="Filled",
        broker_terminal_proven=True,
        created_at_utc=T1,
        updated_at_utc=T4,
        terminal_at_utc=T4,
        last_broker_proof_at_utc=T4,
        failure_reason=None,
    )
    return BrokerOperationSnapshot(operation=operation, attempt=attempt)


def fill(
    operation: BrokerOperationSnapshot,
    *,
    exec_id: str,
    shares: int,
    cumulative_qty: int,
    price: float,
    executed_at_utc: str,
    commission_complete: bool = False,
) -> BrokerFillFactV1:
    commission = (
        BrokerCommissionFactV1(
            exec_id=exec_id,
            commission=1.25,
            currency="USD",
            realized_pnl=0.0,
            reported_at_utc=T5,
        )
        if commission_complete
        else None
    )
    return BrokerFillFactV1(
        exec_id=exec_id,
        account_id=ACCOUNT,
        order_ref=operation.attempt.order_ref,
        broker_order_id=8001,
        broker_perm_id=9001,
        client_id=320,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=BrokerOrderSide.SELL,
        shares=shares,
        price=price,
        cumulative_qty=cumulative_qty,
        average_price=price,
        exchange="CME",
        executed_at_utc=executed_at_utc,
        observed_at_utc=T5,
        commission=commission,
    )


def target_snapshot(*, target_quantity: int = 1) -> BrokerPositionSnapshotV1:
    return BrokerPositionSnapshotV1.complete(
        snapshot_id=new_id("position_snapshot"),
        account_id=ACCOUNT,
        captured_at_utc=T5,
        published_at_utc=T5,
        source_session_id=new_id("ib_session"),
        rows=(
            BrokerPositionRowV1(
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                symbol=INSTRUMENT,
                sec_type="FUT",
                exchange="CME",
                currency="USD",
                signed_quantity=-target_quantity,
                average_cost=57_100.0,
            ),
        ),
    )


def policy() -> ReverseFinalizationPolicyV1:
    return ReverseFinalizationPolicyV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        strategy_policy_hash=POLICY_HASH,
        position_max_age_seconds=10.0,
        protective_policy=protective_policy(),
    )


def finalized(
    *,
    source_quantity: int = 1,
    target_quantity: int = 1,
    fills: tuple[BrokerFillFactV1, ...] | None = None,
):
    source = old_plan(quantity=source_quantity)
    command = reverse_command(target_quantity=target_quantity)
    operation = reverse_operation(command, source_quantity=source_quantity)
    actual_fills = fills or (
        fill(
            operation,
            exec_id="reverse-exec-1",
            shares=source_quantity + target_quantity,
            cumulative_qty=source_quantity + target_quantity,
            price=28_590.0,
            executed_at_utc=T3,
        ),
    )
    result = finalize_reverse_position(
        operation=operation,
        command=command,
        fills=actual_fills,
        broker_snapshot=target_snapshot(target_quantity=target_quantity),
        old_episode=source.episode,
        old_protection=source.protection,
        old_position=source.strategy_position,
        current_readiness=source.execution_readiness,
        policy=policy(),
        observed_at_utc=T5,
    )
    return source, command, operation, actual_fills, result


class MemoryValueSource:
    def __init__(self, value) -> None:
        self.value = value

    def read_snapshot(self, _operation_id):
        return self.value

    def read_command_state(self, _command_id):
        return self.value

    def read_fills(self, _attempt_id):
        return self.value

    def read_latest_complete(self):
        return self.value

    def read_episode(self, _episode_id):
        return self.value

    def read_protection_by_episode(self, _episode_id):
        return self.value

    def read_snapshot_by_episode(self, _episode_id):
        return self.value


class MemoryExecutionState:
    def __init__(self, position, readiness) -> None:
        self.position = position
        self.readiness = readiness

    def read_position(self, **_scope):
        return self.position

    def read_readiness(self, **_scope):
        return self.readiness


class MemoryRepository:
    def __init__(self) -> None:
        self.value = None
        self.publish_count = 0
        self.refresh_count = 0

    def read_by_operation(self, _operation_id):
        return self.value

    def publish_finalization(self, **values):
        self.publish_count += 1
        self.value = values["result"]
        return self.value

    def refresh_commission_completion(self, *, current, updated):
        if current != self.value:
            raise AssertionError("memory reverse finalization changed")
        self.refresh_count += 1
        self.value = updated
        return updated


class ReverseFinalizationDomainTest(unittest.TestCase):
    def test_single_execution_is_split_between_close_and_open(self) -> None:
        _source, _command, _operation, _fills, result = finalized()
        self.assertEqual(len(result.allocations), 1)
        allocation = result.allocations[0]
        self.assertEqual(allocation.close_quantity, 1)
        self.assertEqual(allocation.open_quantity, 1)
        self.assertEqual(result.closed_episode.status, PositionEpisodeStatus.CLOSED)
        self.assertEqual(result.new_plan.episode.side, StrategyPositionSide.SHORT)
        self.assertEqual(result.new_plan.episode.quantity, 1)
        self.assertEqual(result.new_plan.episode.entry_average_price, 28_590.0)
        self.assertEqual(result.new_plan.episode.source_exec_ids, ("reverse-exec-1",))
        self.assertEqual(result.new_plan.protection.stop_order.stop_price, 28_740.0)
        self.assertEqual(
            result.new_plan.protection.take_profit_order.limit_price,
            28_515.0,
        )
        self.assertFalse(result.commission_complete)

    def test_multiple_fills_use_only_opening_allocation_for_entry_price(self) -> None:
        source = old_plan(quantity=2)
        command = reverse_command(target_quantity=1)
        operation = reverse_operation(command, source_quantity=2)
        fills = (
            fill(
                operation,
                exec_id="reverse-exec-a",
                shares=1,
                cumulative_qty=1,
                price=28_600.0,
                executed_at_utc=T2,
                commission_complete=True,
            ),
            fill(
                operation,
                exec_id="reverse-exec-b",
                shares=2,
                cumulative_qty=3,
                price=28_590.0,
                executed_at_utc=T3,
                commission_complete=True,
            ),
        )
        result = finalize_reverse_position(
            operation=operation,
            command=command,
            fills=fills,
            broker_snapshot=target_snapshot(),
            old_episode=source.episode,
            old_protection=source.protection,
            old_position=source.strategy_position,
            current_readiness=source.execution_readiness,
            policy=policy(),
            observed_at_utc=T5,
        )
        self.assertEqual(
            [(item.close_quantity, item.open_quantity) for item in result.allocations],
            [(1, 0), (1, 1)],
        )
        self.assertEqual(result.new_plan.episode.entry_average_price, 28_590.0)
        self.assertEqual(result.new_plan.episode.source_exec_ids, ("reverse-exec-b",))
        self.assertTrue(result.commission_complete)

    def test_conflicting_broker_position_is_rejected(self) -> None:
        source = old_plan()
        command = reverse_command()
        operation = reverse_operation(command, source_quantity=1)
        wrong_snapshot = BrokerPositionSnapshotV1.complete(
            snapshot_id=new_id("position_snapshot"),
            account_id=ACCOUNT,
            captured_at_utc=T5,
            published_at_utc=T5,
            source_session_id=new_id("ib_session"),
            rows=(),
        )
        with self.assertRaisesRegex(
            ReverseFinalizationError,
            "does not prove the reverse target",
        ):
            finalize_reverse_position(
                operation=operation,
                command=command,
                fills=(
                    fill(
                        operation,
                        exec_id="reverse-exec-1",
                        shares=2,
                        cumulative_qty=2,
                        price=28_590.0,
                        executed_at_utc=T3,
                    ),
                ),
                broker_snapshot=wrong_snapshot,
                old_episode=source.episode,
                old_protection=source.protection,
                old_position=source.strategy_position,
                current_readiness=source.execution_readiness,
                policy=policy(),
                observed_at_utc=T5,
            )


class ReverseFinalizationServiceTest(unittest.TestCase):
    def test_repeat_is_idempotent_and_late_commission_is_enriched(self) -> None:
        source = old_plan()
        command = reverse_command()
        operation = reverse_operation(command, source_quantity=1)
        incomplete_fill = fill(
            operation,
            exec_id="reverse-exec-1",
            shares=2,
            cumulative_qty=2,
            price=28_590.0,
            executed_at_utc=T3,
        )
        fill_source = MemoryValueSource((incomplete_fill,))
        repository = MemoryRepository()
        service = ReverseFinalizationService(
            policy=policy(),
            operation_source=MemoryValueSource(operation),
            command_state_source=MemoryValueSource(command),
            fill_source=fill_source,
            position_snapshot_source=MemoryValueSource(target_snapshot()),
            execution_state_source=MemoryExecutionState(
                source.strategy_position,
                source.execution_readiness,
            ),
            protection_state_source=MemoryValueSource(source.episode),
            liquidation_state_source=MemoryValueSource(None),
            repository=repository,
        )
        service.protection_state_source = type(
            "ProtectionSource",
            (),
            {
                "read_episode": lambda _self, _id: source.episode,
                "read_protection_by_episode": (
                    lambda _self, _id: source.protection
                ),
            },
        )()
        first = service.finalize_from_operation(
            operation_id=operation.operation.operation_id,
            observed_at_utc=T5,
        )
        self.assertTrue(first.finalization_created)
        self.assertFalse(first.finalization.commission_complete)
        self.assertEqual(repository.publish_count, 1)

        complete_fill = replace(
            incomplete_fill,
            commission=BrokerCommissionFactV1(
                exec_id=incomplete_fill.exec_id,
                commission=1.25,
                currency="USD",
                realized_pnl=0.0,
                reported_at_utc=T5,
            ),
        )
        fill_source.value = (complete_fill,)
        second = service.finalize_from_operation(
            operation_id=operation.operation.operation_id,
            observed_at_utc=T5,
        )
        self.assertFalse(second.finalization_created)
        self.assertTrue(second.commission_completion_refreshed)
        self.assertTrue(second.finalization.commission_complete)
        self.assertEqual(repository.publish_count, 1)
        self.assertEqual(repository.refresh_count, 1)

        third = service.finalize_from_operation(
            operation_id=operation.operation.operation_id,
            observed_at_utc=T5,
        )
        self.assertFalse(third.finalization_created)
        self.assertFalse(third.commission_completion_refreshed)
        self.assertEqual(repository.refresh_count, 1)


def apply_schema(database: Path) -> None:
    store_name, migrations = load_migration_manifest(
        ROOT / "migrations" / "execution.v1.json"
    )
    SQLiteMigrationRunner(
        database_path=database,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()
    manifest = json.loads(
        (ROOT / "migrations" / "execution.reverse_finalization.v1.json").read_text(
            encoding="utf-8"
        )
    )
    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        for statement in manifest["statements"]:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO execution_target_schema_components ("
            "component_name, component_version, checksum, applied_at_utc, "
            "application_version) VALUES (?, ?, ?, ?, ?)",
            (
                manifest["component_name"],
                manifest["component_version"],
                "test-checksum",
                T0,
                "test",
            ),
        )
        connection.commit()
    finally:
        connection.close()


class ReverseFinalizationStoreTest(unittest.TestCase):
    def test_atomic_publish_is_idempotent_and_commission_can_enrich(self) -> None:
        source, _command, operation, _fills, result = finalized()
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "execution.sqlite3"
            apply_schema(database)
            SQLiteProtectionStore(database).publish_plan(source)
            SQLiteBrokerAttemptStore(database).publish_initial(operation)
            store = SQLiteReverseFinalizationStore(database)
            store.validate_schema()

            first = store.publish_finalization(
                current_episode=source.episode,
                current_protection=source.protection,
                current_position=source.strategy_position,
                current_readiness=source.execution_readiness,
                result=result,
            )
            second = store.publish_finalization(
                current_episode=source.episode,
                current_protection=source.protection,
                current_position=source.strategy_position,
                current_readiness=source.execution_readiness,
                result=result,
            )
            self.assertEqual(first, second)
            self.assertEqual(
                store.read_allocations(operation.operation.operation_id),
                result.allocations,
            )

            protection_reader = SQLiteProtectionReader(database)
            self.assertEqual(
                protection_reader.read_episode(source.episode.position_episode_id).status,
                PositionEpisodeStatus.CLOSED,
            )
            self.assertEqual(
                protection_reader.read_episode(
                    result.new_plan.episode.position_episode_id
                ),
                result.new_plan.episode,
            )
            state_reader = SQLiteExecutionStateReader(database)
            self.assertEqual(
                state_reader.read_position(
                    account_id=ACCOUNT,
                    strategy_id=STRATEGY,
                    deployment_id=DEPLOYMENT,
                    instrument_id=INSTRUMENT,
                ),
                result.new_plan.strategy_position,
            )

            enriched = replace(
                result,
                allocations=tuple(
                    replace(item, commission_complete=True)
                    for item in result.allocations
                ),
                commission_complete=True,
            )
            refreshed = store.refresh_commission_completion(
                current=result,
                updated=enriched,
            )
            self.assertTrue(refreshed.commission_complete)
            self.assertTrue(
                store.read_by_operation(
                    operation.operation.operation_id
                ).commission_complete
            )
            connection = sqlite3.connect(str(database))
            try:
                counts = connection.execute(
                    "SELECT "
                    "(SELECT COUNT(*) FROM internal_reverse_finalizations), "
                    "(SELECT COUNT(*) FROM internal_reverse_fill_allocations)"
                ).fetchone()
            finally:
                connection.close()
            self.assertEqual(counts, (1, 1))


if __name__ == "__main__":
    unittest.main()
