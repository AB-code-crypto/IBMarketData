from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ibmd.execution.adapters import (
    SQLiteExecutionPositionFeedReader,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
)
from ibmd.execution.domain import (
    ExecutionFoundationFixtureV1,
    PositionProjectionPolicyV1,
    RegisteredFuturesContractV1,
    merge_position_projection_readiness,
    project_strategy_position,
)
from ibmd.foundation.identity import new_id
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.position_feed.adapters import SQLiteBrokerPositionSnapshotStore
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
from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotV1,
)

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "U0000000"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "shadow-mnq-account1"
INSTRUMENT = "MNQ"
OBSERVED = "2026-07-24T10:00:05Z"
ACTIVE_CON_ID = 793356225
ACTIVE_SYMBOL = "MNQU6"
OLD_CON_ID = 749749350
OLD_SYMBOL = "MNQM6"


def apply_schema(manifest: Path, database: Path) -> None:
    store_name, migrations = load_migration_manifest(manifest)
    SQLiteMigrationRunner(
        database_path=database,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()


def policy(max_age: float = 10.0) -> PositionProjectionPolicyV1:
    return PositionProjectionPolicyV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        max_snapshot_age_seconds=max_age,
    )


def registry(
    *,
    active_con_id: int | None = ACTIVE_CON_ID,
) -> tuple[RegisteredFuturesContractV1, ...]:
    return (
        RegisteredFuturesContractV1(
            con_id=OLD_CON_ID,
            local_symbol=OLD_SYMBOL,
            contract_is_active=active_con_id == OLD_CON_ID,
        ),
        RegisteredFuturesContractV1(
            con_id=ACTIVE_CON_ID,
            local_symbol=ACTIVE_SYMBOL,
            contract_is_active=active_con_id == ACTIVE_CON_ID,
        ),
    )


def row(
    *,
    con_id: int = ACTIVE_CON_ID,
    local_symbol: str = ACTIVE_SYMBOL,
    quantity: float = 1.0,
    symbol: str = INSTRUMENT,
    sec_type: str = "FUT",
) -> BrokerPositionRowV1:
    return BrokerPositionRowV1(
        con_id=con_id,
        local_symbol=local_symbol,
        symbol=symbol,
        sec_type=sec_type,
        exchange="CME",
        currency="USD",
        signed_quantity=quantity,
        average_cost=57_000.0,
    )


def snapshot(
    *rows: BrokerPositionRowV1,
    captured_at: str = "2026-07-24T10:00:00Z",
    account_id: str = ACCOUNT,
) -> BrokerPositionSnapshotV1:
    return BrokerPositionSnapshotV1.complete(
        snapshot_id=new_id("position_snapshot"),
        account_id=account_id,
        captured_at_utc=captured_at,
        published_at_utc=captured_at,
        source_session_id=new_id("ib_session"),
        rows=tuple(rows),
    )


def owned_position(
    *,
    quantity: int = 1,
    side: StrategyPositionSide = StrategyPositionSide.LONG,
    con_id: int = ACTIVE_CON_ID,
    local_symbol: str = ACTIVE_SYMBOL,
    active: bool = True,
) -> StrategyPositionV1:
    signed = quantity if side == StrategyPositionSide.LONG else -quantity
    return StrategyPositionV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_episode_id=new_id("position_episode"),
        side=side,
        quantity=quantity,
        contracts=(
            PositionContractV1(
                con_id=con_id,
                local_symbol=local_symbol,
                signed_quantity=signed,
                contract_is_active=active,
            ),
        ),
        projection_status=StrategyPositionStatus.OPEN,
        broker_snapshot_id=new_id("position_snapshot"),
        updated_at_utc="2026-07-24T09:59:55Z",
        source_freshness_seconds=1.0,
    )


def ready() -> ExecutionReadinessV1:
    return ExecutionReadinessV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=ExecutionReadinessStatus.READY,
        command_intake_enabled=True,
        broker_actions_enabled=False,
        reconciliation_complete=True,
        clock_healthy=True,
        blocking_reasons=(),
        updated_at_utc="2026-07-24T09:59:55Z",
    )


def risk() -> DailyRiskStateV1:
    return DailyRiskStateV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        trading_day="2026-07-24",
        status=DailyRiskStatus.MONITORING,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        total_pnl=0.0,
        target_pnl=500.0,
        pnl_ready=True,
        cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
        updated_at_utc=OBSERVED,
    )


class StrategyPositionProjectorTest(unittest.TestCase):
    def test_empty_complete_snapshot_projects_flat_and_keeps_ready(self) -> None:
        result = project_strategy_position(
            snapshot=snapshot(),
            previous=None,
            policy=policy(),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            result.position.projection_status,
            StrategyPositionStatus.FLAT,
        )
        self.assertEqual(result.position.side, StrategyPositionSide.FLAT)
        self.assertEqual(result.blocking_reasons, ())

        readiness = merge_position_projection_readiness(
            previous=ready(),
            projection=result,
            policy=policy(),
            observed_at_utc=OBSERVED,
        )
        self.assertEqual(readiness.status, ExecutionReadinessStatus.READY)
        self.assertTrue(readiness.command_intake_enabled)
        self.assertFalse(readiness.broker_actions_enabled)

    def test_owned_position_is_open_and_episode_survives_refresh(self) -> None:
        previous = owned_position(side=StrategyPositionSide.SHORT)
        result = project_strategy_position(
            snapshot=snapshot(row(quantity=-1.0)),
            previous=previous,
            policy=policy(),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            result.position.projection_status,
            StrategyPositionStatus.OPEN,
        )
        self.assertEqual(
            result.position.position_episode_id,
            previous.position_episode_id,
        )
        self.assertEqual(result.position.side, StrategyPositionSide.SHORT)
        self.assertEqual(result.blocking_reasons, ())

    def test_unowned_broker_position_is_not_claimed_by_strategy(self) -> None:
        result = project_strategy_position(
            snapshot=snapshot(row(quantity=1.0)),
            previous=None,
            policy=policy(),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            result.position.projection_status,
            StrategyPositionStatus.OWNERSHIP_UNPROVEN,
        )
        self.assertIsNone(result.position.position_episode_id)
        self.assertIn(
            "position_projection:ownership_unproven",
            result.blocking_reasons,
        )
        readiness = merge_position_projection_readiness(
            previous=ready(),
            projection=result,
            policy=policy(),
            observed_at_utc=OBSERVED,
        )
        self.assertEqual(readiness.status, ExecutionReadinessStatus.BLOCKED)

    def test_multiple_contracts_are_an_incident_not_an_aggregate(self) -> None:
        result = project_strategy_position(
            snapshot=snapshot(
                row(quantity=1.0),
                row(
                    con_id=OLD_CON_ID,
                    local_symbol=OLD_SYMBOL,
                    quantity=-1.0,
                ),
            ),
            previous=None,
            policy=policy(),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            result.position.projection_status,
            StrategyPositionStatus.MULTI_CONTRACT_INCIDENT,
        )
        self.assertEqual(result.position.side, StrategyPositionSide.UNKNOWN)
        self.assertEqual(result.position.quantity, 0)
        self.assertEqual(len(result.position.contracts), 2)

    def test_unknown_contract_and_fractional_quantity_fail_closed(self) -> None:
        unknown = project_strategy_position(
            snapshot=snapshot(
                row(
                    con_id=999_999_999,
                    local_symbol="MNQZ9",
                )
            ),
            previous=None,
            policy=policy(),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            unknown.position.projection_status,
            StrategyPositionStatus.UNKNOWN,
        )
        self.assertIn(
            "position_projection:contract_identity_unknown",
            unknown.blocking_reasons,
        )

        fractional = project_strategy_position(
            snapshot=snapshot(row(quantity=1.5)),
            previous=None,
            policy=policy(),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            fractional.position.projection_status,
            StrategyPositionStatus.UNKNOWN,
        )
        self.assertIn(
            "position_projection:quantity_not_integral",
            fractional.blocking_reasons,
        )

    def test_stale_snapshot_preserves_owned_episode_and_fresh_recovers(self) -> None:
        previous = owned_position()
        stale = project_strategy_position(
            snapshot=snapshot(
                row(),
                captured_at="2026-07-24T09:59:00Z",
            ),
            previous=previous,
            policy=policy(max_age=10.0),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            stale.position.projection_status,
            StrategyPositionStatus.STALE,
        )
        self.assertEqual(
            stale.position.position_episode_id,
            previous.position_episode_id,
        )

        recovered = project_strategy_position(
            snapshot=snapshot(row()),
            previous=stale.position,
            policy=policy(),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            recovered.position.projection_status,
            StrategyPositionStatus.OPEN,
        )
        self.assertEqual(
            recovered.position.position_episode_id,
            previous.position_episode_id,
        )

    def test_non_active_owned_contract_blocks_normal_commands(self) -> None:
        previous = owned_position(
            con_id=OLD_CON_ID,
            local_symbol=OLD_SYMBOL,
            active=False,
        )
        result = project_strategy_position(
            snapshot=snapshot(
                row(
                    con_id=OLD_CON_ID,
                    local_symbol=OLD_SYMBOL,
                )
            ),
            previous=previous,
            policy=policy(),
            registry=registry(),
            observed_at_utc=OBSERVED,
            active_contract_available=True,
        )
        self.assertEqual(
            result.position.projection_status,
            StrategyPositionStatus.OPEN,
        )
        self.assertFalse(result.position.contracts[0].contract_is_active)
        self.assertIn(
            "position_projection:held_contract_not_active",
            result.blocking_reasons,
        )


class PositionProjectionPersistenceTest(unittest.TestCase):
    def test_public_position_feed_is_projected_into_execution_store(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            position_database = root / "position_feed.sqlite3"
            execution_database = root / "execution.sqlite3"
            apply_schema(
                ROOT / "migrations" / "position_feed.v1.json",
                position_database,
            )
            apply_schema(
                ROOT / "migrations" / "execution.v1.json",
                execution_database,
            )

            source_store = SQLiteBrokerPositionSnapshotStore(
                position_database
            )
            source_store.publish(snapshot())
            source_reader = SQLiteExecutionPositionFeedReader(
                position_database
            )
            source_reader.validate_schema()
            latest = source_reader.read_latest_complete()
            self.assertIsNotNone(latest)
            self.assertEqual(latest.row_count, 0)

            execution_store = SQLiteExecutionStore(execution_database)
            execution_store.validate_schema()
            initial = ExecutionFoundationFixtureV1(
                observed_at_utc=OBSERVED,
                readiness=ready(),
                position=StrategyPositionV1(
                    account_id=ACCOUNT,
                    strategy_id=STRATEGY,
                    deployment_id=DEPLOYMENT,
                    instrument_id=INSTRUMENT,
                    position_episode_id=None,
                    side=StrategyPositionSide.FLAT,
                    quantity=0,
                    contracts=(),
                    projection_status=StrategyPositionStatus.FLAT,
                    broker_snapshot_id=latest.snapshot_id,
                    updated_at_utc=OBSERVED,
                    source_freshness_seconds=5.0,
                ),
                daily_risk=risk(),
            )
            execution_store.publish_fixture(initial)

            state_reader = SQLiteExecutionStateReader(execution_database)
            state_reader.validate_schema()
            self.assertEqual(
                state_reader.read_position(
                    account_id=ACCOUNT,
                    strategy_id=STRATEGY,
                    deployment_id=DEPLOYMENT,
                    instrument_id=INSTRUMENT,
                ),
                initial.position,
            )
            self.assertEqual(
                state_reader.read_readiness(
                    account_id=ACCOUNT,
                    strategy_id=STRATEGY,
                    deployment_id=DEPLOYMENT,
                    instrument_id=INSTRUMENT,
                ),
                initial.readiness,
            )
            self.assertEqual(
                state_reader.read_latest_daily_risk(
                    account_id=ACCOUNT,
                    strategy_id=STRATEGY,
                    deployment_id=DEPLOYMENT,
                ),
                initial.daily_risk,
            )


if __name__ == "__main__":
    unittest.main()
