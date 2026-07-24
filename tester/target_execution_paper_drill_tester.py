from __future__ import annotations

import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ibmd.decision.adapters import SQLiteDecisionStore
from ibmd.execution.adapters import (
    SQLiteBrokerAttemptStore,
    SQLiteExecutionStateReader,
    SQLiteExecutionStore,
)
from ibmd.execution.application.paper_drill import (
    PaperDrillPolicy,
    PaperDrillPreparationError,
    PaperExecutionDrillPreparer,
)
from ibmd.execution.domain import RegisteredFuturesContractV1
from ibmd.foundation.identity import new_id
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.public_contracts.decision import DesiredTargetSide
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    StrategyPositionStatus,
)
from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotV1,
)

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "DU000000"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "paper-drill-mnq-account1"
INSTRUMENT = "MNQ"
POLICY_HASH = "a" * 64
CON_ID = 793_356_225
LOCAL_SYMBOL = "MNQU6"
T0 = "2026-07-24T10:00:00Z"
T1 = "2026-07-24T10:00:01Z"
T2 = "2026-07-24T10:00:02Z"


def apply_manifest(path: Path, manifest_name: str) -> None:
    store_name, migrations = load_migration_manifest(
        ROOT / "migrations" / manifest_name
    )
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()


class SnapshotSource:
    def __init__(self, snapshot: BrokerPositionSnapshotV1 | None) -> None:
        self.snapshot = snapshot
        self.read_count = 0

    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None:
        self.read_count += 1
        return self.snapshot


def position_snapshot(
    *,
    captured_at_utc: str = T1,
    rows: tuple[BrokerPositionRowV1, ...] = (),
) -> BrokerPositionSnapshotV1:
    return BrokerPositionSnapshotV1.complete(
        snapshot_id=new_id("position_snapshot"),
        account_id=ACCOUNT,
        captured_at_utc=captured_at_utc,
        published_at_utc=captured_at_utc,
        source_session_id=new_id("ib_session"),
        rows=rows,
    )


def policy(
    *,
    environment: str = "paper",
    confirmed_account: str = ACCOUNT,
    target_side: DesiredTargetSide = DesiredTargetSide.LONG,
    drill_id: str = "drill-001",
    deployment_id: str = DEPLOYMENT,
) -> PaperDrillPolicy:
    return PaperDrillPolicy(
        drill_id=drill_id,
        account_id=ACCOUNT,
        environment=environment,
        confirmed_paper_account_id=confirmed_account,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=deployment_id,
        instrument_id=INSTRUMENT,
        policy_hash=POLICY_HASH,
        target_side=target_side,
        target_quantity=1,
        command_ttl_seconds=120,
        position_max_age_seconds=30.0,
        daily_risk_timezone="Europe/Moscow",
        daily_risk_target=500.0,
        active_contract=RegisteredFuturesContractV1(
            con_id=CON_ID,
            local_symbol=LOCAL_SYMBOL,
            contract_is_active=True,
        ),
    )


class PaperDrillPreparationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.decision_database = root / "decision.sqlite3"
        self.execution_database = root / "execution.sqlite3"
        apply_manifest(self.decision_database, "decision.v1.json")
        apply_manifest(self.execution_database, "execution.v1.json")
        self.decision_store = SQLiteDecisionStore(self.decision_database)
        self.execution_store = SQLiteExecutionStore(self.execution_database)
        self.execution_state = SQLiteExecutionStateReader(
            self.execution_database
        )
        self.attempt_store = SQLiteBrokerAttemptStore(
            self.execution_database
        )
        self.registry = (
            RegisteredFuturesContractV1(
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                contract_is_active=True,
            ),
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def preparer(
        self,
        *,
        snapshot: BrokerPositionSnapshotV1 | None = None,
        policy_value: PaperDrillPolicy | None = None,
    ) -> PaperExecutionDrillPreparer:
        return PaperExecutionDrillPreparer(
            policy=policy_value or policy(),
            decision_repository=self.decision_store,
            execution_repository=self.execution_store,
            execution_state_source=self.execution_state,
            position_snapshot_source=SnapshotSource(
                position_snapshot() if snapshot is None else snapshot
            ),
            broker_attempt_source=self.attempt_store,
            contract_registry=self.registry,
        )

    def test_fresh_flat_snapshot_stages_one_admitted_command_without_order(self) -> None:
        result = self.preparer().prepare(observed_at_utc=T2)
        self.assertTrue(result.ready_for_submit)
        self.assertEqual(
            result.command_state.state,
            ExecutionCommandState.ADMITTED,
        )
        self.assertEqual(
            result.fixture.position.projection_status,
            StrategyPositionStatus.FLAT,
        )
        self.assertTrue(
            result.fixture.readiness.broker_actions_enabled
        )
        self.assertEqual(
            self.decision_store.read_command(result.command.command_id),
            result.command,
        )
        self.assertEqual(
            self.execution_store.read_command_state(result.command.command_id),
            result.command_state,
        )

        connection = sqlite3.connect(self.execution_database)
        try:
            operation_count = connection.execute(
                "SELECT COUNT(*) FROM internal_broker_order_operations"
            ).fetchone()[0]
            transition_count = connection.execute(
                "SELECT COUNT(*) FROM internal_execution_command_transitions"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(operation_count, 0)
        self.assertEqual(transition_count, 2)

    def test_same_drill_id_reuses_command_and_refreshes_fixture(self) -> None:
        preparer = self.preparer()
        first = preparer.prepare(observed_at_utc=T1)
        second = preparer.prepare(observed_at_utc=T2)
        self.assertEqual(first.command.command_id, second.command.command_id)
        self.assertTrue(second.reused_existing_command)
        self.assertEqual(
            self.execution_store.read_transition_states(
                first.command.command_id
            ),
            ("RECEIVED", "ADMITTED"),
        )
        readiness = self.execution_state.read_readiness(
            account_id=ACCOUNT,
            strategy_id=STRATEGY,
            deployment_id=DEPLOYMENT,
            instrument_id=INSTRUMENT,
        )
        self.assertEqual(readiness.updated_at_utc, T2.replace("Z", ".000000Z"))

    def test_existing_drill_id_cannot_change_target_side(self) -> None:
        self.preparer().prepare(observed_at_utc=T1)
        changed = self.preparer(
            policy_value=policy(target_side=DesiredTargetSide.SHORT)
        )
        with self.assertRaisesRegex(
            PaperDrillPreparationError,
            "conflicts",
        ):
            changed.prepare(observed_at_utc=T2)

    def test_open_broker_position_is_rejected(self) -> None:
        snapshot = position_snapshot(
            rows=(
                BrokerPositionRowV1(
                    con_id=CON_ID,
                    local_symbol=LOCAL_SYMBOL,
                    symbol=INSTRUMENT,
                    sec_type="FUT",
                    exchange="CME",
                    currency="USD",
                    signed_quantity=1.0,
                    average_cost=28_650.0,
                ),
            ),
        )
        with self.assertRaisesRegex(
            PaperDrillPreparationError,
            "broker-proven FLAT",
        ):
            self.preparer(snapshot=snapshot).prepare(observed_at_utc=T2)

    def test_stale_position_snapshot_is_rejected(self) -> None:
        stale = position_snapshot(captured_at_utc=T0)
        with self.assertRaisesRegex(
            PaperDrillPreparationError,
            "broker-proven FLAT",
        ):
            self.preparer(
                snapshot=stale,
                policy_value=replace(
                    policy(),
                    position_max_age_seconds=1.0,
                ),
            ).prepare(observed_at_utc=T2)

    def test_live_environment_and_wrong_confirmation_are_rejected(self) -> None:
        with self.assertRaisesRegex(
            PaperDrillPreparationError,
            "IBMD_ENVIRONMENT=paper",
        ):
            self.preparer(
                policy_value=policy(environment="live")
            ).prepare(observed_at_utc=T2)
        with self.assertRaisesRegex(
            PaperDrillPreparationError,
            "confirmation",
        ):
            self.preparer(
                policy_value=policy(confirmed_account="DU999999")
            ).prepare(observed_at_utc=T2)

    def test_dedicated_deployment_marker_is_required(self) -> None:
        with self.assertRaisesRegex(
            PaperDrillPreparationError,
            "dedicated deployment_id",
        ):
            policy(deployment_id="shadow-mnq-account1")


if __name__ == "__main__":
    unittest.main()
