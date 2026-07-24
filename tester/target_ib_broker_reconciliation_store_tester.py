from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ibmd.execution.adapters import (
    SQLiteBrokerAttemptStore,
    SQLiteBrokerReconciliationStore,
)
from ibmd.execution.application.read_only_reconciliation import (
    ReadOnlyBrokerReconciliationService,
)
from ibmd.execution.domain import mark_attempt_submitting, plan_broker_operation
from ibmd.ib_gateway.fake_broker_reconciliation import (
    ScriptedBrokerReconciliationReader,
)
from ibmd.operations.migrations import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerOperationState,
)
from ibmd.public_contracts.broker_reconciliation import BrokerOrderSource

from tester.target_ib_broker_reconciliation_tester import (
    ACCOUNT,
    ORDER_ID,
    T0,
    T1,
    active_contract,
    broker_snapshot,
    command_state,
    fill_fact,
    flat_position,
    order_fact,
)

ROOT = Path(__file__).resolve().parents[1]


def apply_execution_schema(path: Path) -> None:
    store_name, migrations = load_migration_manifest(
        ROOT / "migrations" / "execution.v1.json"
    )
    SQLiteMigrationRunner(
        database_path=path,
        store_name=store_name,
        migrations=migrations,
        application_version="test",
    ).apply()


class BrokerReconciliationPersistenceAndRecoveryTest(unittest.TestCase):
    def test_read_only_service_adopts_live_order_without_new_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_execution_schema(database)
            attempt_store = SQLiteBrokerAttemptStore(database)
            reconciliation_store = SQLiteBrokerReconciliationStore(database)
            planned = plan_broker_operation(
                command=command_state(),
                position=flat_position(),
                active_contract=active_contract(),
                account_id=ACCOUNT,
                observed_at_utc=T0,
            )
            current = mark_attempt_submitting(
                planned,
                observed_at_utc=T1,
                broker_order_id=ORDER_ID,
            )
            attempt_store.publish_initial(planned)
            attempt_store.publish_state(current)
            snapshot = broker_snapshot(
                open_orders=(
                    order_fact(order_ref=current.attempt.order_ref),
                )
            )
            service = ReadOnlyBrokerReconciliationService(
                account_id=ACCOUNT,
                broker_source=ScriptedBrokerReconciliationReader((snapshot,)),
                attempt_source=attempt_store,
                reconciliation_store=reconciliation_store,
            )
            result = asyncio.run(service.run_once())
            self.assertEqual(len(result.reconciled), 1)
            after = result.reconciled[0].after
            self.assertEqual(after.operation.state, BrokerOperationState.LIVE)
            self.assertEqual(after.attempt.state, BrokerAttemptState.LIVE)
            self.assertEqual(after.attempt.attempt_no, 1)
            self.assertEqual(after.attempt.attempt_id, current.attempt.attempt_id)

    def test_fill_is_immutable_and_later_pass_completes_commission(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "execution.sqlite3"
            apply_execution_schema(database)
            attempt_store = SQLiteBrokerAttemptStore(database)
            reconciliation_store = SQLiteBrokerReconciliationStore(database)
            planned = plan_broker_operation(
                command=command_state(),
                position=flat_position(),
                active_contract=active_contract(),
                account_id=ACCOUNT,
                observed_at_utc=T0,
            )
            current = mark_attempt_submitting(
                planned,
                observed_at_utc=T1,
                broker_order_id=ORDER_ID,
            )
            attempt_store.publish_initial(planned)
            attempt_store.publish_state(current)

            order = order_fact(
                order_ref=current.attempt.order_ref,
                status="Filled",
                source=BrokerOrderSource.COMPLETED,
                filled=1,
                remaining=0,
                completed_status="Filled",
            )
            first_snapshot = broker_snapshot(
                completed_orders=(order,),
                fills=(
                    fill_fact(
                        order_ref=current.attempt.order_ref,
                        with_commission=False,
                    ),
                ),
            )
            first_service = ReadOnlyBrokerReconciliationService(
                account_id=ACCOUNT,
                broker_source=ScriptedBrokerReconciliationReader(
                    (first_snapshot,)
                ),
                attempt_source=attempt_store,
                reconciliation_store=reconciliation_store,
            )
            first = asyncio.run(first_service.run_once())
            self.assertEqual(
                first.reconciled[0].after.operation.state,
                BrokerOperationState.SUCCEEDED,
            )
            stored_fills = reconciliation_store.read_fills(
                current.attempt.attempt_id
            )
            self.assertEqual(len(stored_fills), 1)
            self.assertFalse(stored_fills[0].commission_complete)
            self.assertEqual(
                reconciliation_store.read_commission_pending_operation_ids(),
                (current.operation.operation_id,),
            )

            second_snapshot = broker_snapshot(
                completed_orders=(order,),
                fills=(
                    fill_fact(
                        order_ref=current.attempt.order_ref,
                        with_commission=True,
                    ),
                ),
            )
            second_service = ReadOnlyBrokerReconciliationService(
                account_id=ACCOUNT,
                broker_source=ScriptedBrokerReconciliationReader(
                    (second_snapshot,)
                ),
                attempt_source=attempt_store,
                reconciliation_store=reconciliation_store,
            )
            second = asyncio.run(second_service.run_once())
            self.assertEqual(len(second.reconciled), 1)
            complete = reconciliation_store.read_fills(
                current.attempt.attempt_id
            )
            self.assertEqual(len(complete), 1)
            self.assertTrue(complete[0].commission_complete)
            self.assertEqual(
                reconciliation_store.read_commission_pending_operation_ids(),
                (),
            )

            connection = sqlite3.connect(database)
            try:
                fill_rows = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM internal_broker_reconciliation_observations
                    WHERE outcome = 'FILL'
                    """
                ).fetchone()[0]
                commission_rows = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM internal_broker_reconciliation_observations
                    WHERE outcome = 'COMMISSION'
                    """
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(fill_rows, 1)
            self.assertEqual(commission_rows, 1)


if __name__ == "__main__":
    unittest.main()
