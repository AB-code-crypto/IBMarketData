from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ib_execution.execution_store import initialize_execution_db


class ExecutionSqliteLockingTester(unittest.TestCase):
    def test_initialization_releases_writer_lock_before_async_work(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "trade.sqlite3"
            first = sqlite3.connect(str(db_path), timeout=0.1)
            second = sqlite3.connect(str(db_path), timeout=0.1)

            try:
                first.execute("PRAGMA journal_mode=WAL")
                second.execute("PRAGMA journal_mode=WAL")

                # Reproduce an upgraded production DB: protective_orders already
                # exists, so legacy cleanup executes DELETE and opens a writer
                # transaction even when only ordinary TP/SL rows remain.
                first.execute(
                    "CREATE TABLE protective_orders (order_ref TEXT NOT NULL)"
                )
                first.execute(
                    "CREATE TABLE slot_loss_extensions (id INTEGER PRIMARY KEY)"
                )
                first.executemany(
                    "INSERT INTO protective_orders(order_ref) VALUES (?)",
                    [
                        ("IBMD_INTENT_1_MNQ_SL",),
                        ("IBMD_INTENT_OLD_MNQ_EXT_SL",),
                    ],
                )
                first.commit()

                initialize_execution_db(first)

                self.assertFalse(
                    first.in_transaction,
                    "execution DB initialization must not retain a writer lock",
                )
                remaining_refs = first.execute(
                    "SELECT order_ref FROM protective_orders ORDER BY order_ref"
                ).fetchall()
                self.assertEqual(
                    remaining_refs,
                    [("IBMD_INTENT_1_MNQ_SL",)],
                )
                legacy_table = first.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='slot_loss_extensions'"
                ).fetchone()
                self.assertIsNone(legacy_table)

                # This is the parallel position-sync/daily-PnL write that used to
                # fail with OperationalError: database is locked.
                second.execute(
                    """
                    INSERT INTO positions_latest (
                        instrument_code,
                        side,
                        quantity,
                        broker_contract,
                        broker_con_id,
                        broker_account,
                        contract_is_active,
                        updated_at_ts,
                        updated_at_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "MNQ",
                        "FLAT",
                        0.0,
                        None,
                        None,
                        "DU_TEST",
                        None,
                        1,
                        "1970-01-01 00:00:01",
                    ),
                )
                second.commit()
            finally:
                second.close()
                first.close()


if __name__ == "__main__":
    unittest.main()
