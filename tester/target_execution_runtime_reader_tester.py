from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ibmd.execution.adapters.sqlite_runtime import SQLiteExecutionRuntimeReader
from ibmd.public_contracts.decision import StrategyCommandKind


def create_execution_database(path: Path) -> None:
    connection = sqlite3.connect(str(path))
    try:
        connection.executescript(
            """
            CREATE TABLE broker_operations (
                operation_id TEXT PRIMARY KEY,
                command_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                deployment_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at_ts INTEGER NOT NULL
            );
            CREATE VIEW public_broker_order_operations_v1 AS
            SELECT *, NULL AS payload_json FROM broker_operations;

            CREATE TABLE command_states (
                command_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                strategy_version INTEGER NOT NULL,
                deployment_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                command_kind TEXT NOT NULL,
                desired_target_side TEXT NOT NULL,
                desired_target_quantity INTEGER NOT NULL,
                state TEXT NOT NULL,
                received_at_ts INTEGER NOT NULL
            );
            CREATE VIEW public_execution_command_states_v1 AS
            SELECT *, NULL AS payload_json FROM command_states;

            CREATE TABLE episodes (
                position_episode_id TEXT PRIMARY KEY,
                source_operation_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                deployment_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                status TEXT NOT NULL,
                opened_at_ts INTEGER NOT NULL
            );
            CREATE VIEW public_position_episodes_v1 AS
            SELECT *, NULL AS payload_json FROM episodes;

            CREATE TABLE protection_states (
                protection_set_id TEXT PRIMARY KEY,
                position_episode_id TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_at_ts INTEGER NOT NULL
            );
            CREATE VIEW public_protection_states_v1 AS
            SELECT *, NULL AS payload_json FROM protection_states;

            CREATE TABLE protective_orders (
                protective_order_id TEXT PRIMARY KEY,
                protection_set_id TEXT NOT NULL,
                position_episode_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                state TEXT NOT NULL,
                planned_sequence INTEGER NOT NULL
            );
            CREATE VIEW public_protective_orders_v1 AS
            SELECT *, NULL AS payload_json FROM protective_orders;

            CREATE TABLE liquidation_operations (
                liquidation_operation_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                deployment_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                state TEXT NOT NULL,
                next_action TEXT NOT NULL,
                updated_at_ts INTEGER NOT NULL
            );
            CREATE VIEW public_liquidation_operations_v1 AS
            SELECT *, NULL AS payload_json FROM liquidation_operations;

            CREATE TABLE reverse_finalizations (
                source_operation_id TEXT PRIMARY KEY
            );
            CREATE VIEW public_reverse_finalizations_v1 AS
            SELECT source_operation_id, NULL AS payload_json
            FROM reverse_finalizations;
            """
        )
        connection.commit()
    finally:
        connection.close()


def create_decision_database(path: Path) -> None:
    connection = sqlite3.connect(str(path))
    try:
        connection.executescript(
            """
            CREATE TABLE commands (
                command_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                strategy_version INTEGER NOT NULL,
                deployment_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                created_at_ts INTEGER NOT NULL
            );
            CREATE VIEW public_strategy_command_requests_v1 AS
            SELECT *, NULL AS payload_json FROM commands;
            """
        )
        connection.commit()
    finally:
        connection.close()


class SQLiteExecutionRuntimeReaderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.execution = root / "execution.sqlite3"
        self.decision = root / "decision.sqlite3"
        create_execution_database(self.execution)
        create_decision_database(self.decision)
        self.reader = SQLiteExecutionRuntimeReader(
            self.execution,
            self.decision,
        )
        self.reader.validate_schema()

    def tearDown(self) -> None:
        self.directory.cleanup()

    @staticmethod
    def scope() -> dict:
        return {
            "account_id": "DU000000",
            "strategy_id": "IBMarketData.rolling",
            "deployment_id": "paper-test",
            "instrument_id": "MNQ",
        }

    def insert_execution(self, sql: str, values: tuple) -> None:
        connection = sqlite3.connect(str(self.execution))
        try:
            connection.execute(sql, values)
            connection.commit()
        finally:
            connection.close()

    def insert_decision(self, command_id: str, created_at_ts: int) -> None:
        connection = sqlite3.connect(str(self.decision))
        try:
            connection.execute(
                "INSERT INTO commands VALUES (?, ?, ?, ?, ?, ?)",
                (
                    command_id,
                    "IBMarketData.rolling",
                    1,
                    "paper-test",
                    "MNQ",
                    created_at_ts,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def insert_command(
        self,
        command_id: str,
        *,
        kind: str,
        received_at_ts: int,
    ) -> None:
        self.insert_execution(
            "INSERT INTO command_states VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                command_id,
                "IBMarketData.rolling",
                1,
                "paper-test",
                "MNQ",
                kind,
                "SHORT" if kind == "REVERSE" else "LONG",
                1,
                "ADMITTED",
                received_at_ts,
            ),
        )

    def insert_operation(self, operation_id: str, command_id: str) -> None:
        self.insert_execution(
            "INSERT INTO broker_operations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                operation_id,
                command_id,
                "DU000000",
                "IBMarketData.rolling",
                "paper-test",
                "MNQ",
                "SUCCEEDED",
                10,
            ),
        )

    def test_open_episode_and_finalization_candidates(self) -> None:
        self.insert_command("strategy_command_open", kind="OPEN", received_at_ts=1)
        self.insert_operation("broker_operation_open", "strategy_command_open")
        candidate = self.reader.read_next_finalization_candidate(**self.scope())
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.operation_id, "broker_operation_open")
        self.assertEqual(candidate.command_kind, StrategyCommandKind.OPEN)

        self.insert_execution(
            "INSERT INTO episodes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "position_episode_open",
                "broker_operation_open",
                "DU000000",
                "IBMarketData.rolling",
                "paper-test",
                "MNQ",
                "OPEN",
                20,
            ),
        )
        self.assertEqual(
            self.reader.list_open_episode_ids(**self.scope()),
            ("position_episode_open",),
        )
        self.assertIsNone(
            self.reader.read_next_finalization_candidate(**self.scope())
        )

        self.insert_command(
            "strategy_command_reverse",
            kind="REVERSE",
            received_at_ts=2,
        )
        self.insert_operation(
            "broker_operation_reverse",
            "strategy_command_reverse",
        )
        candidate = self.reader.read_next_finalization_candidate(**self.scope())
        self.assertEqual(candidate.command_kind, StrategyCommandKind.REVERSE)
        self.insert_execution(
            "INSERT INTO reverse_finalizations VALUES (?)",
            ("broker_operation_reverse",),
        )
        self.assertIsNone(
            self.reader.read_next_finalization_candidate(**self.scope())
        )

    def test_liquidation_and_protective_priority(self) -> None:
        self.insert_execution(
            "INSERT INTO episodes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "position_episode_a",
                "broker_operation_a",
                "DU000000",
                "IBMarketData.rolling",
                "paper-test",
                "MNQ",
                "OPEN",
                1,
            ),
        )
        self.insert_execution(
            "INSERT INTO protection_states VALUES (?, ?, ?, ?)",
            ("protection_set_a", "position_episode_a", "PLANNED", 1),
        )
        self.insert_execution(
            "INSERT INTO protective_orders VALUES (?, ?, ?, ?, ?, ?)",
            (
                "protective_order_stop",
                "protection_set_a",
                "position_episode_a",
                "STOP_LOSS",
                "PLANNED",
                1,
            ),
        )
        self.insert_execution(
            "INSERT INTO protective_orders VALUES (?, ?, ?, ?, ?, ?)",
            (
                "protective_order_tp",
                "protection_set_a",
                "position_episode_a",
                "TAKE_PROFIT",
                "PLANNED",
                2,
            ),
        )
        pending = self.reader.read_pending_protective_submission(**self.scope())
        self.assertEqual(pending.subject_id, "protective_order_stop")

        self.insert_execution(
            "UPDATE protective_orders SET state='LIVE' "
            "WHERE protective_order_id=?",
            ("protective_order_stop",),
        )
        pending = self.reader.read_pending_protective_submission(**self.scope())
        self.assertEqual(pending.subject_id, "protective_order_tp")

        self.insert_execution(
            "INSERT INTO liquidation_operations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "liquidation_operation_a",
                "DU000000",
                "IBMarketData.rolling",
                "paper-test",
                "MNQ",
                "REQUESTED",
                "CANCEL_EXITS",
                3,
            ),
        )
        liquidation = self.reader.read_active_liquidation(**self.scope())
        self.assertEqual(
            liquidation.subject_id,
            "liquidation_operation_a",
        )

    def test_command_selection_and_submission_candidates(self) -> None:
        self.insert_decision("strategy_command_1", 1)
        self.insert_decision("strategy_command_2", 2)
        self.insert_command("strategy_command_1", kind="OPEN", received_at_ts=1)
        next_id = self.reader.read_next_decision_command_id(
            strategy_id="IBMarketData.rolling",
            strategy_version=1,
            deployment_id="paper-test",
            instrument_id="MNQ",
        )
        self.assertEqual(next_id, "strategy_command_2")

        pending = self.reader.read_pending_strategic_submission(
            strategy_id="IBMarketData.rolling",
            deployment_id="paper-test",
            instrument_id="MNQ",
        )
        self.assertEqual(pending.subject_id, "strategy_command_1")

        self.insert_command(
            "strategy_command_reverse",
            kind="REVERSE",
            received_at_ts=0,
        )
        reverse = self.reader.read_pending_reverse_handoff(
            strategy_id="IBMarketData.rolling",
            deployment_id="paper-test",
            instrument_id="MNQ",
        )
        self.assertEqual(reverse.subject_id, "strategy_command_reverse")

        self.insert_operation("broker_operation_1", "strategy_command_1")
        pending = self.reader.read_pending_strategic_submission(
            strategy_id="IBMarketData.rolling",
            deployment_id="paper-test",
            instrument_id="MNQ",
        )
        self.assertEqual(pending.subject_id, "strategy_command_reverse")


if __name__ == "__main__":
    unittest.main()
