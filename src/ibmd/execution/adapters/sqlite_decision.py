from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ibmd.public_contracts.decision import StrategyCommandRequestV1


class ExecutionDecisionSourceError(RuntimeError):
    pass


class SQLiteExecutionDecisionReader:
    def __init__(
        self,
        database_path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database_path = Path(database_path)
        self.busy_timeout_ms = int(busy_timeout_ms)

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file():
            raise ExecutionDecisionSourceError(
                f"decision database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        return connection

    def validate_schema(self) -> None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'view'
                  AND name = 'public_strategy_command_requests_v1'
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                raise ExecutionDecisionSourceError(
                    "decision public command view is missing"
                )
        except sqlite3.Error as exc:
            raise ExecutionDecisionSourceError(
                f"cannot validate decision command source: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_command(self, command_id: str) -> StrategyCommandRequestV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_strategy_command_requests_v1
                WHERE command_id = ?
                LIMIT 1
                """,
                (str(command_id),),
            ).fetchone()
            if row is None:
                return None
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError as exc:
                raise ExecutionDecisionSourceError(
                    f"stored strategy command JSON is invalid: {exc}"
                ) from exc
            if not isinstance(payload, dict):
                raise ExecutionDecisionSourceError(
                    "stored strategy command payload must be an object"
                )
            return StrategyCommandRequestV1.from_dict(payload)
        finally:
            connection.close()
