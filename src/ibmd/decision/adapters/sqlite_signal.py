from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ibmd.public_contracts.signal import SignalEventV1


class DecisionSignalReadError(RuntimeError):
    pass


class DecisionSignalSchemaError(DecisionSignalReadError):
    pass


def _event_from_payload(payload: str) -> SignalEventV1:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DecisionSignalReadError(
            f"stored signal event JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DecisionSignalReadError(
            "stored signal event payload must be an object"
        )
    return SignalEventV1.from_dict(value)


class SQLiteDecisionSignalReader:
    def __init__(
        self,
        database_path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database_path = Path(database_path)
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file():
            raise DecisionSignalSchemaError(
                f"signal database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(
            f"PRAGMA busy_timeout = {self.busy_timeout_ms}"
        )
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
                  AND name = 'public_signal_events_v1'
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                raise DecisionSignalSchemaError(
                    "signal public view is missing: public_signal_events_v1"
                )
        except sqlite3.Error as exc:
            raise DecisionSignalSchemaError(
                f"cannot validate signal public view: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_event(self, event_id: str) -> SignalEventV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_signal_events_v1
                WHERE event_id = ?
                LIMIT 1
                """,
                (str(event_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _event_from_payload(str(row["payload_json"]))
            )
        except sqlite3.Error as exc:
            raise DecisionSignalReadError(
                f"cannot read target signal event: {exc}"
            ) from exc
        finally:
            connection.close()
