from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from ibmd.decision.domain import DecisionEvaluation
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.decision import (
    DecisionRecordV1,
    StrategyCommandRequestV1,
)

DECISION_STORE_NAME = "decision"
DECISION_SCHEMA_VERSION = 1

_REQUIRED_OBJECTS = {
    ("table", "schema_migrations"),
    ("table", "internal_decision_records"),
    ("table", "internal_strategy_command_requests"),
    ("view", "public_decision_records_v1"),
    ("view", "public_strategy_command_requests_v1"),
}


class DecisionStoreError(RuntimeError):
    pass


class DecisionSchemaError(DecisionStoreError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DecisionStoreError(f"stored {context} JSON is invalid: {exc}") from exc
    if not isinstance(value, dict):
        raise DecisionStoreError(f"stored {context} payload must be an object")
    return value


def _record_from_payload(payload: str) -> DecisionRecordV1:
    return DecisionRecordV1.from_dict(
        _json_object(payload, context="decision record")
    )


def _command_from_payload(payload: str) -> StrategyCommandRequestV1:
    return StrategyCommandRequestV1.from_dict(
        _json_object(payload, context="strategy command")
    )


class SQLiteDecisionReader:
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
            raise DecisionSchemaError(
                f"decision database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(
            f"PRAGMA busy_timeout = {self.busy_timeout_ms}"
        )
        connection.execute("PRAGMA query_only = ON")
        return connection

    def read_record(self, decision_id: str) -> DecisionRecordV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_decision_records_v1
                WHERE decision_id = ?
                LIMIT 1
                """,
                (str(decision_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _record_from_payload(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_command(
        self,
        command_id: str,
    ) -> StrategyCommandRequestV1 | None:
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
            return (
                None
                if row is None
                else _command_from_payload(str(row["payload_json"]))
            )
        finally:
            connection.close()


class SQLiteDecisionStore:
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
        self.reader = SQLiteDecisionReader(
            self.database_path,
            busy_timeout_ms=self.busy_timeout_ms,
        )
        self._writer_lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            f"PRAGMA busy_timeout = {self.busy_timeout_ms}"
        )
        return connection

    def validate_schema(self) -> None:
        if not self.database_path.is_file():
            raise DecisionSchemaError(
                f"decision database does not exist: {self.database_path}"
            )
        connection = self._connect()
        try:
            objects = {
                (str(row["type"]), str(row["name"]))
                for row in connection.execute(
                    """
                    SELECT type, name
                    FROM sqlite_master
                    WHERE type IN ('table', 'view')
                    """
                ).fetchall()
            }
            missing = sorted(_REQUIRED_OBJECTS - objects)
            if missing:
                raise DecisionSchemaError(
                    f"decision schema objects are missing: {missing}"
                )
            versions = [
                int(row["version"])
                for row in connection.execute(
                    """
                    SELECT version
                    FROM schema_migrations
                    WHERE store_name = ?
                    ORDER BY version
                    """,
                    (DECISION_STORE_NAME,),
                ).fetchall()
            ]
            if versions != list(range(1, DECISION_SCHEMA_VERSION + 1)):
                raise DecisionSchemaError(
                    "decision schema version mismatch: "
                    f"expected=1..{DECISION_SCHEMA_VERSION}, actual={versions}"
                )
        except sqlite3.Error as exc:
            raise DecisionSchemaError(
                f"cannot validate decision schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def publish(self, evaluation: DecisionEvaluation) -> DecisionEvaluation:
        record = evaluation.record
        command = evaluation.command
        record_payload = canonical_json_text(record.to_dict())
        fixture_payload = canonical_json_text(evaluation.fixture_payload)
        command_payload = (
            None if command is None else canonical_json_text(command.to_dict())
        )

        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing_record = connection.execute(
                    """
                    SELECT payload_json, input_payload_json
                    FROM internal_decision_records
                    WHERE decision_id = ?
                    LIMIT 1
                    """,
                    (record.decision_id,),
                ).fetchone()
                if existing_record is not None:
                    stored_record = _record_from_payload(
                        str(existing_record["payload_json"])
                    )
                    stored_fixture = canonical_json_text(
                        _json_object(
                            str(existing_record["input_payload_json"]),
                            context="decision fixture",
                        )
                    )
                    if (
                        stored_record.to_dict() != record.to_dict()
                        or stored_fixture != fixture_payload
                    ):
                        raise DecisionStoreError(
                            "conflicting decision record already exists"
                        )
                    stored_command = (
                        None
                        if stored_record.command_id is None
                        else self._read_command_in_transaction(
                            connection,
                            stored_record.command_id,
                        )
                    )
                    connection.rollback()
                    return DecisionEvaluation(
                        record=stored_record,
                        command=stored_command,
                        fixture_payload=_json_object(
                            str(existing_record["input_payload_json"]),
                            context="decision fixture",
                        ),
                    )

                if command is not None:
                    existing_command = connection.execute(
                        """
                        SELECT payload_json
                        FROM internal_strategy_command_requests
                        WHERE command_id = ?
                        LIMIT 1
                        """,
                        (command.command_id,),
                    ).fetchone()
                    if existing_command is not None:
                        stored_command = _command_from_payload(
                            str(existing_command["payload_json"])
                        )
                        if stored_command.to_dict() != command.to_dict():
                            raise DecisionStoreError(
                                "conflicting strategy command already exists"
                            )

                connection.execute(
                    """
                    INSERT INTO internal_decision_records (
                        decision_id,
                        strategy_id,
                        strategy_version,
                        deployment_id,
                        instrument_id,
                        source_signal_id,
                        evaluated_at_ts,
                        evaluated_at_utc,
                        outcome,
                        reason_code,
                        command_id,
                        input_hash,
                        policy_hash,
                        input_payload_json,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.decision_id,
                        record.strategy_id,
                        record.strategy_version,
                        record.deployment_id,
                        record.instrument_id,
                        record.source_signal_id,
                        int(parse_utc(record.evaluated_at_utc).timestamp()),
                        record.evaluated_at_utc,
                        record.outcome.value,
                        record.reason_code,
                        record.command_id,
                        record.input_hash,
                        record.policy_hash,
                        fixture_payload,
                        record_payload,
                    ),
                )

                if command is not None:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO internal_strategy_command_requests (
                            command_id,
                            decision_id,
                            strategy_id,
                            strategy_version,
                            deployment_id,
                            instrument_id,
                            source_signal_id,
                            created_at_ts,
                            expires_at_ts,
                            command_kind,
                            desired_target_side,
                            desired_target_quantity,
                            policy_hash,
                            payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            command.command_id,
                            record.decision_id,
                            command.strategy_id,
                            command.strategy_version,
                            command.deployment_id,
                            command.instrument_id,
                            command.source_signal_id,
                            int(parse_utc(command.created_at_utc).timestamp()),
                            int(parse_utc(command.expires_at_utc).timestamp()),
                            command.command_kind.value,
                            command.desired_target_side.value,
                            command.desired_target_quantity,
                            command.policy_hash,
                            command_payload,
                        ),
                    )

                connection.commit()
                return evaluation
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, DecisionStoreError):
                    raise
                raise DecisionStoreError(
                    "cannot publish decision evaluation: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    @staticmethod
    def _read_command_in_transaction(
        connection: sqlite3.Connection,
        command_id: str,
    ) -> StrategyCommandRequestV1:
        row = connection.execute(
            """
            SELECT payload_json
            FROM internal_strategy_command_requests
            WHERE command_id = ?
            LIMIT 1
            """,
            (str(command_id),),
        ).fetchone()
        if row is None:
            raise DecisionStoreError(
                "decision record references a missing strategy command: "
                f"{command_id}"
            )
        return _command_from_payload(str(row["payload_json"]))

    def read_record(self, decision_id: str) -> DecisionRecordV1 | None:
        return self.reader.read_record(decision_id)

    def read_command(
        self,
        command_id: str,
    ) -> StrategyCommandRequestV1 | None:
        return self.reader.read_command(command_id)
