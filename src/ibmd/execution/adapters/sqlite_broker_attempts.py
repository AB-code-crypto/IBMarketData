from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from pathlib import Path

from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.broker_execution import (
    BrokerOrderAttemptV1,
    BrokerOrderObservationV1,
    BrokerOrderOperationV1,
)

BROKER_ATTEMPT_SCHEMA_VERSION = 2
_REQUIRED_OBJECTS = {
    ("table", "schema_migrations"),
    ("table", "internal_broker_order_operations"),
    ("table", "internal_broker_order_attempts"),
    ("table", "internal_broker_operation_transitions"),
    ("table", "internal_broker_attempt_transitions"),
    ("table", "internal_broker_reconciliation_observations"),
    ("view", "public_broker_order_operations_v1"),
    ("view", "public_broker_order_attempts_v1"),
}


class BrokerAttemptStoreError(RuntimeError):
    pass


class BrokerAttemptSchemaError(BrokerAttemptStoreError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise BrokerAttemptStoreError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise BrokerAttemptStoreError(f"stored {context} payload must be an object")
    return value


def _operation(payload: str) -> BrokerOrderOperationV1:
    return BrokerOrderOperationV1.from_dict(
        _json_object(payload, context="broker operation")
    )


def _attempt(payload: str) -> BrokerOrderAttemptV1:
    return BrokerOrderAttemptV1.from_dict(
        _json_object(payload, context="broker attempt")
    )


def _stable_id(kind: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]
    return f"{kind}_{digest}"


def _transition_id(entity_id: str, sequence_no: int, state: str) -> str:
    return _stable_id(
        "broker_transition",
        f"{entity_id}:{int(sequence_no)}:{state}",
    )


def _observation_id(
    attempt_id: str,
    payload: str,
) -> str:
    return _stable_id("broker_observation", f"{attempt_id}:{payload}")


def _ts(value: str | None) -> int | None:
    return None if value is None else int(parse_utc(value).timestamp())


class SQLiteBrokerAttemptReader:
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
            raise BrokerAttemptSchemaError(
                f"execution database does not exist: {self.database_path}"
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
                raise BrokerAttemptSchemaError(
                    f"broker-attempt schema objects are missing: {missing}"
                )
            versions = [
                int(row["version"])
                for row in connection.execute(
                    """
                    SELECT version
                    FROM schema_migrations
                    WHERE store_name = 'execution'
                    ORDER BY version
                    """
                ).fetchall()
            ]
            expected = list(range(1, BROKER_ATTEMPT_SCHEMA_VERSION + 1))
            if versions != expected:
                raise BrokerAttemptSchemaError(
                    "execution broker-attempt schema version mismatch: "
                    f"expected={expected}, actual={versions}"
                )
        except sqlite3.Error as exc:
            raise BrokerAttemptSchemaError(
                f"cannot validate broker-attempt schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_operation(self, operation_id: str) -> BrokerOrderOperationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_broker_order_operations_v1
                WHERE operation_id = ?
                LIMIT 1
                """,
                (str(operation_id),),
            ).fetchone()
            return None if row is None else _operation(str(row["payload_json"]))
        finally:
            connection.close()

    def read_operation_by_command(
        self,
        command_id: str,
    ) -> BrokerOrderOperationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_broker_order_operations_v1
                WHERE command_id = ?
                LIMIT 1
                """,
                (str(command_id),),
            ).fetchone()
            return None if row is None else _operation(str(row["payload_json"]))
        finally:
            connection.close()

    def read_attempt(self, attempt_id: str) -> BrokerOrderAttemptV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_broker_order_attempts_v1
                WHERE attempt_id = ?
                LIMIT 1
                """,
                (str(attempt_id),),
            ).fetchone()
            return None if row is None else _attempt(str(row["payload_json"]))
        finally:
            connection.close()

    def read_snapshot(self, operation_id: str) -> BrokerOperationSnapshot | None:
        operation = self.read_operation(operation_id)
        if operation is None:
            return None
        attempt = self.read_attempt(operation.current_attempt_id)
        if attempt is None:
            raise BrokerAttemptStoreError(
                "broker operation references a missing current attempt: "
                f"operation={operation.operation_id}, "
                f"attempt={operation.current_attempt_id}"
            )
        return BrokerOperationSnapshot(operation=operation, attempt=attempt)

    def read_unresolved(self) -> tuple[BrokerOperationSnapshot, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT operation_id
                FROM public_broker_order_operations_v1
                WHERE state NOT IN ('SUCCEEDED', 'FAILED_OPERATOR_REQUIRED')
                ORDER BY created_at_ts, operation_id
                """
            ).fetchall()
        finally:
            connection.close()
        values = []
        for row in rows:
            snapshot = self.read_snapshot(str(row["operation_id"]))
            if snapshot is None:
                raise BrokerAttemptStoreError(
                    "unresolved operation disappeared during read"
                )
            values.append(snapshot)
        return tuple(values)


class SQLiteBrokerAttemptStore:
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
        self.reader = SQLiteBrokerAttemptReader(
            self.database_path,
            busy_timeout_ms=self.busy_timeout_ms,
        )
        self._writer_lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        return connection

    def validate_schema(self) -> None:
        self.reader.validate_schema()

    @staticmethod
    def _insert_operation(
        connection: sqlite3.Connection,
        operation: BrokerOrderOperationV1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO internal_broker_order_operations (
                operation_id,
                command_id,
                account_id,
                strategy_id,
                strategy_version,
                deployment_id,
                instrument_id,
                side,
                order_type,
                con_id,
                local_symbol,
                requested_qty,
                filled_qty,
                remaining_qty,
                state,
                current_attempt_id,
                current_attempt_no,
                created_at_ts,
                created_at_utc,
                updated_at_ts,
                updated_at_utc,
                terminal_at_ts,
                terminal_at_utc,
                blocking_reason,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation.operation_id,
                operation.command_id,
                operation.account_id,
                operation.strategy_id,
                operation.strategy_version,
                operation.deployment_id,
                operation.instrument_id,
                operation.side.value,
                operation.order_type,
                operation.con_id,
                operation.local_symbol,
                operation.requested_qty,
                operation.filled_qty,
                operation.remaining_qty,
                operation.state.value,
                operation.current_attempt_id,
                operation.current_attempt_no,
                _ts(operation.created_at_utc),
                operation.created_at_utc,
                _ts(operation.updated_at_utc),
                operation.updated_at_utc,
                _ts(operation.terminal_at_utc),
                operation.terminal_at_utc,
                operation.blocking_reason,
                canonical_json_text(operation.to_dict()),
            ),
        )

    @staticmethod
    def _update_operation(
        connection: sqlite3.Connection,
        operation: BrokerOrderOperationV1,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE internal_broker_order_operations
            SET
                filled_qty = ?,
                remaining_qty = ?,
                state = ?,
                current_attempt_id = ?,
                current_attempt_no = ?,
                updated_at_ts = ?,
                updated_at_utc = ?,
                terminal_at_ts = ?,
                terminal_at_utc = ?,
                blocking_reason = ?,
                payload_json = ?
            WHERE operation_id = ?
            """,
            (
                operation.filled_qty,
                operation.remaining_qty,
                operation.state.value,
                operation.current_attempt_id,
                operation.current_attempt_no,
                _ts(operation.updated_at_utc),
                operation.updated_at_utc,
                _ts(operation.terminal_at_utc),
                operation.terminal_at_utc,
                operation.blocking_reason,
                canonical_json_text(operation.to_dict()),
                operation.operation_id,
            ),
        )
        if cursor.rowcount != 1:
            raise BrokerAttemptStoreError(
                f"broker operation does not exist: {operation.operation_id}"
            )

    @staticmethod
    def _insert_attempt(
        connection: sqlite3.Connection,
        attempt: BrokerOrderAttemptV1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO internal_broker_order_attempts (
                attempt_id,
                operation_id,
                attempt_no,
                order_ref,
                side,
                order_type,
                con_id,
                local_symbol,
                requested_qty,
                filled_qty,
                remaining_qty,
                state,
                broker_order_id,
                broker_perm_id,
                broker_status,
                broker_terminal_proven,
                created_at_ts,
                created_at_utc,
                updated_at_ts,
                updated_at_utc,
                terminal_at_ts,
                terminal_at_utc,
                last_broker_proof_at_ts,
                last_broker_proof_at_utc,
                failure_reason,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attempt.attempt_id,
                attempt.operation_id,
                attempt.attempt_no,
                attempt.order_ref,
                attempt.side.value,
                attempt.order_type,
                attempt.con_id,
                attempt.local_symbol,
                attempt.requested_qty,
                attempt.filled_qty,
                attempt.remaining_qty,
                attempt.state.value,
                attempt.broker_order_id,
                attempt.broker_perm_id,
                attempt.broker_status,
                int(attempt.broker_terminal_proven),
                _ts(attempt.created_at_utc),
                attempt.created_at_utc,
                _ts(attempt.updated_at_utc),
                attempt.updated_at_utc,
                _ts(attempt.terminal_at_utc),
                attempt.terminal_at_utc,
                _ts(attempt.last_broker_proof_at_utc),
                attempt.last_broker_proof_at_utc,
                attempt.failure_reason,
                canonical_json_text(attempt.to_dict()),
            ),
        )

    @staticmethod
    def _update_attempt(
        connection: sqlite3.Connection,
        attempt: BrokerOrderAttemptV1,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE internal_broker_order_attempts
            SET
                filled_qty = ?,
                remaining_qty = ?,
                state = ?,
                broker_order_id = ?,
                broker_perm_id = ?,
                broker_status = ?,
                broker_terminal_proven = ?,
                updated_at_ts = ?,
                updated_at_utc = ?,
                terminal_at_ts = ?,
                terminal_at_utc = ?,
                last_broker_proof_at_ts = ?,
                last_broker_proof_at_utc = ?,
                failure_reason = ?,
                payload_json = ?
            WHERE attempt_id = ?
            """,
            (
                attempt.filled_qty,
                attempt.remaining_qty,
                attempt.state.value,
                attempt.broker_order_id,
                attempt.broker_perm_id,
                attempt.broker_status,
                int(attempt.broker_terminal_proven),
                _ts(attempt.updated_at_utc),
                attempt.updated_at_utc,
                _ts(attempt.terminal_at_utc),
                attempt.terminal_at_utc,
                _ts(attempt.last_broker_proof_at_utc),
                attempt.last_broker_proof_at_utc,
                attempt.failure_reason,
                canonical_json_text(attempt.to_dict()),
                attempt.attempt_id,
            ),
        )
        if cursor.rowcount != 1:
            raise BrokerAttemptStoreError(
                f"broker attempt does not exist: {attempt.attempt_id}"
            )

    @staticmethod
    def _next_sequence(
        connection: sqlite3.Connection,
        *,
        table: str,
        id_column: str,
        entity_id: str,
    ) -> int:
        row = connection.execute(
            f"SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM {table} "
            f"WHERE {id_column} = ?",
            (entity_id,),
        ).fetchone()
        return int(row[0])

    @classmethod
    def _append_operation_transition(
        cls,
        connection: sqlite3.Connection,
        *,
        operation: BrokerOrderOperationV1,
        from_state: str | None,
        reason: str | None,
    ) -> None:
        sequence = cls._next_sequence(
            connection,
            table="internal_broker_operation_transitions",
            id_column="operation_id",
            entity_id=operation.operation_id,
        )
        connection.execute(
            """
            INSERT INTO internal_broker_operation_transitions (
                transition_id,
                operation_id,
                sequence_no,
                from_state,
                to_state,
                reason,
                occurred_at_ts,
                occurred_at_utc,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _transition_id(
                    operation.operation_id,
                    sequence,
                    operation.state.value,
                ),
                operation.operation_id,
                sequence,
                from_state,
                operation.state.value,
                reason,
                _ts(operation.updated_at_utc),
                operation.updated_at_utc,
                canonical_json_text(operation.to_dict()),
            ),
        )

    @classmethod
    def _append_attempt_transition(
        cls,
        connection: sqlite3.Connection,
        *,
        attempt: BrokerOrderAttemptV1,
        from_state: str | None,
        reason: str | None,
    ) -> None:
        sequence = cls._next_sequence(
            connection,
            table="internal_broker_attempt_transitions",
            id_column="attempt_id",
            entity_id=attempt.attempt_id,
        )
        connection.execute(
            """
            INSERT INTO internal_broker_attempt_transitions (
                transition_id,
                attempt_id,
                sequence_no,
                from_state,
                to_state,
                reason,
                occurred_at_ts,
                occurred_at_utc,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _transition_id(
                    attempt.attempt_id,
                    sequence,
                    attempt.state.value,
                ),
                attempt.attempt_id,
                sequence,
                from_state,
                attempt.state.value,
                reason,
                _ts(attempt.updated_at_utc),
                attempt.updated_at_utc,
                canonical_json_text(attempt.to_dict()),
            ),
        )

    @classmethod
    def _append_observation(
        cls,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        attempt_id: str,
        observation: BrokerOrderObservationV1,
    ) -> None:
        payload = canonical_json_text(observation.to_dict())
        observation_id = _observation_id(attempt_id, payload)
        existing = connection.execute(
            """
            SELECT payload_json
            FROM internal_broker_reconciliation_observations
            WHERE observation_id = ?
            LIMIT 1
            """,
            (observation_id,),
        ).fetchone()
        if existing is not None:
            if str(existing["payload_json"]) != payload:
                raise BrokerAttemptStoreError(
                    "stable reconciliation observation identity conflicted"
                )
            return
        sequence = cls._next_sequence(
            connection,
            table="internal_broker_reconciliation_observations",
            id_column="attempt_id",
            entity_id=attempt_id,
        )
        connection.execute(
            """
            INSERT INTO internal_broker_reconciliation_observations (
                observation_id,
                operation_id,
                attempt_id,
                sequence_no,
                outcome,
                observed_at_ts,
                observed_at_utc,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation_id,
                operation_id,
                attempt_id,
                sequence,
                observation.outcome.value,
                _ts(observation.observed_at_utc),
                observation.observed_at_utc,
                payload,
            ),
        )

    def publish_initial(
        self,
        snapshot: BrokerOperationSnapshot,
    ) -> BrokerOperationSnapshot:
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT payload_json
                    FROM internal_broker_order_operations
                    WHERE operation_id = ? OR command_id = ?
                    LIMIT 1
                    """,
                    (
                        snapshot.operation.operation_id,
                        snapshot.operation.command_id,
                    ),
                ).fetchone()
                if existing is not None:
                    stored_operation = _operation(str(existing["payload_json"]))
                    stored_attempt_row = connection.execute(
                        """
                        SELECT payload_json
                        FROM internal_broker_order_attempts
                        WHERE attempt_id = ?
                        LIMIT 1
                        """,
                        (stored_operation.current_attempt_id,),
                    ).fetchone()
                    if stored_attempt_row is None:
                        raise BrokerAttemptStoreError(
                            "stored operation references missing attempt"
                        )
                    stored = BrokerOperationSnapshot(
                        operation=stored_operation,
                        attempt=_attempt(str(stored_attempt_row["payload_json"])),
                    )
                    if stored != snapshot:
                        raise BrokerAttemptStoreError(
                            "conflicting broker operation already exists"
                        )
                    connection.rollback()
                    return stored

                self._insert_operation(connection, snapshot.operation)
                self._insert_attempt(connection, snapshot.attempt)
                self._append_operation_transition(
                    connection,
                    operation=snapshot.operation,
                    from_state=None,
                    reason=None,
                )
                self._append_attempt_transition(
                    connection,
                    attempt=snapshot.attempt,
                    from_state=None,
                    reason=None,
                )
                connection.commit()
                return snapshot
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, BrokerAttemptStoreError):
                    raise
                raise BrokerAttemptStoreError(
                    "cannot publish initial broker operation: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def publish_state(
        self,
        snapshot: BrokerOperationSnapshot,
        *,
        observation: BrokerOrderObservationV1 | None = None,
    ) -> BrokerOperationSnapshot:
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                operation_row = connection.execute(
                    """
                    SELECT payload_json
                    FROM internal_broker_order_operations
                    WHERE operation_id = ?
                    LIMIT 1
                    """,
                    (snapshot.operation.operation_id,),
                ).fetchone()
                if operation_row is None:
                    raise BrokerAttemptStoreError(
                        "broker operation must be created before state updates"
                    )
                stored_operation = _operation(str(operation_row["payload_json"]))
                immutable_operation = (
                    stored_operation.command_id,
                    stored_operation.account_id,
                    stored_operation.strategy_id,
                    stored_operation.strategy_version,
                    stored_operation.deployment_id,
                    stored_operation.instrument_id,
                    stored_operation.side,
                    stored_operation.order_type,
                    stored_operation.con_id,
                    stored_operation.local_symbol,
                    stored_operation.requested_qty,
                    stored_operation.created_at_utc,
                )
                incoming_operation = (
                    snapshot.operation.command_id,
                    snapshot.operation.account_id,
                    snapshot.operation.strategy_id,
                    snapshot.operation.strategy_version,
                    snapshot.operation.deployment_id,
                    snapshot.operation.instrument_id,
                    snapshot.operation.side,
                    snapshot.operation.order_type,
                    snapshot.operation.con_id,
                    snapshot.operation.local_symbol,
                    snapshot.operation.requested_qty,
                    snapshot.operation.created_at_utc,
                )
                if immutable_operation != incoming_operation:
                    raise BrokerAttemptStoreError(
                        "broker operation immutable fields changed"
                    )

                attempt_row = connection.execute(
                    """
                    SELECT payload_json
                    FROM internal_broker_order_attempts
                    WHERE attempt_id = ?
                    LIMIT 1
                    """,
                    (snapshot.attempt.attempt_id,),
                ).fetchone()
                new_attempt = attempt_row is None
                stored_attempt = (
                    None
                    if attempt_row is None
                    else _attempt(str(attempt_row["payload_json"]))
                )
                if new_attempt:
                    if (
                        snapshot.attempt.attempt_no
                        != stored_operation.current_attempt_no + 1
                    ):
                        raise BrokerAttemptStoreError(
                            "new broker attempt number is not sequential"
                        )
                    self._insert_attempt(connection, snapshot.attempt)
                    self._append_attempt_transition(
                        connection,
                        attempt=snapshot.attempt,
                        from_state=None,
                        reason=None,
                    )
                else:
                    immutable_attempt = (
                        stored_attempt.operation_id,
                        stored_attempt.attempt_no,
                        stored_attempt.order_ref,
                        stored_attempt.side,
                        stored_attempt.order_type,
                        stored_attempt.con_id,
                        stored_attempt.local_symbol,
                        stored_attempt.requested_qty,
                        stored_attempt.created_at_utc,
                    )
                    incoming_attempt = (
                        snapshot.attempt.operation_id,
                        snapshot.attempt.attempt_no,
                        snapshot.attempt.order_ref,
                        snapshot.attempt.side,
                        snapshot.attempt.order_type,
                        snapshot.attempt.con_id,
                        snapshot.attempt.local_symbol,
                        snapshot.attempt.requested_qty,
                        snapshot.attempt.created_at_utc,
                    )
                    if immutable_attempt != incoming_attempt:
                        raise BrokerAttemptStoreError(
                            "broker attempt immutable fields changed"
                        )
                    if stored_attempt.to_dict() != snapshot.attempt.to_dict():
                        self._update_attempt(connection, snapshot.attempt)
                        if stored_attempt.state != snapshot.attempt.state:
                            self._append_attempt_transition(
                                connection,
                                attempt=snapshot.attempt,
                                from_state=stored_attempt.state.value,
                                reason=(
                                    snapshot.attempt.failure_reason
                                    or snapshot.operation.blocking_reason
                                ),
                            )

                if stored_operation.to_dict() != snapshot.operation.to_dict():
                    self._update_operation(connection, snapshot.operation)
                    if stored_operation.state != snapshot.operation.state:
                        self._append_operation_transition(
                            connection,
                            operation=snapshot.operation,
                            from_state=stored_operation.state.value,
                            reason=snapshot.operation.blocking_reason,
                        )

                if observation is not None:
                    self._append_observation(
                        connection,
                        operation_id=snapshot.operation.operation_id,
                        attempt_id=snapshot.attempt.attempt_id,
                        observation=observation,
                    )
                connection.commit()
                return snapshot
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, BrokerAttemptStoreError):
                    raise
                raise BrokerAttemptStoreError(
                    "cannot publish broker operation state: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def read_snapshot(self, operation_id: str) -> BrokerOperationSnapshot | None:
        return self.reader.read_snapshot(operation_id)

    def read_operation_by_command(
        self,
        command_id: str,
    ) -> BrokerOrderOperationV1 | None:
        return self.reader.read_operation_by_command(command_id)

    def read_unresolved(self) -> tuple[BrokerOperationSnapshot, ...]:
        return self.reader.read_unresolved()

    def transition_counts(self, snapshot: BrokerOperationSnapshot) -> tuple[int, int]:
        connection = self._connect()
        try:
            operation_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM internal_broker_operation_transitions
                    WHERE operation_id = ?
                    """,
                    (snapshot.operation.operation_id,),
                ).fetchone()[0]
            )
            attempt_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM internal_broker_attempt_transitions
                    WHERE attempt_id = ?
                    """,
                    (snapshot.attempt.attempt_id,),
                ).fetchone()[0]
            )
            return operation_count, attempt_count
        finally:
            connection.close()

    def observation_count(self, attempt_id: str) -> int:
        connection = self._connect()
        try:
            return int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM internal_broker_reconciliation_observations
                    WHERE attempt_id = ?
                    """,
                    (str(attempt_id),),
                ).fetchone()[0]
            )
        finally:
            connection.close()
