from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.execution.domain.ib_reconciliation import (
    BrokerAttemptReconciliationResult,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.public_contracts.broker_execution import (
    BrokerOrderAttemptV1,
    BrokerOrderOperationV1,
)
from ibmd.public_contracts.broker_reconciliation import BrokerFillFactV1

from .sqlite_broker_reconciliation import (
    BrokerReconciliationStoreError,
    SQLiteBrokerReconciliationReader,
    _attempt,
    _base_fill,
    _commission_observation_id,
    _fill_observation_id,
    _observation_id,
    _operation,
    _transition_id,
    _ts,
)


class SQLiteBrokerReconciliationStore:
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
        self.reader = SQLiteBrokerReconciliationReader(
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
        from_state: str,
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
                operation.blocking_reason,
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
        from_state: str,
        operation_reason: str | None,
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
                attempt.failure_reason or operation_reason,
                _ts(attempt.updated_at_utc),
                attempt.updated_at_utc,
                canonical_json_text(attempt.to_dict()),
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
            raise BrokerReconciliationStoreError(
                f"broker operation does not exist: {operation.operation_id}"
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
            raise BrokerReconciliationStoreError(
                f"broker attempt does not exist: {attempt.attempt_id}"
            )

    @classmethod
    def _append_evidence(
        cls,
        connection: sqlite3.Connection,
        *,
        observation_id: str,
        operation_id: str,
        attempt_id: str,
        outcome: str,
        observed_at_utc: str,
        payload: str,
    ) -> None:
        existing = connection.execute(
            """
            SELECT operation_id, attempt_id, outcome, payload_json
            FROM internal_broker_reconciliation_observations
            WHERE observation_id = ?
            LIMIT 1
            """,
            (observation_id,),
        ).fetchone()
        if existing is not None:
            if (
                str(existing["operation_id"]) != operation_id
                or str(existing["attempt_id"]) != attempt_id
                or str(existing["outcome"]) != outcome
                or str(existing["payload_json"]) != payload
            ):
                raise BrokerReconciliationStoreError(
                    "stable broker evidence identity conflicted: "
                    f"{observation_id}"
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
                outcome,
                _ts(observed_at_utc),
                observed_at_utc,
                payload,
            ),
        )

    @classmethod
    def _append_fill_evidence(
        cls,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        attempt_id: str,
        fill: BrokerFillFactV1,
    ) -> None:
        base = _base_fill(fill)
        cls._append_evidence(
            connection,
            observation_id=_fill_observation_id(base.exec_id),
            operation_id=operation_id,
            attempt_id=attempt_id,
            outcome="FILL",
            observed_at_utc=base.observed_at_utc,
            payload=canonical_json_text(base.to_dict()),
        )
        if fill.commission is not None:
            cls._append_evidence(
                connection,
                observation_id=_commission_observation_id(fill.exec_id),
                operation_id=operation_id,
                attempt_id=attempt_id,
                outcome="COMMISSION",
                observed_at_utc=fill.commission.reported_at_utc,
                payload=canonical_json_text(fill.commission.to_dict()),
            )

    @classmethod
    def _append_order_observation(
        cls,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        attempt_id: str,
        result: BrokerAttemptReconciliationResult,
    ) -> None:
        evidence_payload = canonical_json_text(
            {
                "observation": result.observation.to_dict(),
                "source_session_id": result.source_session_id,
                "captured_at_utc": result.captured_at_utc,
                "exec_ids": [item.exec_id for item in result.fills],
                "commission_complete": result.commission_complete,
            }
        )
        cls._append_evidence(
            connection,
            observation_id=_observation_id(attempt_id, evidence_payload),
            operation_id=operation_id,
            attempt_id=attempt_id,
            outcome=result.observation.outcome.value,
            observed_at_utc=result.observation.observed_at_utc,
            payload=evidence_payload,
        )

    def publish(
        self,
        *,
        current: BrokerOperationSnapshot,
        updated: BrokerOperationSnapshot,
        result: BrokerAttemptReconciliationResult,
    ) -> BrokerOperationSnapshot:
        if current.operation.operation_id != updated.operation.operation_id:
            raise BrokerReconciliationStoreError(
                "current and updated operation identities differ"
            )
        if current.attempt.attempt_id != updated.attempt.attempt_id:
            raise BrokerReconciliationStoreError(
                "reconciliation cannot replace the current attempt"
            )
        if result.observation.order_ref != current.attempt.order_ref:
            raise BrokerReconciliationStoreError(
                "reconciliation result order_ref differs from current attempt"
            )

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
                    (current.operation.operation_id,),
                ).fetchone()
                attempt_row = connection.execute(
                    """
                    SELECT payload_json
                    FROM internal_broker_order_attempts
                    WHERE attempt_id = ?
                    LIMIT 1
                    """,
                    (current.attempt.attempt_id,),
                ).fetchone()
                if operation_row is None or attempt_row is None:
                    raise BrokerReconciliationStoreError(
                        "current broker operation/attempt is missing from execution store"
                    )
                stored_operation = _operation(
                    str(operation_row["payload_json"])
                )
                stored_attempt = _attempt(str(attempt_row["payload_json"]))
                if stored_operation != current.operation or stored_attempt != current.attempt:
                    raise BrokerReconciliationStoreError(
                        "broker operation changed concurrently before reconciliation commit"
                    )

                for fill in result.fills:
                    self._append_fill_evidence(
                        connection,
                        operation_id=current.operation.operation_id,
                        attempt_id=current.attempt.attempt_id,
                        fill=fill,
                    )

                if stored_attempt.to_dict() != updated.attempt.to_dict():
                    self._update_attempt(connection, updated.attempt)
                    if stored_attempt.state != updated.attempt.state:
                        self._append_attempt_transition(
                            connection,
                            attempt=updated.attempt,
                            from_state=stored_attempt.state.value,
                            operation_reason=updated.operation.blocking_reason,
                        )
                if stored_operation.to_dict() != updated.operation.to_dict():
                    self._update_operation(connection, updated.operation)
                    if stored_operation.state != updated.operation.state:
                        self._append_operation_transition(
                            connection,
                            operation=updated.operation,
                            from_state=stored_operation.state.value,
                        )
                self._append_order_observation(
                    connection,
                    operation_id=current.operation.operation_id,
                    attempt_id=current.attempt.attempt_id,
                    result=result,
                )
                connection.commit()
                return updated
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, BrokerReconciliationStoreError):
                    raise
                raise BrokerReconciliationStoreError(
                    "cannot publish read-only broker reconciliation: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def read_fills(self, attempt_id: str) -> tuple[BrokerFillFactV1, ...]:
        return self.reader.read_fills(attempt_id)

    def read_commission_pending_operation_ids(self) -> tuple[str, ...]:
        return self.reader.read_commission_pending_operation_ids()
