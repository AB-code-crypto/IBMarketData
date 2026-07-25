from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import replace
from pathlib import Path

from ibmd.execution.domain.liquidation import (
    LiquidationRequestResult,
    LiquidationSnapshot,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.broker_execution import BrokerOrderObservationV1
from ibmd.public_contracts.broker_reconciliation import (
    BrokerCommissionFactV1,
    BrokerFillFactV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import (
    LiquidationAttemptV1,
    LiquidationOperationV1,
    LiquidationTriggerV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeV1,
    ProtectionStateV1,
)

from .sqlite_protection import (
    ProtectionStoreError,
    SQLiteProtectionStore,
    _episode,
    _protection,
    _ts,
)

_COMPONENT_NAME = "execution_liquidation"
_COMPONENT_VERSION = 1
_COMPONENT_LEDGER = "execution_target_schema_components"
_REQUIRED_OBJECTS = {
    ("table", "internal_liquidation_operations"),
    ("table", "internal_liquidation_attempts"),
    ("table", "internal_liquidation_triggers"),
    ("table", "internal_liquidation_operation_transitions"),
    ("table", "internal_liquidation_attempt_transitions"),
    ("table", "internal_liquidation_reconciliation_observations"),
    ("table", "internal_liquidation_fill_evidence"),
    ("table", "internal_liquidation_commission_evidence"),
    ("view", "public_liquidation_operations_v1"),
    ("view", "public_liquidation_attempts_v1"),
    ("view", "public_liquidation_triggers_v1"),
    ("view", "public_liquidation_fills_v1"),
}


class LiquidationStoreError(ProtectionStoreError):
    pass


class LiquidationSchemaError(LiquidationStoreError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise LiquidationStoreError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise LiquidationStoreError(
            f"stored {context} payload must be an object"
        )
    return value


def _operation(payload: str) -> LiquidationOperationV1:
    return LiquidationOperationV1.from_dict(
        _json_object(payload, context="liquidation operation")
    )


def _attempt(payload: str) -> LiquidationAttemptV1:
    return LiquidationAttemptV1.from_dict(
        _json_object(payload, context="liquidation attempt")
    )


def _trigger(payload: str) -> LiquidationTriggerV1:
    return LiquidationTriggerV1.from_dict(
        _json_object(payload, context="liquidation trigger")
    )


def _readiness(payload: str) -> ExecutionReadinessV1:
    return ExecutionReadinessV1.from_dict(
        _json_object(payload, context="execution readiness")
    )


def _position(payload: str) -> StrategyPositionV1:
    return StrategyPositionV1.from_dict(
        _json_object(payload, context="strategy position")
    )


def _stable_id(kind: str, payload: object) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


class SQLiteLiquidationStore(SQLiteProtectionStore):
    def __init__(
        self,
        database_path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        super().__init__(database_path, busy_timeout_ms=busy_timeout_ms)
        self._liquidation_lock = threading.RLock()

    def validate_schema(self) -> None:
        super().validate_schema()
        connection = self._connect()
        try:
            objects = {
                (str(row["type"]), str(row["name"]))
                for row in connection.execute(
                    "SELECT type, name FROM sqlite_master "
                    "WHERE type IN ('table', 'view')"
                ).fetchall()
            }
            missing = sorted(_REQUIRED_OBJECTS - objects)
            if missing:
                raise LiquidationSchemaError(
                    f"liquidation schema objects are missing: {missing}"
                )
            row = connection.execute(
                f"SELECT component_version FROM {_COMPONENT_LEDGER} "
                "WHERE component_name=? LIMIT 1",
                (_COMPONENT_NAME,),
            ).fetchone()
            if row is None or int(row["component_version"]) != _COMPONENT_VERSION:
                raise LiquidationSchemaError(
                    "liquidation component is not installed: "
                    f"expected={_COMPONENT_NAME}@{_COMPONENT_VERSION}"
                )
        except sqlite3.Error as exc:
            raise LiquidationSchemaError(
                f"cannot validate liquidation schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_operation_by_episode(
        self,
        position_episode_id: str,
    ) -> LiquidationOperationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_liquidation_operations_v1 "
                "WHERE position_episode_id=? LIMIT 1",
                (str(position_episode_id),),
            ).fetchone()
            return None if row is None else _operation(str(row["payload_json"]))
        finally:
            connection.close()

    def read_operation(
        self,
        liquidation_operation_id: str,
    ) -> LiquidationOperationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_liquidation_operations_v1 "
                "WHERE liquidation_operation_id=? LIMIT 1",
                (str(liquidation_operation_id),),
            ).fetchone()
            return None if row is None else _operation(str(row["payload_json"]))
        finally:
            connection.close()

    def read_attempt(
        self,
        liquidation_attempt_id: str,
    ) -> LiquidationAttemptV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_liquidation_attempts_v1 "
                "WHERE liquidation_attempt_id=? LIMIT 1",
                (str(liquidation_attempt_id),),
            ).fetchone()
            return None if row is None else _attempt(str(row["payload_json"]))
        finally:
            connection.close()

    def read_triggers(
        self,
        liquidation_operation_id: str,
    ) -> tuple[LiquidationTriggerV1, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT payload_json FROM public_liquidation_triggers_v1 "
                "WHERE liquidation_operation_id=? "
                "ORDER BY triggered_at_ts, trigger_id",
                (str(liquidation_operation_id),),
            ).fetchall()
            return tuple(_trigger(str(row["payload_json"])) for row in rows)
        finally:
            connection.close()

    def read_snapshot_by_episode(
        self,
        position_episode_id: str,
    ) -> LiquidationSnapshot | None:
        operation = self.read_operation_by_episode(position_episode_id)
        if operation is None:
            return None
        attempt = (
            None
            if operation.current_attempt_id is None
            else self.read_attempt(operation.current_attempt_id)
        )
        if operation.current_attempt_id is not None and attempt is None:
            raise LiquidationStoreError(
                "liquidation operation references a missing attempt: "
                f"{operation.current_attempt_id}"
            )
        return LiquidationSnapshot(
            operation=operation,
            attempt=attempt,
            triggers=self.read_triggers(operation.liquidation_operation_id),
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
            f"WHERE {id_column}=?",
            (entity_id,),
        ).fetchone()
        return int(row[0])

    @classmethod
    def _append_operation_transition(
        cls,
        connection: sqlite3.Connection,
        *,
        operation: LiquidationOperationV1,
        from_state: str | None,
    ) -> None:
        sequence = cls._next_sequence(
            connection,
            table="internal_liquidation_operation_transitions",
            id_column="liquidation_operation_id",
            entity_id=operation.liquidation_operation_id,
        )
        payload = canonical_json_text(operation.to_dict())
        connection.execute(
            """
            INSERT INTO internal_liquidation_operation_transitions (
                transition_id, liquidation_operation_id, sequence_no,
                from_state, to_state, reason, occurred_at_ts,
                occurred_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _stable_id(
                    "liquidation_transition",
                    {
                        "operation_id": operation.liquidation_operation_id,
                        "sequence_no": sequence,
                        "state": operation.state.value,
                    },
                ),
                operation.liquidation_operation_id,
                sequence,
                from_state,
                operation.state.value,
                operation.blocking_reason,
                _ts(operation.updated_at_utc),
                operation.updated_at_utc,
                payload,
            ),
        )

    @classmethod
    def _append_attempt_transition(
        cls,
        connection: sqlite3.Connection,
        *,
        attempt: LiquidationAttemptV1,
        from_state: str | None,
    ) -> None:
        sequence = cls._next_sequence(
            connection,
            table="internal_liquidation_attempt_transitions",
            id_column="liquidation_attempt_id",
            entity_id=attempt.liquidation_attempt_id,
        )
        payload = canonical_json_text(attempt.to_dict())
        connection.execute(
            """
            INSERT INTO internal_liquidation_attempt_transitions (
                transition_id, liquidation_attempt_id, sequence_no,
                from_state, to_state, reason, occurred_at_ts,
                occurred_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _stable_id(
                    "liquidation_transition",
                    {
                        "attempt_id": attempt.liquidation_attempt_id,
                        "sequence_no": sequence,
                        "state": attempt.state.value,
                    },
                ),
                attempt.liquidation_attempt_id,
                sequence,
                from_state,
                attempt.state.value,
                attempt.failure_reason,
                _ts(attempt.updated_at_utc),
                attempt.updated_at_utc,
                payload,
            ),
        )

    @staticmethod
    def _insert_operation(
        connection: sqlite3.Connection,
        operation: LiquidationOperationV1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO internal_liquidation_operations (
                liquidation_operation_id, position_episode_id, account_id,
                strategy_id, strategy_version, deployment_id, instrument_id,
                state, next_action, current_attempt_id, current_attempt_no,
                broker_remaining_quantity, updated_at_ts, updated_at_utc,
                terminal_at_ts, terminal_at_utc, blocking_reason, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation.liquidation_operation_id,
                operation.position_episode_id,
                operation.account_id,
                operation.strategy_id,
                operation.strategy_version,
                operation.deployment_id,
                operation.instrument_id,
                operation.state.value,
                operation.next_action.value,
                operation.current_attempt_id,
                operation.current_attempt_no,
                operation.broker_remaining_quantity,
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
        operation: LiquidationOperationV1,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE internal_liquidation_operations
            SET state=?, next_action=?, current_attempt_id=?,
                current_attempt_no=?, broker_remaining_quantity=?,
                updated_at_ts=?, updated_at_utc=?, terminal_at_ts=?,
                terminal_at_utc=?, blocking_reason=?, payload_json=?
            WHERE liquidation_operation_id=?
            """,
            (
                operation.state.value,
                operation.next_action.value,
                operation.current_attempt_id,
                operation.current_attempt_no,
                operation.broker_remaining_quantity,
                _ts(operation.updated_at_utc),
                operation.updated_at_utc,
                _ts(operation.terminal_at_utc),
                operation.terminal_at_utc,
                operation.blocking_reason,
                canonical_json_text(operation.to_dict()),
                operation.liquidation_operation_id,
            ),
        )
        if cursor.rowcount != 1:
            raise LiquidationStoreError(
                "liquidation operation does not exist: "
                f"{operation.liquidation_operation_id}"
            )

    @staticmethod
    def _insert_attempt(
        connection: sqlite3.Connection,
        attempt: LiquidationAttemptV1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO internal_liquidation_attempts (
                liquidation_attempt_id, liquidation_operation_id, attempt_no,
                order_ref, state, broker_order_id, broker_perm_id,
                requested_qty, filled_qty, remaining_qty, updated_at_ts,
                updated_at_utc, terminal_at_ts, terminal_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attempt.liquidation_attempt_id,
                attempt.liquidation_operation_id,
                attempt.attempt_no,
                attempt.order_ref,
                attempt.state.value,
                attempt.broker_order_id,
                attempt.broker_perm_id,
                attempt.requested_qty,
                attempt.filled_qty,
                attempt.remaining_qty,
                _ts(attempt.updated_at_utc),
                attempt.updated_at_utc,
                _ts(attempt.terminal_at_utc),
                attempt.terminal_at_utc,
                canonical_json_text(attempt.to_dict()),
            ),
        )

    @staticmethod
    def _update_attempt(
        connection: sqlite3.Connection,
        attempt: LiquidationAttemptV1,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE internal_liquidation_attempts
            SET state=?, broker_order_id=?, broker_perm_id=?, requested_qty=?,
                filled_qty=?, remaining_qty=?, updated_at_ts=?, updated_at_utc=?,
                terminal_at_ts=?, terminal_at_utc=?, payload_json=?
            WHERE liquidation_attempt_id=?
            """,
            (
                attempt.state.value,
                attempt.broker_order_id,
                attempt.broker_perm_id,
                attempt.requested_qty,
                attempt.filled_qty,
                attempt.remaining_qty,
                _ts(attempt.updated_at_utc),
                attempt.updated_at_utc,
                _ts(attempt.terminal_at_utc),
                attempt.terminal_at_utc,
                canonical_json_text(attempt.to_dict()),
                attempt.liquidation_attempt_id,
            ),
        )
        if cursor.rowcount != 1:
            raise LiquidationStoreError(
                "liquidation attempt does not exist: "
                f"{attempt.liquidation_attempt_id}"
            )

    @staticmethod
    def _insert_trigger(
        connection: sqlite3.Connection,
        trigger: LiquidationTriggerV1,
    ) -> bool:
        payload = canonical_json_text(trigger.to_dict())
        row = connection.execute(
            "SELECT payload_json FROM internal_liquidation_triggers "
            "WHERE trigger_id=? LIMIT 1",
            (trigger.trigger_id,),
        ).fetchone()
        if row is not None:
            if str(row["payload_json"]) != payload:
                raise LiquidationStoreError(
                    f"liquidation trigger identity conflicted: {trigger.trigger_id}"
                )
            return False
        connection.execute(
            """
            INSERT INTO internal_liquidation_triggers (
                trigger_id, liquidation_operation_id, reason, source_ref,
                triggered_at_ts, triggered_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trigger.trigger_id,
                trigger.liquidation_operation_id,
                trigger.reason.value,
                trigger.source_ref,
                _ts(trigger.triggered_at_utc),
                trigger.triggered_at_utc,
                payload,
            ),
        )
        return True

    @staticmethod
    def _update_readiness(
        connection: sqlite3.Connection,
        readiness: ExecutionReadinessV1,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE internal_execution_readiness
            SET status=?, command_intake_enabled=?, broker_actions_enabled=?,
                updated_at_ts=?, updated_at_utc=?, payload_json=?
            WHERE account_id=? AND strategy_id=? AND deployment_id=?
              AND instrument_id=?
            """,
            (
                readiness.status.value,
                int(readiness.command_intake_enabled),
                int(readiness.broker_actions_enabled),
                _ts(readiness.updated_at_utc),
                readiness.updated_at_utc,
                canonical_json_text(readiness.to_dict()),
                readiness.account_id,
                readiness.strategy_id,
                readiness.deployment_id,
                readiness.instrument_id,
            ),
        )
        if cursor.rowcount != 1:
            raise LiquidationStoreError(
                "execution readiness does not exist for liquidation scope"
            )

    def publish_request(
        self,
        *,
        current: LiquidationSnapshot | None,
        result: LiquidationRequestResult,
    ) -> LiquidationSnapshot:
        updated = result.snapshot
        with self._liquidation_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload_json FROM internal_liquidation_operations "
                    "WHERE liquidation_operation_id=? LIMIT 1",
                    (updated.operation.liquidation_operation_id,),
                ).fetchone()
                if current is None:
                    if row is not None:
                        raise LiquidationStoreError(
                            "liquidation operation appeared concurrently"
                        )
                    self._insert_operation(connection, updated.operation)
                    self._append_operation_transition(
                        connection,
                        operation=updated.operation,
                        from_state=None,
                    )
                else:
                    if row is None:
                        raise LiquidationStoreError(
                            "current liquidation operation disappeared"
                        )
                    stored = _operation(str(row["payload_json"]))
                    if stored.to_dict() != current.operation.to_dict():
                        raise LiquidationStoreError(
                            "liquidation operation changed concurrently"
                        )
                    if stored.to_dict() != updated.operation.to_dict():
                        self._update_operation(connection, updated.operation)
                        if stored.state != updated.operation.state:
                            self._append_operation_transition(
                                connection,
                                operation=updated.operation,
                                from_state=stored.state.value,
                            )
                self._insert_trigger(connection, result.trigger)
                self._update_readiness(connection, result.execution_readiness)
                connection.commit()
                return updated
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, LiquidationStoreError):
                    raise
                raise LiquidationStoreError(
                    "cannot publish liquidation request: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def publish_state(
        self,
        *,
        current: LiquidationSnapshot,
        updated: LiquidationSnapshot,
        readiness: ExecutionReadinessV1,
        current_protection: ProtectionStateV1 | None = None,
        updated_protection: ProtectionStateV1 | None = None,
        episode: PositionEpisodeV1 | None = None,
        strategy_position: StrategyPositionV1 | None = None,
        observation: BrokerOrderObservationV1 | None = None,
        source_session_id: str | None = None,
        captured_at_utc: str | None = None,
        fills: tuple[BrokerFillFactV1, ...] = (),
    ) -> LiquidationSnapshot:
        if (
            current.operation.liquidation_operation_id
            != updated.operation.liquidation_operation_id
        ):
            raise LiquidationStoreError("liquidation operation identity changed")
        with self._liquidation_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload_json FROM internal_liquidation_operations "
                    "WHERE liquidation_operation_id=? LIMIT 1",
                    (current.operation.liquidation_operation_id,),
                ).fetchone()
                if row is None:
                    raise LiquidationStoreError("liquidation operation is missing")
                stored_operation = _operation(str(row["payload_json"]))
                if stored_operation.to_dict() != current.operation.to_dict():
                    raise LiquidationStoreError(
                        "liquidation operation changed concurrently"
                    )

                stored_attempt = None
                if current.attempt is not None:
                    attempt_row = connection.execute(
                        "SELECT payload_json FROM internal_liquidation_attempts "
                        "WHERE liquidation_attempt_id=? LIMIT 1",
                        (current.attempt.liquidation_attempt_id,),
                    ).fetchone()
                    if attempt_row is None:
                        raise LiquidationStoreError(
                            "current liquidation attempt is missing"
                        )
                    stored_attempt = _attempt(str(attempt_row["payload_json"]))
                    if stored_attempt.to_dict() != current.attempt.to_dict():
                        raise LiquidationStoreError(
                            "liquidation attempt changed concurrently"
                        )

                if current.attempt is None and updated.attempt is not None:
                    self._insert_attempt(connection, updated.attempt)
                    self._append_attempt_transition(
                        connection,
                        attempt=updated.attempt,
                        from_state=None,
                    )
                elif (
                    stored_attempt is not None
                    and updated.attempt is not None
                    and stored_attempt.to_dict() != updated.attempt.to_dict()
                ):
                    self._update_attempt(connection, updated.attempt)
                    if stored_attempt.state != updated.attempt.state:
                        self._append_attempt_transition(
                            connection,
                            attempt=updated.attempt,
                            from_state=stored_attempt.state.value,
                        )

                if stored_operation.to_dict() != updated.operation.to_dict():
                    self._update_operation(connection, updated.operation)
                    if stored_operation.state != updated.operation.state:
                        self._append_operation_transition(
                            connection,
                            operation=updated.operation,
                            from_state=stored_operation.state.value,
                        )

                if (current_protection is None) != (updated_protection is None):
                    raise LiquidationStoreError(
                        "current and updated protection must be supplied together"
                    )
                if current_protection is not None and updated_protection is not None:
                    self._update_protection_state(
                        connection,
                        current=current_protection,
                        updated=updated_protection,
                    )
                self._update_readiness(connection, readiness)
                if episode is not None:
                    self._update_episode(connection, episode)
                if strategy_position is not None:
                    self._update_strategy_position(connection, strategy_position)
                if observation is not None:
                    if updated.attempt is None:
                        raise LiquidationStoreError(
                            "broker observation requires liquidation attempt"
                        )
                    self._append_reconciliation(
                        connection,
                        operation=updated.operation,
                        attempt=updated.attempt,
                        observation=observation,
                        source_session_id=source_session_id,
                        captured_at_utc=captured_at_utc,
                    )
                    for fill in fills:
                        self._insert_fill(
                            connection,
                            operation=updated.operation,
                            attempt=updated.attempt,
                            fill=fill,
                        )
                connection.commit()
                return updated
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, LiquidationStoreError):
                    raise
                raise LiquidationStoreError(
                    "cannot publish liquidation state: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    @classmethod
    def _update_protection_state(
        cls,
        connection: sqlite3.Connection,
        *,
        current: ProtectionStateV1,
        updated: ProtectionStateV1,
    ) -> None:
        row = connection.execute(
            "SELECT payload_json FROM internal_protection_sets "
            "WHERE protection_set_id=? LIMIT 1",
            (current.protection_set_id,),
        ).fetchone()
        if row is None or _protection(str(row["payload_json"])).to_dict() != current.to_dict():
            raise LiquidationStoreError(
                "protection state changed concurrently before liquidation update"
            )
        connection.execute(
            """
            UPDATE internal_protection_sets
            SET status=?, updated_at_ts=?, updated_at_utc=?, terminal_at_ts=?,
                terminal_at_utc=?, blocking_reason=?, payload_json=?
            WHERE protection_set_id=?
            """,
            (
                updated.status.value,
                _ts(updated.updated_at_utc),
                updated.updated_at_utc,
                _ts(updated.terminal_at_utc),
                updated.terminal_at_utc,
                updated.blocking_reason,
                canonical_json_text(updated.to_dict()),
                updated.protection_set_id,
            ),
        )
        current_orders = {
            item.protective_order_id: item for item in current.orders
        }
        for order in updated.orders:
            previous = current_orders.get(order.protective_order_id)
            if previous is None:
                raise LiquidationStoreError(
                    "protective order identity changed during liquidation"
                )
            if previous.to_dict() == order.to_dict():
                continue
            connection.execute(
                """
                UPDATE internal_protective_orders
                SET state=?, filled_qty=?, remaining_qty=?, broker_order_id=?,
                    broker_perm_id=?, broker_status=?, broker_terminal_proven=?,
                    updated_at_ts=?, updated_at_utc=?, terminal_at_ts=?,
                    terminal_at_utc=?, last_broker_proof_at_ts=?,
                    last_broker_proof_at_utc=?, failure_reason=?, payload_json=?
                WHERE protective_order_id=?
                """,
                (
                    order.state.value,
                    order.filled_qty,
                    order.remaining_qty,
                    order.broker_order_id,
                    order.broker_perm_id,
                    order.broker_status,
                    int(order.broker_terminal_proven),
                    _ts(order.updated_at_utc),
                    order.updated_at_utc,
                    _ts(order.terminal_at_utc),
                    order.terminal_at_utc,
                    _ts(order.last_broker_proof_at_utc),
                    order.last_broker_proof_at_utc,
                    order.failure_reason,
                    canonical_json_text(order.to_dict()),
                    order.protective_order_id,
                ),
            )
            cls._append_order_transition(
                connection,
                order=order,
                from_state=previous.state.value,
            )
        if current.status != updated.status:
            cls._append_set_transition(
                connection,
                protection=updated,
                from_state=current.status.value,
            )

    @staticmethod
    def _update_episode(
        connection: sqlite3.Connection,
        episode: PositionEpisodeV1,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE internal_position_episodes
            SET status=?, closed_at_ts=?, closed_at_utc=?,
                closing_operation_id=?, payload_json=?
            WHERE position_episode_id=?
            """,
            (
                episode.status.value,
                _ts(episode.closed_at_utc),
                episode.closed_at_utc,
                episode.closing_operation_id,
                canonical_json_text(episode.to_dict()),
                episode.position_episode_id,
            ),
        )
        if cursor.rowcount != 1:
            raise LiquidationStoreError(
                f"position episode does not exist: {episode.position_episode_id}"
            )

    @staticmethod
    def _update_strategy_position(
        connection: sqlite3.Connection,
        position: StrategyPositionV1,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE internal_strategy_positions
            SET projection_status=?, side=?, quantity=?, updated_at_ts=?,
                updated_at_utc=?, payload_json=?
            WHERE account_id=? AND strategy_id=? AND deployment_id=?
              AND instrument_id=?
            """,
            (
                position.projection_status.value,
                position.side.value,
                position.quantity,
                _ts(position.updated_at_utc),
                position.updated_at_utc,
                canonical_json_text(position.to_dict()),
                position.account_id,
                position.strategy_id,
                position.deployment_id,
                position.instrument_id,
            ),
        )
        if cursor.rowcount != 1:
            raise LiquidationStoreError(
                "strategy position does not exist for liquidation scope"
            )

    @classmethod
    def _append_reconciliation(
        cls,
        connection: sqlite3.Connection,
        *,
        operation: LiquidationOperationV1,
        attempt: LiquidationAttemptV1,
        observation: BrokerOrderObservationV1,
        source_session_id: str | None,
        captured_at_utc: str | None,
    ) -> None:
        if not source_session_id or not captured_at_utc:
            raise LiquidationStoreError(
                "liquidation observation requires source session and capture time"
            )
        payload = canonical_json_text(observation.to_dict())
        observation_id = _stable_id(
            "liquidation_observation",
            {
                "attempt_id": attempt.liquidation_attempt_id,
                "source_session_id": source_session_id,
                "captured_at_utc": captured_at_utc,
                "observation": observation.to_dict(),
            },
        )
        row = connection.execute(
            "SELECT payload_json FROM internal_liquidation_reconciliation_observations "
            "WHERE observation_id=? LIMIT 1",
            (observation_id,),
        ).fetchone()
        if row is not None:
            if str(row["payload_json"]) != payload:
                raise LiquidationStoreError(
                    "liquidation observation identity conflicted"
                )
            return
        sequence = cls._next_sequence(
            connection,
            table="internal_liquidation_reconciliation_observations",
            id_column="liquidation_attempt_id",
            entity_id=attempt.liquidation_attempt_id,
        )
        connection.execute(
            """
            INSERT INTO internal_liquidation_reconciliation_observations (
                observation_id, liquidation_operation_id,
                liquidation_attempt_id, sequence_no, outcome,
                source_session_id, captured_at_ts, captured_at_utc,
                observed_at_ts, observed_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation_id,
                operation.liquidation_operation_id,
                attempt.liquidation_attempt_id,
                sequence,
                observation.outcome.value,
                source_session_id,
                _ts(captured_at_utc),
                captured_at_utc,
                _ts(observation.observed_at_utc),
                observation.observed_at_utc,
                payload,
            ),
        )

    @staticmethod
    def _insert_fill(
        connection: sqlite3.Connection,
        *,
        operation: LiquidationOperationV1,
        attempt: LiquidationAttemptV1,
        fill: BrokerFillFactV1,
    ) -> None:
        if fill.order_ref != attempt.order_ref:
            raise LiquidationStoreError(
                "liquidation fill order_ref differs from attempt"
            )
        base = replace(fill, commission=None)
        payload = canonical_json_text(base.to_dict())
        row = connection.execute(
            "SELECT liquidation_operation_id, liquidation_attempt_id, payload_json "
            "FROM internal_liquidation_fill_evidence WHERE exec_id=? LIMIT 1",
            (fill.exec_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO internal_liquidation_fill_evidence (
                    exec_id, liquidation_operation_id, liquidation_attempt_id,
                    order_ref, executed_at_ts, executed_at_utc, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.exec_id,
                    operation.liquidation_operation_id,
                    attempt.liquidation_attempt_id,
                    fill.order_ref,
                    _ts(fill.executed_at_utc),
                    fill.executed_at_utc,
                    payload,
                ),
            )
        elif (
            str(row["liquidation_operation_id"])
            != operation.liquidation_operation_id
            or str(row["liquidation_attempt_id"])
            != attempt.liquidation_attempt_id
            or str(row["payload_json"]) != payload
        ):
            raise LiquidationStoreError(
                f"conflicting liquidation fill evidence for execId={fill.exec_id}"
            )
        if fill.commission is None:
            return
        commission: BrokerCommissionFactV1 = fill.commission
        commission_payload = canonical_json_text(commission.to_dict())
        row = connection.execute(
            "SELECT liquidation_attempt_id, payload_json "
            "FROM internal_liquidation_commission_evidence "
            "WHERE exec_id=? LIMIT 1",
            (fill.exec_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO internal_liquidation_commission_evidence (
                    exec_id, liquidation_operation_id, liquidation_attempt_id,
                    reported_at_ts, reported_at_utc, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.exec_id,
                    operation.liquidation_operation_id,
                    attempt.liquidation_attempt_id,
                    _ts(commission.reported_at_utc),
                    commission.reported_at_utc,
                    commission_payload,
                ),
            )
        elif (
            str(row["liquidation_attempt_id"])
            != attempt.liquidation_attempt_id
            or str(row["payload_json"]) != commission_payload
        ):
            raise LiquidationStoreError(
                f"conflicting liquidation commission for execId={fill.exec_id}"
            )

    def read_fills(
        self,
        liquidation_operation_id: str,
    ) -> tuple[BrokerFillFactV1, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT fill_payload_json, commission_payload_json "
                "FROM public_liquidation_fills_v1 "
                "WHERE liquidation_operation_id=? "
                "ORDER BY executed_at_ts, exec_id",
                (str(liquidation_operation_id),),
            ).fetchall()
            values = []
            for row in rows:
                fill = BrokerFillFactV1.from_dict(
                    _json_object(
                        str(row["fill_payload_json"]),
                        context="liquidation fill",
                    )
                )
                if row["commission_payload_json"] is not None:
                    fill = replace(
                        fill,
                        commission=BrokerCommissionFactV1.from_dict(
                            _json_object(
                                str(row["commission_payload_json"]),
                                context="liquidation commission",
                            )
                        ),
                    )
                values.append(fill)
            return tuple(values)
        finally:
            connection.close()
