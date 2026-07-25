from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from pathlib import Path

from ibmd.execution.domain.protection import PositionEpisodeProtectionPlan
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.execution import (
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeV1,
    ProtectionStateV1,
    ProtectiveOrderV1,
)

PROTECTION_SCHEMA_VERSION = 3
_REQUIRED_OBJECTS = {
    ("table", "schema_migrations"),
    ("table", "internal_position_episodes"),
    ("table", "internal_protection_sets"),
    ("table", "internal_protective_orders"),
    ("table", "internal_protection_set_transitions"),
    ("table", "internal_protective_order_transitions"),
    ("view", "public_position_episodes_v1"),
    ("view", "public_protection_states_v1"),
    ("view", "public_protective_orders_v1"),
}


class ProtectionStoreError(RuntimeError):
    pass


class ProtectionSchemaError(ProtectionStoreError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ProtectionStoreError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ProtectionStoreError(
            f"stored {context} payload must be an object"
        )
    return value


def _episode(payload: str) -> PositionEpisodeV1:
    return PositionEpisodeV1.from_dict(
        _json_object(payload, context="position episode")
    )


def _protection(payload: str) -> ProtectionStateV1:
    return ProtectionStateV1.from_dict(
        _json_object(payload, context="protection state")
    )


def _order(payload: str) -> ProtectiveOrderV1:
    return ProtectiveOrderV1.from_dict(
        _json_object(payload, context="protective order")
    )


def _ts(value: str | None) -> int | None:
    return None if value is None else int(parse_utc(value).timestamp())


def _stable_id(kind: str, payload: str) -> str:
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
    return f"{kind}_{digest}"


def _transition_id(
    entity_id: str,
    sequence_no: int,
    state: str,
) -> str:
    return _stable_id(
        "protection_transition",
        f"{entity_id}:{sequence_no}:{state}",
    )


class SQLiteProtectionReader:
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
            raise ProtectionSchemaError(
                f"execution database does not exist: {self.database_path}"
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
            objects = {
                (str(row["type"]), str(row["name"]))
                for row in connection.execute(
                    "SELECT type, name FROM sqlite_master "
                    "WHERE type IN ('table', 'view')"
                ).fetchall()
            }
            missing = sorted(_REQUIRED_OBJECTS - objects)
            if missing:
                raise ProtectionSchemaError(
                    f"protection schema objects are missing: {missing}"
                )
            versions = [
                int(row["version"])
                for row in connection.execute(
                    "SELECT version FROM schema_migrations "
                    "WHERE store_name='execution' ORDER BY version"
                ).fetchall()
            ]
            expected = list(range(1, PROTECTION_SCHEMA_VERSION + 1))
            if versions != expected:
                raise ProtectionSchemaError(
                    "execution protection schema version mismatch: "
                    f"expected={expected}, actual={versions}"
                )
        except sqlite3.Error as exc:
            raise ProtectionSchemaError(
                f"cannot validate protection schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_position_episodes_v1 "
                "WHERE position_episode_id=? LIMIT 1",
                (str(position_episode_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _episode(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_episode_by_operation(
        self,
        operation_id: str,
    ) -> PositionEpisodeV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_position_episodes_v1 "
                "WHERE source_operation_id=? LIMIT 1",
                (str(operation_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _episode(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_protection_states_v1 "
                "WHERE position_episode_id=? LIMIT 1",
                (str(position_episode_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _protection(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_protective_order(
        self,
        protective_order_id: str,
    ) -> ProtectiveOrderV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_protective_orders_v1 "
                "WHERE protective_order_id=? LIMIT 1",
                (str(protective_order_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _order(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_transition_states(
        self,
        protection_set_id: str,
    ) -> tuple[str, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT to_state FROM internal_protection_set_transitions "
                "WHERE protection_set_id=? ORDER BY sequence_no",
                (str(protection_set_id),),
            ).fetchall()
            return tuple(str(row["to_state"]) for row in rows)
        finally:
            connection.close()


class SQLiteProtectionStore:
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
        self.reader = SQLiteProtectionReader(
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
            f"WHERE {id_column}=?",
            (entity_id,),
        ).fetchone()
        return int(row[0])

    @classmethod
    def _append_set_transition(
        cls,
        connection: sqlite3.Connection,
        *,
        protection: ProtectionStateV1,
        from_state: str | None,
    ) -> None:
        sequence = cls._next_sequence(
            connection,
            table="internal_protection_set_transitions",
            id_column="protection_set_id",
            entity_id=protection.protection_set_id,
        )
        payload = canonical_json_text(protection.to_dict())
        connection.execute(
            """
            INSERT INTO internal_protection_set_transitions (
                transition_id, protection_set_id, sequence_no, from_state,
                to_state, reason, occurred_at_ts, occurred_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _transition_id(
                    protection.protection_set_id,
                    sequence,
                    protection.status.value,
                ),
                protection.protection_set_id,
                sequence,
                from_state,
                protection.status.value,
                protection.blocking_reason,
                _ts(protection.updated_at_utc),
                protection.updated_at_utc,
                payload,
            ),
        )

    @classmethod
    def _append_order_transition(
        cls,
        connection: sqlite3.Connection,
        *,
        order: ProtectiveOrderV1,
        from_state: str | None,
    ) -> None:
        sequence = cls._next_sequence(
            connection,
            table="internal_protective_order_transitions",
            id_column="protective_order_id",
            entity_id=order.protective_order_id,
        )
        payload = canonical_json_text(order.to_dict())
        connection.execute(
            """
            INSERT INTO internal_protective_order_transitions (
                transition_id, protective_order_id, sequence_no, from_state,
                to_state, reason, occurred_at_ts, occurred_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _transition_id(
                    order.protective_order_id,
                    sequence,
                    order.state.value,
                ),
                order.protective_order_id,
                sequence,
                from_state,
                order.state.value,
                order.failure_reason,
                _ts(order.updated_at_utc),
                order.updated_at_utc,
                payload,
            ),
        )

    @staticmethod
    def _upsert_position_and_readiness(
        connection: sqlite3.Connection,
        *,
        position: StrategyPositionV1,
        readiness: ExecutionReadinessV1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO internal_strategy_positions (
                account_id, strategy_id, deployment_id, instrument_id,
                projection_status, side, quantity, updated_at_ts,
                updated_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, strategy_id, deployment_id, instrument_id)
            DO UPDATE SET
                projection_status=excluded.projection_status,
                side=excluded.side,
                quantity=excluded.quantity,
                updated_at_ts=excluded.updated_at_ts,
                updated_at_utc=excluded.updated_at_utc,
                payload_json=excluded.payload_json
            """,
            (
                position.account_id,
                position.strategy_id,
                position.deployment_id,
                position.instrument_id,
                position.projection_status.value,
                position.side.value,
                position.quantity,
                _ts(position.updated_at_utc),
                position.updated_at_utc,
                canonical_json_text(position.to_dict()),
            ),
        )
        connection.execute(
            """
            INSERT INTO internal_execution_readiness (
                account_id, strategy_id, deployment_id, instrument_id,
                status, command_intake_enabled, broker_actions_enabled,
                updated_at_ts, updated_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, strategy_id, deployment_id, instrument_id)
            DO UPDATE SET
                status=excluded.status,
                command_intake_enabled=excluded.command_intake_enabled,
                broker_actions_enabled=excluded.broker_actions_enabled,
                updated_at_ts=excluded.updated_at_ts,
                updated_at_utc=excluded.updated_at_utc,
                payload_json=excluded.payload_json
            """,
            (
                readiness.account_id,
                readiness.strategy_id,
                readiness.deployment_id,
                readiness.instrument_id,
                readiness.status.value,
                int(readiness.command_intake_enabled),
                int(readiness.broker_actions_enabled),
                _ts(readiness.updated_at_utc),
                readiness.updated_at_utc,
                canonical_json_text(readiness.to_dict()),
            ),
        )

    def publish_plan(
        self,
        plan: PositionEpisodeProtectionPlan,
    ) -> PositionEpisodeProtectionPlan:
        if not isinstance(plan, PositionEpisodeProtectionPlan):
            raise ProtectionStoreError(
                "plan must be PositionEpisodeProtectionPlan"
            )
        episode = plan.episode
        protection = plan.protection
        episode_payload = canonical_json_text(episode.to_dict())
        protection_payload = canonical_json_text(protection.to_dict())

        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing_episode = connection.execute(
                    "SELECT payload_json FROM internal_position_episodes "
                    "WHERE position_episode_id=? OR source_operation_id=? "
                    "LIMIT 1",
                    (
                        episode.position_episode_id,
                        episode.source_operation_id,
                    ),
                ).fetchone()
                if existing_episode is not None:
                    stored_episode = _episode(
                        str(existing_episode["payload_json"])
                    )
                    stored_protection_row = connection.execute(
                        "SELECT payload_json FROM internal_protection_sets "
                        "WHERE position_episode_id=? LIMIT 1",
                        (stored_episode.position_episode_id,),
                    ).fetchone()
                    if stored_protection_row is None:
                        raise ProtectionStoreError(
                            "position episode exists without protection state"
                        )
                    stored_protection = _protection(
                        str(stored_protection_row["payload_json"])
                    )
                    if (
                        stored_episode.to_dict() != episode.to_dict()
                        or stored_protection.to_dict()
                        != protection.to_dict()
                    ):
                        raise ProtectionStoreError(
                            "conflicting position episode/protection plan "
                            "already exists"
                        )
                    self._upsert_position_and_readiness(
                        connection,
                        position=plan.strategy_position,
                        readiness=plan.execution_readiness,
                    )
                    connection.commit()
                    return PositionEpisodeProtectionPlan(
                        episode=stored_episode,
                        strategy_position=plan.strategy_position,
                        execution_readiness=plan.execution_readiness,
                        protection=stored_protection,
                    )

                connection.execute(
                    """
                    INSERT INTO internal_position_episodes (
                        position_episode_id, account_id, strategy_id,
                        strategy_version, deployment_id, instrument_id,
                        source_command_id, source_operation_id,
                        source_attempt_id, side, quantity, con_id,
                        local_symbol, entry_average_price,
                        broker_snapshot_id, opened_at_ts, opened_at_utc,
                        status, strategy_policy_hash,
                        protective_policy_hash, closed_at_ts,
                        closed_at_utc, closing_operation_id, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        episode.position_episode_id,
                        episode.account_id,
                        episode.strategy_id,
                        episode.strategy_version,
                        episode.deployment_id,
                        episode.instrument_id,
                        episode.source_command_id,
                        episode.source_operation_id,
                        episode.source_attempt_id,
                        episode.side.value,
                        episode.quantity,
                        episode.con_id,
                        episode.local_symbol,
                        episode.entry_average_price,
                        episode.broker_snapshot_id,
                        _ts(episode.opened_at_utc),
                        episode.opened_at_utc,
                        episode.status.value,
                        episode.strategy_policy_hash,
                        episode.protective_policy_hash,
                        _ts(episode.closed_at_utc),
                        episode.closed_at_utc,
                        episode.closing_operation_id,
                        episode_payload,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO internal_protection_sets (
                        protection_set_id, position_episode_id, account_id,
                        strategy_id, strategy_version, deployment_id,
                        instrument_id, status, created_at_ts,
                        created_at_utc, updated_at_ts, updated_at_utc,
                        terminal_at_ts, terminal_at_utc, blocking_reason,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        protection.protection_set_id,
                        protection.position_episode_id,
                        protection.account_id,
                        protection.strategy_id,
                        protection.strategy_version,
                        protection.deployment_id,
                        protection.instrument_id,
                        protection.status.value,
                        _ts(protection.created_at_utc),
                        protection.created_at_utc,
                        _ts(protection.updated_at_utc),
                        protection.updated_at_utc,
                        _ts(protection.terminal_at_utc),
                        protection.terminal_at_utc,
                        protection.blocking_reason,
                        protection_payload,
                    ),
                )
                for order in protection.orders:
                    connection.execute(
                        """
                        INSERT INTO internal_protective_orders (
                            protective_order_id, protection_set_id,
                            position_episode_id, kind, state,
                            planned_sequence, order_ref, side, order_type,
                            quantity, con_id, local_symbol, stop_price,
                            limit_price, time_in_force, outside_rth,
                            oca_group, filled_qty, remaining_qty,
                            broker_order_id, broker_perm_id, broker_status,
                            broker_terminal_proven, created_at_ts,
                            created_at_utc, updated_at_ts, updated_at_utc,
                            terminal_at_ts, terminal_at_utc,
                            last_broker_proof_at_ts,
                            last_broker_proof_at_utc, failure_reason,
                            payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            order.protective_order_id,
                            order.protection_set_id,
                            order.position_episode_id,
                            order.kind.value,
                            order.state.value,
                            order.planned_sequence,
                            order.order_ref,
                            order.side.value,
                            order.order_type.value,
                            order.quantity,
                            order.con_id,
                            order.local_symbol,
                            order.stop_price,
                            order.limit_price,
                            order.time_in_force,
                            int(order.outside_rth),
                            order.oca_group,
                            order.filled_qty,
                            order.remaining_qty,
                            order.broker_order_id,
                            order.broker_perm_id,
                            order.broker_status,
                            int(order.broker_terminal_proven),
                            _ts(order.created_at_utc),
                            order.created_at_utc,
                            _ts(order.updated_at_utc),
                            order.updated_at_utc,
                            _ts(order.terminal_at_utc),
                            order.terminal_at_utc,
                            _ts(order.last_broker_proof_at_utc),
                            order.last_broker_proof_at_utc,
                            order.failure_reason,
                            canonical_json_text(order.to_dict()),
                        ),
                    )
                self._append_set_transition(
                    connection,
                    protection=protection,
                    from_state=None,
                )
                for order in protection.orders:
                    self._append_order_transition(
                        connection,
                        order=order,
                        from_state=None,
                    )
                self._upsert_position_and_readiness(
                    connection,
                    position=plan.strategy_position,
                    readiness=plan.execution_readiness,
                )
                connection.commit()
                return plan
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ProtectionStoreError):
                    raise
                raise ProtectionStoreError(
                    "cannot publish position episode/protection plan: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def publish_protection_state(
        self,
        *,
        current: ProtectionStateV1,
        updated: ProtectionStateV1,
    ) -> ProtectionStateV1:
        if current.protection_set_id != updated.protection_set_id:
            raise ProtectionStoreError(
                "protection state identity changed"
            )
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload_json FROM internal_protection_sets "
                    "WHERE protection_set_id=? LIMIT 1",
                    (current.protection_set_id,),
                ).fetchone()
                if row is None:
                    raise ProtectionStoreError(
                        "protection state does not exist: "
                        f"{current.protection_set_id}"
                    )
                stored = _protection(str(row["payload_json"]))
                if stored.to_dict() != current.to_dict():
                    raise ProtectionStoreError(
                        "protection state changed concurrently"
                    )
                if updated.to_dict() == current.to_dict():
                    connection.rollback()
                    return current
                connection.execute(
                    """
                    UPDATE internal_protection_sets
                    SET status=?, updated_at_ts=?, updated_at_utc=?,
                        terminal_at_ts=?, terminal_at_utc=?,
                        blocking_reason=?, payload_json=?
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
                    item.protective_order_id: item
                    for item in current.orders
                }
                for order in updated.orders:
                    previous = current_orders[order.protective_order_id]
                    if order.to_dict() == previous.to_dict():
                        continue
                    connection.execute(
                        """
                        UPDATE internal_protective_orders
                        SET state=?, filled_qty=?, remaining_qty=?,
                            broker_order_id=?, broker_perm_id=?,
                            broker_status=?, broker_terminal_proven=?,
                            updated_at_ts=?, updated_at_utc=?,
                            terminal_at_ts=?, terminal_at_utc=?,
                            last_broker_proof_at_ts=?,
                            last_broker_proof_at_utc=?, failure_reason=?,
                            payload_json=?
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
                    self._append_order_transition(
                        connection,
                        order=order,
                        from_state=previous.state.value,
                    )
                self._append_set_transition(
                    connection,
                    protection=updated,
                    from_state=current.status.value,
                )
                connection.commit()
                return updated
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ProtectionStoreError):
                    raise
                raise ProtectionStoreError(
                    "cannot publish protection state: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def read_episode_by_operation(
        self,
        operation_id: str,
    ) -> PositionEpisodeV1 | None:
        return self.reader.read_episode_by_operation(operation_id)

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None:
        return self.reader.read_protection_by_episode(position_episode_id)
