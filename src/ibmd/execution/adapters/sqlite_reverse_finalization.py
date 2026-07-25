from __future__ import annotations

import json
import sqlite3
from ibmd.execution.domain.protection import PositionEpisodeProtectionPlan
from ibmd.execution.domain.reverse_finalization import (
    ReversePositionFinalizationV1,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.public_contracts.execution import (
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeV1,
    ProtectionStateV1,
)
from ibmd.public_contracts.reverse import ReverseFillAllocationV1

from .sqlite_protection import (
    ProtectionSchemaError,
    ProtectionStoreError,
    SQLiteProtectionStore,
    _episode,
    _protection,
    _ts,
)

_COMPONENT_NAME = "execution_reverse_finalization"
_COMPONENT_VERSION = 1
_COMPONENT_LEDGER = "execution_target_schema_components"
_REQUIRED_OBJECTS = {
    ("table", _COMPONENT_LEDGER),
    ("table", "internal_reverse_finalizations"),
    ("table", "internal_reverse_fill_allocations"),
    ("view", "public_reverse_finalizations_v1"),
    ("view", "public_reverse_fill_allocations_v1"),
}


class ReverseFinalizationStoreError(ProtectionStoreError):
    pass


class ReverseFinalizationSchemaError(ProtectionSchemaError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ReverseFinalizationStoreError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ReverseFinalizationStoreError(
            f"stored {context} payload must be an object"
        )
    return value


def _position(payload: str) -> StrategyPositionV1:
    return StrategyPositionV1.from_dict(
        _json_object(payload, context="strategy position")
    )


def _readiness(payload: str) -> ExecutionReadinessV1:
    return ExecutionReadinessV1.from_dict(
        _json_object(payload, context="execution readiness")
    )


def _payload(value: ReversePositionFinalizationV1) -> dict:
    return {
        "closed_episode": value.closed_episode.to_dict(),
        "closed_protection": value.closed_protection.to_dict(),
        "new_plan": {
            "episode": value.new_plan.episode.to_dict(),
            "strategy_position": value.new_plan.strategy_position.to_dict(),
            "execution_readiness": value.new_plan.execution_readiness.to_dict(),
            "protection": value.new_plan.protection.to_dict(),
        },
        "allocations": [item.to_dict() for item in value.allocations],
        "closing_completed_at_utc": value.closing_completed_at_utc,
        "opening_started_at_utc": value.opening_started_at_utc,
        "commission_complete": value.commission_complete,
    }


def _commission_material(value: ReversePositionFinalizationV1) -> str:
    payload = _payload(value)
    payload["commission_complete"] = False
    for item in payload["allocations"]:
        item["commission_complete"] = False
    return canonical_json_text(payload)


def _finalization(payload: str) -> ReversePositionFinalizationV1:
    value = _json_object(payload, context="reverse finalization")
    expected = {
        "closed_episode",
        "closed_protection",
        "new_plan",
        "allocations",
        "closing_completed_at_utc",
        "opening_started_at_utc",
        "commission_complete",
    }
    if set(value) != expected:
        raise ReverseFinalizationStoreError(
            "stored reverse finalization fields mismatch"
        )
    plan = value["new_plan"]
    allocations = value["allocations"]
    if not isinstance(plan, dict) or not isinstance(allocations, list):
        raise ReverseFinalizationStoreError(
            "stored reverse finalization plan/allocations types are invalid"
        )
    expected_plan = {
        "episode",
        "strategy_position",
        "execution_readiness",
        "protection",
    }
    if set(plan) != expected_plan:
        raise ReverseFinalizationStoreError(
            "stored reverse finalization plan fields mismatch"
        )
    return ReversePositionFinalizationV1(
        closed_episode=PositionEpisodeV1.from_dict(value["closed_episode"]),
        closed_protection=ProtectionStateV1.from_dict(
            value["closed_protection"]
        ),
        new_plan=PositionEpisodeProtectionPlan(
            episode=PositionEpisodeV1.from_dict(plan["episode"]),
            strategy_position=StrategyPositionV1.from_dict(
                plan["strategy_position"]
            ),
            execution_readiness=ExecutionReadinessV1.from_dict(
                plan["execution_readiness"]
            ),
            protection=ProtectionStateV1.from_dict(plan["protection"]),
        ),
        allocations=tuple(
            ReverseFillAllocationV1.from_dict(item) for item in allocations
        ),
        closing_completed_at_utc=str(
            value["closing_completed_at_utc"]
        ),
        opening_started_at_utc=str(value["opening_started_at_utc"]),
        commission_complete=value["commission_complete"],
    )


class SQLiteReverseFinalizationStore(SQLiteProtectionStore):
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
                raise ReverseFinalizationSchemaError(
                    f"reverse finalization schema objects are missing: {missing}"
                )
            row = connection.execute(
                f"SELECT component_version FROM {_COMPONENT_LEDGER} "
                "WHERE component_name=? LIMIT 1",
                (_COMPONENT_NAME,),
            ).fetchone()
            if row is None or int(row["component_version"]) != _COMPONENT_VERSION:
                raise ReverseFinalizationSchemaError(
                    "reverse finalization component is not installed: "
                    f"expected={_COMPONENT_NAME}@{_COMPONENT_VERSION}"
                )
        except sqlite3.Error as exc:
            raise ReverseFinalizationSchemaError(
                f"cannot validate reverse finalization schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_by_operation(
        self,
        operation_id: str,
    ) -> ReversePositionFinalizationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_reverse_finalizations_v1 "
                "WHERE source_operation_id=? LIMIT 1",
                (str(operation_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _finalization(str(row["payload_json"]))
            )
        finally:
            connection.close()

    def read_allocations(
        self,
        operation_id: str,
    ) -> tuple[ReverseFillAllocationV1, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT payload_json FROM public_reverse_fill_allocations_v1 "
                "WHERE source_operation_id=? ORDER BY sequence_no",
                (str(operation_id),),
            ).fetchall()
            return tuple(
                ReverseFillAllocationV1.from_dict(
                    _json_object(
                        str(row["payload_json"]),
                        context="reverse fill allocation",
                    )
                )
                for row in rows
            )
        finally:
            connection.close()

    @staticmethod
    def _insert_episode(
        connection: sqlite3.Connection,
        episode: PositionEpisodeV1,
    ) -> None:
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
                canonical_json_text(episode.to_dict()),
            ),
        )

    @classmethod
    def _insert_protection(
        cls,
        connection: sqlite3.Connection,
        protection: ProtectionStateV1,
    ) -> None:
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
                canonical_json_text(protection.to_dict()),
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
        cls._append_set_transition(
            connection,
            protection=protection,
            from_state=None,
        )
        for order in protection.orders:
            cls._append_order_transition(
                connection,
                order=order,
                from_state=None,
            )

    @classmethod
    def _close_source(
        cls,
        connection: sqlite3.Connection,
        *,
        current_episode: PositionEpisodeV1,
        current_protection: ProtectionStateV1,
        result: ReversePositionFinalizationV1,
    ) -> None:
        closed_episode = result.closed_episode
        cursor = connection.execute(
            """
            UPDATE internal_position_episodes
            SET status=?, closed_at_ts=?, closed_at_utc=?,
                closing_operation_id=?, payload_json=?
            WHERE position_episode_id=?
            """,
            (
                closed_episode.status.value,
                _ts(closed_episode.closed_at_utc),
                closed_episode.closed_at_utc,
                closed_episode.closing_operation_id,
                canonical_json_text(closed_episode.to_dict()),
                closed_episode.position_episode_id,
            ),
        )
        if cursor.rowcount != 1:
            raise ReverseFinalizationStoreError(
                "source position episode disappeared during finalization"
            )
        closed_protection = result.closed_protection
        cursor = connection.execute(
            """
            UPDATE internal_protection_sets
            SET status=?, updated_at_ts=?, updated_at_utc=?,
                terminal_at_ts=?, terminal_at_utc=?,
                blocking_reason=?, payload_json=?
            WHERE protection_set_id=?
            """,
            (
                closed_protection.status.value,
                _ts(closed_protection.updated_at_utc),
                closed_protection.updated_at_utc,
                _ts(closed_protection.terminal_at_utc),
                closed_protection.terminal_at_utc,
                closed_protection.blocking_reason,
                canonical_json_text(closed_protection.to_dict()),
                closed_protection.protection_set_id,
            ),
        )
        if cursor.rowcount != 1:
            raise ReverseFinalizationStoreError(
                "source protection disappeared during finalization"
            )
        cls._append_set_transition(
            connection,
            protection=closed_protection,
            from_state=current_protection.status.value,
        )

    @staticmethod
    def _insert_allocations(
        connection: sqlite3.Connection,
        allocations: tuple[ReverseFillAllocationV1, ...],
    ) -> None:
        for item in allocations:
            connection.execute(
                """
                INSERT INTO internal_reverse_fill_allocations (
                    reverse_allocation_id, source_operation_id,
                    source_attempt_id, exec_id, sequence_no,
                    closing_position_episode_id,
                    opening_position_episode_id, close_quantity,
                    open_quantity, executed_at_ts, executed_at_utc,
                    commission_complete, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.reverse_allocation_id,
                    item.source_operation_id,
                    item.source_attempt_id,
                    item.exec_id,
                    item.sequence_no,
                    item.closing_position_episode_id,
                    item.opening_position_episode_id,
                    item.close_quantity,
                    item.open_quantity,
                    _ts(item.executed_at_utc),
                    item.executed_at_utc,
                    int(item.commission_complete),
                    canonical_json_text(item.to_dict()),
                ),
            )

    def refresh_commission_completion(
        self,
        *,
        current: ReversePositionFinalizationV1,
        updated: ReversePositionFinalizationV1,
    ) -> ReversePositionFinalizationV1:
        operation_id = current.new_plan.episode.source_operation_id
        if updated.new_plan.episode.source_operation_id != operation_id:
            raise ReverseFinalizationStoreError(
                "reverse commission refresh operation identity changed"
            )
        if current.commission_complete and not updated.commission_complete:
            raise ReverseFinalizationStoreError(
                "reverse commission completeness cannot regress"
            )
        if _commission_material(current) != _commission_material(updated):
            raise ReverseFinalizationStoreError(
                "reverse commission refresh changed economic finalization facts"
            )
        current_by_id = {
            item.reverse_allocation_id: item for item in current.allocations
        }
        updated_by_id = {
            item.reverse_allocation_id: item for item in updated.allocations
        }
        if set(current_by_id) != set(updated_by_id):
            raise ReverseFinalizationStoreError(
                "reverse commission refresh allocation identities changed"
            )
        for allocation_id, stored in current_by_id.items():
            incoming = updated_by_id[allocation_id]
            if stored.commission_complete and not incoming.commission_complete:
                raise ReverseFinalizationStoreError(
                    "reverse allocation commission completeness cannot regress"
                )
        if current == updated:
            return current
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload_json FROM internal_reverse_finalizations "
                    "WHERE source_operation_id=? LIMIT 1",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise ReverseFinalizationStoreError(
                        "reverse finalization disappeared before commission refresh"
                    )
                stored = _finalization(str(row["payload_json"]))
                if canonical_json_text(_payload(stored)) != canonical_json_text(
                    _payload(current)
                ):
                    raise ReverseFinalizationStoreError(
                        "reverse finalization changed concurrently"
                    )
                for allocation in updated.allocations:
                    previous = current_by_id[allocation.reverse_allocation_id]
                    if previous.commission_complete == allocation.commission_complete:
                        continue
                    cursor = connection.execute(
                        "UPDATE internal_reverse_fill_allocations "
                        "SET commission_complete=?, payload_json=? "
                        "WHERE reverse_allocation_id=?",
                        (
                            int(allocation.commission_complete),
                            canonical_json_text(allocation.to_dict()),
                            allocation.reverse_allocation_id,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ReverseFinalizationStoreError(
                            "reverse allocation disappeared during commission refresh"
                        )
                cursor = connection.execute(
                    "UPDATE internal_reverse_finalizations "
                    "SET commission_complete=?, payload_json=? "
                    "WHERE source_operation_id=?",
                    (
                        int(updated.commission_complete),
                        canonical_json_text(_payload(updated)),
                        operation_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise ReverseFinalizationStoreError(
                        "reverse finalization disappeared during commission refresh"
                    )
                connection.commit()
                return updated
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ReverseFinalizationStoreError):
                    raise
                raise ReverseFinalizationStoreError(
                    "cannot refresh reverse commission completion: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def publish_finalization(
        self,
        *,
        current_episode: PositionEpisodeV1,
        current_protection: ProtectionStateV1,
        current_position: StrategyPositionV1,
        current_readiness: ExecutionReadinessV1,
        result: ReversePositionFinalizationV1,
    ) -> ReversePositionFinalizationV1:
        operation_id = result.new_plan.episode.source_operation_id
        payload = canonical_json_text(_payload(result))
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT payload_json FROM internal_reverse_finalizations "
                    "WHERE source_operation_id=? LIMIT 1",
                    (operation_id,),
                ).fetchone()
                if existing is not None:
                    stored_payload = str(existing["payload_json"])
                    if stored_payload != payload:
                        raise ReverseFinalizationStoreError(
                            "conflicting reverse finalization already exists"
                        )
                    connection.rollback()
                    return _finalization(stored_payload)

                episode_row = connection.execute(
                    "SELECT payload_json FROM internal_position_episodes "
                    "WHERE position_episode_id=? LIMIT 1",
                    (current_episode.position_episode_id,),
                ).fetchone()
                protection_row = connection.execute(
                    "SELECT payload_json FROM internal_protection_sets "
                    "WHERE protection_set_id=? LIMIT 1",
                    (current_protection.protection_set_id,),
                ).fetchone()
                position_row = connection.execute(
                    "SELECT payload_json FROM internal_strategy_positions "
                    "WHERE account_id=? AND strategy_id=? "
                    "AND deployment_id=? AND instrument_id=? LIMIT 1",
                    (
                        current_position.account_id,
                        current_position.strategy_id,
                        current_position.deployment_id,
                        current_position.instrument_id,
                    ),
                ).fetchone()
                readiness_row = connection.execute(
                    "SELECT payload_json FROM internal_execution_readiness "
                    "WHERE account_id=? AND strategy_id=? "
                    "AND deployment_id=? AND instrument_id=? LIMIT 1",
                    (
                        current_readiness.account_id,
                        current_readiness.strategy_id,
                        current_readiness.deployment_id,
                        current_readiness.instrument_id,
                    ),
                ).fetchone()
                if None in (
                    episode_row,
                    protection_row,
                    position_row,
                    readiness_row,
                ):
                    raise ReverseFinalizationStoreError(
                        "source reverse state is incomplete"
                    )
                if (
                    _episode(str(episode_row["payload_json"])).to_dict()
                    != current_episode.to_dict()
                    or _protection(
                        str(protection_row["payload_json"])
                    ).to_dict()
                    != current_protection.to_dict()
                    or _position(str(position_row["payload_json"])).to_dict()
                    != current_position.to_dict()
                    or _readiness(
                        str(readiness_row["payload_json"])
                    ).to_dict()
                    != current_readiness.to_dict()
                ):
                    raise ReverseFinalizationStoreError(
                        "source reverse state changed concurrently"
                    )
                conflict = connection.execute(
                    "SELECT 1 FROM internal_position_episodes "
                    "WHERE position_episode_id=? OR source_operation_id=? LIMIT 1",
                    (
                        result.new_plan.episode.position_episode_id,
                        operation_id,
                    ),
                ).fetchone()
                if conflict is not None:
                    raise ReverseFinalizationStoreError(
                        "opening reverse position episode already exists without "
                        "finalization record"
                    )

                self._close_source(
                    connection,
                    current_episode=current_episode,
                    current_protection=current_protection,
                    result=result,
                )
                self._insert_episode(connection, result.new_plan.episode)
                self._insert_protection(
                    connection,
                    result.new_plan.protection,
                )
                self._upsert_position_and_readiness(
                    connection,
                    position=result.new_plan.strategy_position,
                    readiness=result.new_plan.execution_readiness,
                )
                self._insert_allocations(connection, result.allocations)
                finalized_at = result.new_plan.strategy_position.updated_at_utc
                connection.execute(
                    """
                    INSERT INTO internal_reverse_finalizations (
                        source_operation_id, source_attempt_id,
                        closing_position_episode_id,
                        opening_position_episode_id,
                        closing_completed_at_ts,
                        closing_completed_at_utc,
                        opening_started_at_ts,
                        opening_started_at_utc,
                        commission_complete, finalized_at_ts,
                        finalized_at_utc, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        operation_id,
                        result.new_plan.episode.source_attempt_id,
                        result.closed_episode.position_episode_id,
                        result.new_plan.episode.position_episode_id,
                        _ts(result.closing_completed_at_utc),
                        result.closing_completed_at_utc,
                        _ts(result.opening_started_at_utc),
                        result.opening_started_at_utc,
                        int(result.commission_complete),
                        _ts(finalized_at),
                        finalized_at,
                        payload,
                    ),
                )
                connection.commit()
                return result
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ReverseFinalizationStoreError):
                    raise
                raise ReverseFinalizationStoreError(
                    "cannot publish reverse finalization: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()
