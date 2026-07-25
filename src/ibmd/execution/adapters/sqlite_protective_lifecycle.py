from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

from ibmd.execution.domain.protective_lifecycle import ProtectiveLifecycleUpdate
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.broker_reconciliation import (
    BrokerCommissionFactV1,
    BrokerFillFactV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeV1,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderV1,
)

from .sqlite_protection import (
    ProtectionSchemaError,
    ProtectionStoreError,
    SQLiteProtectionStore,
    _episode,
    _protection,
    _ts,
)

_COMPONENT_NAME = "execution_protective_lifecycle"
_COMPONENT_VERSION = 1
_COMPONENT_LEDGER = "execution_target_schema_components"
_REQUIRED_OBJECTS = {
    ("table", _COMPONENT_LEDGER),
    ("table", "internal_protective_fill_evidence"),
    ("table", "internal_protective_commission_evidence"),
    ("table", "internal_protective_reconciliation_observations"),
    ("view", "public_protective_fills_v1"),
    ("view", "public_protective_reconciliation_observations_v1"),
}


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ProtectionStoreError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ProtectionStoreError(f"stored {context} payload must be an object")
    return value


def _position(payload: str) -> StrategyPositionV1:
    return StrategyPositionV1.from_dict(
        _json_object(payload, context="strategy position")
    )


def _readiness(payload: str) -> ExecutionReadinessV1:
    return ExecutionReadinessV1.from_dict(
        _json_object(payload, context="execution readiness")
    )


def _stable_id(kind: str, payload: str) -> str:
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
    return f"{kind}_{digest}"


class SQLiteProtectiveLifecycleStore(SQLiteProtectionStore):
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
                raise ProtectionSchemaError(
                    f"protective lifecycle schema objects are missing: {missing}"
                )
            row = connection.execute(
                f"SELECT component_version FROM {_COMPONENT_LEDGER} "
                "WHERE component_name=? LIMIT 1",
                (_COMPONENT_NAME,),
            ).fetchone()
            if row is None or int(row["component_version"]) != _COMPONENT_VERSION:
                raise ProtectionSchemaError(
                    "protective lifecycle component is not installed: "
                    f"expected={_COMPONENT_NAME}@{_COMPONENT_VERSION}"
                )
        except sqlite3.Error as exc:
            raise ProtectionSchemaError(
                f"cannot validate protective lifecycle schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_fills(
        self,
        position_episode_id: str,
    ) -> tuple[BrokerFillFactV1, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT fill_payload_json, commission_payload_json
                FROM public_protective_fills_v1
                WHERE position_episode_id=?
                ORDER BY executed_at_ts, exec_id
                """,
                (str(position_episode_id),),
            ).fetchall()
            values = []
            for row in rows:
                fill = BrokerFillFactV1.from_dict(
                    _json_object(
                        str(row["fill_payload_json"]),
                        context="protective fill evidence",
                    )
                )
                commission_payload = row["commission_payload_json"]
                if commission_payload is not None:
                    fill = replace(
                        fill,
                        commission=BrokerCommissionFactV1.from_dict(
                            _json_object(
                                str(commission_payload),
                                context="protective commission evidence",
                            )
                        ),
                    )
                values.append(fill)
            return tuple(values)
        finally:
            connection.close()

    def read_commission_pending_exec_ids(
        self,
        position_episode_id: str,
    ) -> tuple[str, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT exec_id
                FROM public_protective_fills_v1
                WHERE position_episode_id=? AND commission_complete=0
                ORDER BY executed_at_ts, exec_id
                """,
                (str(position_episode_id),),
            ).fetchall()
            return tuple(str(row["exec_id"]) for row in rows)
        finally:
            connection.close()

    @staticmethod
    def _next_sequence(
        connection: sqlite3.Connection,
        *,
        protective_order_id: str,
    ) -> int:
        row = connection.execute(
            "SELECT COALESCE(MAX(sequence_no), 0) + 1 "
            "FROM internal_protective_reconciliation_observations "
            "WHERE protective_order_id=?",
            (protective_order_id,),
        ).fetchone()
        return int(row[0])

    @classmethod
    def _append_observation(
        cls,
        connection: sqlite3.Connection,
        *,
        order: ProtectiveOrderV1,
        protection: ProtectionStateV1,
        evidence,
    ) -> None:
        observation_payload = canonical_json_text(evidence.result.observation.to_dict())
        identity_payload = canonical_json_text(
            {
                "protective_order_id": order.protective_order_id,
                "source_session_id": evidence.result.source_session_id,
                "captured_at_utc": evidence.result.captured_at_utc,
                "observation": evidence.result.observation.to_dict(),
            }
        )
        observation_id = _stable_id(
            "protective_observation",
            identity_payload,
        )
        existing = connection.execute(
            "SELECT payload_json FROM internal_protective_reconciliation_observations "
            "WHERE observation_id=? LIMIT 1",
            (observation_id,),
        ).fetchone()
        if existing is not None:
            if str(existing["payload_json"]) != observation_payload:
                raise ProtectionStoreError(
                    "protective reconciliation observation identity conflicted"
                )
            return
        sequence = cls._next_sequence(
            connection,
            protective_order_id=order.protective_order_id,
        )
        observed = evidence.result.observation.observed_at_utc
        captured = evidence.result.captured_at_utc
        connection.execute(
            """
            INSERT INTO internal_protective_reconciliation_observations (
                observation_id, protective_order_id, protection_set_id,
                position_episode_id, sequence_no, outcome, source_session_id,
                captured_at_ts, captured_at_utc, observed_at_ts,
                observed_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation_id,
                order.protective_order_id,
                protection.protection_set_id,
                protection.position_episode_id,
                sequence,
                evidence.result.observation.outcome.value,
                evidence.result.source_session_id,
                int(parse_utc(captured).timestamp()),
                captured,
                int(parse_utc(observed).timestamp()),
                observed,
                observation_payload,
            ),
        )

    @staticmethod
    def _insert_fill(
        connection: sqlite3.Connection,
        *,
        order: ProtectiveOrderV1,
        protection: ProtectionStateV1,
        fill: BrokerFillFactV1,
    ) -> None:
        if fill.order_ref != order.order_ref:
            raise ProtectionStoreError(
                "protective fill order_ref differs from protective order"
            )
        base_fill = replace(fill, commission=None)
        fill_payload = canonical_json_text(base_fill.to_dict())
        existing = connection.execute(
            """
            SELECT protective_order_id, protection_set_id,
                   position_episode_id, payload_json
            FROM internal_protective_fill_evidence
            WHERE exec_id=? LIMIT 1
            """,
            (fill.exec_id,),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO internal_protective_fill_evidence (
                    exec_id, protective_order_id, protection_set_id,
                    position_episode_id, order_ref, broker_order_id,
                    broker_perm_id, executed_at_ts, executed_at_utc,
                    observed_at_ts, observed_at_utc, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.exec_id,
                    order.protective_order_id,
                    protection.protection_set_id,
                    protection.position_episode_id,
                    fill.order_ref,
                    fill.broker_order_id,
                    fill.broker_perm_id,
                    int(parse_utc(fill.executed_at_utc).timestamp()),
                    fill.executed_at_utc,
                    int(parse_utc(fill.observed_at_utc).timestamp()),
                    fill.observed_at_utc,
                    fill_payload,
                ),
            )
        elif (
            str(existing["protective_order_id"]) != order.protective_order_id
            or str(existing["protection_set_id"])
            != protection.protection_set_id
            or str(existing["position_episode_id"])
            != protection.position_episode_id
            or str(existing["payload_json"]) != fill_payload
        ):
            raise ProtectionStoreError(
                f"conflicting protective fill evidence for execId={fill.exec_id}"
            )

        if fill.commission is None:
            return
        commission = fill.commission
        commission_payload = canonical_json_text(commission.to_dict())
        existing_commission = connection.execute(
            "SELECT protective_order_id, payload_json "
            "FROM internal_protective_commission_evidence "
            "WHERE exec_id=? LIMIT 1",
            (fill.exec_id,),
        ).fetchone()
        if existing_commission is None:
            connection.execute(
                """
                INSERT INTO internal_protective_commission_evidence (
                    exec_id, protective_order_id, protection_set_id,
                    position_episode_id, reported_at_ts, reported_at_utc,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.exec_id,
                    order.protective_order_id,
                    protection.protection_set_id,
                    protection.position_episode_id,
                    int(parse_utc(commission.reported_at_utc).timestamp()),
                    commission.reported_at_utc,
                    commission_payload,
                ),
            )
        elif (
            str(existing_commission["protective_order_id"])
            != order.protective_order_id
            or str(existing_commission["payload_json"]) != commission_payload
        ):
            raise ProtectionStoreError(
                "conflicting protective commission evidence for "
                f"execId={fill.exec_id}"
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
            raise ProtectionStoreError(
                f"position episode does not exist: {episode.position_episode_id}"
            )

    @classmethod
    def _update_protection(
        cls,
        connection: sqlite3.Connection,
        *,
        current: ProtectionStateV1,
        updated: ProtectionStateV1,
    ) -> None:
        connection.execute(
            """
            UPDATE internal_protection_sets
            SET status=?, updated_at_ts=?, updated_at_utc=?,
                terminal_at_ts=?, terminal_at_utc=?, blocking_reason=?,
                payload_json=?
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
        previous_orders = {
            item.protective_order_id: item for item in current.orders
        }
        for order in updated.orders:
            previous = previous_orders.get(order.protective_order_id)
            if previous is None:
                raise ProtectionStoreError(
                    "protective order identity changed during lifecycle update"
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
            if previous.state != order.state:
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
    def _update_position_and_readiness(
        connection: sqlite3.Connection,
        *,
        position: StrategyPositionV1,
        readiness: ExecutionReadinessV1,
    ) -> None:
        position_cursor = connection.execute(
            """
            UPDATE internal_strategy_positions
            SET projection_status=?, side=?, quantity=?, updated_at_ts=?,
                updated_at_utc=?, payload_json=?
            WHERE account_id=? AND strategy_id=?
              AND deployment_id=? AND instrument_id=?
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
        readiness_cursor = connection.execute(
            """
            UPDATE internal_execution_readiness
            SET status=?, command_intake_enabled=?, broker_actions_enabled=?,
                updated_at_ts=?, updated_at_utc=?, payload_json=?
            WHERE account_id=? AND strategy_id=?
              AND deployment_id=? AND instrument_id=?
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
        if position_cursor.rowcount != 1 or readiness_cursor.rowcount != 1:
            raise ProtectionStoreError(
                "execution position/readiness does not exist for lifecycle scope"
            )

    def publish_lifecycle(
        self,
        *,
        current_episode: PositionEpisodeV1,
        current_protection: ProtectionStateV1,
        current_position: StrategyPositionV1,
        current_readiness: ExecutionReadinessV1,
        update: ProtectiveLifecycleUpdate,
    ) -> ProtectiveLifecycleUpdate:
        if update.episode.position_episode_id != current_episode.position_episode_id:
            raise ProtectionStoreError("position episode identity changed")
        if update.protection.protection_set_id != current_protection.protection_set_id:
            raise ProtectionStoreError("protection set identity changed")
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
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
                if None in {
                    episode_row,
                    protection_row,
                    position_row,
                    readiness_row,
                }:
                    raise ProtectionStoreError(
                        "protective lifecycle current state is incomplete"
                    )
                if _episode(str(episode_row["payload_json"])) != current_episode:
                    raise ProtectionStoreError(
                        "position episode changed concurrently"
                    )
                if _protection(str(protection_row["payload_json"])) != current_protection:
                    raise ProtectionStoreError(
                        "protection state changed concurrently"
                    )
                if _position(str(position_row["payload_json"])) != current_position:
                    raise ProtectionStoreError(
                        "strategy position changed concurrently"
                    )
                if _readiness(str(readiness_row["payload_json"])) != current_readiness:
                    raise ProtectionStoreError(
                        "execution readiness changed concurrently"
                    )

                orders_by_kind = {
                    item.kind: item for item in update.protection.orders
                }
                for evidence in update.evidence:
                    order = orders_by_kind[evidence.kind]
                    self._append_observation(
                        connection,
                        order=order,
                        protection=update.protection,
                        evidence=evidence,
                    )
                    for fill in evidence.result.fills:
                        self._insert_fill(
                            connection,
                            order=order,
                            protection=update.protection,
                            fill=fill,
                        )

                if current_episode != update.episode:
                    self._update_episode(connection, update.episode)
                if current_protection != update.protection:
                    self._update_protection(
                        connection,
                        current=current_protection,
                        updated=update.protection,
                    )
                if (
                    current_position != update.strategy_position
                    or current_readiness != update.execution_readiness
                ):
                    self._update_position_and_readiness(
                        connection,
                        position=update.strategy_position,
                        readiness=update.execution_readiness,
                    )
                connection.commit()
                return update
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ProtectionStoreError):
                    raise
                raise ProtectionStoreError(
                    "cannot publish protective lifecycle: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()
