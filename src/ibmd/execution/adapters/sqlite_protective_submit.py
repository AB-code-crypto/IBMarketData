from __future__ import annotations

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.public_contracts.execution import ExecutionReadinessV1
from ibmd.public_contracts.protection import ProtectionStateV1

from .sqlite_protection import (
    ProtectionStoreError,
    SQLiteProtectionStore,
    _protection,
    _ts,
)


class SQLiteProtectiveSubmitStore(SQLiteProtectionStore):
    def publish_state_and_readiness(
        self,
        *,
        current: ProtectionStateV1,
        updated: ProtectionStateV1,
        readiness: ExecutionReadinessV1,
    ) -> ProtectionStateV1:
        if current.protection_set_id != updated.protection_set_id:
            raise ProtectionStoreError("protection state identity changed")
        expected_scope = (
            updated.account_id,
            updated.strategy_id,
            updated.deployment_id,
            updated.instrument_id,
        )
        readiness_scope = (
            readiness.account_id,
            readiness.strategy_id,
            readiness.deployment_id,
            readiness.instrument_id,
        )
        if readiness_scope != expected_scope:
            raise ProtectionStoreError(
                "execution readiness belongs to another protection scope"
            )
        current_ids = {
            item.protective_order_id for item in current.orders
        }
        updated_ids = {
            item.protective_order_id for item in updated.orders
        }
        if current_ids != updated_ids:
            raise ProtectionStoreError(
                "protective order identities changed during state update"
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

                changed = updated.to_dict() != current.to_dict()
                if changed:
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

                cursor = connection.execute(
                    """
                    UPDATE internal_execution_readiness
                    SET status=?, command_intake_enabled=?,
                        broker_actions_enabled=?, updated_at_ts=?,
                        updated_at_utc=?, payload_json=?
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
                if cursor.rowcount != 1:
                    raise ProtectionStoreError(
                        "execution readiness does not exist for protection scope"
                    )
                connection.commit()
                return updated
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ProtectionStoreError):
                    raise
                raise ProtectionStoreError(
                    "cannot publish protective submission state: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()
