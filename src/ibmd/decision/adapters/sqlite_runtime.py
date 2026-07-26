from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ibmd.decision.domain import (
    DailyRiskStatus,
    ExecutionDecisionFixtureV1,
    PositionProjectionStatus,
    PositionSide,
    StrategyPositionFixtureV1,
)
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.signal import SignalEventV1


class DecisionRuntimeReadError(RuntimeError):
    pass


class DecisionRuntimeSchemaError(DecisionRuntimeReadError):
    pass


class DecisionRuntimeStateIncomplete(DecisionRuntimeReadError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DecisionRuntimeReadError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DecisionRuntimeReadError(
            f"stored {context} payload must be an object"
        )
    return value


class SQLiteDecisionRuntimeReader:
    def __init__(
        self,
        *,
        signal_database: str | Path,
        decision_database: str | Path,
        execution_database: str | Path,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.signal_database = Path(signal_database)
        self.decision_database = Path(decision_database)
        self.execution_database = Path(execution_database)
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")

    def _connect(self, database: Path) -> sqlite3.Connection:
        if not database.is_file():
            raise DecisionRuntimeSchemaError(
                f"decision runtime database does not exist: {database}"
            )
        uri = f"file:{database.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        return connection

    @staticmethod
    def _validate_views(
        connection: sqlite3.Connection,
        *,
        required: set[str],
        context: str,
    ) -> None:
        placeholders = ",".join("?" for _ in required)
        rows = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='view' AND name IN (" + placeholders + ")",
            tuple(sorted(required)),
        ).fetchall()
        existing = {str(row["name"]) for row in rows}
        missing = sorted(required - existing)
        if missing:
            raise DecisionRuntimeSchemaError(
                f"{context} public views are missing: {missing}"
            )

    def validate_schema(self) -> None:
        checks = (
            (
                self.signal_database,
                {"public_signal_events_v1"},
                "signal",
            ),
            (
                self.decision_database,
                {
                    "public_decision_records_v1",
                    "public_strategy_command_requests_v1",
                },
                "decision",
            ),
            (
                self.execution_database,
                {
                    "public_strategy_positions_v1",
                    "public_execution_readiness_v1",
                    "public_daily_risk_states_v1",
                    "public_execution_command_states_v1",
                    "public_broker_order_operations_v1",
                },
                "execution",
            ),
        )
        for database, required, context in checks:
            connection = self._connect(database)
            try:
                self._validate_views(
                    connection,
                    required=required,
                    context=context,
                )
            except sqlite3.Error as exc:
                raise DecisionRuntimeSchemaError(
                    f"cannot validate {context} decision-runtime source: {exc}"
                ) from exc
            finally:
                connection.close()

    def read_next_pending_event(
        self,
        *,
        strategy_id: str,
        strategy_version: int,
        deployment_id: str,
        instrument_id: str,
        configuration_hash: str,
        policy_hash: str,
    ) -> SignalEventV1 | None:
        decision_connection = self._connect(self.decision_database)
        try:
            processed = {
                str(row["source_signal_id"])
                for row in decision_connection.execute(
                    """
                    SELECT source_signal_id
                    FROM public_decision_records_v1
                    WHERE strategy_id = ?
                      AND strategy_version = ?
                      AND deployment_id = ?
                      AND instrument_id = ?
                      AND policy_hash = ?
                    """,
                    (
                        str(strategy_id),
                        int(strategy_version),
                        str(deployment_id),
                        str(instrument_id),
                        str(policy_hash),
                    ),
                ).fetchall()
            }
        except sqlite3.Error as exc:
            raise DecisionRuntimeReadError(
                f"cannot read processed decision signal ids: {exc}"
            ) from exc
        finally:
            decision_connection.close()

        signal_connection = self._connect(self.signal_database)
        try:
            rows = signal_connection.execute(
                """
                SELECT event_id, payload_json
                FROM public_signal_events_v1
                WHERE strategy_id = ?
                  AND strategy_version = ?
                  AND instrument_id = ?
                  AND configuration_hash = ?
                ORDER BY signal_bar_ts, event_id
                """,
                (
                    str(strategy_id),
                    int(strategy_version),
                    str(instrument_id),
                    str(configuration_hash),
                ),
            ).fetchall()
            for row in rows:
                event_id = str(row["event_id"])
                if event_id in processed:
                    continue
                return SignalEventV1.from_dict(
                    _json_object(
                        str(row["payload_json"]),
                        context="signal event",
                    )
                )
            return None
        except sqlite3.Error as exc:
            raise DecisionRuntimeReadError(
                f"cannot read pending target signal events: {exc}"
            ) from exc
        finally:
            signal_connection.close()

    @staticmethod
    def _scope(
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[str, str, str, str]:
        return (
            str(account_id),
            str(strategy_id),
            str(deployment_id),
            str(instrument_id),
        )

    def _read_execution_state(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[StrategyPositionV1, ExecutionReadinessV1, DailyRiskStateV1]:
        connection = self._connect(self.execution_database)
        scope = self._scope(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            instrument_id=instrument_id,
        )
        try:
            position_row = connection.execute(
                """
                SELECT payload_json
                FROM public_strategy_positions_v1
                WHERE account_id=? AND strategy_id=?
                  AND deployment_id=? AND instrument_id=?
                LIMIT 1
                """,
                scope,
            ).fetchone()
            readiness_row = connection.execute(
                """
                SELECT payload_json
                FROM public_execution_readiness_v1
                WHERE account_id=? AND strategy_id=?
                  AND deployment_id=? AND instrument_id=?
                LIMIT 1
                """,
                scope,
            ).fetchone()
            risk_row = connection.execute(
                """
                SELECT payload_json
                FROM public_daily_risk_states_v1
                WHERE account_id=? AND strategy_id=? AND deployment_id=?
                ORDER BY trading_day DESC, updated_at_ts DESC
                LIMIT 1
                """,
                scope[:3],
            ).fetchone()
        except sqlite3.Error as exc:
            raise DecisionRuntimeReadError(
                f"cannot read execution public state for decision: {exc}"
            ) from exc
        finally:
            connection.close()

        missing = [
            name
            for name, row in (
                ("strategy_position", position_row),
                ("execution_readiness", readiness_row),
                ("daily_risk", risk_row),
            )
            if row is None
        ]
        if missing:
            raise DecisionRuntimeStateIncomplete(
                "execution public state is incomplete for decision: "
                f"missing={missing}"
            )
        return (
            StrategyPositionV1.from_dict(
                _json_object(
                    str(position_row["payload_json"]),
                    context="strategy position",
                )
            ),
            ExecutionReadinessV1.from_dict(
                _json_object(
                    str(readiness_row["payload_json"]),
                    context="execution readiness",
                )
            ),
            DailyRiskStateV1.from_dict(
                _json_object(
                    str(risk_row["payload_json"]),
                    context="daily risk",
                )
            ),
        )

    def _has_unresolved_command(
        self,
        *,
        strategy_id: str,
        strategy_version: int,
        deployment_id: str,
        instrument_id: str,
    ) -> bool:
        decision_connection = self._connect(self.decision_database)
        try:
            command_ids = tuple(
                str(row["command_id"])
                for row in decision_connection.execute(
                    """
                    SELECT command_id
                    FROM public_strategy_command_requests_v1
                    WHERE strategy_id=? AND strategy_version=?
                      AND deployment_id=? AND instrument_id=?
                    ORDER BY created_at_ts, command_id
                    """,
                    (
                        str(strategy_id),
                        int(strategy_version),
                        str(deployment_id),
                        str(instrument_id),
                    ),
                ).fetchall()
            )
        except sqlite3.Error as exc:
            raise DecisionRuntimeReadError(
                f"cannot read decision command outbox: {exc}"
            ) from exc
        finally:
            decision_connection.close()

        if not command_ids:
            return False
        execution_connection = self._connect(self.execution_database)
        try:
            for command_id in command_ids:
                command_row = execution_connection.execute(
                    """
                    SELECT state
                    FROM public_execution_command_states_v1
                    WHERE command_id=?
                    LIMIT 1
                    """,
                    (command_id,),
                ).fetchone()
                if command_row is None:
                    return True
                if str(command_row["state"]) == "REJECTED":
                    continue
                operation_row = execution_connection.execute(
                    """
                    SELECT state
                    FROM public_broker_order_operations_v1
                    WHERE command_id=?
                    LIMIT 1
                    """,
                    (command_id,),
                ).fetchone()
                if operation_row is None:
                    return True
                if str(operation_row["state"]) != "SUCCEEDED":
                    return True
            return False
        except sqlite3.Error as exc:
            raise DecisionRuntimeReadError(
                f"cannot resolve decision command lifecycle: {exc}"
            ) from exc
        finally:
            execution_connection.close()

    def read_fixture(
        self,
        *,
        account_id: str,
        strategy_id: str,
        strategy_version: int,
        deployment_id: str,
        instrument_id: str,
        observed_at_utc: str,
    ) -> ExecutionDecisionFixtureV1:
        position, readiness, risk = self._read_execution_state(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            instrument_id=instrument_id,
        )
        expected_scope = self._scope(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            instrument_id=instrument_id,
        )
        position_scope = self._scope(
            account_id=position.account_id,
            strategy_id=position.strategy_id,
            deployment_id=position.deployment_id,
            instrument_id=position.instrument_id,
        )
        readiness_scope = self._scope(
            account_id=readiness.account_id,
            strategy_id=readiness.strategy_id,
            deployment_id=readiness.deployment_id,
            instrument_id=readiness.instrument_id,
        )
        risk_scope = (
            risk.account_id,
            risk.strategy_id,
            risk.deployment_id,
        )
        if (
            position_scope != expected_scope
            or readiness_scope != expected_scope
            or risk_scope != expected_scope[:3]
        ):
            raise DecisionRuntimeReadError(
                "execution public state belongs to another decision scope"
            )

        unresolved = self._has_unresolved_command(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            deployment_id=deployment_id,
            instrument_id=instrument_id,
        )
        if position.projection_status == StrategyPositionStatus.OPEN:
            contract_is_active = (
                len(position.contracts) == 1
                and position.contracts[0].contract_is_active
            )
        else:
            contract_is_active = None

        execution_ready = (
            readiness.status == ExecutionReadinessStatus.READY
            and readiness.command_intake_enabled
            and readiness.broker_actions_enabled
        )
        reasons = list(readiness.blocking_reasons)
        if not execution_ready and not reasons:
            reasons.append(f"execution_readiness:{readiness.status.value}")
        if not risk.pnl_ready:
            reasons.append("daily_risk:pnl_not_ready")
        if risk.status.value != DailyRiskStatus.MONITORING.value:
            reasons.append(f"daily_risk:{risk.status.value}")
        if unresolved:
            reasons.append("unresolved_command_exists")
        unique_reasons = tuple(dict.fromkeys(reasons))

        return ExecutionDecisionFixtureV1(
            observed_at_utc=observed_at_utc,
            execution_ready=execution_ready,
            execution_clock_healthy=readiness.clock_healthy,
            pnl_reconciliation_ready=risk.pnl_ready,
            unresolved_command=unresolved,
            daily_risk_status=DailyRiskStatus(risk.status.value),
            blocking_reason=(
                None if not unique_reasons else "; ".join(unique_reasons)
            ),
            position=StrategyPositionFixtureV1(
                account_id=position.account_id,
                strategy_id=position.strategy_id,
                deployment_id=position.deployment_id,
                instrument_id=position.instrument_id,
                projection_status=PositionProjectionStatus(
                    position.projection_status.value
                ),
                side=PositionSide(position.side.value),
                quantity=position.quantity,
                contract_is_active=contract_is_active,
            ),
        )
