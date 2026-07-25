from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from pathlib import Path

from ibmd.execution.domain.daily_risk import DailyRiskUpdateV1
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.daily_risk import DailyRiskCalculationV1
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    ExecutionReadinessV1,
)

_COMPONENT_NAME = "execution_daily_risk"
_COMPONENT_VERSION = 1
_COMPONENT_LEDGER = "execution_target_schema_components"
_REQUIRED_OBJECTS = {
    ("table", "internal_daily_risk_states"),
    ("table", "internal_execution_readiness"),
    ("table", "internal_daily_risk_calculations"),
    ("table", "internal_daily_risk_transitions"),
    ("view", "public_daily_risk_states_v1"),
    ("view", "public_daily_risk_calculations_v1"),
    ("view", "public_daily_risk_transitions_v1"),
}


class DailyRiskStoreError(RuntimeError):
    pass


class DailyRiskSchemaError(DailyRiskStoreError):
    pass


def _ts(value: str) -> int:
    return int(parse_utc(value).timestamp())


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DailyRiskStoreError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DailyRiskStoreError(
            f"stored {context} payload must be an object"
        )
    return value


def _state(payload: str) -> DailyRiskStateV1:
    return DailyRiskStateV1.from_dict(
        _json_object(payload, context="daily risk state")
    )


def _readiness(payload: str) -> ExecutionReadinessV1:
    return ExecutionReadinessV1.from_dict(
        _json_object(payload, context="execution readiness")
    )


def _calculation(payload: str) -> DailyRiskCalculationV1:
    return DailyRiskCalculationV1.from_dict(
        _json_object(payload, context="daily risk calculation")
    )


def _stable_id(kind: str, payload: object) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


class SQLiteDailyRiskStore:
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
            raise DailyRiskSchemaError(
                f"execution database does not exist: {self.database_path}"
            )
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
                raise DailyRiskSchemaError(
                    f"daily-risk schema objects are missing: {missing}"
                )
            row = connection.execute(
                f"SELECT component_version FROM {_COMPONENT_LEDGER} "
                "WHERE component_name=? LIMIT 1",
                (_COMPONENT_NAME,),
            ).fetchone()
            if row is None or int(row["component_version"]) != _COMPONENT_VERSION:
                raise DailyRiskSchemaError(
                    "daily-risk component is not installed: "
                    f"expected={_COMPONENT_NAME}@{_COMPONENT_VERSION}"
                )
        except sqlite3.Error as exc:
            raise DailyRiskSchemaError(
                f"cannot validate daily-risk schema: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_latest_state(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
    ) -> DailyRiskStateV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_daily_risk_states_v1
                WHERE account_id = ?
                  AND strategy_id = ?
                  AND deployment_id = ?
                ORDER BY trading_day DESC, updated_at_ts DESC
                LIMIT 1
                """,
                (
                    str(account_id),
                    str(strategy_id),
                    str(deployment_id),
                ),
            ).fetchone()
            return None if row is None else _state(str(row["payload_json"]))
        finally:
            connection.close()

    def read_calculation(
        self,
        calculation_id: str,
    ) -> DailyRiskCalculationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM public_daily_risk_calculations_v1 "
                "WHERE calculation_id=? LIMIT 1",
                (str(calculation_id),),
            ).fetchone()
            return (
                None
                if row is None
                else _calculation(str(row["payload_json"]))
            )
        finally:
            connection.close()

    @staticmethod
    def _latest_state_row(
        connection: sqlite3.Connection,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT payload_json
            FROM internal_daily_risk_states
            WHERE account_id = ?
              AND strategy_id = ?
              AND deployment_id = ?
            ORDER BY trading_day DESC, updated_at_ts DESC
            LIMIT 1
            """,
            (account_id, strategy_id, deployment_id),
        ).fetchone()

    @staticmethod
    def _insert_calculation(
        connection: sqlite3.Connection,
        calculation: DailyRiskCalculationV1,
    ) -> None:
        payload = canonical_json_text(calculation.to_dict())
        row = connection.execute(
            "SELECT payload_json FROM internal_daily_risk_calculations "
            "WHERE calculation_id=? LIMIT 1",
            (calculation.calculation_id,),
        ).fetchone()
        if row is not None:
            if str(row["payload_json"]) != payload:
                raise DailyRiskStoreError(
                    "daily-risk calculation identity conflicted: "
                    f"{calculation.calculation_id}"
                )
            return
        connection.execute(
            """
            INSERT INTO internal_daily_risk_calculations (
                calculation_id, account_id, strategy_id, strategy_version,
                deployment_id, instrument_id, trading_day, pnl_ready,
                reason_code, calculated_at_ts, calculated_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                calculation.calculation_id,
                calculation.account_id,
                calculation.strategy_id,
                calculation.strategy_version,
                calculation.deployment_id,
                calculation.instrument_id,
                calculation.trading_day,
                int(calculation.pnl_ready),
                calculation.reason_code,
                _ts(calculation.calculated_at_utc),
                calculation.calculated_at_utc,
                payload,
            ),
        )

    @staticmethod
    def _write_state(
        connection: sqlite3.Connection,
        state: DailyRiskStateV1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO internal_daily_risk_states (
                account_id, strategy_id, deployment_id, trading_day,
                status, pnl_ready, cleanup_status, updated_at_ts,
                updated_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, strategy_id, deployment_id, trading_day)
            DO UPDATE SET
                status=excluded.status,
                pnl_ready=excluded.pnl_ready,
                cleanup_status=excluded.cleanup_status,
                updated_at_ts=excluded.updated_at_ts,
                updated_at_utc=excluded.updated_at_utc,
                payload_json=excluded.payload_json
            """,
            (
                state.account_id,
                state.strategy_id,
                state.deployment_id,
                state.trading_day,
                state.status.value,
                int(state.pnl_ready),
                state.cleanup_status.value,
                _ts(state.updated_at_utc),
                state.updated_at_utc,
                canonical_json_text(state.to_dict()),
            ),
        )

    @staticmethod
    def _update_readiness(
        connection: sqlite3.Connection,
        *,
        current: ExecutionReadinessV1,
        updated: ExecutionReadinessV1,
    ) -> None:
        row = connection.execute(
            "SELECT payload_json FROM internal_execution_readiness "
            "WHERE account_id=? AND strategy_id=? AND deployment_id=? "
            "AND instrument_id=? LIMIT 1",
            (
                current.account_id,
                current.strategy_id,
                current.deployment_id,
                current.instrument_id,
            ),
        ).fetchone()
        if row is None:
            raise DailyRiskStoreError(
                "execution readiness is missing before daily-risk update"
            )
        stored = _readiness(str(row["payload_json"]))
        if stored.to_dict() != current.to_dict():
            raise DailyRiskStoreError(
                "execution readiness changed concurrently before daily-risk update"
            )
        connection.execute(
            """
            UPDATE internal_execution_readiness
            SET status=?, command_intake_enabled=?, broker_actions_enabled=?,
                updated_at_ts=?, updated_at_utc=?, payload_json=?
            WHERE account_id=? AND strategy_id=? AND deployment_id=?
              AND instrument_id=?
            """,
            (
                updated.status.value,
                int(updated.command_intake_enabled),
                int(updated.broker_actions_enabled),
                _ts(updated.updated_at_utc),
                updated.updated_at_utc,
                canonical_json_text(updated.to_dict()),
                updated.account_id,
                updated.strategy_id,
                updated.deployment_id,
                updated.instrument_id,
            ),
        )

    @staticmethod
    def _append_transition(
        connection: sqlite3.Connection,
        *,
        previous: DailyRiskStateV1 | None,
        updated: DailyRiskStateV1,
        calculation: DailyRiskCalculationV1,
    ) -> None:
        if previous is not None and (
            previous.trading_day == updated.trading_day
            and previous.status == updated.status
            and previous.cleanup_status == updated.cleanup_status
        ):
            return
        row = connection.execute(
            "SELECT COALESCE(MAX(sequence_no), 0) + 1 "
            "FROM internal_daily_risk_transitions "
            "WHERE account_id=? AND strategy_id=? AND deployment_id=? "
            "AND trading_day=?",
            (
                updated.account_id,
                updated.strategy_id,
                updated.deployment_id,
                updated.trading_day,
            ),
        ).fetchone()
        sequence = int(row[0])
        transition_id = _stable_id(
            "daily_risk_transition",
            {
                "scope": (
                    updated.account_id,
                    updated.strategy_id,
                    updated.deployment_id,
                ),
                "trading_day": updated.trading_day,
                "sequence_no": sequence,
                "from_status": None if previous is None else previous.status.value,
                "to_status": updated.status.value,
                "from_cleanup": (
                    None if previous is None else previous.cleanup_status.value
                ),
                "to_cleanup": updated.cleanup_status.value,
                "calculation_id": calculation.calculation_id,
            },
        )
        connection.execute(
            """
            INSERT INTO internal_daily_risk_transitions (
                transition_id, account_id, strategy_id, deployment_id,
                trading_day, sequence_no, from_status, to_status,
                from_cleanup_status, to_cleanup_status, calculation_id,
                occurred_at_ts, occurred_at_utc, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transition_id,
                updated.account_id,
                updated.strategy_id,
                updated.deployment_id,
                updated.trading_day,
                sequence,
                None if previous is None else previous.status.value,
                updated.status.value,
                None if previous is None else previous.cleanup_status.value,
                updated.cleanup_status.value,
                calculation.calculation_id,
                _ts(updated.updated_at_utc),
                updated.updated_at_utc,
                canonical_json_text(
                    {
                        "previous": (
                            None if previous is None else previous.to_dict()
                        ),
                        "updated": updated.to_dict(),
                        "calculation_id": calculation.calculation_id,
                    }
                ),
            ),
        )

    def publish(
        self,
        *,
        current_state: DailyRiskStateV1 | None,
        current_readiness: ExecutionReadinessV1,
        update: DailyRiskUpdateV1,
    ) -> DailyRiskUpdateV1:
        if not isinstance(update, DailyRiskUpdateV1):
            raise DailyRiskStoreError("update must be DailyRiskUpdateV1")
        state = update.state
        calculation = update.calculation
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                latest_row = self._latest_state_row(
                    connection,
                    account_id=state.account_id,
                    strategy_id=state.strategy_id,
                    deployment_id=state.deployment_id,
                )
                stored_latest = (
                    None
                    if latest_row is None
                    else _state(str(latest_row["payload_json"]))
                )
                if stored_latest != current_state:
                    raise DailyRiskStoreError(
                        "daily-risk state changed concurrently"
                    )
                day_row = connection.execute(
                    "SELECT payload_json FROM internal_daily_risk_states "
                    "WHERE account_id=? AND strategy_id=? AND deployment_id=? "
                    "AND trading_day=? LIMIT 1",
                    (
                        state.account_id,
                        state.strategy_id,
                        state.deployment_id,
                        state.trading_day,
                    ),
                ).fetchone()
                previous_day_state = (
                    None if day_row is None else _state(str(day_row["payload_json"]))
                )
                self._insert_calculation(connection, calculation)
                self._write_state(connection, state)
                self._append_transition(
                    connection,
                    previous=previous_day_state,
                    updated=state,
                    calculation=calculation,
                )
                self._update_readiness(
                    connection,
                    current=current_readiness,
                    updated=update.execution_readiness,
                )
                connection.commit()
                return update
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, DailyRiskStoreError):
                    raise
                raise DailyRiskStoreError(
                    "cannot publish daily-risk update: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()
