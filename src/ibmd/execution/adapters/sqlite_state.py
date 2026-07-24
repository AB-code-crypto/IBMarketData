from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    ExecutionReadinessV1,
    StrategyPositionV1,
)


class ExecutionStateReadError(RuntimeError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ExecutionStateReadError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ExecutionStateReadError(
            f"stored {context} payload must be an object"
        )
    return value


class SQLiteExecutionStateReader:
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
            raise ExecutionStateReadError(
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
        required = {
            "public_strategy_positions_v1",
            "public_execution_readiness_v1",
            "public_daily_risk_states_v1",
        }
        connection = self._connect()
        try:
            existing = {
                str(row["name"])
                for row in connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'view'
                      AND name IN (
                          'public_strategy_positions_v1',
                          'public_execution_readiness_v1',
                          'public_daily_risk_states_v1'
                      )
                    """
                ).fetchall()
            }
            missing = sorted(required - existing)
            if missing:
                raise ExecutionStateReadError(
                    f"execution public state views are missing: {missing}"
                )
        except sqlite3.Error as exc:
            raise ExecutionStateReadError(
                f"cannot validate execution public state: {exc}"
            ) from exc
        finally:
            connection.close()

    @staticmethod
    def _scope_values(
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

    def read_position(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> StrategyPositionV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_strategy_positions_v1
                WHERE account_id = ?
                  AND strategy_id = ?
                  AND deployment_id = ?
                  AND instrument_id = ?
                LIMIT 1
                """,
                self._scope_values(
                    account_id=account_id,
                    strategy_id=strategy_id,
                    deployment_id=deployment_id,
                    instrument_id=instrument_id,
                ),
            ).fetchone()
            return (
                None
                if row is None
                else StrategyPositionV1.from_dict(
                    _json_object(
                        str(row["payload_json"]),
                        context="strategy position",
                    )
                )
            )
        finally:
            connection.close()

    def read_readiness(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionReadinessV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_execution_readiness_v1
                WHERE account_id = ?
                  AND strategy_id = ?
                  AND deployment_id = ?
                  AND instrument_id = ?
                LIMIT 1
                """,
                self._scope_values(
                    account_id=account_id,
                    strategy_id=strategy_id,
                    deployment_id=deployment_id,
                    instrument_id=instrument_id,
                ),
            ).fetchone()
            return (
                None
                if row is None
                else ExecutionReadinessV1.from_dict(
                    _json_object(
                        str(row["payload_json"]),
                        context="execution readiness",
                    )
                )
            )
        finally:
            connection.close()

    def read_latest_daily_risk(
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
            return (
                None
                if row is None
                else DailyRiskStateV1.from_dict(
                    _json_object(
                        str(row["payload_json"]),
                        context="daily risk state",
                    )
                )
            )
        finally:
            connection.close()
