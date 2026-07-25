from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.protection import PositionEpisodeV1, ProtectionStateV1

from .sqlite_protection import SQLiteProtectionReader
from .sqlite_state import SQLiteExecutionStateReader


class LiquidationTriggerReadError(RuntimeError):
    pass


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise LiquidationTriggerReadError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise LiquidationTriggerReadError(
            f"stored {context} payload must be an object"
        )
    return value


class SQLiteLiquidationTriggerReader:
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
        self.protection_reader = SQLiteProtectionReader(
            self.database_path,
            busy_timeout_ms=self.busy_timeout_ms,
        )
        self.state_reader = SQLiteExecutionStateReader(
            self.database_path,
            busy_timeout_ms=self.busy_timeout_ms,
        )

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file():
            raise LiquidationTriggerReadError(
                f"execution database does not exist: {self.database_path}"
            )
        uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        return connection

    def validate_schema(self) -> None:
        self.protection_reader.validate_schema()
        self.state_reader.validate_schema()
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='view' AND name='public_position_episodes_v1'"
            ).fetchone()
            if row is None:
                raise LiquidationTriggerReadError(
                    "public_position_episodes_v1 is missing"
                )
        except sqlite3.Error as exc:
            raise LiquidationTriggerReadError(
                f"cannot validate liquidation-trigger source: {exc}"
            ) from exc
        finally:
            connection.close()

    def list_open_episodes(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[PositionEpisodeV1, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT payload_json
                FROM public_position_episodes_v1
                WHERE account_id=?
                  AND strategy_id=?
                  AND deployment_id=?
                  AND instrument_id=?
                  AND status='OPEN'
                ORDER BY opened_at_ts, position_episode_id
                """,
                (
                    str(account_id),
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchall()
            return tuple(
                PositionEpisodeV1.from_dict(
                    _json_object(
                        str(row["payload_json"]),
                        context="position episode",
                    )
                )
                for row in rows
            )
        except sqlite3.Error as exc:
            raise LiquidationTriggerReadError(
                f"cannot read open position episodes: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None:
        return self.protection_reader.read_protection_by_episode(
            position_episode_id
        )

    def read_position(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> StrategyPositionV1 | None:
        return self.state_reader.read_position(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            instrument_id=instrument_id,
        )

    def read_readiness(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionReadinessV1 | None:
        return self.state_reader.read_readiness(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            instrument_id=instrument_id,
        )

    def read_latest_daily_risk(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
    ) -> DailyRiskStateV1 | None:
        return self.state_reader.read_latest_daily_risk(
            account_id=account_id,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
        )
