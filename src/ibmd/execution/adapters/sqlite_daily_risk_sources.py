from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

from ibmd.execution.domain.daily_risk import (
    DailyRiskFillKind,
    DailyRiskMarketMarkV1,
    DailyRiskOwnedFillV1,
)
from ibmd.foundation.time import parse_utc
from ibmd.public_contracts.broker_reconciliation import (
    BrokerCommissionFactV1,
    BrokerFillFactV1,
)
from ibmd.public_contracts.liquidation import LiquidationOperationV1


class DailyRiskSourceError(RuntimeError):
    pass


_REQUIRED_EXECUTION_OBJECTS = {
    ("table", "internal_broker_reconciliation_observations"),
    ("table", "internal_broker_order_operations"),
    ("table", "internal_execution_command_states"),
    ("view", "public_protective_fills_v1"),
    ("view", "public_liquidation_fills_v1"),
    ("view", "public_liquidation_operations_v1"),
    ("view", "public_position_episodes_v1"),
}
_REQUIRED_MARKET_OBJECTS = {
    ("view", "public_market_data_latest_v1"),
}


def _json_object(payload: str, *, context: str) -> dict:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DailyRiskSourceError(
            f"stored {context} JSON is invalid: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DailyRiskSourceError(
            f"stored {context} payload must be an object"
        )
    return value


def _attach_commission(
    fill: BrokerFillFactV1,
    commission_payload: str | None,
) -> BrokerFillFactV1:
    if commission_payload is None:
        return replace(fill, commission=None)
    commission = BrokerCommissionFactV1.from_dict(
        _json_object(
            commission_payload,
            context=f"commission for execId={fill.exec_id}",
        )
    )
    if commission.exec_id != fill.exec_id:
        raise DailyRiskSourceError(
            "commission execId differs from its fill: "
            f"fill={fill.exec_id}, commission={commission.exec_id}"
        )
    return replace(fill, commission=commission)


class SQLiteDailyRiskExecutionReader:
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
            raise DailyRiskSourceError(
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
            missing = sorted(_REQUIRED_EXECUTION_OBJECTS - objects)
            if missing:
                raise DailyRiskSourceError(
                    f"daily-risk execution objects are missing: {missing}"
                )
        except sqlite3.Error as exc:
            raise DailyRiskSourceError(
                f"cannot validate daily-risk execution sources: {exc}"
            ) from exc
        finally:
            connection.close()

    def _strategic_fills(
        self,
        connection: sqlite3.Connection,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[DailyRiskOwnedFillV1, ...]:
        rows = connection.execute(
            """
            SELECT
                evidence.outcome,
                evidence.payload_json,
                commands.command_kind
            FROM internal_broker_reconciliation_observations AS evidence
            JOIN internal_broker_order_operations AS operations
              ON operations.operation_id = evidence.operation_id
            JOIN internal_execution_command_states AS commands
              ON commands.command_id = operations.command_id
            WHERE operations.account_id = ?
              AND operations.strategy_id = ?
              AND operations.deployment_id = ?
              AND operations.instrument_id = ?
              AND evidence.outcome IN ('FILL', 'COMMISSION')
            ORDER BY evidence.attempt_id, evidence.sequence_no
            """,
            (
                str(account_id),
                str(strategy_id),
                str(deployment_id),
                str(instrument_id),
            ),
        ).fetchall()
        fills: dict[str, tuple[BrokerFillFactV1, str]] = {}
        commissions: dict[str, BrokerCommissionFactV1] = {}
        for row in rows:
            outcome = str(row["outcome"])
            payload = _json_object(
                str(row["payload_json"]),
                context=f"strategic {outcome.lower()} evidence",
            )
            if outcome == "FILL":
                fill = BrokerFillFactV1.from_dict(payload)
                command_kind = str(row["command_kind"])
                existing = fills.get(fill.exec_id)
                if existing is not None and existing != (fill, command_kind):
                    raise DailyRiskSourceError(
                        f"conflicting strategic fill evidence: {fill.exec_id}"
                    )
                fills[fill.exec_id] = (fill, command_kind)
            else:
                commission = BrokerCommissionFactV1.from_dict(payload)
                existing = commissions.get(commission.exec_id)
                if existing is not None and existing != commission:
                    raise DailyRiskSourceError(
                        "conflicting strategic commission evidence: "
                        f"{commission.exec_id}"
                    )
                commissions[commission.exec_id] = commission
        unknown_commissions = sorted(set(commissions) - set(fills))
        if unknown_commissions:
            raise DailyRiskSourceError(
                "strategic commissions have no owned fill evidence: "
                f"{unknown_commissions}"
            )
        values = []
        for exec_id, (base_fill, command_kind) in sorted(fills.items()):
            fill = replace(
                base_fill,
                commission=commissions.get(exec_id),
            )
            if command_kind == "OPEN":
                kind = DailyRiskFillKind.STRATEGIC_OPEN
            elif command_kind == "REVERSE":
                kind = DailyRiskFillKind.STRATEGIC_REVERSE
            else:
                raise DailyRiskSourceError(
                    f"unsupported strategic command kind: {command_kind!r}"
                )
            values.append(DailyRiskOwnedFillV1(kind=kind, fill=fill))
        return tuple(values)

    @staticmethod
    def _component_fills(
        connection: sqlite3.Connection,
        *,
        view_name: str,
        kind: DailyRiskFillKind,
        scope_join: str,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[DailyRiskOwnedFillV1, ...]:
        rows = connection.execute(
            f"""
            SELECT
                fills.fill_payload_json,
                fills.commission_payload_json
            FROM {view_name} AS fills
            {scope_join}
            WHERE scope.account_id = ?
              AND scope.strategy_id = ?
              AND scope.deployment_id = ?
              AND scope.instrument_id = ?
            ORDER BY fills.executed_at_ts, fills.exec_id
            """,
            (
                str(account_id),
                str(strategy_id),
                str(deployment_id),
                str(instrument_id),
            ),
        ).fetchall()
        values = []
        for row in rows:
            fill = BrokerFillFactV1.from_dict(
                _json_object(
                    str(row["fill_payload_json"]),
                    context=f"{kind.value.lower()} fill evidence",
                )
            )
            fill = _attach_commission(
                fill,
                (
                    None
                    if row["commission_payload_json"] is None
                    else str(row["commission_payload_json"])
                ),
            )
            values.append(DailyRiskOwnedFillV1(kind=kind, fill=fill))
        return tuple(values)

    def read_owned_fills(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[DailyRiskOwnedFillV1, ...]:
        connection = self._connect()
        try:
            strategic = self._strategic_fills(
                connection,
                account_id=account_id,
                strategy_id=strategy_id,
                deployment_id=deployment_id,
                instrument_id=instrument_id,
            )
            protective = self._component_fills(
                connection,
                view_name="public_protective_fills_v1",
                kind=DailyRiskFillKind.PROTECTIVE_EXIT,
                scope_join=(
                    "JOIN internal_position_episodes AS scope "
                    "ON scope.position_episode_id = fills.position_episode_id"
                ),
                account_id=account_id,
                strategy_id=strategy_id,
                deployment_id=deployment_id,
                instrument_id=instrument_id,
            )
            liquidation = self._component_fills(
                connection,
                view_name="public_liquidation_fills_v1",
                kind=DailyRiskFillKind.LIQUIDATION_EXIT,
                scope_join=(
                    "JOIN internal_liquidation_operations AS scope "
                    "ON scope.liquidation_operation_id = "
                    "fills.liquidation_operation_id"
                ),
                account_id=account_id,
                strategy_id=strategy_id,
                deployment_id=deployment_id,
                instrument_id=instrument_id,
            )
            values = strategic + protective + liquidation
            exec_ids = [item.fill.exec_id for item in values]
            if len(exec_ids) != len(set(exec_ids)):
                raise DailyRiskSourceError(
                    "one execId appears in multiple strategy-owned fill sources"
                )
            return tuple(
                sorted(
                    values,
                    key=lambda item: (
                        parse_utc(item.fill.executed_at_utc),
                        item.fill.exec_id,
                    ),
                )
            )
        except sqlite3.Error as exc:
            raise DailyRiskSourceError(
                f"cannot read strategy-owned daily-risk fills: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_latest_liquidation_operation(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> LiquidationOperationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_liquidation_operations_v1
                WHERE account_id = ?
                  AND strategy_id = ?
                  AND deployment_id = ?
                  AND instrument_id = ?
                ORDER BY updated_at_ts DESC, liquidation_operation_id DESC
                LIMIT 1
                """,
                (
                    str(account_id),
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchone()
            return (
                None
                if row is None
                else LiquidationOperationV1.from_dict(
                    _json_object(
                        str(row["payload_json"]),
                        context="latest liquidation operation",
                    )
                )
            )
        except sqlite3.Error as exc:
            raise DailyRiskSourceError(
                f"cannot read latest liquidation operation: {exc}"
            ) from exc
        finally:
            connection.close()


class SQLiteDailyRiskMarketDataReader:
    def __init__(
        self,
        database_path: str | Path,
        *,
        instrument_id: str,
        price_precision: int,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database_path = Path(database_path)
        self.instrument_id = str(instrument_id or "").strip()
        if not self.instrument_id:
            raise ValueError("instrument_id is required")
        self.price_precision = int(price_precision)
        if self.price_precision < 0:
            raise ValueError("price_precision must be non-negative")
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file():
            raise DailyRiskSourceError(
                f"market-data database does not exist: {self.database_path}"
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
            missing = sorted(_REQUIRED_MARKET_OBJECTS - objects)
            if missing:
                raise DailyRiskSourceError(
                    f"daily-risk market-data objects are missing: {missing}"
                )
        except sqlite3.Error as exc:
            raise DailyRiskSourceError(
                f"cannot validate daily-risk market-data source: {exc}"
            ) from exc
        finally:
            connection.close()

    def read_latest_mark(
        self,
        *,
        observed_at_utc: str,
    ) -> DailyRiskMarketMarkV1 | None:
        observed = parse_utc(observed_at_utc)
        connection = self._connect()
        try:
            row = connection.execute(
                f"""
                SELECT
                    bar_id,
                    instrument_id,
                    con_id,
                    local_symbol,
                    bar_end_utc,
                    ROUND(
                        (bid_close + ask_close) / 2.0,
                        {self.price_precision}
                    ) AS mid_price
                FROM public_market_data_latest_v1
                WHERE instrument_id = ?
                LIMIT 1
                """,
                (self.instrument_id,),
            ).fetchone()
            if row is None:
                return None
            bar_end = parse_utc(str(row["bar_end_utc"]))
            return DailyRiskMarketMarkV1(
                bar_id=str(row["bar_id"]),
                instrument_id=str(row["instrument_id"]),
                con_id=int(row["con_id"]),
                local_symbol=str(row["local_symbol"]),
                bar_end_utc=str(row["bar_end_utc"]),
                mid_price=float(row["mid_price"]),
                age_seconds=max(0.0, (observed - bar_end).total_seconds()),
            )
        except sqlite3.Error as exc:
            raise DailyRiskSourceError(
                f"cannot read latest daily-risk market mark: {exc}"
            ) from exc
        finally:
            connection.close()
