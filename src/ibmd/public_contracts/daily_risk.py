from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, ClassVar, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class DailyRiskContractError(ValueError):
    pass


def _exact_keys(value: Mapping[str, Any], expected: set[str], *, context: str) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        raise DailyRiskContractError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


def _text(value: object, *, field_name: str, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    parsed = str(value or "").strip()
    if not parsed and not optional:
        raise DailyRiskContractError(f"{field_name} is required")
    return parsed or None


def _identifier(value: object, *, field_name: str) -> str:
    parsed = str(_text(value, field_name=field_name))
    if not _IDENTIFIER_RE.fullmatch(parsed):
        raise DailyRiskContractError(f"invalid {field_name}: {value!r}")
    return parsed


def _integer(value: object, *, field_name: str, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise DailyRiskContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise DailyRiskContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < minimum or exact != float(parsed):
        raise DailyRiskContractError(
            f"{field_name} must be an integer >= {minimum}: {value!r}"
        )
    return parsed


def _finite(
    value: object,
    *,
    field_name: str,
    positive: bool = False,
) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise DailyRiskContractError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(parsed) or (positive and parsed <= 0.0):
        qualifier = "finite and positive" if positive else "finite"
        raise DailyRiskContractError(
            f"{field_name} must be {qualifier}: {value!r}"
        )
    return parsed


def _optional_finite(value: object | None, *, field_name: str) -> float | None:
    return None if value is None else _finite(value, field_name=field_name)


def _strict_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise DailyRiskContractError(f"{field_name} must be boolean")
    return value


def _unique_texts(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    parsed = tuple(str(_text(item, field_name=field_name)) for item in values)
    if len(parsed) != len(set(parsed)):
        raise DailyRiskContractError(f"{field_name} values must be unique")
    return parsed


@dataclass(frozen=True)
class DailyRiskCalculationV1:
    calculation_id: str
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    trading_day: str
    timezone_name: str
    calculated_at_utc: str
    pnl_ready: bool
    reason_code: str
    reason_detail: str | None
    realized_pnl: float | None
    unrealized_pnl: float | None
    total_pnl: float | None
    target_pnl: float
    open_position_episode_id: str | None
    market_bar_id: str | None
    market_bar_end_utc: str | None
    mark_price: float | None
    strategic_exec_ids: tuple[str, ...]
    protective_exec_ids: tuple[str, ...]
    liquidation_exec_ids: tuple[str, ...]
    missing_commission_exec_ids: tuple[str, ...]
    liquidation_operation_id: str | None
    liquidation_state: str | None

    SCHEMA_NAME: ClassVar[str] = "DailyRiskCalculation"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "calculation_id",
        "account_id",
        "strategy_id",
        "strategy_version",
        "deployment_id",
        "instrument_id",
        "trading_day",
        "timezone_name",
        "calculated_at_utc",
        "pnl_ready",
        "reason_code",
        "reason_detail",
        "realized_pnl",
        "unrealized_pnl",
        "total_pnl",
        "target_pnl",
        "open_position_episode_id",
        "market_bar_id",
        "market_bar_end_utc",
        "mark_price",
        "strategic_exec_ids",
        "protective_exec_ids",
        "liquidation_exec_ids",
        "missing_commission_exec_ids",
        "liquidation_operation_id",
        "liquidation_state",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "calculation_id",
            validate_id(
                self.calculation_id,
                expected_kind="daily_risk_calculation",
            ),
        )
        object.__setattr__(
            self,
            "account_id",
            str(_text(self.account_id, field_name="account_id")),
        )
        for field_name in ("strategy_id", "deployment_id", "instrument_id"):
            object.__setattr__(
                self,
                field_name,
                _identifier(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "strategy_version",
            _integer(
                self.strategy_version,
                field_name="strategy_version",
                minimum=1,
            ),
        )
        try:
            parsed_day = date.fromisoformat(str(self.trading_day))
        except ValueError as exc:
            raise DailyRiskContractError(
                f"trading_day must be ISO date: {self.trading_day!r}"
            ) from exc
        object.__setattr__(self, "trading_day", parsed_day.isoformat())
        timezone_name = str(_text(self.timezone_name, field_name="timezone_name"))
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise DailyRiskContractError(
                f"unknown daily-risk timezone: {timezone_name!r}"
            ) from exc
        object.__setattr__(self, "timezone_name", timezone_name)
        object.__setattr__(
            self,
            "calculated_at_utc",
            format_utc(parse_utc(self.calculated_at_utc)),
        )
        object.__setattr__(
            self,
            "pnl_ready",
            _strict_bool(self.pnl_ready, field_name="pnl_ready"),
        )
        object.__setattr__(
            self,
            "reason_code",
            _identifier(self.reason_code, field_name="reason_code"),
        )
        object.__setattr__(
            self,
            "reason_detail",
            _text(
                self.reason_detail,
                field_name="reason_detail",
                optional=True,
            ),
        )
        target = _finite(self.target_pnl, field_name="target_pnl", positive=True)
        object.__setattr__(self, "target_pnl", target)
        realized = _optional_finite(self.realized_pnl, field_name="realized_pnl")
        unrealized = _optional_finite(
            self.unrealized_pnl,
            field_name="unrealized_pnl",
        )
        total = _optional_finite(self.total_pnl, field_name="total_pnl")
        object.__setattr__(self, "realized_pnl", realized)
        object.__setattr__(self, "unrealized_pnl", unrealized)
        object.__setattr__(self, "total_pnl", total)
        if self.pnl_ready:
            if None in (realized, unrealized, total):
                raise DailyRiskContractError(
                    "pnl_ready calculation requires realized/unrealized/total PnL"
                )
            if abs(float(realized) + float(unrealized) - float(total)) > 1e-9:
                raise DailyRiskContractError(
                    "daily-risk total_pnl must equal realized_pnl + unrealized_pnl"
                )
            if self.reason_code != "READY":
                raise DailyRiskContractError(
                    "pnl_ready calculation requires reason_code=READY"
                )
        else:
            if any(item is not None for item in (realized, unrealized, total)):
                raise DailyRiskContractError(
                    "pnl_not_ready calculation cannot publish partial PnL values"
                )
            if self.reason_code == "READY":
                raise DailyRiskContractError(
                    "pnl_not_ready calculation cannot use reason_code=READY"
                )

        episode_id = (
            None
            if self.open_position_episode_id is None
            else validate_id(
                self.open_position_episode_id,
                expected_kind="position_episode",
            )
        )
        object.__setattr__(self, "open_position_episode_id", episode_id)
        market_bar_id = (
            None
            if self.market_bar_id is None
            else validate_id(self.market_bar_id, expected_kind="market_bar")
        )
        market_end = (
            None
            if self.market_bar_end_utc is None
            else format_utc(parse_utc(self.market_bar_end_utc))
        )
        mark_price = (
            None
            if self.mark_price is None
            else _finite(self.mark_price, field_name="mark_price", positive=True)
        )
        market_values = (market_bar_id, market_end, mark_price)
        if any(item is None for item in market_values) and any(
            item is not None for item in market_values
        ):
            raise DailyRiskContractError(
                "market_bar_id, market_bar_end_utc and mark_price are all-or-none"
            )
        if episode_id is None and any(item is not None for item in market_values):
            raise DailyRiskContractError(
                "flat daily-risk calculation cannot publish market mark evidence"
            )
        if self.pnl_ready and episode_id is not None and market_bar_id is None:
            raise DailyRiskContractError(
                "open pnl_ready calculation requires market mark evidence"
            )
        object.__setattr__(self, "market_bar_id", market_bar_id)
        object.__setattr__(self, "market_bar_end_utc", market_end)
        object.__setattr__(self, "mark_price", mark_price)

        categories = []
        for field_name in (
            "strategic_exec_ids",
            "protective_exec_ids",
            "liquidation_exec_ids",
            "missing_commission_exec_ids",
        ):
            parsed = _unique_texts(
                tuple(getattr(self, field_name)),
                field_name=field_name,
            )
            object.__setattr__(self, field_name, parsed)
            if field_name != "missing_commission_exec_ids":
                categories.extend(parsed)
        if len(categories) != len(set(categories)):
            raise DailyRiskContractError(
                "one execId cannot belong to multiple daily-risk fill categories"
            )
        known = set(categories)
        if not set(self.missing_commission_exec_ids).issubset(known):
            raise DailyRiskContractError(
                "missing commission execIds must belong to owned fill evidence"
            )
        if self.pnl_ready and self.missing_commission_exec_ids:
            raise DailyRiskContractError(
                "pnl_ready calculation cannot have missing commission evidence"
            )

        liquidation_operation_id = (
            None
            if self.liquidation_operation_id is None
            else validate_id(
                self.liquidation_operation_id,
                expected_kind="liquidation_operation",
            )
        )
        liquidation_state = _text(
            self.liquidation_state,
            field_name="liquidation_state",
            optional=True,
        )
        if (liquidation_operation_id is None) != (liquidation_state is None):
            raise DailyRiskContractError(
                "liquidation operation id and state are all-or-none"
            )
        object.__setattr__(
            self,
            "liquidation_operation_id",
            liquidation_operation_id,
        )
        object.__setattr__(self, "liquidation_state", liquidation_state)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DailyRiskCalculationV1":
        _exact_keys(value, cls.KEYS, context="daily risk calculation")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise DailyRiskContractError(
                "unsupported daily-risk-calculation schema"
            )
        list_fields = (
            "strategic_exec_ids",
            "protective_exec_ids",
            "liquidation_exec_ids",
            "missing_commission_exec_ids",
        )
        if any(not isinstance(value[field], list) for field in list_fields):
            raise DailyRiskContractError(
                "daily-risk exec-id fields must be lists"
            )
        return cls(
            calculation_id=str(value["calculation_id"]),
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            trading_day=str(value["trading_day"]),
            timezone_name=str(value["timezone_name"]),
            calculated_at_utc=str(value["calculated_at_utc"]),
            pnl_ready=value["pnl_ready"],
            reason_code=str(value["reason_code"]),
            reason_detail=(
                None
                if value["reason_detail"] is None
                else str(value["reason_detail"])
            ),
            realized_pnl=value["realized_pnl"],
            unrealized_pnl=value["unrealized_pnl"],
            total_pnl=value["total_pnl"],
            target_pnl=value["target_pnl"],
            open_position_episode_id=(
                None
                if value["open_position_episode_id"] is None
                else str(value["open_position_episode_id"])
            ),
            market_bar_id=(
                None
                if value["market_bar_id"] is None
                else str(value["market_bar_id"])
            ),
            market_bar_end_utc=(
                None
                if value["market_bar_end_utc"] is None
                else str(value["market_bar_end_utc"])
            ),
            mark_price=value["mark_price"],
            strategic_exec_ids=tuple(str(item) for item in value["strategic_exec_ids"]),
            protective_exec_ids=tuple(str(item) for item in value["protective_exec_ids"]),
            liquidation_exec_ids=tuple(str(item) for item in value["liquidation_exec_ids"]),
            missing_commission_exec_ids=tuple(
                str(item) for item in value["missing_commission_exec_ids"]
            ),
            liquidation_operation_id=(
                None
                if value["liquidation_operation_id"] is None
                else str(value["liquidation_operation_id"])
            ),
            liquidation_state=(
                None
                if value["liquidation_state"] is None
                else str(value["liquidation_state"])
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "calculation_id": self.calculation_id,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "trading_day": self.trading_day,
            "timezone_name": self.timezone_name,
            "calculated_at_utc": self.calculated_at_utc,
            "pnl_ready": self.pnl_ready,
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl,
            "target_pnl": self.target_pnl,
            "open_position_episode_id": self.open_position_episode_id,
            "market_bar_id": self.market_bar_id,
            "market_bar_end_utc": self.market_bar_end_utc,
            "mark_price": self.mark_price,
            "strategic_exec_ids": list(self.strategic_exec_ids),
            "protective_exec_ids": list(self.protective_exec_ids),
            "liquidation_exec_ids": list(self.liquidation_exec_ids),
            "missing_commission_exec_ids": list(
                self.missing_commission_exec_ids
            ),
            "liquidation_operation_id": self.liquidation_operation_id,
            "liquidation_state": self.liquidation_state,
        }
