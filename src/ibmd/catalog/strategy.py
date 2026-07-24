from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .common import (
    CatalogError,
    boolean,
    exact_keys,
    identifier,
    positive_float,
    positive_int,
    required_text,
    seconds_of_day,
    validate_content_hash,
    validate_hash,
)


@dataclass(frozen=True)
class SignalPolicyV1:
    source_bar_size_seconds: int
    complete_bid_ask_required: bool
    max_complete_bar_lag_seconds: int
    decision_pipeline_max_age_seconds: int
    rolling_step_seconds: int
    pattern_lookback_minutes: int
    potential_horizon_minutes: int
    historical_lookback_days: int
    pearson_minimum: float
    minmax_hard_filter_max_ratio: float
    score_pearson_weight: float
    score_end_delta_weight: float
    score_minmax_weight: float
    potential_candidate_min_count: int
    potential_candidate_max_count: int
    minimum_abs_potential_end_delta_points: float
    candidate_hour_profile: str
    plot_enabled: bool
    plot_retention_days: int

    KEYS: ClassVar[set[str]] = {
        "source_bar_size_seconds",
        "complete_bid_ask_required",
        "max_complete_bar_lag_seconds",
        "decision_pipeline_max_age_seconds",
        "rolling_step_seconds",
        "pattern_lookback_minutes",
        "potential_horizon_minutes",
        "historical_lookback_days",
        "pearson_minimum",
        "minmax_hard_filter_max_ratio",
        "score_pearson_weight",
        "score_end_delta_weight",
        "score_minmax_weight",
        "potential_candidate_min_count",
        "potential_candidate_max_count",
        "minimum_abs_potential_end_delta_points",
        "candidate_hour_profile",
        "plot_enabled",
        "plot_retention_days",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "complete_bid_ask_required",
            boolean(
                self.complete_bid_ask_required,
                field_name="complete_bid_ask_required",
            ),
        )
        object.__setattr__(
            self,
            "plot_enabled",
            boolean(self.plot_enabled, field_name="plot_enabled"),
        )
        for field_name in (
            "source_bar_size_seconds",
            "max_complete_bar_lag_seconds",
            "decision_pipeline_max_age_seconds",
            "rolling_step_seconds",
            "pattern_lookback_minutes",
            "potential_horizon_minutes",
            "historical_lookback_days",
            "potential_candidate_min_count",
            "potential_candidate_max_count",
            "plot_retention_days",
        ):
            object.__setattr__(
                self,
                field_name,
                positive_int(getattr(self, field_name), field_name=field_name),
            )
        for field_name in (
            "pearson_minimum",
            "minmax_hard_filter_max_ratio",
            "score_pearson_weight",
            "score_end_delta_weight",
            "score_minmax_weight",
            "minimum_abs_potential_end_delta_points",
        ):
            object.__setattr__(
                self,
                field_name,
                positive_float(getattr(self, field_name), field_name=field_name),
            )
        if self.potential_candidate_min_count > self.potential_candidate_max_count:
            raise CatalogError("potential candidate min count exceeds max count")
        object.__setattr__(
            self,
            "candidate_hour_profile",
            identifier(
                self.candidate_hour_profile,
                field_name="candidate_hour_profile",
            ),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SignalPolicyV1":
        exact_keys(value, cls.KEYS, context="signal policy")
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in sorted(self.KEYS)}


@dataclass(frozen=True)
class ProtectivePolicyV1:
    stop_required: bool
    take_profit_enabled: bool
    stop_loss_points: float
    take_profit_points: float
    time_in_force: str
    stop_outside_rth: bool
    take_profit_outside_rth: bool
    price_watchdog_enabled: bool
    stale_feed_market_close_enabled: bool
    price_stale_max_seconds: int

    KEYS: ClassVar[set[str]] = {
        "stop_required",
        "take_profit_enabled",
        "stop_loss_points",
        "take_profit_points",
        "time_in_force",
        "stop_outside_rth",
        "take_profit_outside_rth",
        "price_watchdog_enabled",
        "stale_feed_market_close_enabled",
        "price_stale_max_seconds",
    }

    def __post_init__(self) -> None:
        for field_name in (
            "stop_required",
            "take_profit_enabled",
            "stop_outside_rth",
            "take_profit_outside_rth",
            "price_watchdog_enabled",
            "stale_feed_market_close_enabled",
        ):
            object.__setattr__(
                self,
                field_name,
                boolean(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "stop_loss_points",
            positive_float(self.stop_loss_points, field_name="stop_loss_points"),
        )
        object.__setattr__(
            self,
            "take_profit_points",
            positive_float(
                self.take_profit_points,
                field_name="take_profit_points",
            ),
        )
        tif = str(self.time_in_force or "").strip().upper()
        if tif not in {"DAY", "GTC"}:
            raise CatalogError(
                f"unsupported protective time_in_force: {self.time_in_force!r}"
            )
        object.__setattr__(self, "time_in_force", tif)
        object.__setattr__(
            self,
            "price_stale_max_seconds",
            positive_int(
                self.price_stale_max_seconds,
                field_name="price_stale_max_seconds",
            ),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ProtectivePolicyV1":
        exact_keys(value, cls.KEYS, context="protective policy")
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in sorted(self.KEYS)}


@dataclass(frozen=True)
class DailyFlatPolicyV1:
    enabled: bool
    session_id: str
    liquidation_start_local: str
    no_new_risk_local: str
    risk_blocked_until_local: str

    KEYS: ClassVar[set[str]] = {
        "enabled",
        "session_id",
        "liquidation_start_local",
        "no_new_risk_local",
        "risk_blocked_until_local",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "enabled",
            boolean(self.enabled, field_name="enabled"),
        )
        object.__setattr__(
            self,
            "session_id",
            identifier(self.session_id, field_name="session_id"),
        )
        start = seconds_of_day(
            self.liquidation_start_local,
            field_name="liquidation_start_local",
        )
        no_risk = seconds_of_day(
            self.no_new_risk_local,
            field_name="no_new_risk_local",
        )
        blocked_until = seconds_of_day(
            self.risk_blocked_until_local,
            field_name="risk_blocked_until_local",
        )
        if not (start <= no_risk < blocked_until):
            raise CatalogError("daily-flat local boundaries are not ordered")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DailyFlatPolicyV1":
        exact_keys(value, cls.KEYS, context="daily-flat policy")
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in sorted(self.KEYS)}


@dataclass(frozen=True)
class DailyPnlPolicyV1:
    enabled: bool
    target_usd: float
    timezone: str

    KEYS: ClassVar[set[str]] = {"enabled", "target_usd", "timezone"}

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "enabled",
            boolean(self.enabled, field_name="enabled"),
        )
        object.__setattr__(
            self,
            "target_usd",
            positive_float(self.target_usd, field_name="target_usd"),
        )
        timezone_name = required_text(self.timezone, field_name="timezone")
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise CatalogError(f"unknown timezone: {timezone_name!r}") from exc
        object.__setattr__(self, "timezone", timezone_name)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DailyPnlPolicyV1":
        exact_keys(value, cls.KEYS, context="daily PnL policy")
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "target_usd": self.target_usd,
            "timezone": self.timezone,
        }


@dataclass(frozen=True)
class StrategyInstrumentPolicyV1:
    instrument_id: str
    trading_enabled: bool
    target_quantity: int
    strategic_order_type: str
    signal: SignalPolicyV1
    protective: ProtectivePolicyV1
    daily_flat: DailyFlatPolicyV1
    daily_pnl: DailyPnlPolicyV1

    KEYS: ClassVar[set[str]] = {
        "instrument_id",
        "trading_enabled",
        "target_quantity",
        "strategic_order_type",
        "signal",
        "protective",
        "daily_flat",
        "daily_pnl",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            identifier(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "trading_enabled",
            boolean(self.trading_enabled, field_name="trading_enabled"),
        )
        object.__setattr__(
            self,
            "target_quantity",
            positive_int(self.target_quantity, field_name="target_quantity"),
        )
        order_type = str(self.strategic_order_type or "").strip().upper()
        if order_type != "MARKET":
            raise CatalogError("target v1 strategic_order_type must be MARKET")
        object.__setattr__(self, "strategic_order_type", order_type)

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "StrategyInstrumentPolicyV1":
        exact_keys(value, cls.KEYS, context="strategy instrument policy")
        for nested in ("signal", "protective", "daily_flat", "daily_pnl"):
            if not isinstance(value[nested], Mapping):
                raise CatalogError(f"{nested} must be an object")
        return cls(
            instrument_id=str(value["instrument_id"]),
            trading_enabled=value["trading_enabled"],
            target_quantity=value["target_quantity"],
            strategic_order_type=str(value["strategic_order_type"]),
            signal=SignalPolicyV1.from_dict(value["signal"]),
            protective=ProtectivePolicyV1.from_dict(value["protective"]),
            daily_flat=DailyFlatPolicyV1.from_dict(value["daily_flat"]),
            daily_pnl=DailyPnlPolicyV1.from_dict(value["daily_pnl"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "trading_enabled": self.trading_enabled,
            "target_quantity": self.target_quantity,
            "strategic_order_type": self.strategic_order_type,
            "signal": self.signal.to_dict(),
            "protective": self.protective.to_dict(),
            "daily_flat": self.daily_flat.to_dict(),
            "daily_pnl": self.daily_pnl.to_dict(),
        }


@dataclass(frozen=True)
class StrategyPolicyCatalogV1:
    artifact_version: str
    source_runtime_commit: str
    strategy_id: str
    strategy_version: int
    instruments: tuple[StrategyInstrumentPolicyV1, ...]
    content_hash: str

    SCHEMA_NAME: ClassVar[str] = "StrategyPolicyCatalog"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "artifact_version",
        "source_runtime_commit",
        "strategy_id",
        "strategy_version",
        "instruments",
        "content_hash",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_version",
            identifier(self.artifact_version, field_name="artifact_version"),
        )
        source = str(self.source_runtime_commit or "").strip()
        if not re.fullmatch(r"[0-9a-f]{40}", source):
            raise CatalogError("source_runtime_commit must be a 40-char git SHA")
        object.__setattr__(self, "source_runtime_commit", source)
        object.__setattr__(
            self,
            "strategy_id",
            identifier(self.strategy_id, field_name="strategy_id"),
        )
        object.__setattr__(
            self,
            "strategy_version",
            positive_int(self.strategy_version, field_name="strategy_version"),
        )
        if not self.instruments:
            raise CatalogError("strategy policy cannot be empty")
        ids = [item.instrument_id for item in self.instruments]
        if len(ids) != len(set(ids)):
            raise CatalogError(f"duplicate strategy instrument policies: {ids}")
        object.__setattr__(self, "content_hash", validate_hash(self.content_hash))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "StrategyPolicyCatalogV1":
        exact_keys(value, cls.KEYS, context="strategy policy catalog")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise CatalogError("unsupported strategy-policy schema")
        validate_content_hash(value)
        raw = value["instruments"]
        if not isinstance(raw, list):
            raise CatalogError("strategy instruments must be a list")
        return cls(
            artifact_version=str(value["artifact_version"]),
            source_runtime_commit=str(value["source_runtime_commit"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            instruments=tuple(
                StrategyInstrumentPolicyV1.from_dict(item) for item in raw
            ),
            content_hash=str(value["content_hash"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "artifact_version": self.artifact_version,
            "source_runtime_commit": self.source_runtime_commit,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "instruments": [item.to_dict() for item in self.instruments],
            "content_hash": self.content_hash,
        }

    def require(self, instrument_id: str) -> StrategyInstrumentPolicyV1:
        expected = str(instrument_id)
        for item in self.instruments:
            if item.instrument_id == expected:
                return item
        raise CatalogError(
            "strategy policy is not registered for instrument: "
            f"{instrument_id}"
        )
