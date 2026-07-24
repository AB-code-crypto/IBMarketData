from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ExecutionContractError(ValueError):
    pass


class ExecutionCommandState(str, Enum):
    RECEIVED = "RECEIVED"
    ADMITTED = "ADMITTED"
    REJECTED = "REJECTED"


class StrategyPositionSide(str, Enum):
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"


class StrategyPositionStatus(str, Enum):
    FLAT = "FLAT"
    OPEN = "OPEN"
    UNKNOWN = "UNKNOWN"
    STALE = "STALE"
    MULTI_CONTRACT_INCIDENT = "MULTI_CONTRACT_INCIDENT"
    OWNERSHIP_UNPROVEN = "OWNERSHIP_UNPROVEN"


class ExecutionReadinessStatus(str, Enum):
    NOT_READY = "NOT_READY"
    READY = "READY"
    BLOCKED = "BLOCKED"


class DailyRiskStatus(str, Enum):
    NOT_READY = "NOT_READY"
    MONITORING = "MONITORING"
    TRIGGERED = "TRIGGERED"
    CLOSING = "CLOSING"
    HALTED = "HALTED"


class DailyRiskCleanupStatus(str, Enum):
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


def _exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    *,
    context: str,
) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        raise ExecutionContractError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


def _identifier(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ExecutionContractError(f"invalid {field_name}: {value!r}")
    return text


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ExecutionContractError(f"{field_name} is required")
    return text


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value).strip() or None


def _strict_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ExecutionContractError(f"{field_name} must be a boolean")
    return value


def _integer(
    value: object,
    *,
    field_name: str,
    minimum: int = 0,
) -> int:
    if isinstance(value, bool):
        raise ExecutionContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < minimum or exact != float(parsed):
        raise ExecutionContractError(
            f"{field_name} must be an integer >= {minimum}: {value!r}"
        )
    return parsed


def _signed_integer(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ExecutionContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed == 0 or exact != float(parsed):
        raise ExecutionContractError(
            f"{field_name} must be a non-zero integer: {value!r}"
        )
    return parsed


def _finite(
    value: object,
    *,
    field_name: str,
    minimum: float | None = None,
) -> float:
    if isinstance(value, bool):
        raise ExecutionContractError(f"{field_name} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionContractError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise ExecutionContractError(f"{field_name} must be finite")
    if minimum is not None and number < minimum:
        raise ExecutionContractError(
            f"{field_name} must be >= {minimum}: {number}"
        )
    return number


def _optional_finite(
    value: object | None,
    *,
    field_name: str,
) -> float | None:
    if value is None:
        return None
    return _finite(value, field_name=field_name)


@dataclass(frozen=True)
class PositionContractV1:
    con_id: int
    local_symbol: str
    signed_quantity: int
    contract_is_active: bool

    KEYS: ClassVar[set[str]] = {
        "con_id",
        "local_symbol",
        "signed_quantity",
        "contract_is_active",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "con_id",
            _integer(self.con_id, field_name="con_id", minimum=1),
        )
        object.__setattr__(
            self,
            "local_symbol",
            _required_text(self.local_symbol, field_name="local_symbol"),
        )
        object.__setattr__(
            self,
            "signed_quantity",
            _signed_integer(
                self.signed_quantity,
                field_name="signed_quantity",
            ),
        )
        object.__setattr__(
            self,
            "contract_is_active",
            _strict_bool(
                self.contract_is_active,
                field_name="contract_is_active",
            ),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PositionContractV1":
        _exact_keys(value, cls.KEYS, context="position contract")
        return cls(
            con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]),
            signed_quantity=value["signed_quantity"],
            contract_is_active=value["contract_is_active"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "signed_quantity": self.signed_quantity,
            "contract_is_active": self.contract_is_active,
        }


@dataclass(frozen=True)
class StrategyPositionV1:
    account_id: str
    strategy_id: str
    deployment_id: str
    instrument_id: str
    position_episode_id: str | None
    side: StrategyPositionSide
    quantity: int
    contracts: tuple[PositionContractV1, ...]
    projection_status: StrategyPositionStatus
    broker_snapshot_id: str | None
    updated_at_utc: str
    source_freshness_seconds: float | None

    SCHEMA_NAME: ClassVar[str] = "StrategyPosition"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "account_id",
        "strategy_id",
        "deployment_id",
        "instrument_id",
        "position_episode_id",
        "side",
        "quantity",
        "contracts",
        "projection_status",
        "broker_snapshot_id",
        "updated_at_utc",
        "source_freshness_seconds",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_id",
            _required_text(self.account_id, field_name="account_id"),
        )
        for field_name in ("strategy_id", "deployment_id", "instrument_id"):
            object.__setattr__(
                self,
                field_name,
                _identifier(getattr(self, field_name), field_name=field_name),
            )
        if self.position_episode_id is not None:
            object.__setattr__(
                self,
                "position_episode_id",
                validate_id(
                    self.position_episode_id,
                    expected_kind="position_episode",
                ),
            )
        if not isinstance(self.side, StrategyPositionSide):
            raise ExecutionContractError(f"invalid position side: {self.side!r}")
        if not isinstance(self.projection_status, StrategyPositionStatus):
            raise ExecutionContractError(
                f"invalid projection_status: {self.projection_status!r}"
            )
        object.__setattr__(
            self,
            "quantity",
            _integer(self.quantity, field_name="quantity", minimum=0),
        )
        contracts = tuple(self.contracts)
        if any(not isinstance(item, PositionContractV1) for item in contracts):
            raise ExecutionContractError(
                "contracts must contain PositionContractV1 values"
            )
        identities = [(item.con_id, item.local_symbol) for item in contracts]
        if len(identities) != len(set(identities)):
            raise ExecutionContractError("position contracts must be unique")
        object.__setattr__(self, "contracts", contracts)
        if self.broker_snapshot_id is not None:
            object.__setattr__(
                self,
                "broker_snapshot_id",
                validate_id(
                    self.broker_snapshot_id,
                    expected_kind="position_snapshot",
                ),
            )
        object.__setattr__(
            self,
            "updated_at_utc",
            format_utc(parse_utc(self.updated_at_utc)),
        )
        object.__setattr__(
            self,
            "source_freshness_seconds",
            (
                None
                if self.source_freshness_seconds is None
                else _finite(
                    self.source_freshness_seconds,
                    field_name="source_freshness_seconds",
                    minimum=0.0,
                )
            ),
        )

        if self.projection_status == StrategyPositionStatus.FLAT:
            if self.side != StrategyPositionSide.FLAT or self.quantity != 0:
                raise ExecutionContractError(
                    "FLAT projection requires side=FLAT and quantity=0"
                )
            if contracts or self.position_episode_id is not None:
                raise ExecutionContractError(
                    "FLAT projection cannot have contracts or position episode"
                )
        elif self.projection_status == StrategyPositionStatus.OPEN:
            if self.side not in {
                StrategyPositionSide.LONG,
                StrategyPositionSide.SHORT,
            }:
                raise ExecutionContractError(
                    "OPEN projection requires LONG or SHORT side"
                )
            if self.quantity <= 0 or not contracts:
                raise ExecutionContractError(
                    "OPEN projection requires positive quantity and contracts"
                )
            if self.position_episode_id is None or self.broker_snapshot_id is None:
                raise ExecutionContractError(
                    "OPEN projection requires position episode and broker snapshot"
                )
            signed_total = sum(item.signed_quantity for item in contracts)
            expected = (
                self.quantity
                if self.side == StrategyPositionSide.LONG
                else -self.quantity
            )
            if signed_total != expected:
                raise ExecutionContractError(
                    "position contract quantities do not match projected side/quantity"
                )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "StrategyPositionV1":
        _exact_keys(value, cls.KEYS, context="strategy position")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ExecutionContractError("unsupported strategy-position schema")
        raw_contracts = value["contracts"]
        if not isinstance(raw_contracts, list):
            raise ExecutionContractError("strategy position contracts must be a list")
        try:
            side = StrategyPositionSide(str(value["side"]))
            status = StrategyPositionStatus(str(value["projection_status"]))
        except ValueError as exc:
            raise ExecutionContractError(
                "invalid strategy-position enum value"
            ) from exc
        return cls(
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            position_episode_id=(
                None
                if value["position_episode_id"] is None
                else str(value["position_episode_id"])
            ),
            side=side,
            quantity=value["quantity"],
            contracts=tuple(
                PositionContractV1.from_dict(item) for item in raw_contracts
            ),
            projection_status=status,
            broker_snapshot_id=(
                None
                if value["broker_snapshot_id"] is None
                else str(value["broker_snapshot_id"])
            ),
            updated_at_utc=str(value["updated_at_utc"]),
            source_freshness_seconds=value["source_freshness_seconds"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "position_episode_id": self.position_episode_id,
            "side": self.side.value,
            "quantity": self.quantity,
            "contracts": [item.to_dict() for item in self.contracts],
            "projection_status": self.projection_status.value,
            "broker_snapshot_id": self.broker_snapshot_id,
            "updated_at_utc": self.updated_at_utc,
            "source_freshness_seconds": self.source_freshness_seconds,
        }


@dataclass(frozen=True)
class ExecutionReadinessV1:
    account_id: str
    strategy_id: str
    deployment_id: str
    instrument_id: str
    status: ExecutionReadinessStatus
    command_intake_enabled: bool
    broker_actions_enabled: bool
    reconciliation_complete: bool
    clock_healthy: bool
    blocking_reasons: tuple[str, ...]
    updated_at_utc: str

    SCHEMA_NAME: ClassVar[str] = "ExecutionReadiness"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "account_id",
        "strategy_id",
        "deployment_id",
        "instrument_id",
        "status",
        "command_intake_enabled",
        "broker_actions_enabled",
        "reconciliation_complete",
        "clock_healthy",
        "blocking_reasons",
        "updated_at_utc",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_id",
            _required_text(self.account_id, field_name="account_id"),
        )
        for field_name in ("strategy_id", "deployment_id", "instrument_id"):
            object.__setattr__(
                self,
                field_name,
                _identifier(getattr(self, field_name), field_name=field_name),
            )
        if not isinstance(self.status, ExecutionReadinessStatus):
            raise ExecutionContractError(
                f"invalid execution readiness status: {self.status!r}"
            )
        for field_name in (
            "command_intake_enabled",
            "broker_actions_enabled",
            "reconciliation_complete",
            "clock_healthy",
        ):
            object.__setattr__(
                self,
                field_name,
                _strict_bool(getattr(self, field_name), field_name=field_name),
            )
        reasons = tuple(
            _required_text(item, field_name="blocking_reason")
            for item in self.blocking_reasons
        )
        if len(reasons) != len(set(reasons)):
            raise ExecutionContractError("blocking_reasons must be unique")
        object.__setattr__(self, "blocking_reasons", reasons)
        object.__setattr__(
            self,
            "updated_at_utc",
            format_utc(parse_utc(self.updated_at_utc)),
        )
        if self.status == ExecutionReadinessStatus.READY:
            if reasons:
                raise ExecutionContractError(
                    "READY execution readiness cannot have blocking reasons"
                )
            if not (
                self.command_intake_enabled
                and self.reconciliation_complete
                and self.clock_healthy
            ):
                raise ExecutionContractError(
                    "READY execution requires command intake, reconciliation and clock"
                )
        elif self.status == ExecutionReadinessStatus.BLOCKED and not reasons:
            raise ExecutionContractError(
                "BLOCKED execution readiness requires a blocking reason"
            )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionReadinessV1":
        _exact_keys(value, cls.KEYS, context="execution readiness")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ExecutionContractError("unsupported execution-readiness schema")
        raw_reasons = value["blocking_reasons"]
        if not isinstance(raw_reasons, list):
            raise ExecutionContractError("blocking_reasons must be a list")
        try:
            status = ExecutionReadinessStatus(str(value["status"]))
        except ValueError as exc:
            raise ExecutionContractError(
                f"invalid readiness status: {value['status']!r}"
            ) from exc
        return cls(
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            status=status,
            command_intake_enabled=value["command_intake_enabled"],
            broker_actions_enabled=value["broker_actions_enabled"],
            reconciliation_complete=value["reconciliation_complete"],
            clock_healthy=value["clock_healthy"],
            blocking_reasons=tuple(str(item) for item in raw_reasons),
            updated_at_utc=str(value["updated_at_utc"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "status": self.status.value,
            "command_intake_enabled": self.command_intake_enabled,
            "broker_actions_enabled": self.broker_actions_enabled,
            "reconciliation_complete": self.reconciliation_complete,
            "clock_healthy": self.clock_healthy,
            "blocking_reasons": list(self.blocking_reasons),
            "updated_at_utc": self.updated_at_utc,
        }


@dataclass(frozen=True)
class DailyRiskStateV1:
    account_id: str
    strategy_id: str
    deployment_id: str
    trading_day: str
    status: DailyRiskStatus
    realized_pnl: float | None
    unrealized_pnl: float | None
    total_pnl: float | None
    target_pnl: float
    pnl_ready: bool
    cleanup_status: DailyRiskCleanupStatus
    updated_at_utc: str

    SCHEMA_NAME: ClassVar[str] = "DailyRiskState"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "account_id",
        "strategy_id",
        "deployment_id",
        "trading_day",
        "status",
        "realized_pnl",
        "unrealized_pnl",
        "total_pnl",
        "target_pnl",
        "pnl_ready",
        "cleanup_status",
        "updated_at_utc",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_id",
            _required_text(self.account_id, field_name="account_id"),
        )
        for field_name in ("strategy_id", "deployment_id"):
            object.__setattr__(
                self,
                field_name,
                _identifier(getattr(self, field_name), field_name=field_name),
            )
        try:
            parsed_day = date.fromisoformat(str(self.trading_day))
        except ValueError as exc:
            raise ExecutionContractError(
                f"trading_day must be ISO date: {self.trading_day!r}"
            ) from exc
        object.__setattr__(self, "trading_day", parsed_day.isoformat())
        if not isinstance(self.status, DailyRiskStatus):
            raise ExecutionContractError(f"invalid daily risk status: {self.status!r}")
        if not isinstance(self.cleanup_status, DailyRiskCleanupStatus):
            raise ExecutionContractError(
                f"invalid cleanup status: {self.cleanup_status!r}"
            )
        object.__setattr__(
            self,
            "pnl_ready",
            _strict_bool(self.pnl_ready, field_name="pnl_ready"),
        )
        for field_name in ("realized_pnl", "unrealized_pnl", "total_pnl"):
            object.__setattr__(
                self,
                field_name,
                _optional_finite(getattr(self, field_name), field_name=field_name),
            )
        target = _finite(
            self.target_pnl,
            field_name="target_pnl",
            minimum=0.0,
        )
        if target <= 0.0:
            raise ExecutionContractError("target_pnl must be positive")
        object.__setattr__(self, "target_pnl", target)
        object.__setattr__(
            self,
            "updated_at_utc",
            format_utc(parse_utc(self.updated_at_utc)),
        )
        values = (self.realized_pnl, self.unrealized_pnl, self.total_pnl)
        if self.pnl_ready:
            if any(item is None for item in values):
                raise ExecutionContractError(
                    "pnl_ready daily risk requires realized/unrealized/total PnL"
                )
            expected = float(self.realized_pnl) + float(self.unrealized_pnl)
            if abs(expected - float(self.total_pnl)) > 1e-9:
                raise ExecutionContractError(
                    "total_pnl must equal realized_pnl + unrealized_pnl"
                )
        elif any(item is not None for item in values):
            raise ExecutionContractError(
                "pnl_not_ready daily risk cannot publish partial PnL values"
            )
        if self.status == DailyRiskStatus.MONITORING and not self.pnl_ready:
            raise ExecutionContractError("MONITORING daily risk requires ready PnL")
        if (
            self.status == DailyRiskStatus.HALTED
            and self.cleanup_status != DailyRiskCleanupStatus.COMPLETE
        ):
            raise ExecutionContractError(
                "HALTED daily risk requires COMPLETE cleanup"
            )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DailyRiskStateV1":
        _exact_keys(value, cls.KEYS, context="daily risk state")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ExecutionContractError("unsupported daily-risk schema")
        try:
            status = DailyRiskStatus(str(value["status"]))
            cleanup = DailyRiskCleanupStatus(str(value["cleanup_status"]))
        except ValueError as exc:
            raise ExecutionContractError("invalid daily-risk enum value") from exc
        return cls(
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            deployment_id=str(value["deployment_id"]),
            trading_day=str(value["trading_day"]),
            status=status,
            realized_pnl=value["realized_pnl"],
            unrealized_pnl=value["unrealized_pnl"],
            total_pnl=value["total_pnl"],
            target_pnl=value["target_pnl"],
            pnl_ready=value["pnl_ready"],
            cleanup_status=cleanup,
            updated_at_utc=str(value["updated_at_utc"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "deployment_id": self.deployment_id,
            "trading_day": self.trading_day,
            "status": self.status.value,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl,
            "target_pnl": self.target_pnl,
            "pnl_ready": self.pnl_ready,
            "cleanup_status": self.cleanup_status.value,
            "updated_at_utc": self.updated_at_utc,
        }


@dataclass(frozen=True)
class ExecutionCommandStateV1:
    command_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    command_kind: StrategyCommandKind
    desired_target_side: DesiredTargetSide
    desired_target_quantity: int
    state: ExecutionCommandState
    requested_qty: int
    filled_qty: int
    remaining_qty: int
    latest_attempt_id: str | None
    blocking_reason: str | None
    received_at_utc: str
    updated_at_utc: str
    terminal_at_utc: str | None

    SCHEMA_NAME: ClassVar[str] = "ExecutionCommandState"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "command_id",
        "strategy_id",
        "strategy_version",
        "deployment_id",
        "instrument_id",
        "command_kind",
        "desired_target_side",
        "desired_target_quantity",
        "state",
        "requested_qty",
        "filled_qty",
        "remaining_qty",
        "latest_attempt_id",
        "blocking_reason",
        "received_at_utc",
        "updated_at_utc",
        "terminal_at_utc",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "command_id",
            validate_id(self.command_id, expected_kind="strategy_command"),
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
        if not isinstance(self.command_kind, StrategyCommandKind):
            raise ExecutionContractError(
                f"invalid command_kind: {self.command_kind!r}"
            )
        if not isinstance(self.desired_target_side, DesiredTargetSide):
            raise ExecutionContractError(
                f"invalid desired_target_side: {self.desired_target_side!r}"
            )
        if not isinstance(self.state, ExecutionCommandState):
            raise ExecutionContractError(f"invalid command state: {self.state!r}")
        desired = _integer(
            self.desired_target_quantity,
            field_name="desired_target_quantity",
            minimum=1,
        )
        requested = _integer(
            self.requested_qty,
            field_name="requested_qty",
            minimum=1,
        )
        filled = _integer(self.filled_qty, field_name="filled_qty", minimum=0)
        remaining = _integer(
            self.remaining_qty,
            field_name="remaining_qty",
            minimum=0,
        )
        if filled + remaining != requested:
            raise ExecutionContractError(
                "filled_qty + remaining_qty must equal requested_qty"
            )
        object.__setattr__(self, "desired_target_quantity", desired)
        object.__setattr__(self, "requested_qty", requested)
        object.__setattr__(self, "filled_qty", filled)
        object.__setattr__(self, "remaining_qty", remaining)
        if self.latest_attempt_id is not None:
            object.__setattr__(
                self,
                "latest_attempt_id",
                validate_id(
                    self.latest_attempt_id,
                    expected_kind="order_attempt",
                ),
            )
        object.__setattr__(
            self,
            "blocking_reason",
            _optional_text(self.blocking_reason),
        )
        received = parse_utc(self.received_at_utc)
        updated = parse_utc(self.updated_at_utc)
        if updated < received:
            raise ExecutionContractError(
                "updated_at_utc cannot precede received_at_utc"
            )
        terminal = (
            None
            if self.terminal_at_utc is None
            else parse_utc(self.terminal_at_utc)
        )
        if terminal is not None and terminal < received:
            raise ExecutionContractError(
                "terminal_at_utc cannot precede received_at_utc"
            )
        object.__setattr__(self, "received_at_utc", format_utc(received))
        object.__setattr__(self, "updated_at_utc", format_utc(updated))
        object.__setattr__(
            self,
            "terminal_at_utc",
            None if terminal is None else format_utc(terminal),
        )
        if self.state == ExecutionCommandState.REJECTED:
            if self.blocking_reason is None or terminal is None:
                raise ExecutionContractError(
                    "REJECTED command requires blocking_reason and terminal_at_utc"
                )
        else:
            if terminal is not None:
                raise ExecutionContractError(
                    "non-terminal command state cannot have terminal_at_utc"
                )
            if self.state == ExecutionCommandState.ADMITTED and self.blocking_reason:
                raise ExecutionContractError(
                    "ADMITTED command cannot have blocking_reason"
                )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionCommandStateV1":
        _exact_keys(value, cls.KEYS, context="execution command state")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ExecutionContractError("unsupported execution-command schema")
        try:
            command_kind = StrategyCommandKind(str(value["command_kind"]))
            target_side = DesiredTargetSide(str(value["desired_target_side"]))
            state = ExecutionCommandState(str(value["state"]))
        except ValueError as exc:
            raise ExecutionContractError(
                "invalid execution-command enum value"
            ) from exc
        return cls(
            command_id=str(value["command_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            command_kind=command_kind,
            desired_target_side=target_side,
            desired_target_quantity=value["desired_target_quantity"],
            state=state,
            requested_qty=value["requested_qty"],
            filled_qty=value["filled_qty"],
            remaining_qty=value["remaining_qty"],
            latest_attempt_id=(
                None
                if value["latest_attempt_id"] is None
                else str(value["latest_attempt_id"])
            ),
            blocking_reason=(
                None
                if value["blocking_reason"] is None
                else str(value["blocking_reason"])
            ),
            received_at_utc=str(value["received_at_utc"]),
            updated_at_utc=str(value["updated_at_utc"]),
            terminal_at_utc=(
                None
                if value["terminal_at_utc"] is None
                else str(value["terminal_at_utc"])
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "command_id": self.command_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "command_kind": self.command_kind.value,
            "desired_target_side": self.desired_target_side.value,
            "desired_target_quantity": self.desired_target_quantity,
            "state": self.state.value,
            "requested_qty": self.requested_qty,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "latest_attempt_id": self.latest_attempt_id,
            "blocking_reason": self.blocking_reason,
            "received_at_utc": self.received_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "terminal_at_utc": self.terminal_at_utc,
        }
