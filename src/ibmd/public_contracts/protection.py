from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import BrokerOrderSide
from ibmd.public_contracts.execution import StrategyPositionSide

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ORDER_REF_RE = re.compile(r"^[A-Za-z0-9:._-]{1,64}$")


class ProtectionContractError(ValueError):
    pass


class PositionEpisodeStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class ProtectiveOrderKind(str, Enum):
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"


class ProtectiveOrderType(str, Enum):
    STOP = "STOP"
    LIMIT = "LIMIT"


class ProtectiveOrderState(str, Enum):
    PLANNED = "PLANNED"
    SUBMITTING = "SUBMITTING"
    LIVE = "LIVE"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"
    NOT_REQUIRED = "NOT_REQUIRED"


class ProtectionSetStatus(str, Enum):
    PLANNED = "PLANNED"
    STOP_SUBMITTING = "STOP_SUBMITTING"
    STOP_LIVE = "STOP_LIVE"
    PROTECTED = "PROTECTED"
    UNPROTECTED = "UNPROTECTED"
    EXITED = "EXITED"
    CLOSED = "CLOSED"
    OPERATOR_REQUIRED = "OPERATOR_REQUIRED"


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
        raise ProtectionContractError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


def _text(
    value: object,
    *,
    field_name: str,
    optional: bool = False,
) -> str | None:
    if value is None and optional:
        return None
    parsed = str(value or "").strip()
    if not parsed and not optional:
        raise ProtectionContractError(f"{field_name} is required")
    return parsed or None


def _identifier(value: object, *, field_name: str) -> str:
    parsed = str(_text(value, field_name=field_name))
    if not _IDENTIFIER_RE.fullmatch(parsed):
        raise ProtectionContractError(f"invalid {field_name}: {value!r}")
    return parsed


def _hash(value: object, *, field_name: str) -> str:
    parsed = str(_text(value, field_name=field_name))
    if not _HASH_RE.fullmatch(parsed):
        raise ProtectionContractError(
            f"{field_name} must be lowercase SHA-256 hex: {value!r}"
        )
    return parsed


def _strict_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ProtectionContractError(f"{field_name} must be a boolean")
    return value


def _integer(value: object, *, field_name: str, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise ProtectionContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise ProtectionContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < minimum or exact != float(parsed):
        raise ProtectionContractError(
            f"{field_name} must be an integer >= {minimum}: {value!r}"
        )
    return parsed


def _finite(
    value: object,
    *,
    field_name: str,
    positive: bool = False,
) -> float:
    if isinstance(value, bool):
        raise ProtectionContractError(f"{field_name} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ProtectionContractError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(parsed) or (positive and parsed <= 0.0):
        qualifier = "positive and finite" if positive else "finite"
        raise ProtectionContractError(f"{field_name} must be {qualifier}")
    return parsed


def _optional_finite(
    value: object | None,
    *,
    field_name: str,
) -> float | None:
    return (
        None
        if value is None
        else _finite(value, field_name=field_name, positive=True)
    )


def _utc(value: object, *, field_name: str) -> str:
    try:
        return format_utc(parse_utc(str(value)))
    except (TypeError, ValueError) as exc:
        raise ProtectionContractError(
            f"invalid {field_name}: {value!r}"
        ) from exc


@dataclass(frozen=True)
class PositionEpisodePolicyV1:
    price_tick: float
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

    SCHEMA_NAME: ClassVar[str] = "PositionEpisodePolicy"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "price_tick",
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
        object.__setattr__(
            self,
            "price_tick",
            _finite(self.price_tick, field_name="price_tick", positive=True),
        )
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
                _strict_bool(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "stop_loss_points",
            _finite(
                self.stop_loss_points,
                field_name="stop_loss_points",
                positive=True,
            ),
        )
        object.__setattr__(
            self,
            "take_profit_points",
            _finite(
                self.take_profit_points,
                field_name="take_profit_points",
                positive=True,
            ),
        )
        tif = str(
            _text(self.time_in_force, field_name="time_in_force")
        ).upper()
        if tif not in {"DAY", "GTC"}:
            raise ProtectionContractError(
                f"unsupported protective TIF: {tif!r}"
            )
        object.__setattr__(self, "time_in_force", tif)
        object.__setattr__(
            self,
            "price_stale_max_seconds",
            _integer(
                self.price_stale_max_seconds,
                field_name="price_stale_max_seconds",
                minimum=1,
            ),
        )
        if not self.stop_required:
            raise ProtectionContractError(
                "target v1 requires stop_required=true for position episodes"
            )

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(
            canonical_json_text(self.to_dict()).encode("utf-8")
        ).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "price_tick": self.price_tick,
            "stop_required": self.stop_required,
            "take_profit_enabled": self.take_profit_enabled,
            "stop_loss_points": self.stop_loss_points,
            "take_profit_points": self.take_profit_points,
            "time_in_force": self.time_in_force,
            "stop_outside_rth": self.stop_outside_rth,
            "take_profit_outside_rth": self.take_profit_outside_rth,
            "price_watchdog_enabled": self.price_watchdog_enabled,
            "stale_feed_market_close_enabled": (
                self.stale_feed_market_close_enabled
            ),
            "price_stale_max_seconds": self.price_stale_max_seconds,
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "PositionEpisodePolicyV1":
        _exact_keys(value, cls.KEYS, context="position episode policy")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ProtectionContractError(
                "unsupported position-episode policy schema"
            )
        return cls(
            price_tick=value["price_tick"],
            stop_required=value["stop_required"],
            take_profit_enabled=value["take_profit_enabled"],
            stop_loss_points=value["stop_loss_points"],
            take_profit_points=value["take_profit_points"],
            time_in_force=str(value["time_in_force"]),
            stop_outside_rth=value["stop_outside_rth"],
            take_profit_outside_rth=value["take_profit_outside_rth"],
            price_watchdog_enabled=value["price_watchdog_enabled"],
            stale_feed_market_close_enabled=value[
                "stale_feed_market_close_enabled"
            ],
            price_stale_max_seconds=value["price_stale_max_seconds"],
        )


@dataclass(frozen=True)
class PositionEpisodeV1:
    position_episode_id: str
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    source_command_id: str
    source_operation_id: str
    source_attempt_id: str
    source_exec_ids: tuple[str, ...]
    side: StrategyPositionSide
    quantity: int
    con_id: int
    local_symbol: str
    entry_average_price: float
    broker_snapshot_id: str
    opened_at_utc: str
    status: PositionEpisodeStatus
    strategy_policy_hash: str
    protective_policy_hash: str
    protective_policy: PositionEpisodePolicyV1
    closed_at_utc: str | None = None
    closing_operation_id: str | None = None

    SCHEMA_NAME: ClassVar[str] = "PositionEpisode"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "position_episode_id",
        "account_id",
        "strategy_id",
        "strategy_version",
        "deployment_id",
        "instrument_id",
        "source_command_id",
        "source_operation_id",
        "source_attempt_id",
        "source_exec_ids",
        "side",
        "quantity",
        "con_id",
        "local_symbol",
        "entry_average_price",
        "broker_snapshot_id",
        "opened_at_utc",
        "status",
        "strategy_policy_hash",
        "protective_policy_hash",
        "protective_policy",
        "closed_at_utc",
        "closing_operation_id",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "position_episode_id",
            validate_id(
                self.position_episode_id,
                expected_kind="position_episode",
            ),
        )
        object.__setattr__(
            self,
            "account_id",
            str(_text(self.account_id, field_name="account_id")),
        )
        for field_name in (
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
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
        object.__setattr__(
            self,
            "source_command_id",
            validate_id(
                self.source_command_id,
                expected_kind="strategy_command",
            ),
        )
        object.__setattr__(
            self,
            "source_operation_id",
            validate_id(
                self.source_operation_id,
                expected_kind="broker_operation",
            ),
        )
        object.__setattr__(
            self,
            "source_attempt_id",
            validate_id(
                self.source_attempt_id,
                expected_kind="broker_attempt",
            ),
        )
        exec_ids = tuple(
            str(_text(item, field_name="source_exec_id"))
            for item in self.source_exec_ids
        )
        if not exec_ids or len(exec_ids) != len(set(exec_ids)):
            raise ProtectionContractError(
                "source_exec_ids must be non-empty and unique"
            )
        object.__setattr__(self, "source_exec_ids", exec_ids)
        if self.side not in {
            StrategyPositionSide.LONG,
            StrategyPositionSide.SHORT,
        }:
            raise ProtectionContractError(
                "position episode side must be LONG or SHORT"
            )
        object.__setattr__(
            self,
            "quantity",
            _integer(self.quantity, field_name="quantity", minimum=1),
        )
        object.__setattr__(
            self,
            "con_id",
            _integer(self.con_id, field_name="con_id", minimum=1),
        )
        object.__setattr__(
            self,
            "local_symbol",
            str(_text(self.local_symbol, field_name="local_symbol")),
        )
        object.__setattr__(
            self,
            "entry_average_price",
            _finite(
                self.entry_average_price,
                field_name="entry_average_price",
                positive=True,
            ),
        )
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
            "opened_at_utc",
            _utc(self.opened_at_utc, field_name="opened_at_utc"),
        )
        if not isinstance(self.status, PositionEpisodeStatus):
            raise ProtectionContractError(
                f"invalid episode status: {self.status!r}"
            )
        object.__setattr__(
            self,
            "strategy_policy_hash",
            _hash(
                self.strategy_policy_hash,
                field_name="strategy_policy_hash",
            ),
        )
        if not isinstance(self.protective_policy, PositionEpisodePolicyV1):
            raise ProtectionContractError(
                "protective_policy must be PositionEpisodePolicyV1"
            )
        expected_policy_hash = self.protective_policy.content_hash
        policy_hash = _hash(
            self.protective_policy_hash,
            field_name="protective_policy_hash",
        )
        if policy_hash != expected_policy_hash:
            raise ProtectionContractError(
                "protective_policy_hash does not match protective_policy"
            )
        object.__setattr__(self, "protective_policy_hash", policy_hash)
        closed = (
            None
            if self.closed_at_utc is None
            else _utc(self.closed_at_utc, field_name="closed_at_utc")
        )
        if self.closing_operation_id is None:
            closing_operation = None
        else:
            closing_candidate = str(self.closing_operation_id).strip()
            try:
                closing_operation = validate_id(
                    closing_candidate,
                    expected_kind="broker_operation",
                )
            except ValueError:
                closing_operation = validate_id(
                    closing_candidate,
                    expected_kind="liquidation_operation",
                )
        if self.status == PositionEpisodeStatus.OPEN:
            if closed is not None or closing_operation is not None:
                raise ProtectionContractError(
                    "OPEN position episode cannot have close facts"
                )
        else:
            if closed is None:
                raise ProtectionContractError(
                    "CLOSED position episode requires closed_at_utc"
                )
            if parse_utc(closed) < parse_utc(self.opened_at_utc):
                raise ProtectionContractError(
                    "closed_at_utc cannot precede opened_at_utc"
                )
        object.__setattr__(self, "closed_at_utc", closed)
        object.__setattr__(self, "closing_operation_id", closing_operation)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "position_episode_id": self.position_episode_id,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "source_command_id": self.source_command_id,
            "source_operation_id": self.source_operation_id,
            "source_attempt_id": self.source_attempt_id,
            "source_exec_ids": list(self.source_exec_ids),
            "side": self.side.value,
            "quantity": self.quantity,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "entry_average_price": self.entry_average_price,
            "broker_snapshot_id": self.broker_snapshot_id,
            "opened_at_utc": self.opened_at_utc,
            "status": self.status.value,
            "strategy_policy_hash": self.strategy_policy_hash,
            "protective_policy_hash": self.protective_policy_hash,
            "protective_policy": self.protective_policy.to_dict(),
            "closed_at_utc": self.closed_at_utc,
            "closing_operation_id": self.closing_operation_id,
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "PositionEpisodeV1":
        _exact_keys(value, cls.KEYS, context="position episode")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ProtectionContractError(
                "unsupported position-episode schema"
            )
        raw_policy = value["protective_policy"]
        raw_exec_ids = value["source_exec_ids"]
        if not isinstance(raw_policy, Mapping) or not isinstance(
            raw_exec_ids,
            list,
        ):
            raise ProtectionContractError(
                "position episode protective_policy/source_exec_ids types "
                "are invalid"
            )
        try:
            side = StrategyPositionSide(str(value["side"]))
            status = PositionEpisodeStatus(str(value["status"]))
        except ValueError as exc:
            raise ProtectionContractError(
                "invalid position-episode enum"
            ) from exc
        return cls(
            position_episode_id=str(value["position_episode_id"]),
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            source_command_id=str(value["source_command_id"]),
            source_operation_id=str(value["source_operation_id"]),
            source_attempt_id=str(value["source_attempt_id"]),
            source_exec_ids=tuple(str(item) for item in raw_exec_ids),
            side=side,
            quantity=value["quantity"],
            con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]),
            entry_average_price=value["entry_average_price"],
            broker_snapshot_id=str(value["broker_snapshot_id"]),
            opened_at_utc=str(value["opened_at_utc"]),
            status=status,
            strategy_policy_hash=str(value["strategy_policy_hash"]),
            protective_policy_hash=str(value["protective_policy_hash"]),
            protective_policy=PositionEpisodePolicyV1.from_dict(raw_policy),
            closed_at_utc=(
                None
                if value["closed_at_utc"] is None
                else str(value["closed_at_utc"])
            ),
            closing_operation_id=(
                None
                if value["closing_operation_id"] is None
                else str(value["closing_operation_id"])
            ),
        )


@dataclass(frozen=True)
class ProtectiveOrderV1:
    protective_order_id: str
    protection_set_id: str
    position_episode_id: str
    kind: ProtectiveOrderKind
    state: ProtectiveOrderState
    planned_sequence: int
    order_ref: str
    side: BrokerOrderSide
    order_type: ProtectiveOrderType
    quantity: int
    con_id: int
    local_symbol: str
    stop_price: float | None
    limit_price: float | None
    time_in_force: str
    outside_rth: bool
    oca_group: str | None
    filled_qty: int
    remaining_qty: int
    broker_order_id: int | None
    broker_perm_id: int | None
    broker_status: str | None
    broker_terminal_proven: bool
    created_at_utc: str
    updated_at_utc: str
    terminal_at_utc: str | None
    last_broker_proof_at_utc: str | None
    failure_reason: str | None

    SCHEMA_NAME: ClassVar[str] = "ProtectiveOrder"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "protective_order_id",
        "protection_set_id",
        "position_episode_id",
        "kind",
        "state",
        "planned_sequence",
        "order_ref",
        "side",
        "order_type",
        "quantity",
        "con_id",
        "local_symbol",
        "stop_price",
        "limit_price",
        "time_in_force",
        "outside_rth",
        "oca_group",
        "filled_qty",
        "remaining_qty",
        "broker_order_id",
        "broker_perm_id",
        "broker_status",
        "broker_terminal_proven",
        "created_at_utc",
        "updated_at_utc",
        "terminal_at_utc",
        "last_broker_proof_at_utc",
        "failure_reason",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "protective_order_id",
            validate_id(
                self.protective_order_id,
                expected_kind="protective_order",
            ),
        )
        object.__setattr__(
            self,
            "protection_set_id",
            validate_id(
                self.protection_set_id,
                expected_kind="protection_set",
            ),
        )
        object.__setattr__(
            self,
            "position_episode_id",
            validate_id(
                self.position_episode_id,
                expected_kind="position_episode",
            ),
        )
        if not isinstance(self.kind, ProtectiveOrderKind):
            raise ProtectionContractError(
                f"invalid protective kind: {self.kind!r}"
            )
        if not isinstance(self.state, ProtectiveOrderState):
            raise ProtectionContractError(
                f"invalid protective state: {self.state!r}"
            )
        object.__setattr__(
            self,
            "planned_sequence",
            _integer(
                self.planned_sequence,
                field_name="planned_sequence",
                minimum=1,
            ),
        )
        order_ref = str(_text(self.order_ref, field_name="order_ref"))
        if not _ORDER_REF_RE.fullmatch(order_ref):
            raise ProtectionContractError(
                "order_ref must contain 1..64 safe characters: "
                f"{order_ref!r}"
            )
        object.__setattr__(self, "order_ref", order_ref)
        if not isinstance(self.side, BrokerOrderSide):
            raise ProtectionContractError(
                f"invalid broker side: {self.side!r}"
            )
        if not isinstance(self.order_type, ProtectiveOrderType):
            raise ProtectionContractError(
                f"invalid protective order type: {self.order_type!r}"
            )
        quantity = _integer(
            self.quantity,
            field_name="quantity",
            minimum=1,
        )
        filled = _integer(
            self.filled_qty,
            field_name="filled_qty",
            minimum=0,
        )
        remaining = _integer(
            self.remaining_qty,
            field_name="remaining_qty",
            minimum=0,
        )
        if filled + remaining != quantity:
            raise ProtectionContractError(
                "protective filled_qty + remaining_qty must equal quantity"
            )
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "filled_qty", filled)
        object.__setattr__(self, "remaining_qty", remaining)
        object.__setattr__(
            self,
            "con_id",
            _integer(self.con_id, field_name="con_id", minimum=1),
        )
        object.__setattr__(
            self,
            "local_symbol",
            str(_text(self.local_symbol, field_name="local_symbol")),
        )
        stop_price = _optional_finite(
            self.stop_price,
            field_name="stop_price",
        )
        limit_price = _optional_finite(
            self.limit_price,
            field_name="limit_price",
        )
        if self.kind == ProtectiveOrderKind.STOP_LOSS:
            if (
                self.order_type != ProtectiveOrderType.STOP
                or stop_price is None
                or limit_price is not None
            ):
                raise ProtectionContractError(
                    "STOP_LOSS requires order_type=STOP and only stop_price"
                )
            if self.planned_sequence != 1:
                raise ProtectionContractError(
                    "STOP_LOSS must be planned first"
                )
        else:
            if (
                self.order_type != ProtectiveOrderType.LIMIT
                or limit_price is None
                or stop_price is not None
            ):
                raise ProtectionContractError(
                    "TAKE_PROFIT requires order_type=LIMIT and only limit_price"
                )
            if self.planned_sequence != 2:
                raise ProtectionContractError(
                    "TAKE_PROFIT must be planned second"
                )
        object.__setattr__(self, "stop_price", stop_price)
        object.__setattr__(self, "limit_price", limit_price)
        tif = str(
            _text(self.time_in_force, field_name="time_in_force")
        ).upper()
        if tif not in {"DAY", "GTC"}:
            raise ProtectionContractError(
                f"unsupported protective TIF: {tif!r}"
            )
        object.__setattr__(self, "time_in_force", tif)
        object.__setattr__(
            self,
            "outside_rth",
            _strict_bool(self.outside_rth, field_name="outside_rth"),
        )
        object.__setattr__(
            self,
            "oca_group",
            _text(self.oca_group, field_name="oca_group", optional=True),
        )
        broker_order_id = (
            None
            if self.broker_order_id is None
            else _integer(
                self.broker_order_id,
                field_name="broker_order_id",
                minimum=1,
            )
        )
        broker_perm_id = (
            None
            if self.broker_perm_id is None
            else _integer(
                self.broker_perm_id,
                field_name="broker_perm_id",
                minimum=1,
            )
        )
        object.__setattr__(self, "broker_order_id", broker_order_id)
        object.__setattr__(self, "broker_perm_id", broker_perm_id)
        object.__setattr__(
            self,
            "broker_status",
            _text(self.broker_status, field_name="broker_status", optional=True),
        )
        object.__setattr__(
            self,
            "broker_terminal_proven",
            _strict_bool(
                self.broker_terminal_proven,
                field_name="broker_terminal_proven",
            ),
        )
        created = _utc(self.created_at_utc, field_name="created_at_utc")
        updated = _utc(self.updated_at_utc, field_name="updated_at_utc")
        if parse_utc(updated) < parse_utc(created):
            raise ProtectionContractError(
                "protective updated_at_utc cannot precede created_at_utc"
            )
        object.__setattr__(self, "created_at_utc", created)
        object.__setattr__(self, "updated_at_utc", updated)
        terminal = (
            None
            if self.terminal_at_utc is None
            else _utc(self.terminal_at_utc, field_name="terminal_at_utc")
        )
        proof = (
            None
            if self.last_broker_proof_at_utc is None
            else _utc(
                self.last_broker_proof_at_utc,
                field_name="last_broker_proof_at_utc",
            )
        )
        reason = _text(
            self.failure_reason,
            field_name="failure_reason",
            optional=True,
        )
        object.__setattr__(self, "terminal_at_utc", terminal)
        object.__setattr__(self, "last_broker_proof_at_utc", proof)
        object.__setattr__(self, "failure_reason", reason)

        terminal_states = {
            ProtectiveOrderState.FILLED,
            ProtectiveOrderState.CANCELLED,
            ProtectiveOrderState.REJECTED,
            ProtectiveOrderState.FAILED,
            ProtectiveOrderState.NOT_REQUIRED,
        }
        if self.state == ProtectiveOrderState.PLANNED:
            if (
                broker_order_id is not None
                or proof is not None
                or terminal is not None
            ):
                raise ProtectionContractError(
                    "PLANNED protective order cannot have broker/terminal facts"
                )
        elif self.state == ProtectiveOrderState.SUBMITTING:
            if broker_order_id is None or terminal is not None:
                raise ProtectionContractError(
                    "SUBMITTING protective order requires broker_order_id"
                )
        elif self.state == ProtectiveOrderState.LIVE:
            if broker_order_id is None or proof is None or terminal is not None:
                raise ProtectionContractError(
                    "LIVE protective order requires broker identity and proof"
                )
        elif self.state in terminal_states:
            if terminal is None:
                raise ProtectionContractError(
                    f"{self.state.value} protective order requires terminal_at_utc"
                )
            if self.state != ProtectiveOrderState.NOT_REQUIRED and proof is None:
                raise ProtectionContractError(
                    f"{self.state.value} protective order requires broker proof"
                )
        elif self.state == ProtectiveOrderState.UNKNOWN_OUTCOME:
            if broker_order_id is None or reason is None or remaining <= 0:
                raise ProtectionContractError(
                    "UNKNOWN_OUTCOME requires broker_order_id, remaining qty "
                    "and reason"
                )
        if self.state == ProtectiveOrderState.FILLED and (
            filled != quantity
            or remaining != 0
            or not self.broker_terminal_proven
        ):
            raise ProtectionContractError(
                "FILLED protective order requires complete terminal fill"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "protective_order_id": self.protective_order_id,
            "protection_set_id": self.protection_set_id,
            "position_episode_id": self.position_episode_id,
            "kind": self.kind.value,
            "state": self.state.value,
            "planned_sequence": self.planned_sequence,
            "order_ref": self.order_ref,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "stop_price": self.stop_price,
            "limit_price": self.limit_price,
            "time_in_force": self.time_in_force,
            "outside_rth": self.outside_rth,
            "oca_group": self.oca_group,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "broker_order_id": self.broker_order_id,
            "broker_perm_id": self.broker_perm_id,
            "broker_status": self.broker_status,
            "broker_terminal_proven": self.broker_terminal_proven,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "terminal_at_utc": self.terminal_at_utc,
            "last_broker_proof_at_utc": self.last_broker_proof_at_utc,
            "failure_reason": self.failure_reason,
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "ProtectiveOrderV1":
        _exact_keys(value, cls.KEYS, context="protective order")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ProtectionContractError(
                "unsupported protective-order schema"
            )
        try:
            kind = ProtectiveOrderKind(str(value["kind"]))
            state = ProtectiveOrderState(str(value["state"]))
            side = BrokerOrderSide(str(value["side"]))
            order_type = ProtectiveOrderType(str(value["order_type"]))
        except ValueError as exc:
            raise ProtectionContractError(
                "invalid protective-order enum"
            ) from exc
        return cls(
            protective_order_id=str(value["protective_order_id"]),
            protection_set_id=str(value["protection_set_id"]),
            position_episode_id=str(value["position_episode_id"]),
            kind=kind,
            state=state,
            planned_sequence=value["planned_sequence"],
            order_ref=str(value["order_ref"]),
            side=side,
            order_type=order_type,
            quantity=value["quantity"],
            con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]),
            stop_price=value["stop_price"],
            limit_price=value["limit_price"],
            time_in_force=str(value["time_in_force"]),
            outside_rth=value["outside_rth"],
            oca_group=(
                None if value["oca_group"] is None else str(value["oca_group"])
            ),
            filled_qty=value["filled_qty"],
            remaining_qty=value["remaining_qty"],
            broker_order_id=value["broker_order_id"],
            broker_perm_id=value["broker_perm_id"],
            broker_status=(
                None
                if value["broker_status"] is None
                else str(value["broker_status"])
            ),
            broker_terminal_proven=value["broker_terminal_proven"],
            created_at_utc=str(value["created_at_utc"]),
            updated_at_utc=str(value["updated_at_utc"]),
            terminal_at_utc=(
                None
                if value["terminal_at_utc"] is None
                else str(value["terminal_at_utc"])
            ),
            last_broker_proof_at_utc=(
                None
                if value["last_broker_proof_at_utc"] is None
                else str(value["last_broker_proof_at_utc"])
            ),
            failure_reason=(
                None
                if value["failure_reason"] is None
                else str(value["failure_reason"])
            ),
        )


@dataclass(frozen=True)
class ProtectionStateV1:
    protection_set_id: str
    position_episode_id: str
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    status: ProtectionSetStatus
    orders: tuple[ProtectiveOrderV1, ...]
    created_at_utc: str
    updated_at_utc: str
    terminal_at_utc: str | None
    blocking_reason: str | None

    SCHEMA_NAME: ClassVar[str] = "ProtectionState"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "protection_set_id",
        "position_episode_id",
        "account_id",
        "strategy_id",
        "strategy_version",
        "deployment_id",
        "instrument_id",
        "status",
        "orders",
        "created_at_utc",
        "updated_at_utc",
        "terminal_at_utc",
        "blocking_reason",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "protection_set_id",
            validate_id(
                self.protection_set_id,
                expected_kind="protection_set",
            ),
        )
        object.__setattr__(
            self,
            "position_episode_id",
            validate_id(
                self.position_episode_id,
                expected_kind="position_episode",
            ),
        )
        object.__setattr__(
            self,
            "account_id",
            str(_text(self.account_id, field_name="account_id")),
        )
        for field_name in (
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
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
        if not isinstance(self.status, ProtectionSetStatus):
            raise ProtectionContractError(
                f"invalid protection status: {self.status!r}"
            )
        orders = tuple(
            sorted(self.orders, key=lambda item: item.planned_sequence)
        )
        if any(not isinstance(item, ProtectiveOrderV1) for item in orders):
            raise ProtectionContractError(
                "orders must contain ProtectiveOrderV1 values"
            )
        if not orders or orders[0].kind != ProtectiveOrderKind.STOP_LOSS:
            raise ProtectionContractError(
                "protection set requires STOP_LOSS as first order"
            )
        if (
            len(orders) > 2
            or len({item.kind for item in orders}) != len(orders)
        ):
            raise ProtectionContractError(
                "protection set orders must be unique by kind"
            )
        for order in orders:
            if (
                order.protection_set_id != self.protection_set_id
                or order.position_episode_id != self.position_episode_id
            ):
                raise ProtectionContractError(
                    "protective order belongs to another protection set/episode"
                )
        object.__setattr__(self, "orders", orders)
        created = _utc(self.created_at_utc, field_name="created_at_utc")
        updated = _utc(self.updated_at_utc, field_name="updated_at_utc")
        if parse_utc(updated) < parse_utc(created):
            raise ProtectionContractError(
                "protection updated_at_utc cannot precede created_at_utc"
            )
        object.__setattr__(self, "created_at_utc", created)
        object.__setattr__(self, "updated_at_utc", updated)
        terminal = (
            None
            if self.terminal_at_utc is None
            else _utc(self.terminal_at_utc, field_name="terminal_at_utc")
        )
        reason = _text(
            self.blocking_reason,
            field_name="blocking_reason",
            optional=True,
        )
        object.__setattr__(self, "terminal_at_utc", terminal)
        object.__setattr__(self, "blocking_reason", reason)

        stop = orders[0]
        tp = next(
            (
                item
                for item in orders
                if item.kind == ProtectiveOrderKind.TAKE_PROFIT
            ),
            None,
        )
        terminal_states = {
            ProtectionSetStatus.EXITED,
            ProtectionSetStatus.CLOSED,
        }
        if self.status in terminal_states and terminal is None:
            raise ProtectionContractError(
                f"{self.status.value} protection requires terminal_at_utc"
            )
        if self.status not in terminal_states and terminal is not None:
            raise ProtectionContractError(
                f"nonterminal protection status {self.status.value} "
                "cannot be terminal"
            )
        if self.status in {
            ProtectionSetStatus.UNPROTECTED,
            ProtectionSetStatus.OPERATOR_REQUIRED,
        } and reason is None:
            raise ProtectionContractError(
                f"{self.status.value} protection requires blocking_reason"
            )
        if self.status == ProtectionSetStatus.PROTECTED:
            if stop.state != ProtectiveOrderState.LIVE:
                raise ProtectionContractError(
                    "PROTECTED state requires a proven LIVE STOP"
                )
            if tp is not None and tp.state not in {
                ProtectiveOrderState.LIVE,
                ProtectiveOrderState.FAILED,
                ProtectiveOrderState.REJECTED,
                ProtectiveOrderState.CANCELLED,
                ProtectiveOrderState.NOT_REQUIRED,
            }:
                raise ProtectionContractError(
                    "PROTECTED state has an invalid take-profit state"
                )
        if (
            self.status == ProtectionSetStatus.STOP_LIVE
            and stop.state != ProtectiveOrderState.LIVE
        ):
            raise ProtectionContractError(
                "STOP_LIVE requires LIVE stop"
            )
        if self.status == ProtectionSetStatus.EXITED and not any(
            item.state == ProtectiveOrderState.FILLED for item in orders
        ):
            raise ProtectionContractError(
                "EXITED protection requires a filled order"
            )

    @property
    def stop_order(self) -> ProtectiveOrderV1:
        return self.orders[0]

    @property
    def take_profit_order(self) -> ProtectiveOrderV1 | None:
        return next(
            (
                item
                for item in self.orders
                if item.kind == ProtectiveOrderKind.TAKE_PROFIT
            ),
            None,
        )

    @property
    def stop_proven_live(self) -> bool:
        return self.stop_order.state == ProtectiveOrderState.LIVE

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "protection_set_id": self.protection_set_id,
            "position_episode_id": self.position_episode_id,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "status": self.status.value,
            "orders": [item.to_dict() for item in self.orders],
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "terminal_at_utc": self.terminal_at_utc,
            "blocking_reason": self.blocking_reason,
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "ProtectionStateV1":
        _exact_keys(value, cls.KEYS, context="protection state")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ProtectionContractError(
                "unsupported protection-state schema"
            )
        raw_orders = value["orders"]
        if not isinstance(raw_orders, list):
            raise ProtectionContractError(
                "protection orders must be a list"
            )
        try:
            status = ProtectionSetStatus(str(value["status"]))
        except ValueError as exc:
            raise ProtectionContractError(
                "invalid protection-state enum"
            ) from exc
        return cls(
            protection_set_id=str(value["protection_set_id"]),
            position_episode_id=str(value["position_episode_id"]),
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            status=status,
            orders=tuple(
                ProtectiveOrderV1.from_dict(item) for item in raw_orders
            ),
            created_at_utc=str(value["created_at_utc"]),
            updated_at_utc=str(value["updated_at_utc"]),
            terminal_at_utc=(
                None
                if value["terminal_at_utc"] is None
                else str(value["terminal_at_utc"])
            ),
            blocking_reason=(
                None
                if value["blocking_reason"] is None
                else str(value["blocking_reason"])
            ),
        )
