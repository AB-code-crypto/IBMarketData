from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import BrokerOrderSide
from ibmd.public_contracts.execution import StrategyPositionSide

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ORDER_REF_RE = re.compile(r"^[A-Za-z0-9:._-]{1,64}$")


class LiquidationContractError(ValueError):
    pass


class LiquidationReason(str, Enum):
    MISSING_STOP = "MISSING_STOP"
    STOP_REJECTED = "STOP_REJECTED"
    STOP_BREACHED = "STOP_BREACHED"
    DAILY_FLAT = "DAILY_FLAT"
    DAILY_HALT = "DAILY_HALT"
    ROLLOVER = "ROLLOVER"
    MANUAL_EMERGENCY = "MANUAL_EMERGENCY"


class LiquidationOperationState(str, Enum):
    REQUESTED = "REQUESTED"
    PREPARING = "PREPARING"
    CANCELING_EXITS = "CANCELING_EXITS"
    SUBMITTING = "SUBMITTING"
    LIVE = "LIVE"
    RECONCILING = "RECONCILING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_OPERATOR_REQUIRED = "FAILED_OPERATOR_REQUIRED"
    CANCELLED_AS_ALREADY_FLAT = "CANCELLED_AS_ALREADY_FLAT"


class LiquidationAttemptState(str, Enum):
    PREPARING = "PREPARING"
    SUBMITTING = "SUBMITTING"
    LIVE = "LIVE"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"


class LiquidationNextAction(str, Enum):
    RECONCILE_EXITS = "RECONCILE_EXITS"
    CANCEL_TAKE_PROFIT = "CANCEL_TAKE_PROFIT"
    CANCEL_STOP = "CANCEL_STOP"
    SUBMIT_MARKET_CLOSE = "SUBMIT_MARKET_CLOSE"
    RECONCILE_MARKET_CLOSE = "RECONCILE_MARKET_CLOSE"
    WAIT_FOR_FLAT = "WAIT_FOR_FLAT"
    OPERATOR_REQUIRED = "OPERATOR_REQUIRED"
    NONE = "NONE"


def _exact_keys(value: Mapping[str, Any], expected: set[str], *, context: str) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        raise LiquidationContractError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


def _text(value: object, *, field_name: str, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    parsed = str(value or "").strip()
    if not parsed and not optional:
        raise LiquidationContractError(f"{field_name} is required")
    return parsed or None


def _identifier(value: object, *, field_name: str) -> str:
    parsed = str(_text(value, field_name=field_name))
    if not _IDENTIFIER_RE.fullmatch(parsed):
        raise LiquidationContractError(f"invalid {field_name}: {value!r}")
    return parsed


def _integer(value: object, *, field_name: str, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise LiquidationContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise LiquidationContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < minimum or exact != float(parsed):
        raise LiquidationContractError(
            f"{field_name} must be an integer >= {minimum}: {value!r}"
        )
    return parsed


def _optional_positive_int(value: object | None, *, field_name: str) -> int | None:
    return None if value is None else _integer(value, field_name=field_name, minimum=1)


def _utc(value: object, *, field_name: str) -> str:
    try:
        return format_utc(parse_utc(str(value)))
    except (TypeError, ValueError) as exc:
        raise LiquidationContractError(f"invalid {field_name}: {value!r}") from exc


def _order_ref(value: object) -> str:
    parsed = str(value or "").strip()
    if not _ORDER_REF_RE.fullmatch(parsed):
        raise LiquidationContractError(
            f"order_ref must contain 1..64 safe characters: {value!r}"
        )
    return parsed


@dataclass(frozen=True)
class LiquidationTriggerV1:
    trigger_id: str
    liquidation_operation_id: str
    reason: LiquidationReason
    source_ref: str
    triggered_at_utc: str

    SCHEMA_NAME: ClassVar[str] = "LiquidationTrigger"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "trigger_id",
        "liquidation_operation_id",
        "reason",
        "source_ref",
        "triggered_at_utc",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "trigger_id",
            validate_id(self.trigger_id, expected_kind="liquidation_trigger"),
        )
        object.__setattr__(
            self,
            "liquidation_operation_id",
            validate_id(
                self.liquidation_operation_id,
                expected_kind="liquidation_operation",
            ),
        )
        if not isinstance(self.reason, LiquidationReason):
            raise LiquidationContractError(f"invalid liquidation reason: {self.reason!r}")
        source = str(_text(self.source_ref, field_name="source_ref"))
        if len(source) > 256:
            raise LiquidationContractError("source_ref must not exceed 256 characters")
        object.__setattr__(self, "source_ref", source)
        object.__setattr__(
            self,
            "triggered_at_utc",
            _utc(self.triggered_at_utc, field_name="triggered_at_utc"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "trigger_id": self.trigger_id,
            "liquidation_operation_id": self.liquidation_operation_id,
            "reason": self.reason.value,
            "source_ref": self.source_ref,
            "triggered_at_utc": self.triggered_at_utc,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "LiquidationTriggerV1":
        _exact_keys(value, cls.KEYS, context="liquidation trigger")
        if value["schema_name"] != cls.SCHEMA_NAME or value["schema_version"] != 1:
            raise LiquidationContractError("unsupported liquidation-trigger schema")
        try:
            reason = LiquidationReason(str(value["reason"]))
        except ValueError as exc:
            raise LiquidationContractError("invalid liquidation-trigger reason") from exc
        return cls(
            trigger_id=str(value["trigger_id"]),
            liquidation_operation_id=str(value["liquidation_operation_id"]),
            reason=reason,
            source_ref=str(value["source_ref"]),
            triggered_at_utc=str(value["triggered_at_utc"]),
        )


@dataclass(frozen=True)
class LiquidationAttemptV1:
    liquidation_attempt_id: str
    liquidation_operation_id: str
    attempt_no: int
    order_ref: str
    side: BrokerOrderSide
    order_type: str
    con_id: int
    local_symbol: str
    requested_qty: int
    filled_qty: int
    remaining_qty: int
    state: LiquidationAttemptState
    broker_order_id: int | None
    broker_perm_id: int | None
    broker_status: str | None
    broker_terminal_proven: bool
    created_at_utc: str
    updated_at_utc: str
    terminal_at_utc: str | None
    last_broker_proof_at_utc: str | None
    failure_reason: str | None

    SCHEMA_NAME: ClassVar[str] = "LiquidationAttempt"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "liquidation_attempt_id",
        "liquidation_operation_id",
        "attempt_no",
        "order_ref",
        "side",
        "order_type",
        "con_id",
        "local_symbol",
        "requested_qty",
        "filled_qty",
        "remaining_qty",
        "state",
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
            "liquidation_attempt_id",
            validate_id(self.liquidation_attempt_id, expected_kind="liquidation_attempt"),
        )
        object.__setattr__(
            self,
            "liquidation_operation_id",
            validate_id(
                self.liquidation_operation_id,
                expected_kind="liquidation_operation",
            ),
        )
        object.__setattr__(
            self,
            "attempt_no",
            _integer(self.attempt_no, field_name="attempt_no", minimum=1),
        )
        object.__setattr__(self, "order_ref", _order_ref(self.order_ref))
        if not isinstance(self.side, BrokerOrderSide):
            raise LiquidationContractError(f"invalid liquidation side: {self.side!r}")
        order_type = str(_text(self.order_type, field_name="order_type")).upper()
        if order_type != "MARKET":
            raise LiquidationContractError("liquidation attempts support MARKET only")
        object.__setattr__(self, "order_type", order_type)
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
        requested = _integer(
            self.requested_qty,
            field_name="requested_qty",
            minimum=1,
        )
        filled = _integer(self.filled_qty, field_name="filled_qty")
        remaining = _integer(self.remaining_qty, field_name="remaining_qty")
        if filled + remaining != requested:
            raise LiquidationContractError(
                "liquidation attempt filled_qty + remaining_qty must equal requested_qty"
            )
        object.__setattr__(self, "requested_qty", requested)
        object.__setattr__(self, "filled_qty", filled)
        object.__setattr__(self, "remaining_qty", remaining)
        if not isinstance(self.state, LiquidationAttemptState):
            raise LiquidationContractError(
                f"invalid liquidation attempt state: {self.state!r}"
            )
        object.__setattr__(
            self,
            "broker_order_id",
            _optional_positive_int(self.broker_order_id, field_name="broker_order_id"),
        )
        object.__setattr__(
            self,
            "broker_perm_id",
            _optional_positive_int(self.broker_perm_id, field_name="broker_perm_id"),
        )
        object.__setattr__(
            self,
            "broker_status",
            _text(self.broker_status, field_name="broker_status", optional=True),
        )
        if not isinstance(self.broker_terminal_proven, bool):
            raise LiquidationContractError("broker_terminal_proven must be boolean")
        created = _utc(self.created_at_utc, field_name="created_at_utc")
        updated = _utc(self.updated_at_utc, field_name="updated_at_utc")
        if parse_utc(updated) < parse_utc(created):
            raise LiquidationContractError(
                "liquidation attempt updated_at cannot precede created_at"
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
        reason = _text(self.failure_reason, field_name="failure_reason", optional=True)
        object.__setattr__(self, "terminal_at_utc", terminal)
        object.__setattr__(self, "last_broker_proof_at_utc", proof)
        object.__setattr__(self, "failure_reason", reason)

        terminal_states = {
            LiquidationAttemptState.FILLED,
            LiquidationAttemptState.CANCELLED,
            LiquidationAttemptState.REJECTED,
            LiquidationAttemptState.FAILED,
        }
        if self.state in terminal_states:
            if terminal is None or proof is None or not self.broker_terminal_proven:
                raise LiquidationContractError(
                    f"{self.state.value} liquidation attempt requires terminal broker proof"
                )
        elif terminal is not None:
            raise LiquidationContractError(
                f"nonterminal liquidation attempt {self.state.value} cannot be terminal"
            )
        if self.state == LiquidationAttemptState.FILLED and (
            filled != requested or remaining != 0
        ):
            raise LiquidationContractError(
                "FILLED liquidation attempt requires complete fill"
            )
        if self.state == LiquidationAttemptState.UNKNOWN_OUTCOME and reason is None:
            raise LiquidationContractError("UNKNOWN_OUTCOME requires failure_reason")
        if self.state in {
            LiquidationAttemptState.SUBMITTING,
            LiquidationAttemptState.LIVE,
            LiquidationAttemptState.FILLED,
            LiquidationAttemptState.CANCELLED,
            LiquidationAttemptState.REJECTED,
            LiquidationAttemptState.FAILED,
            LiquidationAttemptState.UNKNOWN_OUTCOME,
        } and self.broker_order_id is None:
            raise LiquidationContractError(
                f"{self.state.value} liquidation attempt requires broker_order_id"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "liquidation_attempt_id": self.liquidation_attempt_id,
            "liquidation_operation_id": self.liquidation_operation_id,
            "attempt_no": self.attempt_no,
            "order_ref": self.order_ref,
            "side": self.side.value,
            "order_type": self.order_type,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "requested_qty": self.requested_qty,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "state": self.state.value,
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
    def from_dict(cls, value: Mapping[str, Any]) -> "LiquidationAttemptV1":
        _exact_keys(value, cls.KEYS, context="liquidation attempt")
        if value["schema_name"] != cls.SCHEMA_NAME or value["schema_version"] != 1:
            raise LiquidationContractError("unsupported liquidation-attempt schema")
        try:
            side = BrokerOrderSide(str(value["side"]))
            state = LiquidationAttemptState(str(value["state"]))
        except ValueError as exc:
            raise LiquidationContractError("invalid liquidation-attempt enum") from exc
        return cls(
            liquidation_attempt_id=str(value["liquidation_attempt_id"]),
            liquidation_operation_id=str(value["liquidation_operation_id"]),
            attempt_no=value["attempt_no"],
            order_ref=str(value["order_ref"]),
            side=side,
            order_type=str(value["order_type"]),
            con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]),
            requested_qty=value["requested_qty"],
            filled_qty=value["filled_qty"],
            remaining_qty=value["remaining_qty"],
            state=state,
            broker_order_id=value["broker_order_id"],
            broker_perm_id=value["broker_perm_id"],
            broker_status=value["broker_status"],
            broker_terminal_proven=value["broker_terminal_proven"],
            created_at_utc=str(value["created_at_utc"]),
            updated_at_utc=str(value["updated_at_utc"]),
            terminal_at_utc=value["terminal_at_utc"],
            last_broker_proof_at_utc=value["last_broker_proof_at_utc"],
            failure_reason=value["failure_reason"],
        )


@dataclass(frozen=True)
class LiquidationOperationV1:
    liquidation_operation_id: str
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    position_episode_id: str
    target_state: str
    initial_side: StrategyPositionSide
    initial_quantity: int
    con_id: int
    local_symbol: str
    broker_remaining_quantity: int
    liquidation_filled_quantity: int
    state: LiquidationOperationState
    trigger_reasons: tuple[LiquidationReason, ...]
    current_attempt_id: str | None
    current_attempt_no: int
    next_action: LiquidationNextAction
    created_at_utc: str
    updated_at_utc: str
    terminal_at_utc: str | None
    blocking_reason: str | None

    SCHEMA_NAME: ClassVar[str] = "LiquidationOperation"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "liquidation_operation_id",
        "account_id",
        "strategy_id",
        "strategy_version",
        "deployment_id",
        "instrument_id",
        "position_episode_id",
        "target_state",
        "initial_side",
        "initial_quantity",
        "con_id",
        "local_symbol",
        "broker_remaining_quantity",
        "liquidation_filled_quantity",
        "state",
        "trigger_reasons",
        "current_attempt_id",
        "current_attempt_no",
        "next_action",
        "created_at_utc",
        "updated_at_utc",
        "terminal_at_utc",
        "blocking_reason",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "liquidation_operation_id",
            validate_id(
                self.liquidation_operation_id,
                expected_kind="liquidation_operation",
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
            _integer(self.strategy_version, field_name="strategy_version", minimum=1),
        )
        object.__setattr__(
            self,
            "position_episode_id",
            validate_id(self.position_episode_id, expected_kind="position_episode"),
        )
        target = str(_text(self.target_state, field_name="target_state")).upper()
        if target != "FLAT":
            raise LiquidationContractError("liquidation target_state must be FLAT")
        object.__setattr__(self, "target_state", target)
        if self.initial_side not in {
            StrategyPositionSide.LONG,
            StrategyPositionSide.SHORT,
        }:
            raise LiquidationContractError(
                "liquidation initial_side must be LONG or SHORT"
            )
        initial = _integer(
            self.initial_quantity,
            field_name="initial_quantity",
            minimum=1,
        )
        remaining = _integer(
            self.broker_remaining_quantity,
            field_name="broker_remaining_quantity",
            minimum=0,
        )
        filled = _integer(
            self.liquidation_filled_quantity,
            field_name="liquidation_filled_quantity",
            minimum=0,
        )
        object.__setattr__(self, "initial_quantity", initial)
        object.__setattr__(self, "broker_remaining_quantity", remaining)
        object.__setattr__(self, "liquidation_filled_quantity", filled)
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
        if not isinstance(self.state, LiquidationOperationState):
            raise LiquidationContractError(
                f"invalid liquidation operation state: {self.state!r}"
            )
        reasons = tuple(sorted(set(self.trigger_reasons), key=lambda item: item.value))
        if not reasons or any(not isinstance(item, LiquidationReason) for item in reasons):
            raise LiquidationContractError(
                "liquidation operation requires unique trigger reasons"
            )
        object.__setattr__(self, "trigger_reasons", reasons)
        current_attempt = (
            None
            if self.current_attempt_id is None
            else validate_id(
                self.current_attempt_id,
                expected_kind="liquidation_attempt",
            )
        )
        attempt_no = _integer(
            self.current_attempt_no,
            field_name="current_attempt_no",
            minimum=0,
        )
        if (current_attempt is None) != (attempt_no == 0):
            raise LiquidationContractError(
                "current_attempt_id and current_attempt_no must be absent together"
            )
        object.__setattr__(self, "current_attempt_id", current_attempt)
        object.__setattr__(self, "current_attempt_no", attempt_no)
        if not isinstance(self.next_action, LiquidationNextAction):
            raise LiquidationContractError(
                f"invalid liquidation next_action: {self.next_action!r}"
            )
        created = _utc(self.created_at_utc, field_name="created_at_utc")
        updated = _utc(self.updated_at_utc, field_name="updated_at_utc")
        if parse_utc(updated) < parse_utc(created):
            raise LiquidationContractError(
                "liquidation operation updated_at cannot precede created_at"
            )
        object.__setattr__(self, "created_at_utc", created)
        object.__setattr__(self, "updated_at_utc", updated)
        terminal = (
            None
            if self.terminal_at_utc is None
            else _utc(self.terminal_at_utc, field_name="terminal_at_utc")
        )
        reason = _text(self.blocking_reason, field_name="blocking_reason", optional=True)
        object.__setattr__(self, "terminal_at_utc", terminal)
        object.__setattr__(self, "blocking_reason", reason)

        terminal_states = {
            LiquidationOperationState.SUCCEEDED,
            LiquidationOperationState.FAILED_OPERATOR_REQUIRED,
            LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
        }
        if self.state in terminal_states:
            if terminal is None or self.next_action != LiquidationNextAction.NONE:
                raise LiquidationContractError(
                    f"{self.state.value} liquidation operation must be terminal"
                )
        elif terminal is not None:
            raise LiquidationContractError(
                f"nonterminal liquidation operation {self.state.value} cannot be terminal"
            )
        if self.state in {
            LiquidationOperationState.SUCCEEDED,
            LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
        } and remaining != 0:
            raise LiquidationContractError(
                f"{self.state.value} requires broker_remaining_quantity=0"
            )
        if self.state == LiquidationOperationState.SUCCEEDED and current_attempt is None:
            raise LiquidationContractError("SUCCEEDED liquidation requires an attempt")
        if self.state == LiquidationOperationState.FAILED_OPERATOR_REQUIRED and reason is None:
            raise LiquidationContractError(
                "FAILED_OPERATOR_REQUIRED liquidation requires blocking_reason"
            )
        if self.state == LiquidationOperationState.FAILED_RETRYABLE and (
            reason is None or remaining <= 0
        ):
            raise LiquidationContractError(
                "FAILED_RETRYABLE liquidation requires remaining quantity and reason"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "liquidation_operation_id": self.liquidation_operation_id,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "position_episode_id": self.position_episode_id,
            "target_state": self.target_state,
            "initial_side": self.initial_side.value,
            "initial_quantity": self.initial_quantity,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "broker_remaining_quantity": self.broker_remaining_quantity,
            "liquidation_filled_quantity": self.liquidation_filled_quantity,
            "state": self.state.value,
            "trigger_reasons": [item.value for item in self.trigger_reasons],
            "current_attempt_id": self.current_attempt_id,
            "current_attempt_no": self.current_attempt_no,
            "next_action": self.next_action.value,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "terminal_at_utc": self.terminal_at_utc,
            "blocking_reason": self.blocking_reason,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "LiquidationOperationV1":
        _exact_keys(value, cls.KEYS, context="liquidation operation")
        if value["schema_name"] != cls.SCHEMA_NAME or value["schema_version"] != 1:
            raise LiquidationContractError("unsupported liquidation-operation schema")
        raw_reasons = value["trigger_reasons"]
        if not isinstance(raw_reasons, list):
            raise LiquidationContractError("trigger_reasons must be a list")
        try:
            state = LiquidationOperationState(str(value["state"]))
            initial_side = StrategyPositionSide(str(value["initial_side"]))
            next_action = LiquidationNextAction(str(value["next_action"]))
            reasons = tuple(LiquidationReason(str(item)) for item in raw_reasons)
        except ValueError as exc:
            raise LiquidationContractError("invalid liquidation-operation enum") from exc
        return cls(
            liquidation_operation_id=str(value["liquidation_operation_id"]),
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            position_episode_id=str(value["position_episode_id"]),
            target_state=str(value["target_state"]),
            initial_side=initial_side,
            initial_quantity=value["initial_quantity"],
            con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]),
            broker_remaining_quantity=value["broker_remaining_quantity"],
            liquidation_filled_quantity=value["liquidation_filled_quantity"],
            state=state,
            trigger_reasons=reasons,
            current_attempt_id=value["current_attempt_id"],
            current_attempt_no=value["current_attempt_no"],
            next_action=next_action,
            created_at_utc=str(value["created_at_utc"]),
            updated_at_utc=str(value["updated_at_utc"]),
            terminal_at_utc=value["terminal_at_utc"],
            blocking_reason=value["blocking_reason"],
        )
