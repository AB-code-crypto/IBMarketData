from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ORDER_REF_RE = re.compile(r"^[A-Za-z0-9:._-]{1,64}$")


class BrokerExecutionContractError(ValueError):
    pass


class BrokerOrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class BrokerOperationState(str, Enum):
    PREPARING = "PREPARING"
    SUBMITTING = "SUBMITTING"
    LIVE = "LIVE"
    RECONCILING = "RECONCILING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_OPERATOR_REQUIRED = "FAILED_OPERATOR_REQUIRED"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"


class BrokerAttemptState(str, Enum):
    PREPARING = "PREPARING"
    SUBMITTING = "SUBMITTING"
    LIVE = "LIVE"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"


class BrokerObservationOutcome(str, Enum):
    LIVE = "LIVE"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND"
    AMBIGUOUS = "AMBIGUOUS"


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
        raise BrokerExecutionContractError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


def _identifier(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise BrokerExecutionContractError(f"invalid {field_name}: {value!r}")
    return text


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise BrokerExecutionContractError(f"{field_name} is required")
    return text


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value).strip() or None


def _integer(
    value: object,
    *,
    field_name: str,
    minimum: int = 0,
) -> int:
    if isinstance(value, bool):
        raise BrokerExecutionContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerExecutionContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < minimum or exact != float(parsed):
        raise BrokerExecutionContractError(
            f"{field_name} must be an integer >= {minimum}: {value!r}"
        )
    return parsed


def _optional_positive_int(value: object | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    return _integer(value, field_name=field_name, minimum=1)


def _strict_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise BrokerExecutionContractError(f"{field_name} must be a boolean")
    return value


def _timestamp(value: object, *, field_name: str) -> str:
    try:
        return format_utc(parse_utc(str(value)))
    except (TypeError, ValueError) as exc:
        raise BrokerExecutionContractError(
            f"invalid {field_name}: {value!r}"
        ) from exc


@dataclass(frozen=True)
class BrokerOrderOperationV1:
    operation_id: str
    command_id: str
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    side: BrokerOrderSide
    order_type: str
    con_id: int
    local_symbol: str
    requested_qty: int
    filled_qty: int
    remaining_qty: int
    state: BrokerOperationState
    current_attempt_id: str
    current_attempt_no: int
    created_at_utc: str
    updated_at_utc: str
    terminal_at_utc: str | None
    blocking_reason: str | None

    SCHEMA_NAME: ClassVar[str] = "BrokerOrderOperation"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "operation_id",
        "command_id",
        "account_id",
        "strategy_id",
        "strategy_version",
        "deployment_id",
        "instrument_id",
        "side",
        "order_type",
        "con_id",
        "local_symbol",
        "requested_qty",
        "filled_qty",
        "remaining_qty",
        "state",
        "current_attempt_id",
        "current_attempt_no",
        "created_at_utc",
        "updated_at_utc",
        "terminal_at_utc",
        "blocking_reason",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "operation_id",
            validate_id(self.operation_id, expected_kind="broker_operation"),
        )
        object.__setattr__(
            self,
            "command_id",
            validate_id(self.command_id, expected_kind="strategy_command"),
        )
        object.__setattr__(
            self,
            "current_attempt_id",
            validate_id(self.current_attempt_id, expected_kind="broker_attempt"),
        )
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
        object.__setattr__(
            self,
            "strategy_version",
            _integer(
                self.strategy_version,
                field_name="strategy_version",
                minimum=1,
            ),
        )
        if not isinstance(self.side, BrokerOrderSide):
            raise BrokerExecutionContractError(f"invalid broker side: {self.side!r}")
        order_type = str(self.order_type or "").strip().upper()
        if order_type != "MARKET":
            raise BrokerExecutionContractError(
                "broker operation v1 supports MARKET orders only"
            )
        object.__setattr__(self, "order_type", order_type)
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
            raise BrokerExecutionContractError(
                "operation filled_qty + remaining_qty must equal requested_qty"
            )
        object.__setattr__(self, "requested_qty", requested)
        object.__setattr__(self, "filled_qty", filled)
        object.__setattr__(self, "remaining_qty", remaining)
        if not isinstance(self.state, BrokerOperationState):
            raise BrokerExecutionContractError(
                f"invalid broker operation state: {self.state!r}"
            )
        object.__setattr__(
            self,
            "current_attempt_no",
            _integer(
                self.current_attempt_no,
                field_name="current_attempt_no",
                minimum=1,
            ),
        )
        created = _timestamp(self.created_at_utc, field_name="created_at_utc")
        updated = _timestamp(self.updated_at_utc, field_name="updated_at_utc")
        if parse_utc(updated) < parse_utc(created):
            raise BrokerExecutionContractError(
                "operation updated_at_utc cannot precede created_at_utc"
            )
        object.__setattr__(self, "created_at_utc", created)
        object.__setattr__(self, "updated_at_utc", updated)
        terminal = (
            None
            if self.terminal_at_utc is None
            else _timestamp(self.terminal_at_utc, field_name="terminal_at_utc")
        )
        if terminal is not None and parse_utc(terminal) < parse_utc(updated):
            raise BrokerExecutionContractError(
                "operation terminal_at_utc cannot precede updated_at_utc"
            )
        object.__setattr__(self, "terminal_at_utc", terminal)
        reason = _optional_text(self.blocking_reason)
        object.__setattr__(self, "blocking_reason", reason)

        if self.state == BrokerOperationState.SUCCEEDED:
            if remaining != 0 or filled != requested or terminal is None:
                raise BrokerExecutionContractError(
                    "SUCCEEDED operation requires a complete fill and terminal time"
                )
            if reason is not None:
                raise BrokerExecutionContractError(
                    "SUCCEEDED operation cannot have a blocking reason"
                )
        elif self.state == BrokerOperationState.FAILED_OPERATOR_REQUIRED:
            if terminal is None or reason is None:
                raise BrokerExecutionContractError(
                    "FAILED_OPERATOR_REQUIRED requires terminal time and reason"
                )
        else:
            if terminal is not None:
                raise BrokerExecutionContractError(
                    f"nonterminal operation state {self.state.value} cannot have terminal_at"
                )
            if self.state in {
                BrokerOperationState.FAILED_RETRYABLE,
                BrokerOperationState.UNKNOWN_OUTCOME,
            } and (remaining <= 0 or reason is None):
                raise BrokerExecutionContractError(
                    f"{self.state.value} requires remaining quantity and reason"
                )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BrokerOrderOperationV1":
        _exact_keys(value, cls.KEYS, context="broker order operation")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise BrokerExecutionContractError(
                "unsupported broker-order-operation schema"
            )
        try:
            side = BrokerOrderSide(str(value["side"]))
            state = BrokerOperationState(str(value["state"]))
        except ValueError as exc:
            raise BrokerExecutionContractError(
                "invalid broker-order-operation enum value"
            ) from exc
        return cls(
            operation_id=str(value["operation_id"]),
            command_id=str(value["command_id"]),
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            side=side,
            order_type=str(value["order_type"]),
            con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]),
            requested_qty=value["requested_qty"],
            filled_qty=value["filled_qty"],
            remaining_qty=value["remaining_qty"],
            state=state,
            current_attempt_id=str(value["current_attempt_id"]),
            current_attempt_no=value["current_attempt_no"],
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "operation_id": self.operation_id,
            "command_id": self.command_id,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "side": self.side.value,
            "order_type": self.order_type,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "requested_qty": self.requested_qty,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "state": self.state.value,
            "current_attempt_id": self.current_attempt_id,
            "current_attempt_no": self.current_attempt_no,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "terminal_at_utc": self.terminal_at_utc,
            "blocking_reason": self.blocking_reason,
        }


@dataclass(frozen=True)
class BrokerOrderAttemptV1:
    attempt_id: str
    operation_id: str
    attempt_no: int
    order_ref: str
    side: BrokerOrderSide
    order_type: str
    con_id: int
    local_symbol: str
    requested_qty: int
    filled_qty: int
    remaining_qty: int
    state: BrokerAttemptState
    broker_order_id: int | None
    broker_perm_id: int | None
    broker_status: str | None
    broker_terminal_proven: bool
    created_at_utc: str
    updated_at_utc: str
    terminal_at_utc: str | None
    last_broker_proof_at_utc: str | None
    failure_reason: str | None

    SCHEMA_NAME: ClassVar[str] = "BrokerOrderAttempt"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "attempt_id",
        "operation_id",
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
            "attempt_id",
            validate_id(self.attempt_id, expected_kind="broker_attempt"),
        )
        object.__setattr__(
            self,
            "operation_id",
            validate_id(self.operation_id, expected_kind="broker_operation"),
        )
        object.__setattr__(
            self,
            "attempt_no",
            _integer(self.attempt_no, field_name="attempt_no", minimum=1),
        )
        order_ref = str(self.order_ref or "").strip()
        if not _ORDER_REF_RE.fullmatch(order_ref):
            raise BrokerExecutionContractError(
                f"invalid order_ref; expected 1..64 safe characters: {self.order_ref!r}"
            )
        object.__setattr__(self, "order_ref", order_ref)
        if not isinstance(self.side, BrokerOrderSide):
            raise BrokerExecutionContractError(f"invalid broker side: {self.side!r}")
        order_type = str(self.order_type or "").strip().upper()
        if order_type != "MARKET":
            raise BrokerExecutionContractError(
                "broker attempt v1 supports MARKET orders only"
            )
        object.__setattr__(self, "order_type", order_type)
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
            raise BrokerExecutionContractError(
                "attempt filled_qty + remaining_qty must equal requested_qty"
            )
        object.__setattr__(self, "requested_qty", requested)
        object.__setattr__(self, "filled_qty", filled)
        object.__setattr__(self, "remaining_qty", remaining)
        if not isinstance(self.state, BrokerAttemptState):
            raise BrokerExecutionContractError(
                f"invalid broker attempt state: {self.state!r}"
            )
        object.__setattr__(
            self,
            "broker_order_id",
            _optional_positive_int(
                self.broker_order_id,
                field_name="broker_order_id",
            ),
        )
        object.__setattr__(
            self,
            "broker_perm_id",
            _optional_positive_int(
                self.broker_perm_id,
                field_name="broker_perm_id",
            ),
        )
        object.__setattr__(self, "broker_status", _optional_text(self.broker_status))
        object.__setattr__(
            self,
            "broker_terminal_proven",
            _strict_bool(
                self.broker_terminal_proven,
                field_name="broker_terminal_proven",
            ),
        )
        created = _timestamp(self.created_at_utc, field_name="created_at_utc")
        updated = _timestamp(self.updated_at_utc, field_name="updated_at_utc")
        if parse_utc(updated) < parse_utc(created):
            raise BrokerExecutionContractError(
                "attempt updated_at_utc cannot precede created_at_utc"
            )
        object.__setattr__(self, "created_at_utc", created)
        object.__setattr__(self, "updated_at_utc", updated)
        terminal = (
            None
            if self.terminal_at_utc is None
            else _timestamp(self.terminal_at_utc, field_name="terminal_at_utc")
        )
        proof = (
            None
            if self.last_broker_proof_at_utc is None
            else _timestamp(
                self.last_broker_proof_at_utc,
                field_name="last_broker_proof_at_utc",
            )
        )
        if terminal is not None and parse_utc(terminal) < parse_utc(updated):
            raise BrokerExecutionContractError(
                "attempt terminal_at_utc cannot precede updated_at_utc"
            )
        if proof is not None and parse_utc(proof) > parse_utc(updated):
            raise BrokerExecutionContractError(
                "last broker proof cannot follow updated_at_utc"
            )
        object.__setattr__(self, "terminal_at_utc", terminal)
        object.__setattr__(self, "last_broker_proof_at_utc", proof)
        failure_reason = _optional_text(self.failure_reason)
        object.__setattr__(self, "failure_reason", failure_reason)

        terminal_states = {
            BrokerAttemptState.FILLED,
            BrokerAttemptState.CANCELLED,
            BrokerAttemptState.REJECTED,
            BrokerAttemptState.FAILED,
        }
        if self.state == BrokerAttemptState.PREPARING:
            if any(
                value is not None
                for value in (
                    self.broker_order_id,
                    self.broker_perm_id,
                    self.broker_status,
                    terminal,
                    proof,
                    failure_reason,
                )
            ) or self.broker_terminal_proven or filled != 0:
                raise BrokerExecutionContractError(
                    "PREPARING attempt cannot contain broker facts or fills"
                )
        elif self.state == BrokerAttemptState.SUBMITTING:
            if terminal is not None or proof is not None or self.broker_terminal_proven:
                raise BrokerExecutionContractError(
                    "SUBMITTING attempt cannot contain terminal broker proof"
                )
        elif self.state == BrokerAttemptState.LIVE:
            if (
                self.broker_order_id is None
                or self.broker_status is None
                or proof is None
                or remaining <= 0
                or terminal is not None
                or self.broker_terminal_proven
            ):
                raise BrokerExecutionContractError(
                    "LIVE attempt requires broker identity, proof and remaining quantity"
                )
        elif self.state in terminal_states:
            if (
                self.broker_order_id is None
                or self.broker_status is None
                or proof is None
                or terminal is None
                or not self.broker_terminal_proven
            ):
                raise BrokerExecutionContractError(
                    "terminal attempt requires broker identity and terminal proof"
                )
            if self.state == BrokerAttemptState.FILLED and remaining != 0:
                raise BrokerExecutionContractError(
                    "FILLED attempt requires remaining_qty=0"
                )
        elif self.state == BrokerAttemptState.UNKNOWN_OUTCOME:
            if terminal is not None or self.broker_terminal_proven or failure_reason is None:
                raise BrokerExecutionContractError(
                    "UNKNOWN_OUTCOME requires a reason and forbids terminal proof"
                )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BrokerOrderAttemptV1":
        _exact_keys(value, cls.KEYS, context="broker order attempt")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise BrokerExecutionContractError("unsupported broker-attempt schema")
        try:
            side = BrokerOrderSide(str(value["side"]))
            state = BrokerAttemptState(str(value["state"]))
        except ValueError as exc:
            raise BrokerExecutionContractError(
                "invalid broker-attempt enum value"
            ) from exc
        return cls(
            attempt_id=str(value["attempt_id"]),
            operation_id=str(value["operation_id"]),
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "attempt_id": self.attempt_id,
            "operation_id": self.operation_id,
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


@dataclass(frozen=True)
class BrokerOrderObservationV1:
    order_ref: str
    outcome: BrokerObservationOutcome
    observed_at_utc: str
    broker_order_id: int | None
    broker_perm_id: int | None
    broker_status: str | None
    requested_qty: int | None
    filled_qty: int | None
    remaining_qty: int | None
    detail: str | None

    SCHEMA_NAME: ClassVar[str] = "BrokerOrderObservation"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "order_ref",
        "outcome",
        "observed_at_utc",
        "broker_order_id",
        "broker_perm_id",
        "broker_status",
        "requested_qty",
        "filled_qty",
        "remaining_qty",
        "detail",
    }

    def __post_init__(self) -> None:
        order_ref = str(self.order_ref or "").strip()
        if not _ORDER_REF_RE.fullmatch(order_ref):
            raise BrokerExecutionContractError(f"invalid observation order_ref: {order_ref!r}")
        object.__setattr__(self, "order_ref", order_ref)
        if not isinstance(self.outcome, BrokerObservationOutcome):
            raise BrokerExecutionContractError(
                f"invalid observation outcome: {self.outcome!r}"
            )
        object.__setattr__(
            self,
            "observed_at_utc",
            _timestamp(self.observed_at_utc, field_name="observed_at_utc"),
        )
        object.__setattr__(
            self,
            "broker_order_id",
            _optional_positive_int(
                self.broker_order_id,
                field_name="broker_order_id",
            ),
        )
        object.__setattr__(
            self,
            "broker_perm_id",
            _optional_positive_int(
                self.broker_perm_id,
                field_name="broker_perm_id",
            ),
        )
        object.__setattr__(self, "broker_status", _optional_text(self.broker_status))
        detail = _optional_text(self.detail)
        object.__setattr__(self, "detail", detail)

        uncertain = self.outcome in {
            BrokerObservationOutcome.NOT_FOUND,
            BrokerObservationOutcome.AMBIGUOUS,
        }
        if uncertain:
            if any(
                value is not None
                for value in (
                    self.broker_order_id,
                    self.broker_perm_id,
                    self.broker_status,
                    self.requested_qty,
                    self.filled_qty,
                    self.remaining_qty,
                )
            ):
                raise BrokerExecutionContractError(
                    "uncertain observation cannot publish one broker order fact"
                )
            if detail is None:
                raise BrokerExecutionContractError(
                    "uncertain observation requires detail"
                )
            return

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
            raise BrokerExecutionContractError(
                "observation filled_qty + remaining_qty must equal requested_qty"
            )
        object.__setattr__(self, "requested_qty", requested)
        object.__setattr__(self, "filled_qty", filled)
        object.__setattr__(self, "remaining_qty", remaining)
        if self.broker_order_id is None or self.broker_status is None:
            raise BrokerExecutionContractError(
                "exact observation requires broker order id and status"
            )
        if self.outcome == BrokerObservationOutcome.LIVE and remaining <= 0:
            raise BrokerExecutionContractError(
                "LIVE observation requires remaining quantity"
            )
        if self.outcome == BrokerObservationOutcome.FILLED and remaining != 0:
            raise BrokerExecutionContractError(
                "FILLED observation requires remaining_qty=0"
            )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BrokerOrderObservationV1":
        _exact_keys(value, cls.KEYS, context="broker order observation")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise BrokerExecutionContractError(
                "unsupported broker-order-observation schema"
            )
        try:
            outcome = BrokerObservationOutcome(str(value["outcome"]))
        except ValueError as exc:
            raise BrokerExecutionContractError(
                f"invalid observation outcome: {value['outcome']!r}"
            ) from exc
        return cls(
            order_ref=str(value["order_ref"]),
            outcome=outcome,
            observed_at_utc=str(value["observed_at_utc"]),
            broker_order_id=value["broker_order_id"],
            broker_perm_id=value["broker_perm_id"],
            broker_status=(
                None
                if value["broker_status"] is None
                else str(value["broker_status"])
            ),
            requested_qty=value["requested_qty"],
            filled_qty=value["filled_qty"],
            remaining_qty=value["remaining_qty"],
            detail=None if value["detail"] is None else str(value["detail"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "order_ref": self.order_ref,
            "outcome": self.outcome.value,
            "observed_at_utc": self.observed_at_utc,
            "broker_order_id": self.broker_order_id,
            "broker_perm_id": self.broker_perm_id,
            "broker_status": self.broker_status,
            "requested_qty": self.requested_qty,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "detail": self.detail,
        }
