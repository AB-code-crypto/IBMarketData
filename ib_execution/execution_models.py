from dataclasses import dataclass
from enum import Enum


class ExecutionStatus(Enum):
    NEW = "NEW"
    SENDING = "SENDING"
    ACCEPTED = "ACCEPTED"
    EXECUTED = "EXECUTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class TradeIntent:
    trade_intent_id: int
    source_signal_id: int
    instrument_code: str
    order_ref: str

    action: str
    target_side: str
    target_qty: float

    position_before_side: str
    position_before_qty: float

    order_type: str
    limit_price: float | None
    limit_offset_points: float | None
    ttl_seconds: int | None

    status: str
    created_at_ts: int


@dataclass(frozen=True)
class ExecutionResult:
    trade_intent_id: int
    order_id: int | None
    order_action: str | None
    order_quantity: int | None
    status: ExecutionStatus
    avg_fill_price: float | None
    total_commission: float | None
    realized_pnl: float | None
    error_text: str | None
