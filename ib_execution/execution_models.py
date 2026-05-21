from dataclasses import dataclass
from enum import Enum


class ExecutionStatus(Enum):
    NEW = "NEW"
    SENDING = "SENDING"
    ACCEPTED = "ACCEPTED"
    EXECUTED = "EXECUTED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class TradeIntent:
    trade_intent_id: int
    decision_id: int
    source_signal_id: int
    instrument_code: str

    action: str
    target_side: str
    target_qty: float

    position_before_side: str
    position_before_qty: float

    status: str
    created_at_ts: int


@dataclass(frozen=True)
class ExecutionOrderResult:
    trade_intent_id: int
    order_id: int | None
    order_action: str
    order_quantity: int
    status: ExecutionStatus
    avg_fill_price: float | None
    error_text: str | None
