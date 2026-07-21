from dataclasses import dataclass, field
from enum import Enum


class TradeDecisionAction(Enum):
    OPEN_POSITION = "OPEN_POSITION"
    CLOSE_POSITION = "CLOSE_POSITION"
    REVERSE_POSITION = "REVERSE_POSITION"


class PositionSide(Enum):
    UNKNOWN = "UNKNOWN"
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass(frozen=True)
class TraderSignalEvent:
    source_signal_id: int
    instrument_code: str
    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    signal_time_msk: str
    direction: str
    entry_price: float
    potential_end_delta_points: float


@dataclass(frozen=True)
class PositionSnapshot:
    instrument_code: str
    side: PositionSide
    quantity: float
    broker_contract: str | None = None
    broker_con_id: int | None = None
    broker_account: str | None = None
    contract_is_active: bool | None = None


@dataclass(frozen=True)
class TradeIntentCreated:
    trade_intent_id: int
    source_signal_id: int
    instrument_code: str
    signal_bar_ts: int
    signal_time_ct: str | None
    intent_source: str
    action: TradeDecisionAction
    reason: str
    signal_direction: str
    entry_price: float
    potential_end_delta_points: float
    order_type: str
    position_before_side: PositionSide
    position_before_qty: float
    position_after_side: PositionSide
    position_after_qty: float


@dataclass(frozen=True)
class PositionSnapshotFreshness:
    instrument_code: str
    updated_at_ts: int | None
    updated_at_utc: str | None
    age_seconds: int | None
    max_age_seconds: int
    is_stale: bool


@dataclass(frozen=True)
class TradeIntentRejected:
    instrument_code: str
    source_signal_id: int
    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    signal_time_msk: str
    reason: str
    action: TradeDecisionAction
    signal_direction: str
    order_type: str
    position_before_side: PositionSide
    position_before_qty: float
    positions_latest_updated_at_ts: int | None
    positions_latest_updated_at_utc: str | None
    positions_latest_age_seconds: int | None
    max_allowed_age_seconds: int


@dataclass(frozen=True)
class TradeProcessResult:
    created: list[TradeIntentCreated]
    rejected: list[TradeIntentRejected]
    guard_warnings: list[str] = field(default_factory=list)
