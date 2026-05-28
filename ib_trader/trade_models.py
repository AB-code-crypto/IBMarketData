from dataclasses import dataclass
from enum import Enum


class TradeDecisionAction(Enum):
    NO_ACTION = "NO_ACTION"
    OPEN_POSITION = "OPEN_POSITION"
    CLOSE_POSITION = "CLOSE_POSITION"
    REDUCE_POSITION = "REDUCE_POSITION"
    REVERSE_POSITION = "REVERSE_POSITION"
    ADD_TO_POSITION = "ADD_TO_POSITION"


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

    best_pearson: float
    candidate_score_best: float | None

    potential_end_delta_points: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float
    potential_used: int

    feature_bar_ts: int | None = None
    regime: int | None = None
    ma_zone: int | None = None

    signal_allowed: bool = True
    signal_reject_reason: str | None = None

    signal_strength: str = "NEUTRAL"

    order_type: str = "MARKET"
    order_policy_reason: str = "default_market"
    limit_offset_points: float | None = None
    limit_price: float | None = None
    ttl_seconds: int | None = None

    signal_rules_json: str = "[]"


@dataclass(frozen=True)
class PositionSnapshot:
    instrument_code: str
    side: PositionSide
    quantity: float


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

    regime: int | None
    ma_zone: int | None
    signal_strength: str

    order_type: str
    limit_price: float | None
    limit_offset_points: float | None
    ttl_seconds: int | None

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

