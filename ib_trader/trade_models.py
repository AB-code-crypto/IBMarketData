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

    # Уже готовая market-интерпретация из signal_events.
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
class MarketFeatureSnapshot:
    instrument_code: str
    signal_bar_ts: int
    feature_bar_ts: int | None
    regime: int | None
    ma_zone: int | None


@dataclass(frozen=True)
class PositionSnapshot:
    instrument_code: str
    side: PositionSide
    quantity: float


@dataclass(frozen=True)
class TradeDecision:
    source_signal_id: int
    instrument_code: str

    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    signal_time_msk: str

    signal_direction: str
    entry_price: float

    best_pearson: float
    candidate_score_best: float | None

    potential_end_delta_points: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float
    potential_used: int

    regime: int | None
    ma_zone: int | None
    signal_strength: str

    order_type: str
    order_policy_reason: str
    limit_offset_points: float | None
    limit_price: float | None
    ttl_seconds: int | None
    rules_json: str

    action: TradeDecisionAction
    reason: str

    position_before_side: PositionSide
    position_before_qty: float

    position_after_side: PositionSide
    position_after_qty: float
