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
    """Что делает: хранит свежий actionable-сигнал из signal_events.
    Зачем нужна: ib_trader теперь принимает решения напрямую по выходу ib_signal, без ib_signal_filters."""
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


@dataclass(frozen=True)
class MarketFeatureSnapshot:
    """Что делает: хранит job-data признаки для бара сигнала.
    Зачем нужна: ib_trader принимает решение с учётом режима и зоны цены."""
    instrument_code: str
    signal_bar_ts: int
    feature_bar_ts: int | None
    regime: int | None
    ma_zone: int | None


@dataclass(frozen=True)
class PositionSnapshot:
    """Что делает: хранит текущую позицию инструмента из trade.sqlite3.
    Зачем нужна: ib_trader должен знать, что позиция подтверждена, а не угадывать её."""
    instrument_code: str
    side: PositionSide
    quantity: float


@dataclass(frozen=True)
class TradeDecision:
    """Что делает: хранит итог решения ib_trader по одному signal_event.
    Зачем нужна: execution-слой должен получать уже не сигнал, а конкретное торговое намерение/отказ."""
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

    action: TradeDecisionAction
    reason: str

    position_before_side: PositionSide
    position_before_qty: float

    position_after_side: PositionSide
    position_after_qty: float
