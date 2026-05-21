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
class FilteredSignalLatest:
    """Что делает: хранит свежий разрешённый сигнал из filtered_signal_latest.
    Зачем нужна: ib_trader принимает решения только по уже прошедшим фильтры сигналам."""
    instrument_code: str
    source_signal_id: int

    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    signal_time_msk: str

    direction: str


@dataclass(frozen=True)
class PositionSnapshot:
    """Что делает: хранит текущую позицию инструмента из trade.sqlite3.
    Зачем нужна: ib_trader должен знать, что позиция подтверждена, а не угадывать её."""
    instrument_code: str
    side: PositionSide
    quantity: float


@dataclass(frozen=True)
class TradeDecision:
    """Что делает: хранит итог решения ib_trader по одному filtered-сигналу.
    Зачем нужна: execution-слой должен получать уже не сигнал, а конкретное торговое намерение/отказ."""
    source_signal_id: int
    instrument_code: str

    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    signal_time_msk: str

    signal_direction: str

    action: TradeDecisionAction
    reason: str

    position_before_side: PositionSide
    position_before_qty: float

    position_after_side: PositionSide
    position_after_qty: float
