from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class SignalVariant:
    rolling_back_minutes: int
    rolling_trade_minutes: int
    pearson_min: float
    minmax_hard_filter_max_ratio: float
    candidate_min_count: int
    candidate_max_count: int
    potential_min_abs_end_delta_points: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionVariant:
    delay_seconds: int
    take_profit_points: float
    stop_loss_points: float
    daily_take_profit_usd: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TesterSignal:
    signal_bar_ts: int
    signal_time_msk: str
    signal_time_ct: str
    direction: str
    reference_price: float
    best_pearson: float
    best_candidate_score: float | None
    potential_end_delta_points: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float
    potential_used: int
    raw_candidates_count: int
    valid_candidates_count: int
    pearson_passed_count: int
    minmax_passed_count: int


@dataclass(frozen=True)
class SignalBatchResult:
    signals: list[TesterSignal]
    calculation_points: int
    skipped_points: int
    no_signal_points: int


@dataclass(frozen=True)
class PriceBar:
    bar_time_ts: int
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float


@dataclass(frozen=True)
class CompletedTrade:
    direction: str
    entry_ts: int
    exit_ts: int
    entry_time_msk: str
    exit_time_msk: str
    entry_price: float
    exit_price: float
    entry_signal_bar_ts: int
    exit_signal_bar_ts: int | None
    exit_reason: str
    gross_points: float
    gross_pnl_usd: float
    entry_commission_usd: float
    exit_commission_usd: float
    net_pnl_usd: float
    mfe_points: float
    mae_points: float
    holding_seconds: int


@dataclass(frozen=True)
class DailyResult:
    moscow_day: str
    net_realized_pnl_usd: float
    commission_usd: float
    closed_trades_count: int
    executed_signals_count: int
    daily_take_profit_triggered: bool


@dataclass(frozen=True)
class SimulationResult:
    trades: list[CompletedTrade]
    daily_results: list[DailyResult]
    metrics: dict[str, Any]
