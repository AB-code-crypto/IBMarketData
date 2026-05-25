from dataclasses import dataclass


@dataclass(frozen=True)
class SignalRuleEvent:
    """Raw-сигнал, достаточный для рыночной интерпретации."""
    instrument_code: str
    signal_bar_ts: int
    signal_time_ct: str | None

    direction: str
    entry_price: float

    best_pearson: float
    candidate_score_best: float | None

    potential_end_delta_points: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float
    potential_used: int


@dataclass(frozen=True)
class SignalMarketFeatureSnapshot:
    """Снимок market-features из job DB на момент сигнала."""
    instrument_code: str
    signal_bar_ts: int
    feature_bar_ts: int | None
    regime: int | None
    ma_zone: int | None


@dataclass(frozen=True)
class SignalRuleEvaluation:
    """Итог рыночной интерпретации сигнала."""
    allowed: bool
    reject_reasons: list[str]

    signal_strength: str

    order_type: str
    order_policy_reason: str
    limit_offset_points: float | None
    ttl_seconds: int | None

    rules_json: str
