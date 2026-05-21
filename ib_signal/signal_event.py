from dataclasses import dataclass, fields
from datetime import datetime, timezone
from enum import Enum
import json
import time
from typing import Any

from ib_signal.signal_config import SignalConfig


@dataclass(frozen=True)
class SignalEvent:
    """Что делает: хранит нормализованный actionable-сигнал, найденный ib_signal.
    Зачем нужна: downstream-фильтры читают единый контракт из state DB, а не внутренности signal-pipeline."""
    signal_id: int | None

    instrument_code: str

    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    created_at_ts: int

    direction: str
    entry_price: float

    signal_window_mode: str
    market_regime_filter_mode: str

    best_pearson: float
    candidate_score_best: float | None

    potential_end_delta_points: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float
    potential_used: int

    settings_json: str


def signal_config_to_dict(settings: SignalConfig) -> dict[str, Any]:
    """Что делает: превращает SignalConfig в JSON-safe dict.
    Зачем нужна: signal_events хранит snapshot настроек, но Enum нельзя напрямую писать в JSON."""
    result: dict[str, Any] = {}

    for field in fields(settings):
        value = getattr(settings, field.name)

        if isinstance(value, Enum):
            result[field.name] = value.value
        else:
            result[field.name] = value

    return result


def signal_config_to_json(settings: SignalConfig) -> str:
    """Что делает: сериализует SignalConfig в стабильный JSON.
    Зачем нужна: из settings_json можно восстановить режим и параметры, с которыми был найден сигнал."""
    return json.dumps(
        signal_config_to_dict(settings),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def format_signal_time_utc(signal_bar_ts: int) -> str:
    """Что делает: форматирует timestamp сигнала в UTC ISO-строку.
    Зачем нужна: в signal_events храним и машинный ts, и читаемое UTC-время сигнала."""
    return datetime.fromtimestamp(int(signal_bar_ts), tz=timezone.utc).isoformat()


def build_signal_event(
        *,
        instrument_code: str,
        signal_bar_ts: int,
        signal_time_ct: str | None,
        direction: str,
        entry_price: float,
        settings: SignalConfig,
        best_pearson: float,
        candidate_score_best: float | None,
        potential_end_delta_points: float,
        potential_max_profit_points: float,
        potential_max_drawdown_points: float,
        potential_used: int,
        created_at_ts: int | None = None,
) -> SignalEvent:
    """Что делает: собирает SignalEvent из итогов signal-pipeline.
    Зачем нужна: signal_runner пишет в state DB только actionable LONG/SHORT события."""
    direction_value = str(direction).upper()
    if direction_value not in {"LONG", "SHORT"}:
        raise ValueError(f"SignalEvent direction должен быть LONG/SHORT, получено: {direction!r}")

    return SignalEvent(
        signal_id=None,
        instrument_code=str(instrument_code),
        signal_bar_ts=int(signal_bar_ts),
        signal_time_utc=format_signal_time_utc(signal_bar_ts),
        signal_time_ct=None if signal_time_ct is None else str(signal_time_ct),
        created_at_ts=int(time.time() if created_at_ts is None else created_at_ts),
        direction=direction_value,
        entry_price=float(entry_price),
        signal_window_mode=settings.signal_window_mode.value,
        market_regime_filter_mode=settings.market_regime_filter_mode.value,
        best_pearson=float(best_pearson),
        candidate_score_best=(
            None
            if candidate_score_best is None
            else float(candidate_score_best)
        ),
        potential_end_delta_points=float(potential_end_delta_points),
        potential_max_profit_points=float(potential_max_profit_points),
        potential_max_drawdown_points=float(potential_max_drawdown_points),
        potential_used=int(potential_used),
        settings_json=signal_config_to_json(settings),
    )
