from dataclasses import dataclass
from enum import Enum


class SignalWindowMode(Enum):
    ROLLING = "ROLLING"
    GRID = "GRID"


class MarketRegimeFilterMode(Enum):
    OFF = "OFF"
    SOFT = "SOFT"
    HARD = "HARD"


@dataclass(frozen=True)
class SignalConfig:
    # Режим построения сигнальных окон.
    signal_window_mode: SignalWindowMode = SignalWindowMode.ROLLING

    # Максимально допустимый возраст последнего job-бара относительно текущего времени.
    # Важно: bar_time_ts — это время начала 5-секундного бара, а не время его закрытия.
    # Поэтому значение должно включать размер бара + запас на приход BID/ASK,
    # запись price DB, обновление job DB и polling signal-сервиса.
    # Другими словами минимальный max_job_bar_lag_seconds = 5 секунд это размер бара + пару секунд на обработку сигнала

    max_job_bar_lag_seconds: int = 15

    # ROLLING-режим.
    rolling_signal_step_seconds: int = 60  # считаем сигнал не на каждом баре, а через это кол-во секунд
    rolling_back_minutes: int = 90  # Смотрим на столько минут назад
    rolling_trade_minutes: int = 30  # Длительность сделки по времени

    # GRID-режим.
    slot_signal_step_seconds: int = 5  # Считаем сигнал на каждом баре в 5 секунд
    slot_step_minutes: int = 60  # Размер слота внутри которого торгуем. Слоты не накладываются друг на друга
    slot_start_minute_of_day: int = 0  # Смещение слота от начала дня. Если хочется торговать не 30-60 минут, а 0-30
    slot_back_minutes: int = 30  # Смотрим на столько минут назад
    slot_entry_minutes: int = 20  # Вход возможно только в первые 20 минут после анализа
    slot_close_before_end_seconds: int = 10  # Закрываем сделку за 10

    # Поиск кандидатов по Pearson.
    price_source: str = "mid_close"  #
    pearson_min: float = 0.7  # минимальный пирсон
    min_candidates: int = 10  # мин и макс сколько нужно кандидатов выше pearson_min
    max_candidates: int = 100  #
    history_lookback_days: int | None = 180  # None = вся доступная история

    # Фильтр кандидатов по market-regime.
    # OFF: только Pearson.
    # SOFT: Pearson + совпадение relation price-regression vs SMA 600 regression.
    # HARD: SOFT + совпадение направлений price-regression и SMA 600 regression.
    # Если текущий relation = mixed_sma, SOFT/HARD пока пропускают расчёт сигнала.
    market_regime_filter_mode: MarketRegimeFilterMode = MarketRegimeFilterMode.HARD

    # Жёсткий отсев кандидатов по размаху движения pattern-window.
    # Сравнение идёт по minmax_bps: max(current_minmax, candidate_minmax) / min(...).
    # Если значение <= 0, фильтр выключен.
    candidate_minmax_hard_filter_max_ratio: float = 1.5

    # Веса компонентов итогового score кандидата.
    # Если вес <= 0, соответствующий компонент не участвует в score.
    candidate_score_pearson_weight: float = 1.0
    candidate_score_end_delta_weight: float = 1.0
    candidate_score_minmax_weight: float = 1.0


DEFAULT_SIGNAL_CONFIG = SignalConfig()
