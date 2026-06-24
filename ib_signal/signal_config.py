from dataclasses import dataclass
from enum import Enum


class SignalWindowMode(Enum):
    ROLLING = "ROLLING"
    SLOT = "SLOT"


class MarketRegimeFilterMode(Enum):
    OFF = "OFF"
    SOFT = "SOFT"
    HARD = "HARD"

PLOT_TOP_CANDIDATES = 5  # сколько выводится кандидатов на PNG

@dataclass(frozen=True)
class SignalConfig:
    # Режим построения сигнальных окон.
    signal_window_mode: SignalWindowMode = SignalWindowMode.SLOT

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

    # SLOT-режим.
    slot_signal_step_seconds: int = 5  # Считаем сигнал на каждом баре в 5 секунд
    slot_step_minutes: int = 60  # Размер слота внутри которого торгуем. Слоты не накладываются друг на друга
    slot_start_minute_of_day: int = 0  # Смещение слота от начала дня. Если хочется торговать не 30-60 минут, а 0-30
    slot_back_minutes: int = 30  # Смотрим на столько минут назад
    slot_entry_minutes: int = 20  # Вход возможно только в первые 20 минут после анализа
    slot_close_before_end_seconds: int = 10  # Закрываем сделку за 10


    # SLOT loss extension: если slot-close пришёл в минусе и цена уже оттолкнулась
    # от максимальной просадки, даём позиции второй шанс до breakeven-buffer.
    # Execution-сервис владеет этой логикой и штатным slot-close, trader её не исполняет.
    slot_loss_extension_enabled: bool = True
    slot_loss_extension_profit_buffer_points: float = 2.0
    slot_loss_extension_min_drawdown_ratio: float = 0.70
    slot_loss_extension_max_drawdown_ratio: float = 0.95
    slot_loss_extension_deadline_minutes: int = 30

    # Safety guard для второго шанса.
    # order_accept_timeout: ждём, что extension TP/SL реально принят IB.
    # watchdog: если price DB показывает пробой SL/касание TP, а позиция всё ещё открыта,
    # execution закрывает позицию market, не ждёт deadline.
    slot_loss_extension_order_accept_timeout_seconds: float = 5.0
    slot_loss_extension_price_watchdog_enabled: bool = True
    slot_loss_extension_price_watchdog_stale_close_enabled: bool = True
    slot_loss_extension_price_stale_max_seconds: int = 15

    # Поиск кандидатов по Pearson.
    price_source: str = "mid_close"  #
    pearson_min: float = 0.7  # минимальный пирсон
    history_lookback_days: int | None = 365  # None = вся доступная история

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

    # Сколько top-score кандидатов использовать для расчёта potential path.
    # Если кандидатов меньше min_count, потенциал не считается.
    candidate_potential_min_count: int = 3
    candidate_potential_max_count: int = 5
    candidate_potential_min_abs_end_delta_points: float = 10.0

    # Сколько дней храним live signal_events в state DB.
    # Если значение <= 0, cleanup отключён.
    signal_event_retention_days: int = 7


DEFAULT_SIGNAL_CONFIG = SignalConfig()
