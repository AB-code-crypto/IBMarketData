from dataclasses import dataclass
from datetime import datetime, timezone

from core.time_utils import build_bar_time_fields_from_utc_dt
from ib_signal.signal_config import SignalConfig, SignalWindowMode
from ib_signal.signal_schedule import get_grid_slot_start_ts

SECONDS_PER_MINUTE = 60


def format_minutes_for_log(seconds: int) -> str:
    """Что делает: форматирует длительность в минутах для логов.
    Зачем нужна: pattern/trade в конфигах задаются в минутах, поэтому секунды в runtime-логах читать неудобно."""
    minutes = seconds / SECONDS_PER_MINUTE

    if minutes.is_integer():
        return str(int(minutes))

    return f"{minutes:.2f}"


@dataclass(frozen=True)
class SignalWindow:
    # Точка принятия решения: последний закрытый job-бар, для которого считаем сигнал.
    signal_bar_ts: int

    # Окно паттерна, который сравниваем с историческими кандидатами.
    pattern_start_ts: int
    pattern_end_ts: int
    pattern_seconds: int

    # Будущее/торговое окно, которое потом будем использовать для оценки кандидатов.
    trade_start_ts: int
    trade_end_ts: int
    trade_seconds: int

    # GRID-служебные поля. Для ROLLING они остаются None.
    slot_start_ts: int | None = None
    slot_offset_seconds: int | None = None


def validate_signal_window(window: SignalWindow) -> None:
    """Что делает: проверяет базовую целостность рассчитанного окна сигнала.
    Зачем нужна: некорректная комбинация настроек должна падать до сборки NumPy-матриц."""
    if window.pattern_seconds <= 0:
        raise ValueError(
            f"Длина pattern window должна быть > 0, получено: {window.pattern_seconds}"
        )

    if window.trade_seconds <= 0:
        raise ValueError(
            f"Длина trade window должна быть > 0, получено: {window.trade_seconds}"
        )

    if window.pattern_end_ts <= window.pattern_start_ts:
        raise ValueError(
            "pattern_end_ts должен быть больше pattern_start_ts: "
            f"{window.pattern_start_ts} -> {window.pattern_end_ts}"
        )

    if window.trade_end_ts <= window.trade_start_ts:
        raise ValueError(
            "trade_end_ts должен быть больше trade_start_ts: "
            f"{window.trade_start_ts} -> {window.trade_end_ts}"
        )


def build_rolling_signal_window(
    *,
    signal_bar_ts: int,
    settings: SignalConfig,
) -> SignalWindow:
    """Что делает: строит ROLLING-окно анализа и future/trade-окно от signal_bar_ts.
    Зачем нужна: ROLLING-кандидаты должны иметь ту же длину паттерна и ту же точку принятия решения."""
    pattern_seconds = settings.rolling_back_minutes * SECONDS_PER_MINUTE
    trade_seconds = settings.rolling_trade_minutes * SECONDS_PER_MINUTE

    window = SignalWindow(
        signal_bar_ts=signal_bar_ts,
        pattern_start_ts=signal_bar_ts - pattern_seconds,
        pattern_end_ts=signal_bar_ts,
        pattern_seconds=pattern_seconds,
        trade_start_ts=signal_bar_ts,
        trade_end_ts=signal_bar_ts + trade_seconds,
        trade_seconds=trade_seconds,
    )

    validate_signal_window(window)
    return window


def build_grid_signal_window(
    *,
    signal_bar_ts: int,
    settings: SignalConfig,
) -> SignalWindow:
    """Что делает: строит GRID-окно анализа от старта слота до signal_bar_ts и future/trade-окно до конца слота.
    Зачем нужна: GRID-кандидаты должны иметь такой же offset внутри своего исторического слота."""
    slot_start_ts = get_grid_slot_start_ts(
        current_bar_ts=signal_bar_ts,
        slot_step_minutes=settings.slot_step_minutes,
        slot_start_minute_of_day=settings.slot_start_minute_of_day,
    )

    slot_step_seconds = settings.slot_step_minutes * SECONDS_PER_MINUTE
    slot_end_ts = slot_start_ts + slot_step_seconds
    trade_end_ts = slot_end_ts - settings.slot_close_before_end_seconds

    pattern_seconds = signal_bar_ts - slot_start_ts
    trade_seconds = trade_end_ts - signal_bar_ts
    slot_offset_seconds = signal_bar_ts - slot_start_ts

    window = SignalWindow(
        signal_bar_ts=signal_bar_ts,
        pattern_start_ts=slot_start_ts,
        pattern_end_ts=signal_bar_ts,
        pattern_seconds=pattern_seconds,
        trade_start_ts=signal_bar_ts,
        trade_end_ts=trade_end_ts,
        trade_seconds=trade_seconds,
        slot_start_ts=slot_start_ts,
        slot_offset_seconds=slot_offset_seconds,
    )

    validate_signal_window(window)
    return window


def build_current_signal_window(
    *,
    signal_bar_ts: int,
    settings: SignalConfig,
) -> SignalWindow:
    """Что делает: выбирает построение окна по активному режиму ROLLING или GRID.
    Зачем нужна: signal-runner и будущий candidate search получают один объект с границами расчёта."""
    if settings.signal_window_mode == SignalWindowMode.ROLLING:
        return build_rolling_signal_window(
            signal_bar_ts=signal_bar_ts,
            settings=settings,
        )

    if settings.signal_window_mode == SignalWindowMode.GRID:
        return build_grid_signal_window(
            signal_bar_ts=signal_bar_ts,
            settings=settings,
        )

    raise ValueError(
        f"Неизвестный режим signal_window_mode: {settings.signal_window_mode!r}"
    )


def format_ts_ct_for_log(ts: int, get_time_text) -> str:
    """Что делает: возвращает CT-время для лога, сначала из job DB, а если бара в DB нет — рассчитывает CT-текст по timestamp.
    Зачем нужна: pattern-границы обычно есть в DB, а future trade_end может быть в будущем и ещё отсутствовать в job DB."""
    time_text = get_time_text(ts)
    if time_text is not None:
        return f"{time_text} CT"

    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    return f"{build_bar_time_fields_from_utc_dt(dt_utc)['bar_time_ct']} CT"


def format_signal_window_for_log(window: SignalWindow, get_time_text) -> str:
    """Что делает: форматирует SignalWindow в человекочитаемую строку с CT-временем.
    Зачем нужна: runtime-логи signal-сервиса должны читаться глазами, без Unix timestamp."""
    parts = [
        f"signal_bar={format_ts_ct_for_log(window.signal_bar_ts, get_time_text)}",
        (
            "pattern="
            f"{format_ts_ct_for_log(window.pattern_start_ts, get_time_text)}"
            " -> "
            f"{format_ts_ct_for_log(window.pattern_end_ts, get_time_text)}"
        ),
        f"pattern_minutes={format_minutes_for_log(window.pattern_seconds)}",
        (
            "trade="
            f"{format_ts_ct_for_log(window.trade_start_ts, get_time_text)}"
            " -> "
            f"{format_ts_ct_for_log(window.trade_end_ts, get_time_text)}"
        ),
        f"trade_minutes={format_minutes_for_log(window.trade_seconds)}",
    ]

    if window.slot_start_ts is not None:
        parts.append(f"slot_start={format_ts_ct_for_log(window.slot_start_ts, get_time_text)}")

    if window.slot_offset_seconds is not None:
        parts.append(f"slot_offset_minutes={format_minutes_for_log(window.slot_offset_seconds)}")

    return ", ".join(parts)
