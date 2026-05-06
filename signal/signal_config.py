"""
Конфигурация IBSignal.

Здесь задаются только базовые параметры первой версии сигнального робота:
- инструмент;
- окно анализа;
- окно торговли / прогнозного движения;
- шаг принятия решения;
- параметры поиска исторических кандидатов по Pearson.

Фильтры рыночного режима, MA, волатильности, времени суток и прочие
условия отбора кандидатов будут добавляться позже отдельными настройками.
"""

# ============================================================
# ИНСТРУМЕНТ
# ============================================================

# Логический инструмент из contracts.py.
# Для фьючерсов указываем именно общий инструмент, например "MNQ" или "NQ",
# а не конкретный квартальный контракт.
TARGET = "MNQ"


# ============================================================
# ОКНА АНАЛИЗА И ТОРГОВЛИ
# ============================================================

# Сколько минут берём назад от момента принятия решения для поиска похожего паттерна.
ANALYSIS_WINDOW_MINUTES = 30

# На сколько минут вперёд смотрим после найденного исторического кандидата.
# Это же окно в будущем будет использоваться торговым ботом как базовая
# длительность сделки/сигнала.
TRADE_WINDOW_MINUTES = 30

# Как часто IBSignal принимает новое решение.
# 60 секунд означает: один расчёт в начале каждой минуты.
DECISION_STEP_SECONDS = 60

# Небольшой запас по времени, чтобы не использовать последние ещё неустойчивые бары.
# Например, если БД пишется 5-секундными барами, то 5-10 секунд запаса помогают
# не попасть в ситуацию, когда последний бар ещё не дописан.
LAST_BAR_SAFETY_SECONDS = 10


# ============================================================
# ПОИСК КАНДИДАТОВ ПО PEARSON
# ============================================================

# Источник цены для построения паттерна.
# На первом этапе используем mid close:
#   mid_close = (bid_close + ask_close) / 2
PRICE_SOURCE = "mid_close"

# Минимально допустимое значение Pearson correlation.
# Кандидаты ниже этого порога не участвуют в прогнозе.
PEARSON_MIN = 0.85

# Минимальное количество кандидатов, необходимое для того,
# чтобы сигнал можно было считать статистически пригодным.
MIN_CANDIDATES = 10

# Максимальное количество лучших кандидатов, которое оставляем после сортировки.
MAX_CANDIDATES = 100

# Шаг скользящего окна при переборе исторических кандидатов.
# 5 секунд = максимально плотный поиск по исходным 5-секундным барам.
# 60 секунд = быстрее и меньше почти дублирующихся кандидатов.
CANDIDATE_SEARCH_STEP_SECONDS = 60

# Глубина истории, по которой ищем похожие паттерны.
# Позже этот параметр можно будет оптимизировать отдельно по инструментам.
HISTORY_LOOKBACK_DAYS = 120

# Запас между концом forward-окна исторического кандидата и текущим decision_ts.
# Нужен для жёсткой защиты от подглядывания в будущее.
CANDIDATE_FUTURE_SAFETY_SECONDS = 60


# ============================================================
# ПРОИЗВОДНЫЕ НАСТРОЙКИ
# ============================================================

ANALYSIS_WINDOW_SECONDS = ANALYSIS_WINDOW_MINUTES * 60
TRADE_WINDOW_SECONDS = TRADE_WINDOW_MINUTES * 60
HISTORY_LOOKBACK_SECONDS = HISTORY_LOOKBACK_DAYS * 24 * 60 * 60


# ============================================================
# ВАЛИДАЦИЯ
# ============================================================


def validate_signal_config() -> None:
    # Проверяет настройки IBSignal до старта основного цикла.
    if not str(TARGET).strip():
        raise ValueError("TARGET должен быть задан")

    if ANALYSIS_WINDOW_SECONDS <= 0:
        raise ValueError("ANALYSIS_WINDOW_SECONDS должен быть больше 0")

    if TRADE_WINDOW_SECONDS <= 0:
        raise ValueError("TRADE_WINDOW_SECONDS должен быть больше 0")

    if ANALYSIS_WINDOW_SECONDS < TRADE_WINDOW_SECONDS:
        raise ValueError(
            "ANALYSIS_WINDOW_SECONDS должен быть больше или равен "
            "TRADE_WINDOW_SECONDS"
        )

    if DECISION_STEP_SECONDS <= 0:
        raise ValueError("DECISION_STEP_SECONDS должен быть больше 0")

    if LAST_BAR_SAFETY_SECONDS < 0:
        raise ValueError("LAST_BAR_SAFETY_SECONDS не может быть меньше 0")

    if PRICE_SOURCE not in {"mid_close"}:
        raise ValueError(f"Неподдерживаемый PRICE_SOURCE: {PRICE_SOURCE}")

    if not -1.0 <= PEARSON_MIN <= 1.0:
        raise ValueError("PEARSON_MIN должен быть в диапазоне [-1.0, 1.0]")

    if MIN_CANDIDATES <= 0:
        raise ValueError("MIN_CANDIDATES должен быть больше 0")

    if MAX_CANDIDATES < MIN_CANDIDATES:
        raise ValueError("MAX_CANDIDATES должен быть >= MIN_CANDIDATES")

    if CANDIDATE_SEARCH_STEP_SECONDS <= 0:
        raise ValueError("CANDIDATE_SEARCH_STEP_SECONDS должен быть больше 0")

    if HISTORY_LOOKBACK_SECONDS <= ANALYSIS_WINDOW_SECONDS + TRADE_WINDOW_SECONDS:
        raise ValueError(
            "HISTORY_LOOKBACK_SECONDS должен быть больше суммы "
            "ANALYSIS_WINDOW_SECONDS и TRADE_WINDOW_SECONDS"
        )

    if CANDIDATE_FUTURE_SAFETY_SECONDS < 0:
        raise ValueError("CANDIDATE_FUTURE_SAFETY_SECONDS не может быть меньше 0")


def get_signal_config_snapshot() -> dict:
    # Возвращает снимок настроек для логирования и записи в debug-таблицы.
    return {
        "target": TARGET,
        "analysis_window_minutes": ANALYSIS_WINDOW_MINUTES,
        "analysis_window_seconds": ANALYSIS_WINDOW_SECONDS,
        "trade_window_minutes": TRADE_WINDOW_MINUTES,
        "trade_window_seconds": TRADE_WINDOW_SECONDS,
        "decision_step_seconds": DECISION_STEP_SECONDS,
        "last_bar_safety_seconds": LAST_BAR_SAFETY_SECONDS,
        "price_source": PRICE_SOURCE,
        "pearson_min": PEARSON_MIN,
        "min_candidates": MIN_CANDIDATES,
        "max_candidates": MAX_CANDIDATES,
        "candidate_search_step_seconds": CANDIDATE_SEARCH_STEP_SECONDS,
        "history_lookback_days": HISTORY_LOOKBACK_DAYS,
        "history_lookback_seconds": HISTORY_LOOKBACK_SECONDS,
        "candidate_future_safety_seconds": CANDIDATE_FUTURE_SAFETY_SECONDS,
    }
