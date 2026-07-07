"""
Разовый инструмент проверки и ремонта внутренних дырок в price DB.

Назначение
----------
Скрипт нужен не для штатной работы робота, а для ручной сервисной проверки БД:

- найти строки с неполными BID/ASK-ценами;
- найти внутренние пропуски между соседними 5-секундными барами;
- показать отчёт по проблемным интервалам;
- при необходимости докачать найденные интервалы через Interactive Brokers.

Главная идея настроек
---------------------
В скрипте есть один целевой инструмент TARGET и одна стартовая дата CHECK_FROM_UTC.

TARGET:
    - для FUT указываем логический инструмент, например "MNQ", "NQ", "MES", "ES";
    - для CASH/CRYPTO также указываем логический инструмент, например "EURUSD" или "BTCUSD".

CHECK_FROM_UTC:
    - дата, с которой сканируем БД;
    - эта же дата является левой границей ремонта найденных проблем.

Для фьючерсов БД уже содержит сшитую непрерывную историю инструмента.
Скрипт сканирует всю таблицу инструмента целиком, а найденные ремонтные
интервалы сам раскладывает на конкретные квартальные контракты по active_from_utc /
active_to_utc из contracts.py.

Режимы
------
MODE = "SCAN"
    Только найти проблемные интервалы и напечатать отчёт.
    IB-соединение не открывается, БД не меняется.

MODE = "REPAIR"
    Найти проблемные интервалы и докачать их через IB.
    В этом режиме скрипт подключается к IB и пишет данные в price DB.

Важные ограничения
------------------
1. Скрипт проверяет внутренние дырки по уже имеющимся строкам БД.
   Начало/конец всей истории лучше закрывает основной history-loader.

2. Если CHECK_FROM_UTC задан раньше первой найденной строки по инструменту,
   скрипт считает разрыв от CHECK_FROM_UTC до первой строки потенциальной дыркой,
   если этот интервал не попадает в ожидаемую рыночную паузу.

3. Для рыночных пауз используется консервативная фильтрация:
   - для CME equity futures учитываются типовые maintenance/weekend/holiday gaps;
   - для FX пропускаются суббота и раннее воскресенье UTC;
   - crypto считается 24/7.

4. Если BID/ASK по инструменту не отдаётся IB вообще, repair не сможет заполнить дырку.
   В этом случае скрипт покажет попытку ремонта, но rows=0 или ошибку historical request.

5. Скрипт не удаляет плохие строки. Он только UPSERT-ит заново загруженные BID/ASK-бары.
"""

import asyncio
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

# Чтобы скрипт можно было запускать как:
# python scripts/price_db_repair.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings_live as settings
from contracts import Instrument
from core.contract_utils import (
    build_instrument_contract,
    get_contract_row_by_local_symbol,
    get_contract_storage_name,
)
from core.history_segment_loader import load_quotes_segment
from core.ib_connector import connect_ib, disconnect_ib
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.logger import get_logger, log_warning, setup_logging
from core.market_sessions import should_load_history_chunk
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import format_utc_ts, parse_utc_iso_to_ts

# ============================================================
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

# Режимы:
# - "SCAN"   -> только найти проблемные интервалы;
# - "REPAIR" -> найти проблемные интервалы и докачать их.
MODE = "REPAIR"

# Логический инструмент, который проверяем.
# Для фьючерсов указываем именно инструмент целиком, а не localSymbol контракта.
TARGET = "MNQ"

# С какой даты проверяем и, если включён REPAIR, с какой даты исправляем.
# Форматы:
#   "YYYY-MM-DD"
#   "YYYY-MM-DD HH:MM:SS"
CHECK_FROM_UTC = "2026-06-01 00:00:00"

# Ожидаемый шаг 5-секундных баров.
EXPECTED_STEP_SECONDS = 5

# Размер одного repair-запроса в IB.
# Даже если дырка занимает 5-10 секунд, запрашиваем час вперёд от начала дырки.
# Пример: hole=17:16:30 -> 17:16:40, load=17:16:30 -> 18:16:30.
REPAIR_LOAD_CHUNK_SECONDS = 3600

# Максимальное количество интервалов, которые реально будем ремонтировать за один запуск.
# None — без ограничения.
MAX_REPAIR_INTERVALS = None

# Печатать ли найденные интервалы.
PRINT_INTERVALS = True

# Если True — пытаемся не считать дырками известные неторговые окна.
IGNORE_EXPECTED_MARKET_PAUSES = True

setup_logging()
logger = get_logger(__name__)

PRICE_COLUMNS = [
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
]

# ============================================================
# ПРАВИЛА ШТАТНЫХ ПАУЗ CME EQUITY INDEX FUTURES
# ============================================================
# Эти правила нужны только для scan-части.
# Они защищают от попыток ремонтировать ежедневный maintenance, выходные
# и типовые ранние закрытия, которые в БД естественно выглядят как разрыв.

CME_IGNORED_GAP_RULES = [
    {
        "name": "daily_clearing",
        "description": "Ежедневный клиринг CME",
        "missing_bars": {720},
        "start_hours_utc": {21, 22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "regular_weekend",
        "description": "Обычные выходные",
        "missing_bars": {35280},
        "start_hours_utc": {21, 22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "dst_spring_forward_weekend",
        "description": "Выходные с весенним переводом часов",
        "missing_bars": {34560},
        "start_hours_utc": {22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "dst_fall_back_weekend",
        "description": "Выходные с осенним переводом часов",
        "missing_bars": {36000},
        "start_hours_utc": {21},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "good_friday_plus_weekend",
        "description": "Good Friday + выходные",
        "missing_bars": {52560},
        "start_hours_utc": {21},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "us_holiday_midday_close",
        "description": "Праздничное закрытие с 12:00 CT до 17:00 CT",
        "missing_bars": {3600},
        "start_hours_utc": {17, 18},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "independence_day_eve_early_close",
        "description": "Раннее закрытие накануне Independence Day",
        "missing_bars": {3420},
        "start_hours_utc": {17},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "thanksgiving_friday_plus_weekend",
        "description": "Раннее закрытие после Thanksgiving + выходные",
        "missing_bars": {37980},
        "start_hours_utc": {18},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "christmas_eve_plus_christmas_day",
        "description": "Christmas Eve early close + Christmas Day",
        "missing_bars": {20700},
        "start_hours_utc": {18},
        "start_minutes_utc": {15},
        "start_seconds_utc": {0},
    },
    {
        "name": "new_year_eve_plus_new_year_day",
        "description": "New Year's Eve close + New Year's Day",
        "missing_bars": {18000},
        "start_hours_utc": {22},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "independence_day_plus_weekend",
        "description": "Independence Day + выходные",
        "missing_bars": {38160},
        "start_hours_utc": {17},
        "start_minutes_utc": {0},
        "start_seconds_utc": {0},
    },
    {
        "name": "national_day_of_mourning_2025_01_09",
        "description": "Национальный день траура 2025-01-09",
        "missing_bars": {6120},
        "start_hours_utc": {14},
        "start_minutes_utc": {30},
        "start_seconds_utc": {0},
        "start_dates_utc": {"2025-01-09"},
    },
]


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def parse_utc_datetime(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def parse_required_utc_text(value: str) -> int:
    value = str(value).strip()
    if not value:
        raise ValueError("CHECK_FROM_UTC должен быть задан")

    if len(value) == 10:
        dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    return int(parse_utc_datetime(value).timestamp())


def utc_text(ts: Optional[int]) -> str:
    if ts is None:
        return "-"
    return format_utc_ts(ts)


def get_instrument_row(instrument_code: str):
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент не найден в contracts.py: {instrument_code}")

    instrument_row = Instrument[instrument_code]

    if instrument_row["barSizeSetting"] != "5 secs":
        raise ValueError(
            f"Скрипт рассчитан только на 5-секундные данные. "
            f"Получено: {instrument_row['barSizeSetting']}"
        )

    return instrument_row


def resolve_target(target: str):
    # TARGET всегда является кодом логического инструмента.
    target = str(target).strip()
    if not target:
        raise ValueError("TARGET должен быть задан")

    return target, get_instrument_row(target)


def parse_contract_bounds(contract_row: dict) -> tuple[int, int]:
    return (
        parse_utc_iso_to_ts(contract_row["active_from_utc"]),
        parse_utc_iso_to_ts(contract_row["active_to_utc"]),
    )


def get_contract_row_for_storage_name(instrument_row, contract_name: str):
    if instrument_row["secType"] != "FUT":
        return None

    return get_contract_row_by_local_symbol(instrument_row, contract_name)


def get_repair_contract_context(instrument_code: str, instrument_row, contract_name: str):
    contract_row = get_contract_row_for_storage_name(instrument_row, contract_name)
    contract = build_instrument_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )
    storage_name = get_contract_storage_name(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )
    return contract, storage_name


def clamp_repair_load_bounds_to_contract(
        *,
        instrument_row,
        contract_name: str,
        load_start_ts: int,
        load_end_ts_exclusive: int,
) -> tuple[int, int]:
    """Что делает: зажимает repair-load интервал в active-границы фьючерсного контракта.
    Зачем нужна: часовой repair-запрос может выйти за active_to_utc конкретного квартального контракта."""
    if instrument_row["secType"] != "FUT":
        return load_start_ts, load_end_ts_exclusive

    contract_row = get_contract_row_for_storage_name(instrument_row, contract_name)

    if contract_row is None:
        raise ValueError(f"Не найден контракт {contract_name} в contracts.py")

    active_from_ts, active_to_ts = parse_contract_bounds(contract_row)

    return (
        max(load_start_ts, active_from_ts),
        min(load_end_ts_exclusive, active_to_ts),
    )


def build_repair_load_intervals(
        *,
        instrument_row,
        contract_name: str,
        hole_start_ts: int,
        hole_end_ts_exclusive: int,
) -> list[tuple[int, int]]:
    """Что делает: строит часовые repair-load интервалы от начала дырки.
    Зачем нужна: IB плохо отдаёт слишком короткие historical-запросы, поэтому маленькую дырку качаем часовым запросом.

    Пример:
        hole = 17:16:30 -> 17:16:40
        load = 17:16:30 -> 18:16:30

    Для длинной дырки:
        hole = 17:16:30 -> 19:20:00
        load #1 = 17:16:30 -> 18:16:30
        load #2 = 18:16:30 -> 19:16:30
        load #3 = 19:16:30 -> 20:16:30
    """
    if hole_end_ts_exclusive <= hole_start_ts:
        raise ValueError(
            f"Некорректный repair interval: "
            f"hole={utc_text(hole_start_ts)} -> {utc_text(hole_end_ts_exclusive)}"
        )

    load_intervals: list[tuple[int, int]] = []
    current_start_ts = int(hole_start_ts)

    while current_start_ts < hole_end_ts_exclusive:
        raw_load_start_ts = current_start_ts
        raw_load_end_ts = current_start_ts + REPAIR_LOAD_CHUNK_SECONDS

        load_start_ts, load_end_ts = clamp_repair_load_bounds_to_contract(
            instrument_row=instrument_row,
            contract_name=contract_name,
            load_start_ts=raw_load_start_ts,
            load_end_ts_exclusive=raw_load_end_ts,
        )

        if load_end_ts <= load_start_ts:
            raise ValueError(
                f"Пустой repair-load интервал после зажима в active-границы: "
                f"contract={contract_name}, "
                f"hole={utc_text(hole_start_ts)} -> {utc_text(hole_end_ts_exclusive)}, "
                f"raw_load={utc_text(raw_load_start_ts)} -> {utc_text(raw_load_end_ts)}, "
                f"load={utc_text(load_start_ts)} -> {utc_text(load_end_ts)}"
            )

        load_intervals.append((load_start_ts, load_end_ts))
        current_start_ts += REPAIR_LOAD_CHUNK_SECONDS

    return load_intervals


def ensure_table_exists(conn, db_path: str, table_name: str) -> None:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()

    if row is None:
        raise ValueError(f"Таблица {table_name!r} не найдена в БД {db_path}")


def get_missing_bars_count(gap_start_ts: int, gap_end_ts: int) -> int:
    missing_seconds = gap_end_ts - gap_start_ts + EXPECTED_STEP_SECONDS
    return missing_seconds // EXPECTED_STEP_SECONDS


def cme_gap_matches_rule(gap_start_ts: int, gap_end_ts: int, rule: dict) -> bool:
    missing_bars = get_missing_bars_count(gap_start_ts, gap_end_ts)

    if missing_bars not in rule["missing_bars"]:
        return False

    gap_start_dt = datetime.fromtimestamp(gap_start_ts, tz=timezone.utc)

    if gap_start_dt.hour not in rule["start_hours_utc"]:
        return False

    if gap_start_dt.minute not in rule["start_minutes_utc"]:
        return False

    if gap_start_dt.second not in rule["start_seconds_utc"]:
        return False

    if "start_dates_utc" in rule:
        start_date_text = gap_start_dt.strftime("%Y-%m-%d")
        if start_date_text not in rule["start_dates_utc"]:
            return False

    return True


def get_cme_ignored_rule_name(gap_start_ts: int, gap_end_ts: int) -> Optional[str]:
    for rule in CME_IGNORED_GAP_RULES:
        if cme_gap_matches_rule(gap_start_ts, gap_end_ts, rule):
            return rule["name"]

    return None


def range_contains_loadable_session(session_model: str, start_ts: int, end_ts_exclusive: int) -> bool:
    # Проверяем, есть ли внутри интервала хотя бы один часовой chunk,
    # который по session_model считается потенциально торговым.
    current = start_ts
    while current < end_ts_exclusive:
        chunk_end = min(current + 3600, end_ts_exclusive)
        if should_load_history_chunk(
                session_model=session_model,
                chunk_start_ts=current,
                chunk_end_ts=chunk_end,
        ):
            return True
        current = chunk_end

    return False


def should_ignore_gap(
        *,
        instrument_row,
        gap_start_ts: int,
        gap_end_ts_exclusive: int,
) -> bool:
    if not IGNORE_EXPECTED_MARKET_PAUSES:
        return False

    session_model = instrument_row["session_model"]

    if session_model == "CME_EQUITY_INDEX":
        ignored_rule_name = get_cme_ignored_rule_name(
            gap_start_ts=gap_start_ts,
            gap_end_ts=gap_end_ts_exclusive - EXPECTED_STEP_SECONDS,
        )
        return ignored_rule_name is not None

    return not range_contains_loadable_session(
        session_model=session_model,
        start_ts=gap_start_ts,
        end_ts_exclusive=gap_end_ts_exclusive,
    )


def split_interval_by_contracts(
        *,
        instrument_code: str,
        instrument_row,
        start_ts: int,
        end_ts_exclusive: int,
        source: str,
) -> list[dict]:
    # Разбивает найденную дырку логического инструмента на интервалы ремонта.
    # Для FUT интервал раскладывается по active_from_utc / active_to_utc контрактов.
    # Для CASH/CRYPTO всё ремонтируется как один single-contract инструмент.
    if end_ts_exclusive <= start_ts:
        return []

    if instrument_row["secType"] != "FUT":
        return [
            {
                "contract_name": instrument_code,
                "start_ts": start_ts,
                "end_ts_exclusive": end_ts_exclusive,
                "sources": {source},
            }
        ]

    intervals = []
    for contract_row in instrument_row["contracts"]:
        active_from_ts, active_to_ts = parse_contract_bounds(contract_row)
        repair_start_ts = max(start_ts, active_from_ts)
        repair_end_ts = min(end_ts_exclusive, active_to_ts)

        if repair_end_ts <= repair_start_ts:
            continue

        intervals.append(
            {
                "contract_name": contract_row["localSymbol"],
                "start_ts": repair_start_ts,
                "end_ts_exclusive": repair_end_ts,
                "sources": {source},
            }
        )

    if not intervals:
        log_warning(
            logger,
            f"Интервал {utc_text(start_ts)} -> {utc_text(end_ts_exclusive)} "
            f"не попал ни в одно active-окно contracts.py для {instrument_code}. Пропускаю.",
            to_telegram=False,
        )

    return intervals


# ============================================================
# ПОИСК ПРОБЛЕМНЫХ ИНТЕРВАЛОВ
# ============================================================

def fetch_problem_rows(
        conn,
        table_name: str,
        *,
        start_ts: int,
):
    null_condition = " OR ".join([f"{column} IS NULL" for column in PRICE_COLUMNS])

    sql = f"""
        SELECT
            bar_time_ts,
            contract
        FROM {table_name}
        WHERE bar_time_ts >= ?
          AND ({null_condition})
        ORDER BY bar_time_ts ASC
    """

    return conn.execute(sql, (start_ts,)).fetchall()


def build_null_intervals(problem_rows) -> list[dict]:
    intervals = []

    current_contract = None
    current_start_ts = None
    current_prev_ts = None

    for row in problem_rows:
        bar_time_ts = int(row["bar_time_ts"])
        contract_name = row["contract"]

        if current_start_ts is None:
            current_contract = contract_name
            current_start_ts = bar_time_ts
            current_prev_ts = bar_time_ts
            continue

        is_same_contract = contract_name == current_contract
        is_next_bar = bar_time_ts == current_prev_ts + EXPECTED_STEP_SECONDS

        if is_same_contract and is_next_bar:
            current_prev_ts = bar_time_ts
            continue

        intervals.append(
            {
                "contract_name": current_contract,
                "start_ts": current_start_ts,
                "end_ts_exclusive": current_prev_ts + EXPECTED_STEP_SECONDS,
                "sources": {"NULL_PRICE"},
            }
        )

        current_contract = contract_name
        current_start_ts = bar_time_ts
        current_prev_ts = bar_time_ts

    if current_start_ts is not None:
        intervals.append(
            {
                "contract_name": current_contract,
                "start_ts": current_start_ts,
                "end_ts_exclusive": current_prev_ts + EXPECTED_STEP_SECONDS,
                "sources": {"NULL_PRICE"},
            }
        )

    return intervals


def fetch_bars_for_gap_scan(
        conn,
        table_name: str,
        *,
        start_ts: int,
):
    sql = f"""
        SELECT
            bar_time_ts,
            contract
        FROM {table_name}
        WHERE bar_time_ts >= ?
        ORDER BY bar_time_ts ASC
    """

    return conn.execute(sql, (start_ts,)).fetchall()


def maybe_build_leading_gap(
        *,
        sorted_rows,
        instrument_code: str,
        instrument_row,
        start_ts: int,
) -> list[dict]:
    # Если первая строка в БД начинается позже CHECK_FROM_UTC,
    # считаем этот интервал потенциальной дыркой.
    if not sorted_rows:
        return []

    first_ts = int(sorted_rows[0]["bar_time_ts"])
    if first_ts <= start_ts:
        return []

    if should_ignore_gap(
            instrument_row=instrument_row,
            gap_start_ts=start_ts,
            gap_end_ts_exclusive=first_ts,
    ):
        return []

    return split_interval_by_contracts(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        start_ts=start_ts,
        end_ts_exclusive=first_ts,
        source="LEADING_GAP",
    )


def build_gap_intervals(sorted_rows, instrument_code: str, instrument_row, start_ts: int) -> list[dict]:
    intervals = []

    if not sorted_rows:
        return intervals

    intervals.extend(
        maybe_build_leading_gap(
            sorted_rows=sorted_rows,
            instrument_code=instrument_code,
            instrument_row=instrument_row,
            start_ts=start_ts,
        )
    )

    previous_row = sorted_rows[0]

    for current_row in sorted_rows[1:]:
        previous_ts = int(previous_row["bar_time_ts"])
        current_ts = int(current_row["bar_time_ts"])

        delta_seconds = current_ts - previous_ts

        if delta_seconds > EXPECTED_STEP_SECONDS:
            gap_start_ts = previous_ts + EXPECTED_STEP_SECONDS
            gap_end_ts_exclusive = current_ts

            if should_ignore_gap(
                    instrument_row=instrument_row,
                    gap_start_ts=gap_start_ts,
                    gap_end_ts_exclusive=gap_end_ts_exclusive,
            ):
                previous_row = current_row
                continue

            intervals.extend(
                split_interval_by_contracts(
                    instrument_code=instrument_code,
                    instrument_row=instrument_row,
                    start_ts=gap_start_ts,
                    end_ts_exclusive=gap_end_ts_exclusive,
                    source="GAP",
                )
            )

        elif delta_seconds < EXPECTED_STEP_SECONDS:
            log_warning(
                logger,
                "Найдена аномальная последовательность bar_time_ts: "
                f"prev_utc={utc_text(previous_ts)}, "
                f"curr_utc={utc_text(current_ts)}, "
                f"delta={delta_seconds} сек",
                to_telegram=False,
            )

        previous_row = current_row

    return intervals


def merge_intervals(intervals: Iterable[dict]) -> list[dict]:
    intervals = list(intervals)
    if not intervals:
        return []

    sorted_intervals = sorted(
        intervals,
        key=lambda item: (
            item["contract_name"],
            item["start_ts"],
            item["end_ts_exclusive"],
        ),
    )

    merged = [
        {
            "contract_name": sorted_intervals[0]["contract_name"],
            "start_ts": sorted_intervals[0]["start_ts"],
            "end_ts_exclusive": sorted_intervals[0]["end_ts_exclusive"],
            "sources": set(sorted_intervals[0]["sources"]),
        }
    ]

    for interval in sorted_intervals[1:]:
        last = merged[-1]

        same_contract = interval["contract_name"] == last["contract_name"]
        overlap_or_touch = interval["start_ts"] <= last["end_ts_exclusive"]

        if same_contract and overlap_or_touch:
            last["end_ts_exclusive"] = max(
                last["end_ts_exclusive"],
                interval["end_ts_exclusive"],
            )
            last["sources"].update(interval["sources"])
        else:
            merged.append(
                {
                    "contract_name": interval["contract_name"],
                    "start_ts": interval["start_ts"],
                    "end_ts_exclusive": interval["end_ts_exclusive"],
                    "sources": set(interval["sources"]),
                }
            )

    return merged


def find_repair_intervals(
        conn,
        table_name: str,
        instrument_code: str,
        instrument_row,
        *,
        start_ts: int,
) -> dict:
    problem_rows = fetch_problem_rows(
        conn=conn,
        table_name=table_name,
        start_ts=start_ts,
    )
    null_intervals = build_null_intervals(problem_rows)

    gap_rows = fetch_bars_for_gap_scan(
        conn=conn,
        table_name=table_name,
        start_ts=start_ts,
    )
    gap_intervals = build_gap_intervals(
        sorted_rows=gap_rows,
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        start_ts=start_ts,
    )

    merged_intervals = merge_intervals(null_intervals + gap_intervals)

    return {
        "rows_count": len(gap_rows),
        "problem_rows_count": len(problem_rows),
        "null_intervals": null_intervals,
        "gap_intervals": gap_intervals,
        "merged_intervals": merged_intervals,
    }


def print_intervals(intervals: list[dict]) -> None:
    if not intervals:
        print("Интервалов для ремонта не найдено.")
        return

    for index, interval in enumerate(intervals, start=1):
        bars_count = int(
            (interval["end_ts_exclusive"] - interval["start_ts"]) // EXPECTED_STEP_SECONDS
        )
        sources_text = ", ".join(sorted(interval["sources"]))

        print(
            f"[{index}] "
            f"contract={interval['contract_name']} | "
            f"{utc_text(interval['start_ts'])} -> "
            f"{utc_text(interval['end_ts_exclusive'])} | "
            f"баров={bars_count} | "
            f"sources={sources_text}"
        )


def limit_intervals(intervals: list[dict]) -> list[dict]:
    if MAX_REPAIR_INTERVALS is None:
        return intervals

    return intervals[:int(MAX_REPAIR_INTERVALS)]


# ============================================================
# РЕМОНТ
# ============================================================

async def repair_interval(
        ib,
        ib_health,
        settings,
        *,
        db_path: str,
        table_name: str,
        instrument_code: str,
        instrument_row,
        interval: dict,
) -> int:
    contract_name = interval["contract_name"]
    contract, storage_name = get_repair_contract_context(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_name=contract_name,
    )

    if storage_name != contract_name:
        raise RuntimeError(
            f"Внутренняя ошибка contract_name: interval={contract_name}, storage={storage_name}"
        )

    load_intervals = build_repair_load_intervals(
        instrument_row=instrument_row,
        contract_name=contract_name,
        hole_start_ts=interval["start_ts"],
        hole_end_ts_exclusive=interval["end_ts_exclusive"],
    )

    total_rows_written = 0

    for load_index, (load_start_ts, load_end_ts) in enumerate(load_intervals, start=1):
        print(
            f"    load [{load_index}/{len(load_intervals)}]: "
            f"{utc_text(load_start_ts)} -> {utc_text(load_end_ts)}"
        )

        rows_written = await load_quotes_segment(
            ib=ib,
            ib_health=ib_health,
            db_path=db_path,
            table_name=table_name,
            contract=contract,
            contract_name=contract_name,
            sec_type=instrument_row["secType"],
            session_model=instrument_row["session_model"],
            bar_size_setting=instrument_row["barSizeSetting"],
            use_rth=instrument_row["useRTH"],
            segment_start_ts=load_start_ts,
            segment_end_ts=load_end_ts,
            segment_kind="manual-repair",
        )

        total_rows_written += rows_written

    return total_rows_written


async def repair_intervals(
        *,
        intervals: list[dict],
        db_path: str,
        table_name: str,
        instrument_code: str,
        instrument_row,
) -> int:
    if not intervals:
        print("Нет интервалов для ремонта.")
        return 0

    ib, ib_health = await connect_ib(settings)
    total_rows_written = 0

    try:
        for index, interval in enumerate(intervals, start=1):
            load_intervals = build_repair_load_intervals(
                instrument_row=instrument_row,
                contract_name=interval["contract_name"],
                hole_start_ts=interval["start_ts"],
                hole_end_ts_exclusive=interval["end_ts_exclusive"],
            )

            print(
                f"Ремонт [{index}/{len(intervals)}]: "
                f"contract={interval['contract_name']} | "
                f"hole={utc_text(interval['start_ts'])} -> {utc_text(interval['end_ts_exclusive'])} | "
                f"loads={len(load_intervals)}"
            )

            rows_written = await repair_interval(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                db_path=db_path,
                table_name=table_name,
                instrument_code=instrument_code,
                instrument_row=instrument_row,
                interval=interval,
            )
            total_rows_written += rows_written

            print(f"  записано строк: {rows_written}")

    finally:
        disconnect_ib(ib)

    return total_rows_written


# ============================================================
# РЕЖИМЫ
# ============================================================

def collect_intervals_for_scan(
        *,
        conn,
        db_path: str,
        table_name: str,
        instrument_code: str,
        instrument_row,
        start_ts: int,
) -> list[dict]:
    ensure_table_exists(conn, db_path, table_name)

    print(f"TARGET     : {TARGET}")
    print(f"instrument : {instrument_code}")
    print(f"DB         : {db_path}")
    print(f"table      : {table_name}")
    print(f"check from : {utc_text(start_ts)}")
    print()

    result = find_repair_intervals(
        conn=conn,
        table_name=table_name,
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        start_ts=start_ts,
    )

    print(
        f"rows={result['rows_count']}, "
        f"NULL rows={result['problem_rows_count']}, "
        f"NULL intervals={len(result['null_intervals'])}, "
        f"GAP intervals={len(result['gap_intervals'])}, "
        f"merged={len(result['merged_intervals'])}"
    )

    return result["merged_intervals"]


async def run_scan_or_repair_mode() -> None:
    instrument_code, instrument_row = resolve_target(TARGET)
    db_path = get_instrument_db_path(settings, instrument_code, instrument_row)
    table_name = get_instrument_table_name(instrument_code, instrument_row)
    start_ts = parse_required_utc_text(CHECK_FROM_UTC)

    conn = open_sqlite_connection(db_path, use_wal=False)
    conn.row_factory = sqlite3.Row

    try:
        intervals = collect_intervals_for_scan(
            conn=conn,
            db_path=db_path,
            table_name=table_name,
            instrument_code=instrument_code,
            instrument_row=instrument_row,
            start_ts=start_ts,
        )
    finally:
        conn.close()

    intervals = limit_intervals(intervals)

    print()
    print("=" * 100)
    print("ИТОГОВЫЕ ИНТЕРВАЛЫ")
    print("=" * 100)
    if PRINT_INTERVALS:
        print_intervals(intervals)
    else:
        print(f"Найдено интервалов: {len(intervals)}")

    if MODE == "SCAN":
        return

    total_rows_written = await repair_intervals(
        intervals=intervals,
        db_path=db_path,
        table_name=table_name,
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )
    print()
    print(f"Ремонт завершён. Всего записано строк: {total_rows_written}")


async def main() -> None:
    if MODE not in {"SCAN", "REPAIR"}:
        raise ValueError(f"Неподдерживаемый MODE: {MODE}")

    await run_scan_or_repair_mode()


if __name__ == "__main__":
    asyncio.run(main())
