"""
Очистка price DB от строк, записанных не тем квартальным фьючерсным контрактом.

Зачем нужен скрипт
------------------
Если run_market_data был запущен до rollover, realtime-подписка могла продолжить
писать старый контракт, например MNQM6, хотя execution уже торгует MNQU6.
Это создаёт разрыв между signal-ценой и реально торгуемым контрактом.

Скрипт проходит по price DB инструментов MES/ES/MNQ/NQ и удаляет строки, где:
    bar_time_ts попадает в configured active-window контракта из contracts.py,
    но поле `contract` в price DB не равно ожидаемому localSymbol.

Пример:
    contracts.py говорит, что на timestamp должен быть MNQU6,
    а в price DB строка имеет contract='MNQM6' -> строка удаляется.

Важно:
    - После реальной очистки price DB перезапусти market_data и signal,
      чтобы оба сервиса читали уже очищенную каноническую историю.
    - По умолчанию включён dry-run. Сначала посмотри отчёт, потом поставь
      APPLY_DELETE = True.
"""

from __future__ import annotations

import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings_live as settings  # noqa: E402
from contracts import Instrument  # noqa: E402
from core.instrument_db import get_instrument_db_path, get_instrument_table_name  # noqa: E402
from core.time_utils import format_utc_ts, parse_utc_iso_to_ts  # noqa: E402

# ============================================================
# НАСТРОЙКИ
# ============================================================

TARGET_INSTRUMENTS = ["MES", "ES", "MNQ", "NQ"]

# False = только отчёт, ничего не удаляет.
# True  = создаёт backup DB и удаляет найденные неправильные строки.
APPLY_DELETE = True

# Обычно оставляем False: строки вне configured active windows только показываем,
# но не удаляем. Если в отчёте видно мусор в rollover-gap или за пределами
# контрактов, можно включить и прогнать отдельно.
DELETE_ROWS_OUTSIDE_CONTRACT_WINDOWS = True

# Создать .bak перед реальным удалением.
CREATE_BACKUP_BEFORE_DELETE = False

# После удаления сделать VACUUM. Может занять время на больших DB.
VACUUM_AFTER_DELETE = True

# Сколько групп в отчёте печатать на инструмент.
MAX_GROUPS_TO_PRINT = 30

# ============================================================
# УТИЛИТЫ
# ============================================================


@dataclass(frozen=True)
class ContractWindow:
    local_symbol: str
    active_from_ts: int
    active_to_ts: int


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def format_ts_or_dash(ts: int | None) -> str:
    if ts is None:
        return "-"
    return format_utc_ts(int(ts))


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table'
          AND name=?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()
    return {str(row[1]) for row in rows}


def get_contract_windows(instrument_code: str, instrument_row: dict) -> list[ContractWindow]:
    if str(instrument_row.get("secType", "")).upper() != "FUT":
        raise ValueError(f"{instrument_code}: ожидаю FUT, получено secType={instrument_row.get('secType')!r}")

    windows: list[ContractWindow] = []
    for contract_row in instrument_row.get("contracts", []):
        windows.append(
            ContractWindow(
                local_symbol=str(contract_row["localSymbol"]),
                active_from_ts=parse_utc_iso_to_ts(str(contract_row["active_from_utc"])),
                active_to_ts=parse_utc_iso_to_ts(str(contract_row["active_to_utc"])),
            )
        )

    windows.sort(key=lambda item: item.active_from_ts)
    return windows


def create_temp_contract_windows(conn: sqlite3.Connection, windows: Iterable[ContractWindow]) -> None:
    conn.execute("DROP TABLE IF EXISTS temp_expected_contract_windows")
    conn.execute(
        """
        CREATE TEMP TABLE temp_expected_contract_windows (
            local_symbol TEXT NOT NULL,
            active_from_ts INTEGER NOT NULL,
            active_to_ts INTEGER NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO temp_expected_contract_windows (
            local_symbol,
            active_from_ts,
            active_to_ts
        )
        VALUES (?, ?, ?)
        """,
        [
            (window.local_symbol, window.active_from_ts, window.active_to_ts)
            for window in windows
        ],
    )


def print_group_rows(title: str, rows: list[sqlite3.Row]) -> None:
    print(title)
    if not rows:
        print("  нет")
        return

    for index, row in enumerate(rows[:MAX_GROUPS_TO_PRINT], start=1):
        actual_contract = row["actual_contract"]
        expected_contract = row["expected_contract"] if "expected_contract" in row.keys() else None
        expected_text = f" expected={expected_contract}" if expected_contract is not None else ""
        print(
            f"  {index:02d}. actual={actual_contract!r}{expected_text} "
            f"rows={int(row['rows_count'])} "
            f"range={format_ts_or_dash(row['min_ts'])} -> {format_ts_or_dash(row['max_ts'])}"
        )

    if len(rows) > MAX_GROUPS_TO_PRINT:
        print(f"  ... ещё групп: {len(rows) - MAX_GROUPS_TO_PRINT}")


def copy_backup(db_path: Path) -> Path:
    backup_path = db_path.with_suffix(db_path.suffix + f".bak.contract_cleanup.{int(time.time())}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def analyze_wrong_in_window(conn: sqlite3.Connection, table_name: str) -> list[sqlite3.Row]:
    table_ref = quote_identifier(table_name)
    return conn.execute(
        f"""
        SELECT
            COALESCE(p.contract, '<NULL>') AS actual_contract,
            w.local_symbol AS expected_contract,
            COUNT(*) AS rows_count,
            MIN(p.bar_time_ts) AS min_ts,
            MAX(p.bar_time_ts) AS max_ts
        FROM {table_ref} AS p
        JOIN temp_expected_contract_windows AS w
          ON p.bar_time_ts >= w.active_from_ts
         AND p.bar_time_ts <  w.active_to_ts
        WHERE p.contract IS NULL
           OR p.contract != w.local_symbol
        GROUP BY COALESCE(p.contract, '<NULL>'), w.local_symbol
        ORDER BY min_ts, actual_contract, expected_contract
        """
    ).fetchall()


def analyze_outside_windows(conn: sqlite3.Connection, table_name: str) -> list[sqlite3.Row]:
    table_ref = quote_identifier(table_name)
    return conn.execute(
        f"""
        SELECT
            COALESCE(p.contract, '<NULL>') AS actual_contract,
            COUNT(*) AS rows_count,
            MIN(p.bar_time_ts) AS min_ts,
            MAX(p.bar_time_ts) AS max_ts
        FROM {table_ref} AS p
        WHERE NOT EXISTS (
            SELECT 1
            FROM temp_expected_contract_windows AS w
            WHERE p.bar_time_ts >= w.active_from_ts
              AND p.bar_time_ts <  w.active_to_ts
        )
        GROUP BY COALESCE(p.contract, '<NULL>')
        ORDER BY min_ts, actual_contract
        """
    ).fetchall()


def count_rows(rows: Iterable[sqlite3.Row]) -> int:
    return sum(int(row["rows_count"]) for row in rows)


def delete_wrong_in_window(conn: sqlite3.Connection, table_name: str) -> int:
    table_ref = quote_identifier(table_name)
    before = conn.total_changes
    conn.execute(
        f"""
        DELETE FROM {table_ref}
        WHERE rowid IN (
            SELECT p.rowid
            FROM {table_ref} AS p
            JOIN temp_expected_contract_windows AS w
              ON p.bar_time_ts >= w.active_from_ts
             AND p.bar_time_ts <  w.active_to_ts
            WHERE p.contract IS NULL
               OR p.contract != w.local_symbol
        )
        """
    )
    return int(conn.total_changes - before)


def delete_outside_windows(conn: sqlite3.Connection, table_name: str) -> int:
    table_ref = quote_identifier(table_name)
    before = conn.total_changes
    conn.execute(
        f"""
        DELETE FROM {table_ref}
        WHERE NOT EXISTS (
            SELECT 1
            FROM temp_expected_contract_windows AS w
            WHERE {table_ref}.bar_time_ts >= w.active_from_ts
              AND {table_ref}.bar_time_ts <  w.active_to_ts
        )
        """
    )
    return int(conn.total_changes - before)


def process_instrument(instrument_code: str) -> dict[str, int]:
    if instrument_code not in Instrument:
        print(f"\n[{instrument_code}] нет в contracts.py, пропускаю")
        return {"wrong": 0, "outside": 0, "deleted": 0}

    instrument_row = Instrument[instrument_code]
    db_path = Path(get_instrument_db_path(settings, instrument_code, instrument_row))
    table_name = get_instrument_table_name(instrument_code, instrument_row)
    windows = get_contract_windows(instrument_code, instrument_row)

    print("\n" + "=" * 100)
    print(f"{instrument_code}")
    print(f"DB    : {db_path}")
    print(f"TABLE : {table_name}")
    print(f"MODE  : {'APPLY DELETE' if APPLY_DELETE else 'DRY RUN'}")
    print("contracts:")
    for window in windows:
        print(
            f"  {window.local_symbol}: "
            f"{format_ts_or_dash(window.active_from_ts)} -> {format_ts_or_dash(window.active_to_ts)}"
        )

    if not db_path.is_file():
        print(f"DB не найдена: {db_path}")
        return {"wrong": 0, "outside": 0, "deleted": 0}

    conn = connect_db(db_path)
    try:
        if not table_exists(conn, table_name):
            print(f"Таблица не найдена: {table_name}")
            return {"wrong": 0, "outside": 0, "deleted": 0}

        columns = get_table_columns(conn, table_name)
        required = {"bar_time_ts", "contract"}
        missing = required - columns
        if missing:
            print(f"Нет обязательных колонок {sorted(missing)} в таблице {table_name}")
            return {"wrong": 0, "outside": 0, "deleted": 0}

        create_temp_contract_windows(conn, windows)
        table_ref = quote_identifier(table_name)
        total_rows = int(conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()[0])
        print(f"rows total: {total_rows}")

        wrong_rows = analyze_wrong_in_window(conn, table_name)
        outside_rows = analyze_outside_windows(conn, table_name)
        wrong_count = count_rows(wrong_rows)
        outside_count = count_rows(outside_rows)

        print_group_rows(
            "wrong contract inside configured active windows:",
            wrong_rows,
        )
        print_group_rows(
            "rows outside all configured active windows:",
            outside_rows,
        )

        if not APPLY_DELETE:
            print(
                f"DRY RUN: будет удалено wrong={wrong_count}; "
                f"outside={outside_count if DELETE_ROWS_OUTSIDE_CONTRACT_WINDOWS else 0} "
                f"(DELETE_ROWS_OUTSIDE_CONTRACT_WINDOWS={DELETE_ROWS_OUTSIDE_CONTRACT_WINDOWS})"
            )
            return {"wrong": wrong_count, "outside": outside_count, "deleted": 0}

        if CREATE_BACKUP_BEFORE_DELETE and (wrong_count > 0 or (DELETE_ROWS_OUTSIDE_CONTRACT_WINDOWS and outside_count > 0)):
            backup_path = copy_backup(db_path)
            print(f"backup создан: {backup_path}")

        deleted_wrong = delete_wrong_in_window(conn, table_name)
        deleted_outside = 0
        if DELETE_ROWS_OUTSIDE_CONTRACT_WINDOWS:
            deleted_outside = delete_outside_windows(conn, table_name)

        conn.commit()

        if VACUUM_AFTER_DELETE and (deleted_wrong > 0 or deleted_outside > 0):
            print("VACUUM...")
            conn.execute("VACUUM")

        print(f"DELETED wrong={deleted_wrong}, outside={deleted_outside}")
        return {
            "wrong": wrong_count,
            "outside": outside_count,
            "deleted": deleted_wrong + deleted_outside,
        }

    finally:
        conn.close()


def main() -> None:
    print("cleanup_wrong_futures_contract_rows.py")
    print(f"PROJECT_ROOT={PROJECT_ROOT}")
    print(f"APPLY_DELETE={APPLY_DELETE}")
    print(f"DELETE_ROWS_OUTSIDE_CONTRACT_WINDOWS={DELETE_ROWS_OUTSIDE_CONTRACT_WINDOWS}")
    print(f"TARGET_INSTRUMENTS={TARGET_INSTRUMENTS}")

    totals = {"wrong": 0, "outside": 0, "deleted": 0}
    for instrument_code in TARGET_INSTRUMENTS:
        result = process_instrument(instrument_code)
        for key in totals:
            totals[key] += int(result[key])

    print("\n" + "=" * 100)
    print("SUMMARY")
    print(f"wrong inside active windows : {totals['wrong']}")
    print(f"outside configured windows  : {totals['outside']}")
    print(f"deleted rows                : {totals['deleted']}")
    if not APPLY_DELETE:
        print("\nЭто был DRY RUN. Для реального удаления поставь APPLY_DELETE = True.")
    else:
        print("\nПосле удаления перезапусти market-data и проверь целостность основной price DB.")


if __name__ == "__main__":
    main()
