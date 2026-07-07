#!/usr/bin/env python3
"""
Дампит price DB и feature/job DB по заданным MSK-окнам.

Запускать из корня репозитория IBMarketData в том же venv, где работает робот.

Никаких CLI-аргументов нет. Всё задаётся переменными ниже.
Архив скрипт не делает: папку результата можно заархивировать вручную.
"""

import csv
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


# =====================================================================
# НАСТРОЙКИ ДАМПА
# =====================================================================

ROBOT_LABEL = "robot"  # например: "big" или "small"
INSTRUMENT_CODE = "MNQ"

# Время задаётся в MSK.
# Для текущего дня и вчерашнего дня просто замени date на нужные даты.
DUMP_WINDOWS = [
    {
        "name": "yesterday",
        "date": "2026-07-06",
        "start_msk": "00:00:00",
        "end_msk": "23:59:59",
    },
    {
        "name": "today",
        "date": "2026-07-07",
        "start_msk": "00:00:00",
        "end_msk": "23:59:59",
    },
]

OUTPUT_ROOT = Path("data/debug_price_feature_dumps")


# =====================================================================
# КОД
# =====================================================================

MSK_TZ = timezone(timedelta(hours=3), name="MSK")


@dataclass(frozen=True)
class DumpWindow:
    name: str
    date: str
    start_msk: str
    end_msk: str

    @property
    def start_dt_msk(self) -> datetime:
        return parse_msk_datetime(self.date, self.start_msk)

    @property
    def end_dt_msk(self) -> datetime:
        return parse_msk_datetime(self.date, self.end_msk)

    @property
    def start_ts(self) -> int:
        return int(self.start_dt_msk.astimezone(timezone.utc).timestamp())

    @property
    def end_ts(self) -> int:
        return int(self.end_dt_msk.astimezone(timezone.utc).timestamp())


def parse_msk_datetime(date_text: str, time_text: str) -> datetime:
    return datetime.strptime(
        f"{date_text} {time_text}",
        "%Y-%m-%d %H:%M:%S",
    ).replace(tzinfo=MSK_TZ)


def format_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_msk(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S MSK")


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def sanitize_table_name(value: str) -> str:
    result = []
    for ch in str(value):
        if ch.isalnum() or ch == "_":
            result.append(ch)
        else:
            result.append("_")
    safe = "".join(result).strip("_")
    return safe or "table"


def open_db(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"DB not found: {path}")

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def get_table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        ORDER BY name
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    table_ref = quote_identifier(table_name)
    rows = conn.execute(f"PRAGMA table_info({table_ref})").fetchall()
    return [str(row[1]) for row in rows]


def table_has_bar_time_ts(conn: sqlite3.Connection, table_name: str) -> bool:
    return "bar_time_ts" in get_table_columns(conn, table_name)


def fetch_window_rows(
        conn: sqlite3.Connection,
        *,
        table_name: str,
        start_ts: int,
        end_ts: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    columns = get_table_columns(conn, table_name)

    if "bar_time_ts" not in columns:
        raise RuntimeError(f"Table {table_name!r} has no bar_time_ts column")

    table_ref = quote_identifier(table_name)
    rows = conn.execute(
        f"""
        SELECT *
        FROM {table_ref}
        WHERE bar_time_ts >= ?
          AND bar_time_ts <= ?
        ORDER BY bar_time_ts ASC
        """,
        (int(start_ts), int(end_ts)),
    ).fetchall()

    return columns, [dict(row) for row in rows]


def write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def sqlite_type_for_values(rows: list[dict[str, Any]], column: str) -> str:
    for row in rows:
        value = row.get(column)

        if value is None:
            continue

        if isinstance(value, int):
            return "INTEGER"

        if isinstance(value, float):
            return "REAL"

        return "TEXT"

    return "TEXT"


def write_rows_to_sqlite(
        conn: sqlite3.Connection,
        *,
        table_name: str,
        columns: list[str],
        rows: list[dict[str, Any]],
) -> None:
    table_ref = quote_identifier(table_name)
    conn.execute(f"DROP TABLE IF EXISTS {table_ref}")

    column_defs = ", ".join(
        f"{quote_identifier(column)} {sqlite_type_for_values(rows, column)}"
        for column in columns
    )
    conn.execute(f"CREATE TABLE {table_ref} ({column_defs})")

    if rows:
        placeholders = ", ".join("?" for _ in columns)
        column_refs = ", ".join(quote_identifier(column) for column in columns)
        insert_sql = f"INSERT INTO {table_ref} ({column_refs}) VALUES ({placeholders})"

        conn.executemany(
            insert_sql,
            [tuple(row.get(column) for column in columns) for row in rows],
        )

    if "bar_time_ts" in columns:
        index_name = sanitize_table_name(f"idx_{table_name}_bar_time_ts")
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {quote_identifier(index_name)} "
            f"ON {table_ref}(bar_time_ts)"
        )


def row_time_bounds(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "first_bar_time_ts": None,
            "first_bar_time_msk": None,
            "last_bar_time_ts": None,
            "last_bar_time_msk": None,
        }

    first_ts = int(rows[0]["bar_time_ts"])
    last_ts = int(rows[-1]["bar_time_ts"])

    return {
        "first_bar_time_ts": first_ts,
        "first_bar_time_msk": format_msk(first_ts),
        "last_bar_time_ts": last_ts,
        "last_bar_time_msk": format_msk(last_ts),
    }


def load_repo_paths(instrument_code: str) -> tuple[Path, str, Path]:
    try:
        from config import settings_live as settings
        from contracts import Instrument
        from core.instrument_db import get_instrument_db_path, get_instrument_table_name
        from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
    except Exception as exc:
        raise RuntimeError(
            "Не удалось импортировать модули репозитория. "
            "Запускай скрипт из корня IBMarketData в venv робота. "
            f"Ошибка: {type(exc).__name__}: {exc}"
        ) from exc

    if instrument_code not in Instrument:
        raise RuntimeError(f"Инструмент {instrument_code!r} не найден в contracts.Instrument")

    instrument_row = Instrument[instrument_code]

    price_db_path = Path(
        get_instrument_db_path(
            settings=settings,
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )
    )

    price_table_name = get_instrument_table_name(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    feature_db_path = Path(
        get_instrument_feature_db_path(
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )
    )

    return price_db_path, price_table_name, feature_db_path


def dump_window(
        *,
        window: DumpWindow,
        price_db_path: Path,
        price_table_name: str,
        feature_db_path: Path,
        output_root: Path,
) -> Path:
    if window.end_ts <= window.start_ts:
        raise ValueError(f"Bad window: {window}")

    safe_start = window.start_msk.replace(":", "")
    safe_end = window.end_msk.replace(":", "")

    dump_dir = (
        output_root
        / f"{ROBOT_LABEL}_{INSTRUMENT_CODE}_{window.name}_{window.date.replace('-', '')}_{safe_start}_{safe_end}_MSK"
    )
    dump_dir.mkdir(parents=True, exist_ok=True)

    out_sqlite_path = dump_dir / "price_feature_dump.sqlite3"
    manifest_path = dump_dir / "manifest.json"

    manifest: dict[str, Any] = {
        "robot_label": ROBOT_LABEL,
        "instrument_code": INSTRUMENT_CODE,
        "window": {
            "name": window.name,
            "date": window.date,
            "start_msk": f"{window.date} {window.start_msk} MSK",
            "end_msk": f"{window.date} {window.end_msk} MSK",
            "start_ts": window.start_ts,
            "end_ts": window.end_ts,
            "start_utc": format_utc(window.start_ts),
            "end_utc": format_utc(window.end_ts),
        },
        "sources": {
            "price_db_path": str(price_db_path),
            "price_table_name": price_table_name,
            "feature_db_path": str(feature_db_path),
        },
        "tables": {},
    }

    out_conn = sqlite3.connect(str(out_sqlite_path))

    try:
        price_conn = open_db(price_db_path)
        try:
            price_columns, price_rows = fetch_window_rows(
                price_conn,
                table_name=price_table_name,
                start_ts=window.start_ts,
                end_ts=window.end_ts,
            )
        finally:
            price_conn.close()

        write_csv(dump_dir / "price_raw.csv", price_columns, price_rows)
        write_rows_to_sqlite(
            out_conn,
            table_name="price_raw",
            columns=price_columns,
            rows=price_rows,
        )
        manifest["tables"]["price_raw"] = {
            "source_table": price_table_name,
            "csv": "price_raw.csv",
            "rows": len(price_rows),
            **row_time_bounds(price_rows),
        }

        feature_conn = open_db(feature_db_path)
        try:
            feature_tables = [
                table_name
                for table_name in get_table_names(feature_conn)
                if table_has_bar_time_ts(feature_conn, table_name)
            ]

            for feature_table_name in feature_tables:
                columns, rows = fetch_window_rows(
                    feature_conn,
                    table_name=feature_table_name,
                    start_ts=window.start_ts,
                    end_ts=window.end_ts,
                )

                csv_name = f"feature_{sanitize_table_name(feature_table_name)}.csv"
                sqlite_table_name = f"feature__{sanitize_table_name(feature_table_name)}"

                write_csv(dump_dir / csv_name, columns, rows)
                write_rows_to_sqlite(
                    out_conn,
                    table_name=sqlite_table_name,
                    columns=columns,
                    rows=rows,
                )

                manifest["tables"][sqlite_table_name] = {
                    "source_table": feature_table_name,
                    "csv": csv_name,
                    "rows": len(rows),
                    **row_time_bounds(rows),
                }

        finally:
            feature_conn.close()

        out_conn.commit()

    finally:
        out_conn.close()

    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return dump_dir


def build_windows() -> list[DumpWindow]:
    return [
        DumpWindow(
            name=str(item["name"]),
            date=str(item["date"]),
            start_msk=str(item["start_msk"]),
            end_msk=str(item["end_msk"]),
        )
        for item in DUMP_WINDOWS
    ]


def main() -> int:
    output_root = Path(OUTPUT_ROOT).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    try:
        price_db_path, price_table_name, feature_db_path = load_repo_paths(INSTRUMENT_CODE)
        windows = build_windows()

        print(f"ROBOT_LABEL : {ROBOT_LABEL}")
        print(f"INSTRUMENT  : {INSTRUMENT_CODE}")
        print(f"Price DB    : {price_db_path}")
        print(f"Price table : {price_table_name}")
        print(f"Feature DB  : {feature_db_path}")
        print(f"Output root : {output_root}")
        print()

        for window in windows:
            dump_dir = dump_window(
                window=window,
                price_db_path=price_db_path,
                price_table_name=price_table_name,
                feature_db_path=feature_db_path,
                output_root=output_root,
            )
            print(f"Dump ready: {dump_dir}")

    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print()
    print("Готово. Заархивируй созданные папки вручную и загрузи сюда.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
