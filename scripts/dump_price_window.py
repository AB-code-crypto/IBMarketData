#!/usr/bin/env python3
"""
Делает компактный dump цен вокруг сделки из большой price DB и feature/job DB.

Запускать из корня репозитория IBMarketData.

Пример:
    python dump_price_window.py

По умолчанию выгружает MNQ за 2026-06-23 08:25:00–09:35:00 MSK
и проверяет уровни второго шанса:
    entry=30166.00, TP=30168.00, SL=30092.25

На выходе создаёт zip в data/debug_price_dumps/:
    - raw_price.csv
    - mid_price_5s.csv
    - price_dump.sqlite3
    - manifest.json
    - README.txt
"""

import argparse
import csv
import json
import sqlite3
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any


MSK_TZ = timezone(timedelta(hours=3), name="MSK")


@dataclass(frozen=True)
class DumpConfig:
    instrument: str
    date: str
    start_msk: str
    end_msk: str
    extension_start_msk: str | None
    deadline_msk: str | None
    side: str
    entry_price: float | None
    take_profit: float | None
    stop_loss: float | None
    output_dir: Path


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def parse_msk_datetime(date_text: str, time_text: str) -> datetime:
    return datetime.strptime(f"{date_text} {time_text}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)


def dt_to_ts(dt: datetime) -> int:
    return int(dt.astimezone(timezone.utc).timestamp())


def format_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_msk(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S MSK")


def fetch_rows(db_path: Path, table_name: str, start_ts: int, end_ts: int) -> tuple[list[str], list[dict[str, Any]]]:
    if not db_path.is_file():
        raise FileNotFoundError(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        table_ref = quote_identifier(table_name)

        exists = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = ?
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()

        if exists is None:
            available = [
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                ).fetchall()
            ]
            raise RuntimeError(
                f"Table {table_name!r} not found in {db_path}. "
                f"Available tables: {available}"
            )

        columns = [
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info({table_ref})").fetchall()
        ]

        if "bar_time_ts" not in columns:
            raise RuntimeError(f"Table {table_name!r} has no bar_time_ts column")

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

    finally:
        conn.close()


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


def write_rows_to_sqlite(conn: sqlite3.Connection, table_name: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
    table_ref = quote_identifier(table_name)
    conn.execute(f"DROP TABLE IF EXISTS {table_ref}")

    column_defs = ", ".join(
        f"{quote_identifier(col)} {sqlite_type_for_values(rows, col)}"
        for col in columns
    )
    conn.execute(f"CREATE TABLE {table_ref} ({column_defs})")

    if not rows:
        return

    placeholders = ", ".join("?" for _ in columns)
    column_refs = ", ".join(quote_identifier(col) for col in columns)
    insert_sql = f"INSERT INTO {table_ref} ({column_refs}) VALUES ({placeholders})"

    conn.executemany(
        insert_sql,
        [
            tuple(row.get(col) for col in columns)
            for row in rows
        ],
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {quote_identifier('idx_' + table_name + '_bar_time_ts')} "
        f"ON {table_ref}(bar_time_ts)"
    )


def nearest_rows(rows: list[dict[str, Any]], target_ts: int, *, count_each_side: int = 3) -> list[dict[str, Any]]:
    if not rows:
        return []

    indexed = list(enumerate(rows))
    best_idx, _ = min(
        indexed,
        key=lambda item: abs(int(item[1].get("bar_time_ts", 0)) - int(target_ts)),
    )

    start = max(0, best_idx - count_each_side)
    end = min(len(rows), best_idx + count_each_side + 1)

    result = []
    for row in rows[start:end]:
        enriched = dict(row)
        ts = int(enriched["bar_time_ts"])
        enriched["_bar_time_utc_debug"] = format_utc(ts)
        enriched["_bar_time_msk_debug"] = format_msk(ts)
        enriched["_seconds_from_target"] = ts - int(target_ts)
        result.append(enriched)

    return result


def first_row_where(rows: list[dict[str, Any]], condition) -> dict[str, Any] | None:
    for row in rows:
        try:
            if condition(row):
                enriched = dict(row)
                ts = int(enriched["bar_time_ts"])
                enriched["_bar_time_utc_debug"] = format_utc(ts)
                enriched["_bar_time_msk_debug"] = format_msk(ts)
                return enriched
        except (TypeError, ValueError, KeyError):
            continue
    return None


def safe_float(row: dict[str, Any], name: str) -> float | None:
    value = row.get(name)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def summarize_numeric(rows: list[dict[str, Any]], columns: list[str]) -> dict[str, dict[str, float | None]]:
    summary: dict[str, dict[str, float | None]] = {}

    for col in columns:
        values: list[float] = []
        for row in rows:
            value = safe_float(row, col)
            if value is not None:
                values.append(value)

        if values:
            summary[col] = {
                "min": min(values),
                "max": max(values),
            }

    return summary


def level_checks_for_long(
    *,
    raw_rows: list[dict[str, Any]],
    mid_rows: list[dict[str, Any]],
    tp: float | None,
    sl: float | None,
) -> dict[str, Any]:
    checks: dict[str, Any] = {}

    if sl is not None:
        checks["raw_first_bid_low_le_sl"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "bid_low") is not None and safe_float(r, "bid_low") <= sl,
        )
        checks["raw_first_bid_close_le_sl"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "bid_close") is not None and safe_float(r, "bid_close") <= sl,
        )
        checks["raw_first_ask_low_le_sl"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "ask_low") is not None and safe_float(r, "ask_low") <= sl,
        )
        checks["mid_first_mid_low_le_sl"] = first_row_where(
            mid_rows,
            lambda r: safe_float(r, "mid_low") is not None and safe_float(r, "mid_low") <= sl,
        )
        checks["mid_first_mid_close_le_sl"] = first_row_where(
            mid_rows,
            lambda r: safe_float(r, "mid_close") is not None and safe_float(r, "mid_close") <= sl,
        )

    if tp is not None:
        checks["raw_first_bid_high_ge_tp"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "bid_high") is not None and safe_float(r, "bid_high") >= tp,
        )
        checks["raw_first_bid_close_ge_tp"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "bid_close") is not None and safe_float(r, "bid_close") >= tp,
        )
        checks["raw_first_ask_high_ge_tp"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "ask_high") is not None and safe_float(r, "ask_high") >= tp,
        )
        checks["mid_first_mid_high_ge_tp"] = first_row_where(
            mid_rows,
            lambda r: safe_float(r, "mid_high") is not None and safe_float(r, "mid_high") >= tp,
        )
        checks["mid_first_mid_close_ge_tp"] = first_row_where(
            mid_rows,
            lambda r: safe_float(r, "mid_close") is not None and safe_float(r, "mid_close") >= tp,
        )

    return checks


def level_checks_for_short(
    *,
    raw_rows: list[dict[str, Any]],
    mid_rows: list[dict[str, Any]],
    tp: float | None,
    sl: float | None,
) -> dict[str, Any]:
    checks: dict[str, Any] = {}

    if sl is not None:
        checks["raw_first_ask_high_ge_sl"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "ask_high") is not None and safe_float(r, "ask_high") >= sl,
        )
        checks["raw_first_ask_close_ge_sl"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "ask_close") is not None and safe_float(r, "ask_close") >= sl,
        )
        checks["raw_first_bid_high_ge_sl"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "bid_high") is not None and safe_float(r, "bid_high") >= sl,
        )
        checks["mid_first_mid_high_ge_sl"] = first_row_where(
            mid_rows,
            lambda r: safe_float(r, "mid_high") is not None and safe_float(r, "mid_high") >= sl,
        )
        checks["mid_first_mid_close_ge_sl"] = first_row_where(
            mid_rows,
            lambda r: safe_float(r, "mid_close") is not None and safe_float(r, "mid_close") >= sl,
        )

    if tp is not None:
        checks["raw_first_ask_low_le_tp"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "ask_low") is not None and safe_float(r, "ask_low") <= tp,
        )
        checks["raw_first_ask_close_le_tp"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "ask_close") is not None and safe_float(r, "ask_close") <= tp,
        )
        checks["raw_first_bid_low_le_tp"] = first_row_where(
            raw_rows,
            lambda r: safe_float(r, "bid_low") is not None and safe_float(r, "bid_low") <= tp,
        )
        checks["mid_first_mid_low_le_tp"] = first_row_where(
            mid_rows,
            lambda r: safe_float(r, "mid_low") is not None and safe_float(r, "mid_low") <= tp,
        )
        checks["mid_first_mid_close_le_tp"] = first_row_where(
            mid_rows,
            lambda r: safe_float(r, "mid_close") is not None and safe_float(r, "mid_close") <= tp,
        )

    return checks


def build_manifest(
    *,
    cfg: DumpConfig,
    price_db_path: Path,
    price_table_name: str,
    feature_db_path: Path,
    feature_table_name: str,
    start_ts: int,
    end_ts: int,
    extension_start_ts: int | None,
    deadline_ts: int | None,
    raw_columns: list[str],
    raw_rows: list[dict[str, Any]],
    mid_columns: list[str],
    mid_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    side = cfg.side.upper()

    if side == "SHORT":
        level_checks = level_checks_for_short(
            raw_rows=raw_rows,
            mid_rows=mid_rows,
            tp=cfg.take_profit,
            sl=cfg.stop_loss,
        )
    else:
        level_checks = level_checks_for_long(
            raw_rows=raw_rows,
            mid_rows=mid_rows,
            tp=cfg.take_profit,
            sl=cfg.stop_loss,
        )

    checkpoints: dict[str, Any] = {}

    if extension_start_ts is not None:
        checkpoints["extension_start"] = {
            "target_ts": extension_start_ts,
            "target_utc": format_utc(extension_start_ts),
            "target_msk": format_msk(extension_start_ts),
            "raw_nearest_rows": nearest_rows(raw_rows, extension_start_ts),
            "mid_nearest_rows": nearest_rows(mid_rows, extension_start_ts),
        }

    if deadline_ts is not None:
        checkpoints["deadline"] = {
            "target_ts": deadline_ts,
            "target_utc": format_utc(deadline_ts),
            "target_msk": format_msk(deadline_ts),
            "raw_nearest_rows": nearest_rows(raw_rows, deadline_ts),
            "mid_nearest_rows": nearest_rows(mid_rows, deadline_ts),
        }

    manifest = {
        "instrument": cfg.instrument,
        "side": side,
        "date": cfg.date,
        "window": {
            "start_msk": f"{cfg.date} {cfg.start_msk} MSK",
            "end_msk": f"{cfg.date} {cfg.end_msk} MSK",
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_utc": format_utc(start_ts),
            "end_utc": format_utc(end_ts),
        },
        "levels": {
            "entry_price": cfg.entry_price,
            "take_profit": cfg.take_profit,
            "stop_loss": cfg.stop_loss,
        },
        "sources": {
            "price_db_path": str(price_db_path),
            "price_table_name": price_table_name,
            "feature_db_path": str(feature_db_path),
            "feature_table_name": feature_table_name,
        },
        "rows": {
            "raw_price": len(raw_rows),
            "mid_price_5s": len(mid_rows),
        },
        "columns": {
            "raw_price": raw_columns,
            "mid_price_5s": mid_columns,
        },
        "raw_price_numeric_minmax": summarize_numeric(
            raw_rows,
            [
                "bid_open", "bid_high", "bid_low", "bid_close",
                "ask_open", "ask_high", "ask_low", "ask_close",
            ],
        ),
        "mid_price_numeric_minmax": summarize_numeric(
            mid_rows,
            [
                "mid_open", "mid_high", "mid_low", "mid_close",
                "spread_open", "spread_high", "spread_low", "spread_close",
            ],
        ),
        "level_checks": level_checks,
        "checkpoints": checkpoints,
    }

    return manifest


def load_repo_helpers(instrument: str):
    """
    Импортируем пути из самого репозитория, чтобы не гадать имена файлов/таблиц.
    """
    try:
        from config import settings_live as settings
        from contracts import Instrument
        from core.instrument_db import get_instrument_db_path, get_instrument_table_name
        from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME
        from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
    except Exception as exc:
        raise RuntimeError(
            "Не удалось импортировать модули репозитория. "
            "Запускай скрипт из корня IBMarketData в том же venv, где работает робот. "
            f"Ошибка: {type(exc).__name__}: {exc}"
        ) from exc

    if instrument not in Instrument:
        raise RuntimeError(f"Инструмент {instrument!r} не найден в contracts.Instrument")

    instrument_row = Instrument[instrument]

    price_db_path = Path(
        get_instrument_db_path(
            settings=settings,
            instrument_code=instrument,
            instrument_row=instrument_row,
        )
    )

    price_table_name = get_instrument_table_name(
        instrument_code=instrument,
        instrument_row=instrument_row,
    )

    feature_db_path = Path(
        get_instrument_feature_db_path(
            instrument_code=instrument,
            instrument_row=instrument_row,
        )
    )

    return price_db_path, price_table_name, feature_db_path, MID_PRICE_TABLE_NAME


def create_readme(path: Path, manifest: dict[str, Any]) -> None:
    text = f"""Price window dump

Instrument: {manifest['instrument']}
Side: {manifest['side']}
Window MSK: {manifest['window']['start_msk']} -> {manifest['window']['end_msk']}
Window UTC: {manifest['window']['start_utc']} -> {manifest['window']['end_utc']}

Levels:
  entry_price: {manifest['levels']['entry_price']}
  take_profit: {manifest['levels']['take_profit']}
  stop_loss:   {manifest['levels']['stop_loss']}

Files:
  raw_price.csv      - rows from raw price DB table
  mid_price_5s.csv   - rows from feature/job DB mid_price_5s
  price_dump.sqlite3 - same rows in SQLite tables raw_price and mid_price_5s
  manifest.json      - metadata, min/max, first level touches, checkpoint rows

Upload the full zip to ChatGPT.
"""
    path.write_text(text, encoding="utf-8")


def make_dump(cfg: DumpConfig) -> Path:
    start_dt = parse_msk_datetime(cfg.date, cfg.start_msk)
    end_dt = parse_msk_datetime(cfg.date, cfg.end_msk)

    if end_dt <= start_dt:
        raise ValueError("end_msk must be greater than start_msk")

    start_ts = dt_to_ts(start_dt)
    end_ts = dt_to_ts(end_dt)

    extension_start_ts = (
        dt_to_ts(parse_msk_datetime(cfg.date, cfg.extension_start_msk))
        if cfg.extension_start_msk
        else None
    )
    deadline_ts = (
        dt_to_ts(parse_msk_datetime(cfg.date, cfg.deadline_msk))
        if cfg.deadline_msk
        else None
    )

    price_db_path, price_table_name, feature_db_path, feature_table_name = load_repo_helpers(cfg.instrument)

    raw_columns, raw_rows = fetch_rows(
        price_db_path,
        price_table_name,
        start_ts,
        end_ts,
    )

    mid_columns, mid_rows = fetch_rows(
        feature_db_path,
        feature_table_name,
        start_ts,
        end_ts,
    )

    safe_start = cfg.start_msk.replace(":", "")
    safe_end = cfg.end_msk.replace(":", "")
    dump_name = f"price_dump_{cfg.instrument}_{cfg.date.replace('-', '')}_{safe_start}_{safe_end}_MSK"
    dump_dir = cfg.output_dir / dump_name
    dump_dir.mkdir(parents=True, exist_ok=True)

    raw_csv_path = dump_dir / "raw_price.csv"
    mid_csv_path = dump_dir / "mid_price_5s.csv"
    sqlite_path = dump_dir / "price_dump.sqlite3"
    manifest_path = dump_dir / "manifest.json"
    readme_path = dump_dir / "README.txt"

    write_csv(raw_csv_path, raw_columns, raw_rows)
    write_csv(mid_csv_path, mid_columns, mid_rows)

    out_conn = sqlite3.connect(str(sqlite_path))
    try:
        write_rows_to_sqlite(out_conn, "raw_price", raw_columns, raw_rows)
        write_rows_to_sqlite(out_conn, "mid_price_5s", mid_columns, mid_rows)
        out_conn.commit()
    finally:
        out_conn.close()

    manifest = build_manifest(
        cfg=cfg,
        price_db_path=price_db_path,
        price_table_name=price_table_name,
        feature_db_path=feature_db_path,
        feature_table_name=feature_table_name,
        start_ts=start_ts,
        end_ts=end_ts,
        extension_start_ts=extension_start_ts,
        deadline_ts=deadline_ts,
        raw_columns=raw_columns,
        raw_rows=raw_rows,
        mid_columns=mid_columns,
        mid_rows=mid_rows,
    )

    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    create_readme(readme_path, manifest)

    zip_path = cfg.output_dir / f"{dump_name}.zip"
    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(dump_dir.iterdir()):
            zf.write(file_path, arcname=file_path.name)

    return zip_path


def parse_args() -> DumpConfig:
    parser = argparse.ArgumentParser(
        description="Dump raw price DB + feature mid_price_5s for a compact MSK time window."
    )

    parser.add_argument("--instrument", default="MNQ")
    parser.add_argument("--date", default="2026-06-23", help="YYYY-MM-DD, MSK date")
    parser.add_argument("--start-msk", default="08:25:00")
    parser.add_argument("--end-msk", default="09:35:00")
    parser.add_argument("--extension-start-msk", default="08:59:50")
    parser.add_argument("--deadline-msk", default="09:30:00")
    parser.add_argument("--side", default="LONG", choices=["LONG", "SHORT", "long", "short"])

    parser.add_argument("--entry-price", type=float, default=30166.00)
    parser.add_argument("--take-profit", type=float, default=30168.00)
    parser.add_argument("--stop-loss", type=float, default=30092.25)

    parser.add_argument(
        "--output-dir",
        default="data/debug_price_dumps",
        help="Output directory relative to current repo root, unless absolute.",
    )

    args = parser.parse_args()

    return DumpConfig(
        instrument=str(args.instrument).upper(),
        date=str(args.date),
        start_msk=str(args.start_msk),
        end_msk=str(args.end_msk),
        extension_start_msk=None if args.extension_start_msk in {"", "none", "None"} else str(args.extension_start_msk),
        deadline_msk=None if args.deadline_msk in {"", "none", "None"} else str(args.deadline_msk),
        side=str(args.side).upper(),
        entry_price=args.entry_price,
        take_profit=args.take_profit,
        stop_loss=args.stop_loss,
        output_dir=Path(args.output_dir).resolve(),
    )


def main() -> int:
    cfg = parse_args()

    try:
        zip_path = make_dump(cfg)
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print()
    print("Готово.")
    print(f"Dump ZIP: {zip_path}")
    print("Загрузи этот zip сюда.")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
