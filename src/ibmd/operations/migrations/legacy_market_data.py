from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ibmd.foundation.identity import new_id
from ibmd.foundation.time import format_utc, parse_utc, utc_now_text

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PRICE_FIELDS = (
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
)
_REQUIRED_LEGACY_COLUMNS = {
    "bar_time_ts",
    "contract",
    *_PRICE_FIELDS,
}


class LegacyMarketDataImportError(RuntimeError):
    pass


@dataclass(frozen=True)
class LegacyContractMapping:
    local_symbol: str
    con_id: int

    def __post_init__(self) -> None:
        symbol = str(self.local_symbol or "").strip()
        if not symbol:
            raise LegacyMarketDataImportError("local_symbol is required")
        try:
            con_id = int(self.con_id)
        except (TypeError, ValueError) as exc:
            raise LegacyMarketDataImportError(
                f"con_id must be an integer: {self.con_id!r}"
            ) from exc
        if con_id <= 0:
            raise LegacyMarketDataImportError(
                f"con_id must be positive: {con_id}"
            )
        object.__setattr__(self, "local_symbol", symbol)
        object.__setattr__(self, "con_id", con_id)


@dataclass(frozen=True)
class LegacyMarketDataImportResult:
    instrument_id: str
    start_utc: str
    end_utc: str
    complete_source_count: int
    existing_exact_count: int
    imported_count: int
    incomplete_source_count: int
    applied: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "complete_source_count": self.complete_source_count,
            "existing_exact_count": self.existing_exact_count,
            "imported_count": self.imported_count,
            "incomplete_source_count": self.incomplete_source_count,
            "applied": self.applied,
        }


def _quote_identifier(value: str) -> str:
    text = str(value or "").strip()
    if not _TABLE_RE.fullmatch(text):
        raise LegacyMarketDataImportError(
            f"invalid SQLite table name: {value!r}"
        )
    return f'"{text}"'


def _database_uri(path: Path, *, mode: str) -> str:
    return f"file:{path.resolve().as_posix()}?mode={mode}"


def _normalize_interval(
    *,
    start_utc: str,
    end_utc: str,
    bar_duration_seconds: int,
) -> tuple[int, int, str, str, int]:
    start = parse_utc(start_utc)
    end = parse_utc(end_utc)
    duration = int(bar_duration_seconds)
    if duration <= 0:
        raise LegacyMarketDataImportError(
            f"bar_duration_seconds must be positive: {duration}"
        )
    if end <= start:
        raise LegacyMarketDataImportError(
            "legacy market-data import interval must be positive"
        )
    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())
    if start_ts % duration != 0 or end_ts % duration != 0:
        raise LegacyMarketDataImportError(
            "legacy market-data import boundaries must be aligned to "
            f"{duration} seconds"
        )
    return (
        start_ts,
        end_ts,
        format_utc(start),
        format_utc(end),
        duration,
    )


def _normalize_mappings(
    mappings: Iterable[LegacyContractMapping],
) -> tuple[tuple[str, int], ...]:
    values = tuple(
        (item.local_symbol, item.con_id)
        for item in mappings
    )
    if not values:
        raise LegacyMarketDataImportError(
            "at least one futures contract mapping is required"
        )
    symbols = [item[0] for item in values]
    con_ids = [item[1] for item in values]
    if len(symbols) != len(set(symbols)):
        raise LegacyMarketDataImportError(
            f"duplicate local_symbol mappings: {symbols}"
        )
    if len(con_ids) != len(set(con_ids)):
        raise LegacyMarketDataImportError(
            f"duplicate con_id mappings: {con_ids}"
        )
    return values


def import_legacy_complete_bars(
    *,
    legacy_database_path: str | Path,
    legacy_table_name: str,
    target_database_path: str | Path,
    instrument_id: str,
    mappings: Iterable[LegacyContractMapping],
    start_utc: str,
    end_utc: str,
    bar_duration_seconds: int = 5,
    apply: bool = False,
    applied_at_utc: str | None = None,
) -> LegacyMarketDataImportResult:
    legacy_path = Path(legacy_database_path)
    target_path = Path(target_database_path)
    if not legacy_path.is_file():
        raise LegacyMarketDataImportError(
            f"legacy market-data database does not exist: {legacy_path}"
        )
    if not target_path.is_file():
        raise LegacyMarketDataImportError(
            f"target market-data database does not exist: {target_path}"
        )
    if legacy_path.resolve() == target_path.resolve():
        raise LegacyMarketDataImportError(
            "legacy and target market-data databases must be different files"
        )

    table = _quote_identifier(legacy_table_name)
    instrument = str(instrument_id or "").strip()
    if not instrument:
        raise LegacyMarketDataImportError("instrument_id is required")
    (
        start_ts,
        end_ts,
        normalized_start,
        normalized_end,
        duration,
    ) = _normalize_interval(
        start_utc=start_utc,
        end_utc=end_utc,
        bar_duration_seconds=bar_duration_seconds,
    )
    mapping_rows = _normalize_mappings(mappings)
    applied_at = (
        utc_now_text()
        if applied_at_utc is None
        else format_utc(parse_utc(applied_at_utc))
    )

    target_uri = _database_uri(target_path, mode="rw")
    connection = sqlite3.connect(
        target_uri,
        uri=True,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    attached = False

    complete_predicate = " AND ".join(
        f"l.{field_name} IS NOT NULL"
        for field_name in _PRICE_FIELDS
    )
    incomplete_predicate = " OR ".join(
        f"l.{field_name} IS NULL"
        for field_name in _PRICE_FIELDS
    )
    equality_predicate = " AND ".join(
        (
            "t.con_id = m.con_id",
            "t.local_symbol = TRIM(l.contract)",
            *(
                f"t.{field_name} = l.{field_name}"
                for field_name in _PRICE_FIELDS
            ),
        )
    )

    try:
        connection.execute(
            "ATTACH DATABASE ? AS legacy_source",
            (_database_uri(legacy_path, mode="ro"),),
        )
        attached = True

        columns = {
            str(row[1])
            for row in connection.execute(
                f"PRAGMA legacy_source.table_info({table})"
            ).fetchall()
        }
        missing_columns = sorted(_REQUIRED_LEGACY_COLUMNS - columns)
        if missing_columns:
            raise LegacyMarketDataImportError(
                "legacy market-data table is missing required columns: "
                f"{missing_columns}"
            )

        connection.execute(
            """
            CREATE TEMP TABLE temp_legacy_contract_map (
                local_symbol TEXT PRIMARY KEY,
                con_id INTEGER NOT NULL UNIQUE
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO temp_legacy_contract_map (local_symbol, con_id)
            VALUES (?, ?)
            """,
            mapping_rows,
        )

        unknown_symbols = [
            str(row[0])
            for row in connection.execute(
                f"""
                SELECT DISTINCT
                    COALESCE(NULLIF(TRIM(l.contract), ''), '<EMPTY>')
                FROM legacy_source.{table} AS l
                LEFT JOIN temp_legacy_contract_map AS m
                  ON m.local_symbol = TRIM(l.contract)
                WHERE l.bar_time_ts >= ?
                  AND l.bar_time_ts < ?
                  AND {complete_predicate}
                  AND m.con_id IS NULL
                ORDER BY 1
                """,
                (start_ts, end_ts),
            ).fetchall()
        ]
        if unknown_symbols:
            raise LegacyMarketDataImportError(
                "legacy interval contains contracts absent from target catalog: "
                f"{unknown_symbols}"
            )

        complete_source_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM legacy_source.{table} AS l
                JOIN temp_legacy_contract_map AS m
                  ON m.local_symbol = TRIM(l.contract)
                WHERE l.bar_time_ts >= ?
                  AND l.bar_time_ts < ?
                  AND {complete_predicate}
                """,
                (start_ts, end_ts),
            ).fetchone()[0]
        )
        incomplete_source_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM legacy_source.{table} AS l
                WHERE l.bar_time_ts >= ?
                  AND l.bar_time_ts < ?
                  AND ({incomplete_predicate})
                """,
                (start_ts, end_ts),
            ).fetchone()[0]
        )
        existing_exact_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM legacy_source.{table} AS l
                JOIN temp_legacy_contract_map AS m
                  ON m.local_symbol = TRIM(l.contract)
                JOIN internal_market_bars AS t
                  ON t.instrument_id = ?
                 AND t.bar_start_ts = l.bar_time_ts
                WHERE l.bar_time_ts >= ?
                  AND l.bar_time_ts < ?
                  AND {complete_predicate}
                  AND {equality_predicate}
                """,
                (instrument, start_ts, end_ts),
            ).fetchone()[0]
        )
        conflicting_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM legacy_source.{table} AS l
                JOIN temp_legacy_contract_map AS m
                  ON m.local_symbol = TRIM(l.contract)
                JOIN internal_market_bars AS t
                  ON t.instrument_id = ?
                 AND t.bar_start_ts = l.bar_time_ts
                WHERE l.bar_time_ts >= ?
                  AND l.bar_time_ts < ?
                  AND {complete_predicate}
                  AND NOT ({equality_predicate})
                """,
                (instrument, start_ts, end_ts),
            ).fetchone()[0]
        )
        if conflicting_count:
            raise LegacyMarketDataImportError(
                "target store already contains conflicting canonical bars: "
                f"count={conflicting_count}, interval={normalized_start}.."
                f"{normalized_end}"
            )

        pending_count = complete_source_count - existing_exact_count
        if not apply:
            return LegacyMarketDataImportResult(
                instrument_id=instrument,
                start_utc=normalized_start,
                end_utc=normalized_end,
                complete_source_count=complete_source_count,
                existing_exact_count=existing_exact_count,
                imported_count=pending_count,
                incomplete_source_count=incomplete_source_count,
                applied=False,
            )

        connection.execute("BEGIN IMMEDIATE")
        changes_before = connection.total_changes
        connection.execute(
            f"""
            INSERT INTO internal_market_bars (
                bar_id,
                instrument_id,
                con_id,
                local_symbol,
                bar_start_ts,
                bar_start_utc,
                bar_end_utc,
                bar_duration_seconds,
                bid_open,
                bid_high,
                bid_low,
                bid_close,
                ask_open,
                ask_high,
                ask_low,
                ask_close,
                source_kind,
                first_published_at_utc,
                published_at_utc,
                revision,
                complete,
                volume,
                average,
                bar_count
            )
            SELECT
                'market_bar_' || lower(hex(randomblob(16))),
                ?,
                m.con_id,
                TRIM(l.contract),
                l.bar_time_ts,
                strftime(
                    '%Y-%m-%dT%H:%M:%fZ',
                    l.bar_time_ts,
                    'unixepoch'
                ),
                strftime(
                    '%Y-%m-%dT%H:%M:%fZ',
                    l.bar_time_ts + ?,
                    'unixepoch'
                ),
                ?,
                l.bid_open,
                l.bid_high,
                l.bid_low,
                l.bid_close,
                l.ask_open,
                l.ask_high,
                l.ask_low,
                l.ask_close,
                'HISTORY',
                ?,
                ?,
                1,
                1,
                NULL,
                NULL,
                NULL
            FROM legacy_source.{table} AS l
            JOIN temp_legacy_contract_map AS m
              ON m.local_symbol = TRIM(l.contract)
            WHERE l.bar_time_ts >= ?
              AND l.bar_time_ts < ?
              AND {complete_predicate}
              AND NOT EXISTS (
                  SELECT 1
                  FROM internal_market_bars AS t
                  WHERE t.instrument_id = ?
                    AND t.bar_start_ts = l.bar_time_ts
              )
            ORDER BY l.bar_time_ts
            """,
            (
                instrument,
                duration,
                duration,
                applied_at,
                applied_at,
                start_ts,
                end_ts,
                instrument,
            ),
        )
        imported_count = int(connection.total_changes - changes_before)
        if imported_count != pending_count:
            raise LegacyMarketDataImportError(
                "legacy import count changed during the write transaction: "
                f"expected={pending_count}, actual={imported_count}"
            )

        if imported_count > 0:
            latest = connection.execute(
                """
                SELECT
                    bar_id,
                    bar_start_ts,
                    bar_end_utc,
                    con_id,
                    local_symbol
                FROM internal_market_bars
                WHERE instrument_id = ?
                ORDER BY bar_start_ts DESC, published_at_utc DESC
                LIMIT 1
                """,
                (instrument,),
            ).fetchone()
            if latest is None:
                raise LegacyMarketDataImportError(
                    "imported bars exist but target latest bar cannot be read"
                )
            connection.execute(
                """
                INSERT INTO internal_market_data_state (
                    instrument_id,
                    latest_complete_bar_id,
                    latest_bar_start_ts,
                    latest_bar_end_utc,
                    latest_con_id,
                    latest_local_symbol,
                    last_ingest_at_utc,
                    last_source_status,
                    last_error_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OK', NULL)
                ON CONFLICT(instrument_id) DO UPDATE SET
                    latest_complete_bar_id = excluded.latest_complete_bar_id,
                    latest_bar_start_ts = excluded.latest_bar_start_ts,
                    latest_bar_end_utc = excluded.latest_bar_end_utc,
                    latest_con_id = excluded.latest_con_id,
                    latest_local_symbol = excluded.latest_local_symbol,
                    last_ingest_at_utc = excluded.last_ingest_at_utc,
                    last_source_status = 'OK',
                    last_error_text = NULL
                """,
                (
                    instrument,
                    str(latest["bar_id"]),
                    int(latest["bar_start_ts"]),
                    str(latest["bar_end_utc"]),
                    int(latest["con_id"]),
                    str(latest["local_symbol"]),
                    applied_at,
                ),
            )

        event_detail = json.dumps(
            {
                "kind": "LEGACY_COMPLETE_BAR_IMPORT",
                "legacy_database": str(legacy_path.resolve()),
                "legacy_table": str(legacy_table_name),
                "start_utc": normalized_start,
                "end_utc": normalized_end,
                "complete_source_count": complete_source_count,
                "existing_exact_count": existing_exact_count,
                "imported_count": imported_count,
                "incomplete_source_count": incomplete_source_count,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        connection.execute(
            """
            INSERT INTO internal_market_data_source_events (
                event_id,
                instrument_id,
                source_session_id,
                observed_at_utc,
                status,
                detail,
                created_at_utc
            ) VALUES (?, ?, ?, ?, 'INFO', ?, ?)
            """,
            (
                new_id("md_source"),
                instrument,
                new_id("ib_session"),
                applied_at,
                event_detail,
                applied_at,
            ),
        )
        connection.commit()

        return LegacyMarketDataImportResult(
            instrument_id=instrument,
            start_utc=normalized_start,
            end_utc=normalized_end,
            complete_source_count=complete_source_count,
            existing_exact_count=existing_exact_count,
            imported_count=imported_count,
            incomplete_source_count=incomplete_source_count,
            applied=True,
        )
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        if attached:
            try:
                connection.execute("DETACH DATABASE legacy_source")
            except sqlite3.Error:
                pass
        connection.close()
