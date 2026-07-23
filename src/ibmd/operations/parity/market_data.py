from __future__ import annotations

import math
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ibmd.foundation.time import parse_utc
from ibmd.market_data.adapters.sqlite_store import SQLiteMarketBarReader
from ibmd.public_contracts.market_data import MarketBarV1

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
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class MarketDataParityError(RuntimeError):
    pass


@dataclass(frozen=True)
class LegacyMarketBar:
    bar_start_ts: int
    local_symbol: str
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float

    @property
    def key(self) -> tuple[int, str]:
        return self.bar_start_ts, self.local_symbol


@dataclass(frozen=True)
class MarketDataParityReport:
    instrument_id: str
    start_utc: str
    end_utc: str
    tolerance: float
    legacy_count: int
    target_count: int
    matched_count: int
    legacy_only_count: int
    target_only_count: int
    value_mismatch_count: int
    legacy_only_samples: tuple[dict[str, Any], ...]
    target_only_samples: tuple[dict[str, Any], ...]
    value_mismatch_samples: tuple[dict[str, Any], ...]

    @property
    def is_match(self) -> bool:
        return (
            self.legacy_only_count == 0
            and self.target_only_count == 0
            and self.value_mismatch_count == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "tolerance": self.tolerance,
            "is_match": self.is_match,
            "legacy_count": self.legacy_count,
            "target_count": self.target_count,
            "matched_count": self.matched_count,
            "legacy_only_count": self.legacy_only_count,
            "target_only_count": self.target_only_count,
            "value_mismatch_count": self.value_mismatch_count,
            "legacy_only_samples": list(self.legacy_only_samples),
            "target_only_samples": list(self.target_only_samples),
            "value_mismatch_samples": list(self.value_mismatch_samples),
        }


def _open_read_only(path: str | Path) -> sqlite3.Connection:
    source = Path(path)
    if not source.is_file():
        raise MarketDataParityError(f"SQLite database does not exist: {source}")
    uri = f"file:{source.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _quote_identifier(value: str) -> str:
    text = str(value or "").strip()
    if not _TABLE_RE.fullmatch(text):
        raise MarketDataParityError(f"invalid SQLite table name: {value!r}")
    return f'"{text}"'


def read_legacy_complete_bars(
    database_path: str | Path,
    *,
    table_name: str,
    start_utc: str,
    end_utc: str,
) -> tuple[LegacyMarketBar, ...]:
    start_ts = int(parse_utc(start_utc).timestamp())
    end_ts = int(parse_utc(end_utc).timestamp())
    if end_ts <= start_ts:
        raise MarketDataParityError("comparison interval must be positive")
    table = _quote_identifier(table_name)
    complete_predicate = " AND ".join(
        f"{field} IS NOT NULL" for field in _PRICE_FIELDS
    )
    connection = _open_read_only(database_path)
    try:
        try:
            rows = connection.execute(
                f"""
                SELECT
                    bar_time_ts,
                    contract,
                    bid_open,
                    bid_high,
                    bid_low,
                    bid_close,
                    ask_open,
                    ask_high,
                    ask_low,
                    ask_close
                FROM {table}
                WHERE bar_time_ts >= ?
                  AND bar_time_ts < ?
                  AND {complete_predicate}
                ORDER BY bar_time_ts, contract
                """,
                (start_ts, end_ts),
            ).fetchall()
        except sqlite3.Error as exc:
            raise MarketDataParityError(
                f"cannot read legacy market-data table {table_name!r}: {exc}"
            ) from exc
    finally:
        connection.close()

    values: list[LegacyMarketBar] = []
    for row in rows:
        symbol = str(row["contract"] or "").strip()
        if not symbol:
            raise MarketDataParityError(
                f"legacy complete bar has empty contract: ts={row['bar_time_ts']}"
            )
        values.append(
            LegacyMarketBar(
                bar_start_ts=int(row["bar_time_ts"]),
                local_symbol=symbol,
                bid_open=float(row["bid_open"]),
                bid_high=float(row["bid_high"]),
                bid_low=float(row["bid_low"]),
                bid_close=float(row["bid_close"]),
                ask_open=float(row["ask_open"]),
                ask_high=float(row["ask_high"]),
                ask_low=float(row["ask_low"]),
                ask_close=float(row["ask_close"]),
            )
        )
    keys = [item.key for item in values]
    if len(keys) != len(set(keys)):
        raise MarketDataParityError(
            "legacy market-data interval contains duplicate timestamp/contract keys"
        )
    return tuple(values)


def _target_key(bar: MarketBarV1) -> tuple[int, str]:
    return int(parse_utc(bar.bar_start_utc).timestamp()), bar.local_symbol


def _key_sample(key: tuple[int, str]) -> dict[str, Any]:
    return {"bar_start_ts": key[0], "local_symbol": key[1]}


def compare_market_data_databases(
    *,
    legacy_database_path: str | Path,
    legacy_table_name: str,
    target_database_path: str | Path,
    instrument_id: str,
    start_utc: str,
    end_utc: str,
    tolerance: float = 1e-9,
    max_samples: int = 20,
) -> MarketDataParityReport:
    numeric_tolerance = float(tolerance)
    if not math.isfinite(numeric_tolerance) or numeric_tolerance < 0.0:
        raise MarketDataParityError(
            f"tolerance must be finite and non-negative: {tolerance!r}"
        )
    sample_limit = int(max_samples)
    if sample_limit < 0:
        raise MarketDataParityError(
            f"max_samples must be non-negative: {max_samples!r}"
        )
    start = parse_utc(start_utc)
    end = parse_utc(end_utc)
    if end <= start:
        raise MarketDataParityError("comparison interval must be positive")

    legacy = read_legacy_complete_bars(
        legacy_database_path,
        table_name=legacy_table_name,
        start_utc=start_utc,
        end_utc=end_utc,
    )
    target = SQLiteMarketBarReader(target_database_path).read_range(
        instrument_id=instrument_id,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    legacy_by_key = {item.key: item for item in legacy}
    target_by_key = {_target_key(item): item for item in target}
    if len(target_by_key) != len(target):
        raise MarketDataParityError(
            "target market-data interval contains duplicate timestamp/contract keys"
        )

    legacy_keys = set(legacy_by_key)
    target_keys = set(target_by_key)
    common_keys = sorted(legacy_keys & target_keys)
    legacy_only = sorted(legacy_keys - target_keys)
    target_only = sorted(target_keys - legacy_keys)

    mismatches: list[dict[str, Any]] = []
    for key in common_keys:
        legacy_bar = legacy_by_key[key]
        target_bar = target_by_key[key]
        differences: dict[str, dict[str, float]] = {}
        for field in _PRICE_FIELDS:
            legacy_value = float(getattr(legacy_bar, field))
            target_value = float(getattr(target_bar, field))
            absolute_error = abs(legacy_value - target_value)
            if absolute_error > numeric_tolerance:
                differences[field] = {
                    "legacy": legacy_value,
                    "target": target_value,
                    "absolute_error": absolute_error,
                }
        if differences:
            mismatches.append(
                {
                    **_key_sample(key),
                    "target_bar_id": target_bar.bar_id,
                    "target_revision": target_bar.revision,
                    "differences": differences,
                }
            )

    return MarketDataParityReport(
        instrument_id=str(instrument_id),
        start_utc=start_utc,
        end_utc=end_utc,
        tolerance=numeric_tolerance,
        legacy_count=len(legacy),
        target_count=len(target),
        matched_count=len(common_keys) - len(mismatches),
        legacy_only_count=len(legacy_only),
        target_only_count=len(target_only),
        value_mismatch_count=len(mismatches),
        legacy_only_samples=tuple(
            _key_sample(key) for key in legacy_only[:sample_limit]
        ),
        target_only_samples=tuple(
            _key_sample(key) for key in target_only[:sample_limit]
        ),
        value_mismatch_samples=tuple(mismatches[:sample_limit]),
    )
