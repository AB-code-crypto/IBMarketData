from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.price_source import (
    complete_mid_close_predicate,
    get_price_db_target,
    mid_close_sql,
    quote_identifier,
)
from core.sqlite_utils import open_sqlite_connection
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.signal_window import SignalWindow


@dataclass(frozen=True)
class PatternMatrixResult:
    current_values: np.ndarray
    candidate_matrix: np.ndarray
    valid_candidates: list[CandidateWindow]
    skipped_candidates_count: int
    expected_points: int


def get_expected_points(window: SignalWindow, bar_size_seconds: int) -> int:
    if window.pattern_seconds % int(bar_size_seconds) != 0:
        raise ValueError(
            "Длина pattern window должна быть кратна размеру бара: "
            f"pattern_seconds={window.pattern_seconds}, bar_size_seconds={bar_size_seconds}"
        )
    expected = window.pattern_seconds // int(bar_size_seconds)
    if expected < 2:
        raise ValueError(f"Для Pearson нужно минимум две точки, получено: {expected}")
    return expected


def validate_pattern_rows(
        *,
        rows: list[tuple[int, float]],
        start_ts: int,
        expected_points: int,
        bar_size_seconds: int,
        context: str,
) -> np.ndarray:
    if len(rows) != int(expected_points):
        raise ValueError(
            f"{context}: неправильное количество точек: "
            f"expected={expected_points}, actual={len(rows)}"
        )
    values = np.empty((expected_points,), dtype=float)
    for index, (bar_time_ts, value) in enumerate(rows):
        expected_ts = int(start_ts) + index * int(bar_size_seconds)
        if int(bar_time_ts) != expected_ts:
            raise ValueError(
                f"{context}: дырка или неверный шаг: index={index}, "
                f"expected_ts={expected_ts}, actual_ts={bar_time_ts}"
            )
        values[index] = float(value)
    return values


def read_current_pattern_values(
        *,
        conn,
        instrument_code: str,
        table_name: str,
        window: SignalWindow,
        expected_points: int,
        bar_size_seconds: int,
) -> np.ndarray:
    rows = [
        (int(row[0]), float(row[1]))
        for row in conn.execute(
            f"""
            SELECT bar_time_ts, {mid_close_sql(instrument_code)} AS mid_close
            FROM {quote_identifier(table_name)}
            WHERE bar_time_ts >= ?
              AND bar_time_ts < ?
              AND {complete_mid_close_predicate()}
            ORDER BY bar_time_ts
            """,
            (int(window.pattern_start_ts), int(window.pattern_end_ts)),
        ).fetchall()
    ]
    return validate_pattern_rows(
        rows=rows,
        start_ts=window.pattern_start_ts,
        expected_points=expected_points,
        bar_size_seconds=bar_size_seconds,
        context="current pattern",
    )


def create_temp_candidate_windows(conn, candidates: list[CandidateWindow]) -> None:
    conn.execute("DROP TABLE IF EXISTS temp_candidate_windows")
    conn.execute(
        """
        CREATE TEMP TABLE temp_candidate_windows (
            candidate_index INTEGER PRIMARY KEY,
            pattern_start_ts INTEGER NOT NULL,
            pattern_end_ts INTEGER NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO temp_candidate_windows (
            candidate_index, pattern_start_ts, pattern_end_ts
        ) VALUES (?, ?, ?)
        """,
        [
            (index, candidate.pattern_start_ts, candidate.pattern_end_ts)
            for index, candidate in enumerate(candidates)
        ],
    )


def read_candidate_pattern_matrix(
        *,
        conn,
        instrument_code: str,
        table_name: str,
        candidates: list[CandidateWindow],
        expected_points: int,
        bar_size_seconds: int,
) -> tuple[np.ndarray, list[CandidateWindow], int]:
    if not candidates:
        return np.empty((0, expected_points), dtype=float), [], 0

    create_temp_candidate_windows(conn, candidates)
    raw_matrix = np.full((len(candidates), expected_points), np.nan, dtype=float)
    counts = np.zeros((len(candidates),), dtype=np.int32)
    invalid_indices: set[int] = set()
    midpoint = mid_close_sql(instrument_code, table_alias="p")

    rows = conn.execute(
        f"""
        SELECT cw.candidate_index, p.bar_time_ts, {midpoint} AS mid_close
        FROM temp_candidate_windows AS cw
        JOIN {quote_identifier(table_name)} AS p
          ON p.bar_time_ts >= cw.pattern_start_ts
         AND p.bar_time_ts < cw.pattern_end_ts
        WHERE {complete_mid_close_predicate(table_alias='p')}
        ORDER BY cw.candidate_index, p.bar_time_ts
        """
    )

    for candidate_index_raw, bar_ts_raw, value_raw in rows:
        candidate_index = int(candidate_index_raw)
        if candidate_index in invalid_indices:
            continue
        point_index = int(counts[candidate_index])
        if point_index >= expected_points:
            invalid_indices.add(candidate_index)
            continue
        expected_ts = (
            candidates[candidate_index].pattern_start_ts
            + point_index * int(bar_size_seconds)
        )
        if int(bar_ts_raw) != expected_ts or value_raw is None:
            invalid_indices.add(candidate_index)
            continue
        raw_matrix[candidate_index, point_index] = float(value_raw)
        counts[candidate_index] += 1

    valid_indices = [
        index
        for index in range(len(candidates))
        if index not in invalid_indices and int(counts[index]) == expected_points
    ]
    valid_candidates = [candidates[index] for index in valid_indices]
    matrix = (
        raw_matrix[valid_indices, :]
        if valid_indices
        else np.empty((0, expected_points), dtype=float)
    )
    return matrix, valid_candidates, len(candidates) - len(valid_candidates)


def build_pattern_matrix(
        *,
        instrument_code: str,
        window: SignalWindow,
        candidates: list[CandidateWindow],
) -> PatternMatrixResult:
    instrument_row = Instrument[instrument_code]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])
    expected_points = get_expected_points(window, bar_size_seconds)
    target = get_price_db_target(instrument_code)
    conn = open_sqlite_connection(
        str(target.db_path),
        require_existing_file=True,
        use_wal=False,
    )
    try:
        try:
            current_values = read_current_pattern_values(
                conn=conn,
                instrument_code=instrument_code,
                table_name=target.table_name,
                window=window,
                expected_points=expected_points,
                bar_size_seconds=bar_size_seconds,
            )
        except ValueError as exc:
            raise SignalDataNotReadyError(str(exc)) from exc
        matrix, valid, skipped = read_candidate_pattern_matrix(
            conn=conn,
            instrument_code=instrument_code,
            table_name=target.table_name,
            candidates=candidates,
            expected_points=expected_points,
            bar_size_seconds=bar_size_seconds,
        )
        return PatternMatrixResult(
            current_values=current_values,
            candidate_matrix=matrix,
            valid_candidates=valid,
            skipped_candidates_count=skipped,
            expected_points=expected_points,
        )
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            raise SignalDataNotReadyError(
                f"price DB locked during pattern matrix build: {exc}"
            ) from exc
        raise
    finally:
        conn.close()


def format_pattern_matrix_result(result: PatternMatrixResult) -> str:
    return (
        f"points={result.expected_points}, current_shape={result.current_values.shape}, "
        f"candidate_matrix_shape={result.candidate_matrix.shape}, "
        f"valid_candidates={len(result.valid_candidates)}, "
        f"skipped_candidates={result.skipped_candidates_count}"
    )
