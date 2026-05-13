from dataclasses import dataclass

import sqlite3
import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.signal_window import SignalWindow

PRICE_SOURCE_COLUMNS = {
    "mid_open",
    "mid_high",
    "mid_low",
    "mid_close",
    "spread_open",
    "spread_high",
    "spread_low",
    "spread_close",
}


@dataclass(frozen=True)
class PatternMatrixResult:
    current_values: np.ndarray
    candidate_matrix: np.ndarray
    valid_candidates: list[CandidateWindow]
    skipped_candidates_count: int
    expected_points: int


def validate_price_source(price_source: str) -> None:
    """Что делает: проверяет, что price_source является разрешённой колонкой job DB.
    Зачем нужна: имя колонки нельзя подставлять в SQL без белого списка."""
    if price_source not in PRICE_SOURCE_COLUMNS:
        raise ValueError(
            f"Неподдерживаемый price_source={price_source!r}. "
            f"Допустимо: {sorted(PRICE_SOURCE_COLUMNS)}"
        )


def get_expected_points(window: SignalWindow, bar_size_seconds: int) -> int:
    """Что делает: считает ожидаемое количество точек в pattern window.
    Зачем нужна: Pearson требует одинаковую длину текущего и всех candidate-паттернов."""
    if window.pattern_seconds % bar_size_seconds != 0:
        raise ValueError(
            "Длина pattern window должна быть кратна размеру бара: "
            f"pattern_seconds={window.pattern_seconds}, "
            f"bar_size_seconds={bar_size_seconds}"
        )

    expected_points = window.pattern_seconds // bar_size_seconds

    if expected_points < 2:
        raise ValueError(
            f"Для Pearson нужно минимум две точки, получено: {expected_points}"
        )

    return expected_points


def validate_pattern_rows(
        *,
        rows: list[tuple[int, float]],
        start_ts: int,
        expected_points: int,
        bar_size_seconds: int,
        context: str,
) -> np.ndarray:
    """Что делает: проверяет полноту и шаг одного pattern window и возвращает NumPy-вектор.
    Зачем нужна: окна с дырками или неверной длиной нельзя отдавать в Pearson."""
    if len(rows) != expected_points:
        raise ValueError(
            f"{context}: неправильное количество точек: "
            f"expected={expected_points}, actual={len(rows)}"
        )

    values = np.empty((expected_points,), dtype=float)

    for index, (bar_time_ts, value) in enumerate(rows):
        expected_ts = start_ts + index * bar_size_seconds

        if int(bar_time_ts) != expected_ts:
            raise ValueError(
                f"{context}: дырка или неверный шаг в pattern window: "
                f"index={index}, expected_ts={expected_ts}, actual_ts={bar_time_ts}"
            )

        values[index] = float(value)

    return values


def read_current_pattern_values(
        *,
        conn,
        window: SignalWindow,
        price_source: str,
        expected_points: int,
        bar_size_seconds: int,
) -> np.ndarray:
    """Что делает: читает текущий pattern window из job DB и валидирует его полноту.
    Зачем нужна: текущий паттерн — reference-вектор для всех Pearson-сравнений."""
    rows = [
        (int(row[0]), float(row[1]))
        for row in conn.execute(
            f"""
            SELECT
                bar_time_ts,
                {quote_identifier(price_source)}
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            WHERE bar_time_ts >= ?
              AND bar_time_ts < ?
            ORDER BY bar_time_ts
            """,
            (window.pattern_start_ts, window.pattern_end_ts),
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
    """Что делает: создаёт временную таблицу candidate windows в SQLite-соединении.
    Зачем нужна: позволяет одним range join прочитать все candidate-паттерны без тысяч отдельных запросов."""
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
            candidate_index,
            pattern_start_ts,
            pattern_end_ts
        )
        VALUES (?, ?, ?)
        """,
        [
            (index, candidate.pattern_start_ts, candidate.pattern_end_ts)
            for index, candidate in enumerate(candidates)
        ],
    )


def read_candidate_pattern_matrix(
        *,
        conn,
        candidates: list[CandidateWindow],
        price_source: str,
        expected_points: int,
        bar_size_seconds: int,
) -> tuple[np.ndarray, list[CandidateWindow], int]:
    """Что делает: читает candidate-паттерны, отбрасывает неполные окна и возвращает NumPy-матрицу.
    Зачем нужна: Pearson считаем только по окнам без дырок и с точной длиной current pattern."""
    if not candidates:
        return (
            np.empty((0, expected_points), dtype=float),
            [],
            0,
        )

    create_temp_candidate_windows(conn, candidates)

    raw_matrix = np.empty((len(candidates), expected_points), dtype=float)
    raw_matrix.fill(np.nan)

    counts = np.zeros((len(candidates),), dtype=np.int32)
    invalid_indices: set[int] = set()

    rows = conn.execute(
        f"""
        SELECT
            cw.candidate_index,
            mp.bar_time_ts,
            mp.{quote_identifier(price_source)}
        FROM temp_candidate_windows AS cw
        JOIN {quote_identifier(MID_PRICE_TABLE_NAME)} AS mp
          ON mp.bar_time_ts >= cw.pattern_start_ts
         AND mp.bar_time_ts < cw.pattern_end_ts
        ORDER BY
            cw.candidate_index,
            mp.bar_time_ts
        """
    )

    for candidate_index_raw, bar_time_ts_raw, value_raw in rows:
        candidate_index = int(candidate_index_raw)

        if candidate_index in invalid_indices:
            continue

        point_index = int(counts[candidate_index])

        if point_index >= expected_points:
            invalid_indices.add(candidate_index)
            continue

        expected_ts = (
            candidates[candidate_index].pattern_start_ts
            + point_index * bar_size_seconds
        )

        if int(bar_time_ts_raw) != expected_ts or value_raw is None:
            invalid_indices.add(candidate_index)
            continue

        raw_matrix[candidate_index, point_index] = float(value_raw)
        counts[candidate_index] += 1

    valid_indices = [
        index
        for index in range(len(candidates))
        if index not in invalid_indices and int(counts[index]) == expected_points
    ]

    valid_candidates = [
        candidates[index]
        for index in valid_indices
    ]

    if valid_indices:
        candidate_matrix = raw_matrix[valid_indices, :]
    else:
        candidate_matrix = np.empty((0, expected_points), dtype=float)

    skipped_count = len(candidates) - len(valid_candidates)

    return candidate_matrix, valid_candidates, skipped_count


def build_pattern_matrix(
        *,
        instrument_code: str,
        window: SignalWindow,
        candidates: list[CandidateWindow],
        price_source: str,
) -> PatternMatrixResult:
    """Что делает: собирает current-вектор и candidate-матрицу значений price_source.
    Зачем нужна: это готовый вход для векторного расчёта Pearson через NumPy."""
    validate_price_source(price_source)

    instrument_row = Instrument[instrument_code]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])
    expected_points = get_expected_points(window, bar_size_seconds)

    job_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        try:
            current_values = read_current_pattern_values(
                conn=conn,
                window=window,
                price_source=price_source,
                expected_points=expected_points,
                bar_size_seconds=bar_size_seconds,
            )
        except ValueError as exc:
            raise SignalDataNotReadyError(str(exc)) from exc

        candidate_matrix, valid_candidates, skipped_candidates_count = (
            read_candidate_pattern_matrix(
                conn=conn,
                candidates=candidates,
                price_source=price_source,
                expected_points=expected_points,
                bar_size_seconds=bar_size_seconds,
            )
        )

        return PatternMatrixResult(
            current_values=current_values,
            candidate_matrix=candidate_matrix,
            valid_candidates=valid_candidates,
            skipped_candidates_count=skipped_candidates_count,
            expected_points=expected_points,
        )

    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            raise SignalDataNotReadyError(
                f"job DB locked during pattern matrix build: {exc}"
            ) from exc
        raise

    finally:
        conn.close()


def format_pattern_matrix_result(result: PatternMatrixResult) -> str:
    """Что делает: форматирует результат сборки pattern matrix для runtime-лога.
    Зачем нужна: быстро видно, сколько кандидатов осталось после проверки полноты окон."""
    return (
        f"points={result.expected_points}, "
        f"current_shape={result.current_values.shape}, "
        f"candidate_matrix_shape={result.candidate_matrix.shape}, "
        f"valid_candidates={len(result.valid_candidates)}, "
        f"skipped_candidates={result.skipped_candidates_count}"
    )
