from dataclasses import dataclass

import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.sma_features import SMA_TABLE_NAME, get_sma_column_name
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_regression import build_linear_regression
from ib_signal.signal_regression_relation import (
    RegressionRelationKind,
    build_regression_relation,
)


@dataclass(frozen=True)
class CandidateRegimeFilterResult:
    # Relation текущего паттерна, по которому фильтровали кандидатов.
    current_relation: RegressionRelationKind

    # Количество candidate windows после проверки полноты pattern matrix.
    source_candidates_count: int

    # Количество кандидатов, прошедших Pearson threshold до regime-фильтра.
    pearson_passed_count: int

    # Количество кандидатов, оставшихся после Pearson + regime-фильтра.
    kept_candidates_count: int

    # Количество Pearson-прошедших кандидатов, у которых не удалось прочитать полную SMA.
    skipped_sma_count: int

    # Количество Pearson-прошедших кандидатов с другим relation.
    relation_mismatch_count: int

    # Кандидаты, оставшиеся после regime-фильтра.
    valid_candidates: list[CandidateWindow]

    # Price pattern matrix только по оставшимся кандидатам.
    candidate_matrix: np.ndarray

    # Pearson scores только по оставшимся кандидатам.
    pearson_scores: np.ndarray


def create_temp_candidate_sma_windows(conn, candidates: list[CandidateWindow]) -> None:
    """Что делает: создаёт временную таблицу candidate windows для чтения SMA.
    Зачем нужна: позволяет одним range join прочитать SMA для всех Pearson-прошедших кандидатов."""
    conn.execute("DROP TABLE IF EXISTS temp_candidate_sma_windows")
    conn.execute(
        """
        CREATE TEMP TABLE temp_candidate_sma_windows (
            candidate_index INTEGER PRIMARY KEY,
            pattern_start_ts INTEGER NOT NULL,
            pattern_end_ts INTEGER NOT NULL
        )
        """
    )

    conn.executemany(
        """
        INSERT INTO temp_candidate_sma_windows (
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


def read_candidate_sma_value_map(
        *,
        conn,
        candidates: list[CandidateWindow],
        period_bars: int,
        expected_points: int,
        bar_size_seconds: int,
) -> dict[int, np.ndarray]:
    """Что делает: читает SMA-ряд для candidate pattern windows.
    Зачем нужна: relation кандидата считается только по его pattern-window, без trade-window и без lookahead."""
    if not candidates:
        return {}

    create_temp_candidate_sma_windows(conn, candidates)

    raw_matrix = np.empty((len(candidates), expected_points), dtype=float)
    raw_matrix.fill(np.nan)

    counts = np.zeros((len(candidates),), dtype=np.int32)
    invalid_indices: set[int] = set()

    sma_column = quote_identifier(get_sma_column_name(period_bars))

    rows = conn.execute(
        f"""
        SELECT
            cw.candidate_index,
            sma.bar_time_ts,
            sma.{sma_column}
        FROM temp_candidate_sma_windows AS cw
        JOIN {quote_identifier(SMA_TABLE_NAME)} AS sma
          ON sma.bar_time_ts >= cw.pattern_start_ts
         AND sma.bar_time_ts < cw.pattern_end_ts
        ORDER BY
            cw.candidate_index,
            sma.bar_time_ts
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

    return {
        index: raw_matrix[index, :].copy()
        for index in range(len(candidates))
        if index not in invalid_indices and int(counts[index]) == expected_points
    }


def filter_candidates_by_regression_relation(
        *,
        instrument_code: str,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
        pearson_min: float,
        current_relation: RegressionRelationKind,
        near_threshold_bps: float,
        sma_period_bars: int = 600,
) -> CandidateRegimeFilterResult:
    """Что делает: оставляет только Pearson-прошедших кандидатов с тем же price-vs-SMA relation.
    Зачем нужна: кандидаты должны совпадать не только по форме price pattern, но и по рыночному режиму."""
    if candidate_matrix.ndim != 2:
        raise ValueError(
            f"candidate_matrix должен быть двумерным, получено shape={candidate_matrix.shape}"
        )

    if len(candidates) != int(candidate_matrix.shape[0]):
        raise ValueError(
            f"candidates и candidate_matrix не совпадают по длине: "
            f"candidates={len(candidates)}, matrix_rows={candidate_matrix.shape[0]}"
        )

    if pearson_scores.shape[0] != candidate_matrix.shape[0]:
        raise ValueError(
            f"pearson_scores и candidate_matrix не совпадают по длине: "
            f"scores={pearson_scores.shape[0]}, matrix_rows={candidate_matrix.shape[0]}"
        )

    expected_points = int(candidate_matrix.shape[1])
    passed_indices = np.flatnonzero(pearson_scores >= pearson_min)

    source_candidates_count = len(candidates)
    pearson_passed_count = int(passed_indices.size)

    if pearson_passed_count == 0:
        return CandidateRegimeFilterResult(
            current_relation=current_relation,
            source_candidates_count=source_candidates_count,
            pearson_passed_count=0,
            kept_candidates_count=0,
            skipped_sma_count=0,
            relation_mismatch_count=0,
            valid_candidates=[],
            candidate_matrix=np.empty((0, expected_points), dtype=float),
            pearson_scores=np.empty((0,), dtype=float),
        )

    passed_candidates = [
        candidates[int(index)]
        for index in passed_indices
    ]
    passed_matrix = candidate_matrix[passed_indices, :]
    passed_scores = pearson_scores[passed_indices]

    instrument_row = Instrument[instrument_code]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])
    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(
        str(feature_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        sma_value_map = read_candidate_sma_value_map(
            conn=conn,
            candidates=passed_candidates,
            period_bars=sma_period_bars,
            expected_points=expected_points,
            bar_size_seconds=bar_size_seconds,
        )

    finally:
        conn.close()

    kept_candidates: list[CandidateWindow] = []
    kept_matrix_rows: list[np.ndarray] = []
    kept_scores: list[float] = []

    skipped_sma_count = 0
    relation_mismatch_count = 0

    for passed_index, candidate in enumerate(passed_candidates):
        sma_values = sma_value_map.get(passed_index)

        if sma_values is None:
            skipped_sma_count += 1
            continue

        candidate_price_regression = build_linear_regression(passed_matrix[passed_index])
        candidate_sma_regression = build_linear_regression(sma_values)
        candidate_relation = build_regression_relation(
            base_regression=candidate_price_regression,
            reference_regression=candidate_sma_regression,
            near_threshold_bps=near_threshold_bps,
        )

        if candidate_relation.relation != current_relation:
            relation_mismatch_count += 1
            continue

        kept_candidates.append(candidate)
        kept_matrix_rows.append(passed_matrix[passed_index].copy())
        kept_scores.append(float(passed_scores[passed_index]))

    kept_matrix = (
        np.vstack(kept_matrix_rows).astype(float)
        if kept_matrix_rows
        else np.empty((0, expected_points), dtype=float)
    )
    kept_scores_array = np.asarray(kept_scores, dtype=float)

    return CandidateRegimeFilterResult(
        current_relation=current_relation,
        source_candidates_count=source_candidates_count,
        pearson_passed_count=pearson_passed_count,
        kept_candidates_count=len(kept_candidates),
        skipped_sma_count=skipped_sma_count,
        relation_mismatch_count=relation_mismatch_count,
        valid_candidates=kept_candidates,
        candidate_matrix=kept_matrix,
        pearson_scores=kept_scores_array,
    )


def format_candidate_regime_filter_result(result: CandidateRegimeFilterResult | None) -> str:
    """Что делает: форматирует результат regime-фильтра кандидатов для runtime-лога.
    Зачем нужна: видно, сколько кандидатов отсекает совпадение relation."""
    if result is None:
        return "disabled"

    return (
        f"enabled=True, "
        f"current_relation={result.current_relation}, "
        f"source_candidates={result.source_candidates_count}, "
        f"pearson_passed={result.pearson_passed_count}, "
        f"kept={result.kept_candidates_count}, "
        f"skipped_sma={result.skipped_sma_count}, "
        f"relation_mismatch={result.relation_mismatch_count}"
    )
