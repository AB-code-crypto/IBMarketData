from dataclasses import dataclass

import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.sma_features import SMA_TABLE_NAME, get_sma_column_name
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_config import MarketRegimeFilterMode
from ib_signal.signal_regression import (
    RegressionDirection,
    build_linear_regression,
    classify_regression_direction,
)
from ib_signal.signal_regression_relation import (
    RegressionRelationKind,
    build_regression_relation,
)


@dataclass(frozen=True)
class CandidateRegimeFilterResult:
    # Активный режим фильтрации: OFF/SOFT/HARD.
    mode: MarketRegimeFilterMode

    # Relation текущего паттерна.
    current_relation: RegressionRelationKind

    # Direction текущей price-regression.
    current_price_direction: RegressionDirection

    # Direction текущей SMA 600 regression.
    current_sma600_direction: RegressionDirection

    # Количество candidate windows после проверки полноты pattern matrix.
    source_candidates_count: int

    # Количество кандидатов, прошедших Pearson threshold до regime-фильтра.
    pearson_passed_count: int

    # Диагностика: сколько кандидатов осталось бы при SOFT-фильтре.
    soft_kept_count: int

    # Диагностика: сколько кандидатов осталось бы при HARD-фильтре.
    hard_kept_count: int

    # Итоговое количество кандидатов по активному mode.
    final_kept_count: int

    # Количество Pearson-прошедших кандидатов, у которых не удалось прочитать полную SMA.
    skipped_sma_count: int

    # Количество Pearson-прошедших кандидатов с другим relation.
    relation_mismatch_count: int

    # Количество SOFT-прошедших кандидатов с другим direction.
    direction_mismatch_count: int

    # Кандидаты, оставшиеся после активного режима фильтрации.
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


def build_empty_filter_result(
        *,
        mode: MarketRegimeFilterMode,
        current_relation: RegressionRelationKind,
        current_price_direction: RegressionDirection,
        current_sma600_direction: RegressionDirection,
        source_candidates_count: int,
        expected_points: int,
) -> CandidateRegimeFilterResult:
    """Что делает: возвращает пустой результат фильтра.
    Зачем нужна: общий формат диагностики сохраняется даже когда Pearson никого не пропустил."""
    return CandidateRegimeFilterResult(
        mode=mode,
        current_relation=current_relation,
        current_price_direction=current_price_direction,
        current_sma600_direction=current_sma600_direction,
        source_candidates_count=source_candidates_count,
        pearson_passed_count=0,
        soft_kept_count=0,
        hard_kept_count=0,
        final_kept_count=0,
        skipped_sma_count=0,
        relation_mismatch_count=0,
        direction_mismatch_count=0,
        valid_candidates=[],
        candidate_matrix=np.empty((0, expected_points), dtype=float),
        pearson_scores=np.empty((0,), dtype=float),
    )


def filter_candidates_by_market_regime(
        *,
        instrument_code: str,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
        pearson_min: float,
        current_relation: RegressionRelationKind,
        current_price_direction: RegressionDirection,
        current_sma600_direction: RegressionDirection,
        near_threshold_bps: float,
        flat_delta_threshold_bps: float,
        mode: MarketRegimeFilterMode,
        sma_period_bars: int = 600,
) -> CandidateRegimeFilterResult:
    """Что делает: считает SOFT/HARD диагностику и возвращает кандидатов по активному mode.
    Зачем нужна: можно сравнивать OFF/SOFT/HARD, не переписывая остальную signal-логику."""
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
    source_candidates_count = len(candidates)
    passed_indices = np.flatnonzero(pearson_scores >= pearson_min)
    pearson_passed_count = int(passed_indices.size)

    if pearson_passed_count == 0:
        return build_empty_filter_result(
            mode=mode,
            current_relation=current_relation,
            current_price_direction=current_price_direction,
            current_sma600_direction=current_sma600_direction,
            source_candidates_count=source_candidates_count,
            expected_points=expected_points,
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

    soft_candidates: list[CandidateWindow] = []
    soft_matrix_rows: list[np.ndarray] = []
    soft_scores: list[float] = []

    hard_candidates: list[CandidateWindow] = []
    hard_matrix_rows: list[np.ndarray] = []
    hard_scores: list[float] = []

    skipped_sma_count = 0
    relation_mismatch_count = 0
    direction_mismatch_count = 0

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

        soft_candidates.append(candidate)
        soft_matrix_rows.append(passed_matrix[passed_index].copy())
        soft_scores.append(float(passed_scores[passed_index]))

        candidate_price_direction = classify_regression_direction(
            candidate_price_regression,
            flat_delta_threshold_bps=flat_delta_threshold_bps,
        )
        candidate_sma600_direction = classify_regression_direction(
            candidate_sma_regression,
            flat_delta_threshold_bps=flat_delta_threshold_bps,
        )

        if (
                candidate_price_direction == current_price_direction
                and candidate_sma600_direction == current_sma600_direction
        ):
            hard_candidates.append(candidate)
            hard_matrix_rows.append(passed_matrix[passed_index].copy())
            hard_scores.append(float(passed_scores[passed_index]))
        else:
            direction_mismatch_count += 1

    if mode == MarketRegimeFilterMode.OFF:
        final_candidates = passed_candidates
        final_matrix_rows = [passed_matrix[index].copy() for index in range(passed_matrix.shape[0])]
        final_scores = [float(score) for score in passed_scores]

    elif mode == MarketRegimeFilterMode.SOFT:
        final_candidates = soft_candidates
        final_matrix_rows = soft_matrix_rows
        final_scores = soft_scores

    elif mode == MarketRegimeFilterMode.HARD:
        final_candidates = hard_candidates
        final_matrix_rows = hard_matrix_rows
        final_scores = hard_scores

    else:
        raise ValueError(f"Неподдерживаемый MarketRegimeFilterMode: {mode}")

    final_matrix = (
        np.vstack(final_matrix_rows).astype(float)
        if final_matrix_rows
        else np.empty((0, expected_points), dtype=float)
    )
    final_scores_array = np.asarray(final_scores, dtype=float)

    return CandidateRegimeFilterResult(
        mode=mode,
        current_relation=current_relation,
        current_price_direction=current_price_direction,
        current_sma600_direction=current_sma600_direction,
        source_candidates_count=source_candidates_count,
        pearson_passed_count=pearson_passed_count,
        soft_kept_count=len(soft_candidates),
        hard_kept_count=len(hard_candidates),
        final_kept_count=len(final_candidates),
        skipped_sma_count=skipped_sma_count,
        relation_mismatch_count=relation_mismatch_count,
        direction_mismatch_count=direction_mismatch_count,
        valid_candidates=final_candidates,
        candidate_matrix=final_matrix,
        pearson_scores=final_scores_array,
    )


def format_candidate_regime_filter_result(
        result: CandidateRegimeFilterResult | None,
        *,
        mode: MarketRegimeFilterMode,
        pearson_passed_count: int,
) -> str:
    """Что делает: форматирует результат regime-фильтра кандидатов для runtime-лога.
    Зачем нужна: лог должен показывать OFF/SOFT/HARD и диагностические soft/hard counts."""
    if result is None:
        return (
            f"mode={mode.value}, "
            f"soft_kept=n/a, "
            f"hard_kept=n/a, "
            f"final_kept={pearson_passed_count}, "
            f"diagnostics=not_calculated"
        )

    return (
        f"mode={result.mode.value}, "
        f"current={result.current_price_direction}/{result.current_sma600_direction}/{result.current_relation}, "
        f"soft_kept={result.soft_kept_count}, "
        f"hard_kept={result.hard_kept_count}, "
        f"final_kept={result.final_kept_count}, "
        f"skipped_sma={result.skipped_sma_count}, "
        f"relation_mismatch={result.relation_mismatch_count}, "
        f"direction_mismatch={result.direction_mismatch_count}"
    )
