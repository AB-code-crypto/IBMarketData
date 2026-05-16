from dataclasses import dataclass

import numpy as np

from ib_signal.signal_candidates import CandidateWindow


@dataclass(frozen=True)
class PatternPathFeatures:
    # Первая цена pattern-window.
    first_value: float

    # Последняя цена pattern-window.
    last_value: float

    # Минимум цены внутри pattern-window.
    min_value: float

    # Максимум цены внутри pattern-window.
    max_value: float

    # Знаковое расстояние от первой до последней точки в пунктах.
    net_delta_points: float

    # Знаковое расстояние от первой до последней точки в базисных пунктах.
    net_delta_bps: float

    # Размах max-min в пунктах.
    range_points: float

    # Размах max-min в базисных пунктах.
    range_bps: float

    # Положение последней цены внутри диапазона min-max: 0=min, 1=max.
    end_position: float


@dataclass(frozen=True)
class CandidatePathFeatureRow:
    candidate: CandidateWindow
    pearson_score: float
    path_features: PatternPathFeatures


@dataclass(frozen=True)
class CandidatePathFeatureResult:
    current_features: PatternPathFeatures
    candidate_rows: list[CandidatePathFeatureRow]


@dataclass(frozen=True)
class CandidateRangeHardFilterResult:
    # Максимально допустимое отношение range_bps.
    max_ratio: float

    # Причина отключения фильтра. None означает, что фильтр реально работал.
    disabled_reason: str | None

    # Range текущего паттерна в bps.
    current_range_bps: float

    # Количество кандидатов на входе фильтра.
    source_candidates_count: int

    # Количество кандидатов, оставшихся после фильтра.
    kept_candidates_count: int

    # Количество кандидатов, отброшенных по range-ratio.
    dropped_candidates_count: int

    # Кандидаты, оставшиеся после range hard filter.
    valid_candidates: list[CandidateWindow]

    # Candidate matrix только по оставшимся кандидатам.
    candidate_matrix: np.ndarray

    # Pearson scores только по оставшимся кандидатам.
    pearson_scores: np.ndarray


def calculate_bps(
        *,
        value: float,
        base_value: float,
) -> float:
    """Что делает: переводит значение из пунктов в базисные пункты относительно base_value.
    Зачем нужна: path-features должны быть сопоставимы при разных уровнях цены."""
    base_abs = abs(float(base_value))

    if base_abs == 0.0:
        raise ValueError("Невозможно посчитать bps: base_value равен нулю")

    return float(value) / base_abs * 10000.0


def calculate_pattern_path_features(values: np.ndarray) -> PatternPathFeatures:
    """Что делает: считает raw path-features по одному pattern-window.
    Зачем нужна: эти признаки позже будут использоваться для ранжирования кандидатов."""
    y = np.asarray(values, dtype=float)

    if y.ndim != 1:
        raise ValueError(
            f"Для path-features нужен одномерный ряд, получено shape={y.shape}"
        )

    if y.size < 2:
        raise ValueError(
            f"Для path-features нужно минимум две точки, получено: {y.size}"
        )

    if not np.all(np.isfinite(y)):
        raise ValueError("Для path-features ряд не должен содержать NaN или inf")

    first_value = float(y[0])
    last_value = float(y[-1])
    min_value = float(y.min())
    max_value = float(y.max())

    net_delta_points = last_value - first_value
    net_delta_bps = calculate_bps(
        value=net_delta_points,
        base_value=first_value,
    )

    range_points = max_value - min_value
    range_bps = calculate_bps(
        value=range_points,
        base_value=first_value,
    )

    if range_points == 0.0:
        end_position = 0.5
    else:
        end_position = (last_value - min_value) / range_points

    return PatternPathFeatures(
        first_value=first_value,
        last_value=last_value,
        min_value=min_value,
        max_value=max_value,
        net_delta_points=float(net_delta_points),
        net_delta_bps=float(net_delta_bps),
        range_points=float(range_points),
        range_bps=float(range_bps),
        end_position=float(end_position),
    )


def build_candidate_path_feature_result(
        *,
        current_values: np.ndarray,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
) -> CandidatePathFeatureResult:
    """Что делает: считает path-features текущего паттерна и выбранных кандидатов.
    Зачем нужна: это диагностический слой перед нормализацией и итоговым ranking score."""
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

    current_features = calculate_pattern_path_features(current_values)

    candidate_rows = [
        CandidatePathFeatureRow(
            candidate=candidates[index],
            pearson_score=float(pearson_scores[index]),
            path_features=calculate_pattern_path_features(candidate_matrix[index, :]),
        )
        for index in range(len(candidates))
    ]

    return CandidatePathFeatureResult(
        current_features=current_features,
        candidate_rows=candidate_rows,
    )


def calculate_range_ratio(
        *,
        current_range_bps: float,
        candidate_range_bps: float,
) -> float:
    """Что делает: считает симметричное отношение range кандидата и текущего паттерна.
    Зачем нужна: range hard filter должен одинаково отсекать слишком широкий и слишком узкий масштаб."""
    current_range = float(current_range_bps)
    candidate_range = float(candidate_range_bps)

    if current_range < 0.0 or candidate_range < 0.0:
        raise ValueError(
            f"Range не может быть отрицательным: "
            f"current={current_range}, candidate={candidate_range}"
        )

    if current_range == 0.0 and candidate_range == 0.0:
        return 1.0

    if current_range == 0.0 or candidate_range == 0.0:
        return float("inf")

    return max(current_range, candidate_range) / min(current_range, candidate_range)


def filter_candidates_by_range_ratio(
        *,
        current_values: np.ndarray,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
        max_ratio: float,
) -> CandidateRangeHardFilterResult:
    """Что делает: отсекает кандидатов с резко отличающимся range_bps.
    Зачем нужна: Pearson может находить похожую форму при совершенно другом масштабе движения."""
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
    current_features = calculate_pattern_path_features(current_values)
    threshold_ratio = float(max_ratio)

    if threshold_ratio <= 0.0:
        return CandidateRangeHardFilterResult(
            max_ratio=threshold_ratio,
            disabled_reason="max_ratio_disabled",
            current_range_bps=current_features.range_bps,
            source_candidates_count=source_candidates_count,
            kept_candidates_count=source_candidates_count,
            dropped_candidates_count=0,
            valid_candidates=list(candidates),
            candidate_matrix=candidate_matrix.copy(),
            pearson_scores=pearson_scores.copy(),
        )

    if current_features.range_bps == 0.0:
        return CandidateRangeHardFilterResult(
            max_ratio=threshold_ratio,
            disabled_reason="current_range_zero",
            current_range_bps=current_features.range_bps,
            source_candidates_count=source_candidates_count,
            kept_candidates_count=source_candidates_count,
            dropped_candidates_count=0,
            valid_candidates=list(candidates),
            candidate_matrix=candidate_matrix.copy(),
            pearson_scores=pearson_scores.copy(),
        )

    kept_candidates: list[CandidateWindow] = []
    kept_matrix_rows: list[np.ndarray] = []
    kept_scores: list[float] = []
    dropped_count = 0

    for index, candidate in enumerate(candidates):
        candidate_features = calculate_pattern_path_features(candidate_matrix[index, :])
        range_ratio = calculate_range_ratio(
            current_range_bps=current_features.range_bps,
            candidate_range_bps=candidate_features.range_bps,
        )

        if range_ratio > threshold_ratio:
            dropped_count += 1
            continue

        kept_candidates.append(candidate)
        kept_matrix_rows.append(candidate_matrix[index, :].copy())
        kept_scores.append(float(pearson_scores[index]))

    kept_matrix = (
        np.vstack(kept_matrix_rows).astype(float)
        if kept_matrix_rows
        else np.empty((0, expected_points), dtype=float)
    )
    kept_scores_array = np.asarray(kept_scores, dtype=float)

    return CandidateRangeHardFilterResult(
        max_ratio=threshold_ratio,
        disabled_reason=None,
        current_range_bps=current_features.range_bps,
        source_candidates_count=source_candidates_count,
        kept_candidates_count=len(kept_candidates),
        dropped_candidates_count=dropped_count,
        valid_candidates=kept_candidates,
        candidate_matrix=kept_matrix,
        pearson_scores=kept_scores_array,
    )


def format_candidate_range_hard_filter_result(result: CandidateRangeHardFilterResult) -> str:
    """Что делает: форматирует результат hard filter по range.
    Зачем нужна: в runtime-логе должно быть видно, сколько кандидатов убил масштабный фильтр."""
    if result.disabled_reason is not None:
        return (
            f"off, reason={result.disabled_reason}, "
            f"max_ratio={result.max_ratio:.2f}, "
            f"current_range={result.current_range_bps:.2f} bps, "
            f"kept={result.kept_candidates_count}/{result.source_candidates_count}"
        )

    return (
        f"on, max_ratio={result.max_ratio:.2f}, "
        f"current_range={result.current_range_bps:.2f} bps, "
        f"kept={result.kept_candidates_count}/{result.source_candidates_count}, "
        f"dropped={result.dropped_candidates_count}"
    )


def format_path_features(features: PatternPathFeatures) -> str:
    """Что делает: компактно форматирует path-features.
    Зачем нужна: runtime-лог должен показывать raw значения без перегруза."""
    return (
        f"net={features.net_delta_bps:.2f} bps / {features.net_delta_points:.2f} pt, "
        f"range={features.range_bps:.2f} bps / {features.range_points:.2f} pt, "
        f"end={features.end_position:.2f}"
    )


def format_candidate_path_feature_result(
        result: CandidatePathFeatureResult,
        *,
        top_limit: int = 3,
) -> str:
    """Что делает: форматирует path-features для текущего паттерна и top-кандидатов по Pearson.
    Зачем нужна: пока scoring ещё не включён, надо видеть raw ranking-признаки в логе."""
    if top_limit <= 0 or not result.candidate_rows:
        top_text = "top_by_pearson=[]"

    else:
        top_rows = sorted(
            result.candidate_rows,
            key=lambda row: row.pearson_score,
            reverse=True,
        )[:top_limit]

        top_parts = [
            (
                f"{index}. {row.candidate.signal_bar_time_ct} CT "
                f"r={row.pearson_score:.2f}, "
                f"{format_path_features(row.path_features)}"
            )
            for index, row in enumerate(top_rows, start=1)
        ]
        top_text = "top_by_pearson=[" + " | ".join(top_parts) + "]"

    return (
        f"current={format_path_features(result.current_features)}; "
        f"candidates={len(result.candidate_rows)}; "
        f"{top_text}"
    )
