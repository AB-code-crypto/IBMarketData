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

    # Знаковая конечная дельта: last - first в пунктах.
    end_delta_points: float

    # Знаковая конечная дельта: last - first в базисных пунктах.
    end_delta_bps: float

    # Размах min-max в пунктах.
    minmax_points: float

    # Размах min-max в базисных пунктах.
    minmax_bps: float


@dataclass(frozen=True)
class CandidateMinmaxHardFilterResult:
    # Максимально допустимое отношение minmax_bps.
    max_ratio: float

    # Причина отключения фильтра. None означает, что фильтр реально работал.
    disabled_reason: str | None

    # Minmax текущего паттерна в bps.
    current_minmax_bps: float

    # Количество кандидатов на входе фильтра.
    source_candidates_count: int

    # Количество кандидатов, оставшихся после фильтра.
    kept_candidates_count: int

    # Количество кандидатов, отброшенных по minmax-ratio.
    dropped_candidates_count: int

    # Кандидаты, оставшиеся после minmax hard filter.
    valid_candidates: list[CandidateWindow]

    # Candidate matrix только по оставшимся кандидатам.
    candidate_matrix: np.ndarray

    # Pearson scores только по оставшимся кандидатам.
    pearson_scores: np.ndarray


@dataclass(frozen=True)
class CandidateScoreRow:
    candidate: CandidateWindow
    pearson_score: float
    path_features: PatternPathFeatures

    # Score-компоненты 0..1.
    pearson_component: float
    end_delta_component: float
    minmax_component: float

    # Итоговый score 0..1.
    total_score: float


@dataclass(frozen=True)
class CandidateScoreResult:
    current_features: PatternPathFeatures
    rows: list[CandidateScoreRow]

    # Кандидаты, отсортированные по total_score desc.
    valid_candidates: list[CandidateWindow]

    # Candidate matrix в том же порядке, что valid_candidates.
    candidate_matrix: np.ndarray

    # Pearson scores в том же порядке, что valid_candidates.
    pearson_scores: np.ndarray

    # Total scores в том же порядке, что valid_candidates.
    candidate_scores: np.ndarray

    pearson_weight: float
    end_delta_weight: float
    minmax_weight: float
    active_weight_sum: float


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
    Зачем нужна: эти признаки позже используются для scoring и сортировки кандидатов."""
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

    end_delta_points = last_value - first_value
    end_delta_bps = calculate_bps(
        value=end_delta_points,
        base_value=first_value,
    )

    minmax_points = max_value - min_value
    minmax_bps = calculate_bps(
        value=minmax_points,
        base_value=first_value,
    )

    return PatternPathFeatures(
        first_value=first_value,
        last_value=last_value,
        min_value=min_value,
        max_value=max_value,
        end_delta_points=float(end_delta_points),
        end_delta_bps=float(end_delta_bps),
        minmax_points=float(minmax_points),
        minmax_bps=float(minmax_bps),
    )


def validate_candidate_feature_inputs(
        *,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
) -> None:
    """Что делает: валидирует согласованность candidates / matrix / scores.
    Зачем нужна: scoring и hard filters должны работать только с одинаковыми индексами."""
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


def calculate_minmax_ratio(
        *,
        current_minmax_bps: float,
        candidate_minmax_bps: float,
) -> float:
    """Что делает: считает симметричное отношение minmax кандидата и текущего паттерна.
    Зачем нужна: minmax hard filter должен одинаково отсекать слишком широкий и слишком узкий масштаб."""
    current_minmax = float(current_minmax_bps)
    candidate_minmax = float(candidate_minmax_bps)

    if current_minmax < 0.0 or candidate_minmax < 0.0:
        raise ValueError(
            f"Minmax не может быть отрицательным: "
            f"current={current_minmax}, candidate={candidate_minmax}"
        )

    if current_minmax == 0.0 and candidate_minmax == 0.0:
        return 1.0

    if current_minmax == 0.0 or candidate_minmax == 0.0:
        return float("inf")

    return max(current_minmax, candidate_minmax) / min(current_minmax, candidate_minmax)


def filter_candidates_by_minmax_ratio(
        *,
        current_values: np.ndarray,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
        max_ratio: float,
) -> CandidateMinmaxHardFilterResult:
    """Что делает: отсекает кандидатов с резко отличающимся minmax_bps.
    Зачем нужна: Pearson может находить похожую форму при совершенно другом масштабе движения."""
    validate_candidate_feature_inputs(
        candidates=candidates,
        candidate_matrix=candidate_matrix,
        pearson_scores=pearson_scores,
    )

    expected_points = int(candidate_matrix.shape[1])
    source_candidates_count = len(candidates)
    current_features = calculate_pattern_path_features(current_values)
    threshold_ratio = float(max_ratio)

    if threshold_ratio <= 0.0:
        return CandidateMinmaxHardFilterResult(
            max_ratio=threshold_ratio,
            disabled_reason="max_ratio_disabled",
            current_minmax_bps=current_features.minmax_bps,
            source_candidates_count=source_candidates_count,
            kept_candidates_count=source_candidates_count,
            dropped_candidates_count=0,
            valid_candidates=list(candidates),
            candidate_matrix=candidate_matrix.copy(),
            pearson_scores=pearson_scores.copy(),
        )

    if current_features.minmax_bps == 0.0:
        return CandidateMinmaxHardFilterResult(
            max_ratio=threshold_ratio,
            disabled_reason="current_minmax_zero",
            current_minmax_bps=current_features.minmax_bps,
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
        minmax_ratio = calculate_minmax_ratio(
            current_minmax_bps=current_features.minmax_bps,
            candidate_minmax_bps=candidate_features.minmax_bps,
        )

        if minmax_ratio > threshold_ratio:
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

    return CandidateMinmaxHardFilterResult(
        max_ratio=threshold_ratio,
        disabled_reason=None,
        current_minmax_bps=current_features.minmax_bps,
        source_candidates_count=source_candidates_count,
        kept_candidates_count=len(kept_candidates),
        dropped_candidates_count=dropped_count,
        valid_candidates=kept_candidates,
        candidate_matrix=kept_matrix,
        pearson_scores=kept_scores_array,
    )


def get_signed_direction(value: float) -> int:
    """Что делает: возвращает знак числа как -1/0/+1.
    Зачем нужна: end_delta с разным знаком не должен получать положительный score."""
    number = float(value)

    if number > 0.0:
        return 1

    if number < 0.0:
        return -1

    return 0


def calculate_signed_similarity_to_current(
        *,
        current_value: float,
        candidate_value: float,
) -> float:
    """Что делает: считает похожесть знакового признака относительно текущего значения.
    Зачем нужна: end_delta должен совпадать не только по размеру, но и по направлению."""
    current = float(current_value)
    candidate = float(candidate_value)

    current_sign = get_signed_direction(current)
    candidate_sign = get_signed_direction(candidate)

    if current_sign == 0 and candidate_sign == 0:
        return 1.0

    if current_sign == 0 or candidate_sign == 0:
        return 0.0

    if current_sign != candidate_sign:
        return 0.0

    current_abs = abs(current)
    distance = abs(current_abs - abs(candidate))

    return max(0.0, min(1.0, 1.0 - distance / current_abs))


def calculate_unsigned_similarity_to_current(
        *,
        current_value: float,
        candidate_value: float,
) -> float:
    """Что делает: считает похожесть беззнакового признака относительно текущего значения.
    Зачем нужна: minmax всегда >= 0, поэтому сравниваем только размер размаха."""
    current = float(current_value)
    candidate = float(candidate_value)

    if current < 0.0 or candidate < 0.0:
        raise ValueError(
            f"Беззнаковый признак не может быть отрицательным: "
            f"current={current}, candidate={candidate}"
        )

    if current == 0.0 and candidate == 0.0:
        return 1.0

    if current == 0.0:
        return 0.0

    distance = abs(current - candidate)

    return max(0.0, min(1.0, 1.0 - distance / current))


def calculate_candidate_total_score(
        *,
        pearson_score: float,
        end_delta_component: float,
        minmax_component: float,
        pearson_weight: float,
        end_delta_weight: float,
        minmax_weight: float,
) -> tuple[float, float]:
    """Что делает: считает weighted score кандидата.
    Зачем нужна: все score-компоненты приводятся к шкале 0..1 и объединяются через веса."""
    weights_and_values = [
        (float(pearson_weight), float(pearson_score)),
        (float(end_delta_weight), float(end_delta_component)),
        (float(minmax_weight), float(minmax_component)),
    ]

    active_weight_sum = sum(
        weight
        for weight, _ in weights_and_values
        if weight > 0.0
    )

    if active_weight_sum <= 0.0:
        return 0.0, 0.0

    weighted_sum = sum(
        weight * value
        for weight, value in weights_and_values
        if weight > 0.0
    )

    total_score = weighted_sum / active_weight_sum

    return max(0.0, min(1.0, float(total_score))), float(active_weight_sum)


def rank_candidates_by_score(
        *,
        current_values: np.ndarray,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
        pearson_weight: float,
        end_delta_weight: float,
        minmax_weight: float,
) -> CandidateScoreResult:
    """Что делает: считает score кандидатов и сортирует их по total_score.
    Зачем нужна: top candidates должны идти не только по Pearson, но и по дополнительным признакам."""
    validate_candidate_feature_inputs(
        candidates=candidates,
        candidate_matrix=candidate_matrix,
        pearson_scores=pearson_scores,
    )

    expected_points = int(candidate_matrix.shape[1])
    current_features = calculate_pattern_path_features(current_values)

    rows: list[CandidateScoreRow] = []
    active_weight_sum = 0.0

    for index, candidate in enumerate(candidates):
        candidate_features = calculate_pattern_path_features(candidate_matrix[index, :])

        pearson_component = max(0.0, min(1.0, float(pearson_scores[index])))
        end_delta_component = calculate_signed_similarity_to_current(
            current_value=current_features.end_delta_bps,
            candidate_value=candidate_features.end_delta_bps,
        )
        minmax_component = calculate_unsigned_similarity_to_current(
            current_value=current_features.minmax_bps,
            candidate_value=candidate_features.minmax_bps,
        )

        total_score, active_weight_sum = calculate_candidate_total_score(
            pearson_score=pearson_component,
            end_delta_component=end_delta_component,
            minmax_component=minmax_component,
            pearson_weight=pearson_weight,
            end_delta_weight=end_delta_weight,
            minmax_weight=minmax_weight,
        )

        rows.append(
            CandidateScoreRow(
                candidate=candidate,
                pearson_score=float(pearson_scores[index]),
                path_features=candidate_features,
                pearson_component=pearson_component,
                end_delta_component=end_delta_component,
                minmax_component=minmax_component,
                total_score=total_score,
            )
        )

    sorted_indices = sorted(
        range(len(rows)),
        key=lambda row_index: (
            rows[row_index].total_score,
            rows[row_index].pearson_score,
        ),
        reverse=True,
    )

    sorted_rows = [
        rows[index]
        for index in sorted_indices
    ]

    sorted_candidates = [
        candidates[index]
        for index in sorted_indices
    ]

    sorted_matrix = (
        candidate_matrix[sorted_indices, :].astype(float)
        if sorted_indices
        else np.empty((0, expected_points), dtype=float)
    )

    sorted_pearson_scores = np.asarray(
        [float(pearson_scores[index]) for index in sorted_indices],
        dtype=float,
    )

    sorted_candidate_scores = np.asarray(
        [row.total_score for row in sorted_rows],
        dtype=float,
    )

    return CandidateScoreResult(
        current_features=current_features,
        rows=sorted_rows,
        valid_candidates=sorted_candidates,
        candidate_matrix=sorted_matrix,
        pearson_scores=sorted_pearson_scores,
        candidate_scores=sorted_candidate_scores,
        pearson_weight=float(pearson_weight),
        end_delta_weight=float(end_delta_weight),
        minmax_weight=float(minmax_weight),
        active_weight_sum=float(active_weight_sum),
    )


def format_candidate_minmax_hard_filter_result(result: CandidateMinmaxHardFilterResult) -> str:
    """Что делает: форматирует результат hard filter по minmax.
    Зачем нужна: в runtime-логе должно быть видно, сколько кандидатов убил масштабный фильтр."""
    if result.disabled_reason is not None:
        return (
            f"off, reason={result.disabled_reason}, "
            f"max_ratio={result.max_ratio:.2f}, "
            f"current_minmax={result.current_minmax_bps:.2f} bps, "
            f"kept={result.kept_candidates_count}/{result.source_candidates_count}"
        )

    return (
        f"on, max_ratio={result.max_ratio:.2f}, "
        f"current_minmax={result.current_minmax_bps:.2f} bps, "
        f"kept={result.kept_candidates_count}/{result.source_candidates_count}, "
        f"dropped={result.dropped_candidates_count}"
    )


def format_path_features(features: PatternPathFeatures) -> str:
    """Что делает: компактно форматирует path-features.
    Зачем нужна: runtime-лог должен показывать raw значения без перегруза."""
    return (
        f"end_delta={features.end_delta_bps:.2f} bps / {features.end_delta_points:.2f} pt, "
        f"minmax={features.minmax_bps:.2f} bps / {features.minmax_points:.2f} pt"
    )


def format_candidate_score_result(
        result: CandidateScoreResult,
        *,
        top_limit: int = 3,
) -> str:
    """Что делает: форматирует score кандидатов.
    Зачем нужна: в логе должно быть видно, почему кандидаты отсортированы именно так."""
    weights_text = (
        f"weights=pearson:{result.pearson_weight:.2f}, "
        f"end_delta:{result.end_delta_weight:.2f}, "
        f"minmax:{result.minmax_weight:.2f}"
    )

    if top_limit <= 0 or not result.rows:
        top_text = "top=[]"

    else:
        top_parts = [
            (
                f"{index}. {row.candidate.signal_bar_time_ct} CT "
                f"score={row.total_score:.2f}, "
                f"r={row.pearson_score:.2f}, "
                f"ed_score={row.end_delta_component:.2f}, "
                f"mm_score={row.minmax_component:.2f}, "
                f"{format_path_features(row.path_features)}"
            )
            for index, row in enumerate(result.rows[:top_limit], start=1)
        ]
        top_text = "top=[" + " | ".join(top_parts) + "]"

    return (
        f"current={format_path_features(result.current_features)}; "
        f"candidates={len(result.rows)}; "
        f"{weights_text}; "
        f"active_weight_sum={result.active_weight_sum:.2f}; "
        f"{top_text}"
    )
