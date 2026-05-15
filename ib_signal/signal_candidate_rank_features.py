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

    # Полная длина пути цены в пунктах: сумма abs(delta между соседними точками).
    path_length_points: float

    # Полная длина пути цены в базисных пунктах.
    path_length_bps: float

    # Эффективность пути: abs(net_delta) / path_length.
    path_efficiency: float


@dataclass(frozen=True)
class CandidatePathFeatureRow:
    candidate: CandidateWindow
    pearson_score: float
    path_features: PatternPathFeatures


@dataclass(frozen=True)
class CandidatePathFeatureResult:
    current_features: PatternPathFeatures
    candidate_rows: list[CandidatePathFeatureRow]


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

    point_deltas = np.diff(y)
    path_length_points = float(np.abs(point_deltas).sum())
    path_length_bps = calculate_bps(
        value=path_length_points,
        base_value=first_value,
    )

    if path_length_points == 0.0:
        path_efficiency = 0.0
    else:
        path_efficiency = abs(net_delta_points) / path_length_points

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
        path_length_points=float(path_length_points),
        path_length_bps=float(path_length_bps),
        path_efficiency=float(path_efficiency),
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


def format_path_features(features: PatternPathFeatures) -> str:
    """Что делает: компактно форматирует path-features.
    Зачем нужна: runtime-лог должен показывать raw значения без перегруза."""
    return (
        f"net={features.net_delta_bps:.2f} bps / {features.net_delta_points:.2f} pt, "
        f"range={features.range_bps:.2f} bps / {features.range_points:.2f} pt, "
        f"end={features.end_position:.2f}, "
        f"eff={features.path_efficiency:.2f}"
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
