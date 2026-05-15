from dataclasses import dataclass
from typing import Literal, TypeAlias

import numpy as np

from ib_signal.signal_regression import LinearRegressionResult

RegressionRelationKind: TypeAlias = Literal[
    "above_sma",
    "below_sma",
    "cross_up_sma",
    "cross_down_sma",
    "near_sma",
    "mixed_sma",
]


@dataclass(frozen=True)
class RegressionRelationResult:
    # Класс взаимного расположения base regression относительно reference regression.
    relation: RegressionRelationKind

    # Разница base - reference в первой точке окна в ценовых пунктах.
    diff_start_points: float

    # Разница base - reference в последней точке окна в ценовых пунктах.
    diff_end_points: float

    # Разница base - reference в первой точке окна в базисных пунктах.
    diff_start_bps: float

    # Разница base - reference в последней точке окна в базисных пунктах.
    diff_end_bps: float

    # Порог близости линий в базисных пунктах.
    near_threshold_bps: float

    # Количество точек в regression window.
    points_count: int


def calculate_regression_diff_bps(
        *,
        diff_points: np.ndarray,
        base_line_values: np.ndarray,
) -> np.ndarray:
    """Что делает: переводит разницу двух regression lines из пунктов в базисные пункты.
    Зачем нужна: расстояние между price-regression и SMA-regression должно сравниваться в относительной шкале."""
    base_values = np.asarray(base_line_values, dtype=float)
    diff_values = np.asarray(diff_points, dtype=float)

    if base_values.shape != diff_values.shape:
        raise ValueError(
            f"diff_points и base_line_values должны иметь одинаковый shape, "
            f"получено diff={diff_values.shape}, base={base_values.shape}"
        )

    base_abs = np.abs(base_values)

    if np.any(base_abs == 0.0):
        raise ValueError("Невозможно посчитать diff bps: base regression line содержит нулевые значения")

    return diff_values / base_abs * 10000.0


def classify_regression_relation(
        *,
        diff_start_bps: float,
        diff_end_bps: float,
        near_threshold_bps: float,
) -> RegressionRelationKind:
    """Что делает: классифицирует взаимное расположение двух regression lines.
    Зачем нужна: market-regime должен учитывать, где price-regression находится относительно SMA-regression."""
    threshold = float(near_threshold_bps)

    if threshold < 0.0:
        raise ValueError(
            f"near_threshold_bps не может быть отрицательным: {threshold}"
        )

    if abs(diff_start_bps) <= threshold and abs(diff_end_bps) <= threshold:
        return "near_sma"

    if diff_start_bps > threshold and diff_end_bps > threshold:
        return "above_sma"

    if diff_start_bps < -threshold and diff_end_bps < -threshold:
        return "below_sma"

    if diff_start_bps < -threshold and diff_end_bps > threshold:
        return "cross_up_sma"

    if diff_start_bps > threshold and diff_end_bps < -threshold:
        return "cross_down_sma"

    return "mixed_sma"


def build_regression_relation(
        *,
        base_regression: LinearRegressionResult,
        reference_regression: LinearRegressionResult,
        near_threshold_bps: float,
) -> RegressionRelationResult:
    """Что делает: считает взаимное расположение base regression относительно reference regression.
    Зачем нужна: один и тот же расчёт можно использовать для текущего паттерна и исторических кандидатов."""
    if base_regression.points_count != reference_regression.points_count:
        raise ValueError(
            f"Нельзя сравнить regression lines разной длины: "
            f"base={base_regression.points_count}, reference={reference_regression.points_count}"
        )

    diff_points = base_regression.line_values - reference_regression.line_values
    diff_bps = calculate_regression_diff_bps(
        diff_points=diff_points,
        base_line_values=base_regression.line_values,
    )

    diff_start_points = float(diff_points[0])
    diff_end_points = float(diff_points[-1])

    diff_start_bps = float(diff_bps[0])
    diff_end_bps = float(diff_bps[-1])

    relation = classify_regression_relation(
        diff_start_bps=diff_start_bps,
        diff_end_bps=diff_end_bps,
        near_threshold_bps=near_threshold_bps,
    )

    return RegressionRelationResult(
        relation=relation,
        diff_start_points=diff_start_points,
        diff_end_points=diff_end_points,
        diff_start_bps=diff_start_bps,
        diff_end_bps=diff_end_bps,
        near_threshold_bps=float(near_threshold_bps),
        points_count=base_regression.points_count,
    )


def format_regression_relation_diagnostics(
        label: str,
        relation: RegressionRelationResult | None,
) -> str:
    """Что делает: форматирует взаимное расположение regression lines для лога.
    Зачем нужна: в консоли должно быть видно не только направление линий, но и их расположение."""
    if relation is None:
        return f"{label}=None"

    return (
        f"{label}_relation={relation.relation}, "
        f"{label}_start_bps={relation.diff_start_bps:.6f}, "
        f"{label}_start_points={relation.diff_start_points:.6f}, "
        f"{label}_end_bps={relation.diff_end_bps:.6f}, "
        f"{label}_end_points={relation.diff_end_points:.6f}"
    )
