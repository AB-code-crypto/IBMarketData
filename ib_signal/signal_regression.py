from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, TypeAlias

import numpy as np

NumberSeries: TypeAlias = Sequence[int | float] | np.ndarray
RegressionDirection: TypeAlias = Literal["up", "down", "flat"]


@dataclass(frozen=True)
class LinearRegressionResult:
    # Наклон линии: изменение значения ряда на один индекс бара.
    slope: float

    # Свободный член линии: значение regression line при x = 0.
    intercept: float

    # Значения regression line по всей длине исходного ряда.
    line_values: np.ndarray

    # Значение regression line на первой точке ряда.
    fitted_start: float

    # Значение regression line на последней точке ряда.
    fitted_end: float

    # Полное изменение regression line от первой до последней точки.
    fitted_delta: float

    # Количество точек в исходном ряду.
    points_count: int


def build_linear_regression(values: NumberSeries) -> LinearRegressionResult:
    """Что делает: строит линейную регрессию по одномерному числовому ряду.
    Зачем нужна: market-regime будет определять направление price pattern и SMA через наклон regression line."""
    y = np.asarray(values, dtype=float)

    if y.ndim != 1:
        raise ValueError(
            f"Для линейной регрессии нужен одномерный ряд, получено shape={y.shape}"
        )

    points_count = int(y.size)

    if points_count < 2:
        raise ValueError(
            f"Для линейной регрессии нужно минимум две точки, получено: {points_count}"
        )

    if not np.all(np.isfinite(y)):
        raise ValueError("Для линейной регрессии ряд не должен содержать NaN или inf")

    x = np.arange(points_count, dtype=float)
    x_mean = float(x.mean())
    y_mean = float(y.mean())

    x_centered = x - x_mean
    y_centered = y - y_mean

    denominator = float(np.dot(x_centered, x_centered))

    if denominator == 0.0:
        # При points_count >= 2 сюда попасть нельзя, но оставляем защиту от будущих изменений.
        raise ValueError("Невозможно построить регрессию: нулевой разброс x")

    slope = float(np.dot(x_centered, y_centered) / denominator)
    intercept = float(y_mean - slope * x_mean)

    line_values = slope * x + intercept
    fitted_start = float(line_values[0])
    fitted_end = float(line_values[-1])
    fitted_delta = fitted_end - fitted_start

    return LinearRegressionResult(
        slope=slope,
        intercept=intercept,
        line_values=line_values,
        fitted_start=fitted_start,
        fitted_end=fitted_end,
        fitted_delta=fitted_delta,
        points_count=points_count,
    )



def calculate_regression_delta_bps(regression: LinearRegressionResult) -> float:
    """Что делает: считает изменение regression line в базисных пунктах.
    Зачем нужна: bps нормализуют движение относительно уровня цены и не зависят от абсолютной цены инструмента."""
    base_value = abs(float(regression.fitted_start))

    if base_value == 0.0:
        raise ValueError("Невозможно посчитать delta bps: fitted_start равен нулю")

    return float(regression.fitted_delta / base_value * 10000.0)


def calculate_regression_threshold_points(
        regression: LinearRegressionResult,
        *,
        flat_delta_threshold_bps: float,
) -> float:
    """Что делает: переводит bps-порог в ценовые пункты для конкретной regression line.
    Зачем нужна: на PNG удобно видеть и относительный порог в bps, и его текущий эквивалент в пунктах."""
    threshold_bps = float(flat_delta_threshold_bps)

    if threshold_bps < 0.0:
        raise ValueError(
            f"Порог flat_delta_threshold_bps не может быть отрицательным: {threshold_bps}"
        )

    return abs(float(regression.fitted_start)) * threshold_bps / 10000.0


def classify_regression_direction(
        regression: LinearRegressionResult,
        *,
        flat_delta_threshold_bps: float,
) -> RegressionDirection:
    """Что делает: классифицирует наклон regression line как up/down/flat по delta в базисных пунктах.
    Зачем нужна: bps дают сопоставимую оценку движения при разных уровнях цены."""
    threshold_bps = float(flat_delta_threshold_bps)

    if threshold_bps < 0.0:
        raise ValueError(
            f"Порог flat_delta_threshold_bps не может быть отрицательным: {threshold_bps}"
        )

    delta_bps = calculate_regression_delta_bps(regression)

    if delta_bps > threshold_bps:
        return "up"

    if delta_bps < -threshold_bps:
        return "down"

    return "flat"


def format_regression_diagnostics(
        label: str,
        regression: LinearRegressionResult | None,
        *,
        flat_delta_threshold_bps: float,
) -> str:
    """Что делает: форматирует delta/direction одной regression line для лога.
    Зачем нужна: лог показывает delta и в bps, и в ценовых пунктах."""
    if regression is None:
        return f"{label}=None"

    delta_bps = calculate_regression_delta_bps(regression)
    direction = classify_regression_direction(
        regression,
        flat_delta_threshold_bps=flat_delta_threshold_bps,
    )

    return (
        f"{label}_delta_bps={delta_bps:.6f}, "
        f"{label}_delta_points={regression.fitted_delta:.6f}, "
        f"{label}_direction={direction}"
    )


if __name__ == "__main__":
    demo_values = np.array(
        [100.0, 100.5, 100.7, 101.2, 101.5, 101.3, 102.0],
        dtype=float,
    )

    regression = build_linear_regression(demo_values)

    print(f"points_count={regression.points_count}")
    print(f"slope={regression.slope:.6f}")
    print(f"intercept={regression.intercept:.6f}")
    print(f"fitted_start={regression.fitted_start:.6f}")
    print(f"fitted_end={regression.fitted_end:.6f}")
    print(f"fitted_delta={regression.fitted_delta:.6f}")
    print(f"delta_bps={calculate_regression_delta_bps(regression):.6f}")
    print(f"direction={classify_regression_direction(regression, flat_delta_threshold_bps=1.0)}")
    print(f"line_values={np.round(regression.line_values, 6).tolist()}")
