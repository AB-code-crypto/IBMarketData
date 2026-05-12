from collections.abc import Sequence
from typing import TypeAlias

import numpy as np

NumberSeries: TypeAlias = Sequence[int | float] | np.ndarray

__all__ = [
    "calculate_centered_pearson",
    "calculate_centered_pearson_batch",
]


def calculate_centered_pearson(
        left_values: NumberSeries,
        right_values: NumberSeries,
) -> float:
    """Что делает: считает центрированную корреляцию Пирсона для двух одномерных рядов.
    Зачем нужна: это базовая метрика похожести текущего паттерна и исторических кандидатов."""
    left_array = np.asarray(left_values, dtype=float)
    right_array = np.asarray(right_values, dtype=float)

    if left_array.shape != right_array.shape:
        raise ValueError(
            "Для расчёта Pearson нужны ряды одинаковой формы: "
            f"left={left_array.shape}, right={right_array.shape}"
        )

    if left_array.ndim != 1:
        raise ValueError("Для расчёта Pearson нужны одномерные ряды")

    values_count = left_array.size

    if values_count < 2:
        raise ValueError(
            "Для расчёта Pearson нужно минимум две точки в каждом ряду"
        )

    centered_left = left_array - left_array.mean()
    centered_right = right_array - right_array.mean()

    denominator = np.linalg.norm(centered_left) * np.linalg.norm(centered_right)

    if denominator == 0.0:
        return 0.0

    return float(np.dot(centered_left, centered_right) / denominator)


def calculate_centered_pearson_batch(
        reference_values: NumberSeries,
        candidate_matrix: np.ndarray,
) -> np.ndarray:
    """Что делает: считает Pearson между одним reference-паттерном и матрицей candidate-паттернов.
    Зачем нужна: signal-сервис должен считать корреляции по всем кандидатам одним NumPy-проходом."""
    reference_array = np.asarray(reference_values, dtype=float)
    matrix = np.asarray(candidate_matrix, dtype=float)

    if reference_array.ndim != 1:
        raise ValueError("reference_values должен быть одномерным рядом")

    if matrix.ndim != 2:
        raise ValueError("candidate_matrix должна быть двумерной матрицей")

    if matrix.shape[1] != reference_array.size:
        raise ValueError(
            "Длина candidate-паттернов должна совпадать с reference: "
            f"matrix_columns={matrix.shape[1]}, reference={reference_array.size}"
        )

    if reference_array.size < 2:
        raise ValueError("Для расчёта Pearson нужно минимум две точки")

    if matrix.shape[0] == 0:
        return np.empty((0,), dtype=float)

    centered_reference = reference_array - reference_array.mean()
    centered_matrix = matrix - matrix.mean(axis=1, keepdims=True)

    numerator = centered_matrix @ centered_reference
    denominator = (
        np.linalg.norm(centered_matrix, axis=1)
        * np.linalg.norm(centered_reference)
    )

    return np.divide(
        numerator,
        denominator,
        out=np.zeros_like(numerator, dtype=float),
        where=denominator != 0.0,
    )


if __name__ == "__main__":
    left_values = [
        100.0, 100.3, 100.8, 101.2, 101.0,
        101.5, 102.1, 102.7, 102.4, 102.9,
        103.4, 103.8, 104.2, 104.0, 104.6,
        105.1, 105.7, 106.0, 106.4, 106.9,
    ]

    right_values = [
        200.0, 200.4, 200.9, 201.1, 201.0,
        201.7, 202.0, 202.8, 202.5, 203.0,
        203.6, 203.7, 204.4, 204.1, 204.8,
        205.0, 205.8, 206.2, 206.3, 207.1,
    ]

    pearson = calculate_centered_pearson(left_values, right_values)

    print(f"left_values length: {len(left_values)}")
    print(f"right_values length: {len(right_values)}")
    print(f"Pearson: {pearson:.6f}")
