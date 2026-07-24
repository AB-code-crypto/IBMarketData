from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

import numpy as np


class SignalCalculationError(ValueError):
    pass


@dataclass(frozen=True)
class SignalWindow:
    signal_bar_ts: int
    pattern_start_ts: int
    pattern_end_ts: int
    trade_start_ts: int
    trade_end_ts: int
    pattern_seconds: int
    trade_seconds: int


@dataclass(frozen=True)
class CandidateWindow:
    signal_bar_ts: int
    signal_time_ct: str
    pattern_start_ts: int
    pattern_end_ts: int
    trade_start_ts: int
    trade_end_ts: int


@dataclass(frozen=True)
class PatternPathFeatures:
    first_value: float
    last_value: float
    min_value: float
    max_value: float
    end_delta_points: float
    end_delta_bps: float
    minmax_points: float
    minmax_bps: float


@dataclass(frozen=True)
class RankedCandidate:
    candidate: CandidateWindow
    pearson_score: float
    candidate_score: float
    features: PatternPathFeatures


@dataclass(frozen=True)
class CandidateRanking:
    current_features: PatternPathFeatures
    ranked_candidates: tuple[RankedCandidate, ...]
    ranked_matrix: np.ndarray
    best_raw_pearson: float
    valid_pattern_count: int
    pearson_pass_count: int
    minmax_pass_count: int


@dataclass(frozen=True)
class PotentialResult:
    available: bool
    reason: str | None
    direction: str
    source_candidate_count: int
    used_candidate_count: int
    end_delta_bps: float
    end_delta_points: float
    max_profit_bps: float
    max_profit_points: float
    max_drawdown_bps: float
    max_drawdown_points: float
    same_direction_count: int
    opposite_direction_count: int
    flat_count: int
    same_direction_weight_share: float
    opposite_direction_weight_share: float
    flat_weight_share: float


def build_signal_window(
    *,
    signal_bar_ts: int,
    pattern_lookback_minutes: int,
    potential_horizon_minutes: int,
) -> SignalWindow:
    signal_ts = int(signal_bar_ts)
    pattern_seconds = int(pattern_lookback_minutes) * 60
    trade_seconds = int(potential_horizon_minutes) * 60
    if signal_ts <= 0:
        raise SignalCalculationError("signal_bar_ts must be positive")
    if pattern_seconds <= 0 or trade_seconds <= 0:
        raise SignalCalculationError(
            "pattern and potential windows must be positive"
        )
    return SignalWindow(
        signal_bar_ts=signal_ts,
        pattern_start_ts=signal_ts - pattern_seconds,
        pattern_end_ts=signal_ts,
        trade_start_ts=signal_ts,
        trade_end_ts=signal_ts + trade_seconds,
        pattern_seconds=pattern_seconds,
        trade_seconds=trade_seconds,
    )


def get_due_signal_bar_ts(
    *,
    latest_complete_bar_end_ts: int,
    rolling_step_seconds: int,
    last_calculated_bar_ts: int | None,
) -> int | None:
    step = int(rolling_step_seconds)
    if step <= 0:
        raise SignalCalculationError(
            "rolling_step_seconds must be positive"
        )
    current = int(latest_complete_bar_end_ts)
    due = current - current % step
    if due == last_calculated_bar_ts:
        return None
    return due


def expected_points(*, seconds: int, bar_size_seconds: int) -> int:
    duration = int(seconds)
    bar_size = int(bar_size_seconds)
    if duration <= 0 or bar_size <= 0 or duration % bar_size:
        raise SignalCalculationError(
            "window seconds must be positive and divisible by bar size"
        )
    points = duration // bar_size
    if points < 2:
        raise SignalCalculationError(
            "a signal pattern requires at least two points"
        )
    return points


def calculate_centered_pearson_batch(
    reference_values: np.ndarray,
    candidate_matrix: np.ndarray,
) -> np.ndarray:
    reference = np.asarray(reference_values, dtype=float)
    matrix = np.asarray(candidate_matrix, dtype=float)
    if reference.ndim != 1:
        raise SignalCalculationError(
            "reference_values must be one-dimensional"
        )
    if matrix.ndim != 2:
        raise SignalCalculationError(
            "candidate_matrix must be two-dimensional"
        )
    if matrix.shape[1] != reference.size:
        raise SignalCalculationError(
            "candidate pattern width does not match current pattern"
        )
    if reference.size < 2:
        raise SignalCalculationError(
            "Pearson calculation requires at least two points"
        )
    if matrix.shape[0] == 0:
        return np.empty((0,), dtype=float)
    if not np.all(np.isfinite(reference)) or not np.all(np.isfinite(matrix)):
        raise SignalCalculationError(
            "Pearson inputs must contain only finite values"
        )

    centered_reference = reference - reference.mean()
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


def calculate_bps(*, value: float, base_value: float) -> float:
    base = abs(float(base_value))
    if base == 0.0:
        raise SignalCalculationError(
            "cannot calculate basis points from a zero base"
        )
    return float(value) / base * 10_000.0


def calculate_path_features(values: np.ndarray) -> PatternPathFeatures:
    series = np.asarray(values, dtype=float)
    if series.ndim != 1 or series.size < 2:
        raise SignalCalculationError(
            "path features require a one-dimensional series with two points"
        )
    if not np.all(np.isfinite(series)):
        raise SignalCalculationError(
            "path features require finite values"
        )
    first = float(series[0])
    last = float(series[-1])
    minimum = float(series.min())
    maximum = float(series.max())
    end_delta = last - first
    minmax = maximum - minimum
    return PatternPathFeatures(
        first_value=first,
        last_value=last,
        min_value=minimum,
        max_value=maximum,
        end_delta_points=end_delta,
        end_delta_bps=calculate_bps(
            value=end_delta,
            base_value=first,
        ),
        minmax_points=minmax,
        minmax_bps=calculate_bps(
            value=minmax,
            base_value=first,
        ),
    )


def _minmax_ratio(
    *,
    current_minmax_bps: float,
    candidate_minmax_bps: float,
) -> float:
    current = float(current_minmax_bps)
    candidate = float(candidate_minmax_bps)
    if current < 0.0 or candidate < 0.0:
        raise SignalCalculationError(
            "minmax values cannot be negative"
        )
    if current == 0.0 and candidate == 0.0:
        return 1.0
    if current == 0.0 or candidate == 0.0:
        return math.inf
    return max(current, candidate) / min(current, candidate)


def _signed_similarity(
    *,
    current_value: float,
    candidate_value: float,
) -> float:
    current = float(current_value)
    candidate = float(candidate_value)
    current_sign = 1 if current > 0.0 else -1 if current < 0.0 else 0
    candidate_sign = (
        1 if candidate > 0.0 else -1 if candidate < 0.0 else 0
    )
    if current_sign == 0 and candidate_sign == 0:
        return 1.0
    if current_sign == 0 or candidate_sign == 0:
        return 0.0
    if current_sign != candidate_sign:
        return 0.0
    return max(
        0.0,
        min(
            1.0,
            1.0
            - abs(abs(current) - abs(candidate)) / abs(current),
        ),
    )


def _unsigned_similarity(
    *,
    current_value: float,
    candidate_value: float,
) -> float:
    current = float(current_value)
    candidate = float(candidate_value)
    if current < 0.0 or candidate < 0.0:
        raise SignalCalculationError(
            "unsigned feature values cannot be negative"
        )
    if current == 0.0 and candidate == 0.0:
        return 1.0
    if current == 0.0:
        return 0.0
    return max(
        0.0,
        min(1.0, 1.0 - abs(current - candidate) / current),
    )


def rank_candidates(
    *,
    current_values: np.ndarray,
    candidates: tuple[CandidateWindow, ...],
    candidate_matrix: np.ndarray,
    pearson_minimum: float,
    minmax_hard_filter_max_ratio: float,
    score_pearson_weight: float,
    score_end_delta_weight: float,
    score_minmax_weight: float,
) -> CandidateRanking:
    current = np.asarray(current_values, dtype=float)
    matrix = np.asarray(candidate_matrix, dtype=float)
    if matrix.ndim != 2:
        raise SignalCalculationError(
            "candidate_matrix must be two-dimensional"
        )
    if len(candidates) != matrix.shape[0]:
        raise SignalCalculationError(
            "candidates and candidate_matrix row count differ"
        )
    if matrix.shape[1] != current.size:
        raise SignalCalculationError(
            "candidate_matrix width differs from current pattern"
        )
    current_features = calculate_path_features(current)
    scores = calculate_centered_pearson_batch(current, matrix)
    best_raw = float(scores.max()) if scores.size else 0.0

    pearson_threshold = float(pearson_minimum)
    passed_indices = [
        index
        for index, value in enumerate(scores)
        if float(value) >= pearson_threshold
    ]

    minmax_threshold = float(minmax_hard_filter_max_ratio)
    filtered_indices: list[int] = []
    feature_by_index: dict[int, PatternPathFeatures] = {}
    for index in passed_indices:
        features = calculate_path_features(matrix[index, :])
        feature_by_index[index] = features
        if (
            minmax_threshold > 0.0
            and current_features.minmax_bps != 0.0
            and _minmax_ratio(
                current_minmax_bps=current_features.minmax_bps,
                candidate_minmax_bps=features.minmax_bps,
            )
            > minmax_threshold
        ):
            continue
        filtered_indices.append(index)

    weights = (
        float(score_pearson_weight),
        float(score_end_delta_weight),
        float(score_minmax_weight),
    )
    active_weight_sum = sum(value for value in weights if value > 0.0)

    rows: list[tuple[int, RankedCandidate]] = []
    for index in filtered_indices:
        features = feature_by_index[index]
        pearson_component = max(
            0.0,
            min(1.0, float(scores[index])),
        )
        end_component = _signed_similarity(
            current_value=current_features.end_delta_bps,
            candidate_value=features.end_delta_bps,
        )
        minmax_component = _unsigned_similarity(
            current_value=current_features.minmax_bps,
            candidate_value=features.minmax_bps,
        )
        if active_weight_sum <= 0.0:
            total_score = 0.0
        else:
            total_score = (
                max(0.0, weights[0]) * pearson_component
                + max(0.0, weights[1]) * end_component
                + max(0.0, weights[2]) * minmax_component
            ) / active_weight_sum
        rows.append(
            (
                index,
                RankedCandidate(
                    candidate=candidates[index],
                    pearson_score=float(scores[index]),
                    candidate_score=max(
                        0.0,
                        min(1.0, float(total_score)),
                    ),
                    features=features,
                ),
            )
        )

    rows.sort(
        key=lambda item: (
            item[1].candidate_score,
            item[1].pearson_score,
        ),
        reverse=True,
    )
    ranked_candidates = tuple(item[1] for item in rows)
    ranked_matrix = (
        matrix[[item[0] for item in rows], :].astype(float)
        if rows
        else np.empty((0, current.size), dtype=float)
    )
    return CandidateRanking(
        current_features=current_features,
        ranked_candidates=ranked_candidates,
        ranked_matrix=ranked_matrix,
        best_raw_pearson=best_raw,
        valid_pattern_count=len(candidates),
        pearson_pass_count=len(passed_indices),
        minmax_pass_count=len(filtered_indices),
    )


def _empty_potential(
    *,
    reason: str,
    source_candidate_count: int,
    used_candidate_count: int = 0,
) -> PotentialResult:
    return PotentialResult(
        available=False,
        reason=reason,
        direction="NONE",
        source_candidate_count=source_candidate_count,
        used_candidate_count=used_candidate_count,
        end_delta_bps=0.0,
        end_delta_points=0.0,
        max_profit_bps=0.0,
        max_profit_points=0.0,
        max_drawdown_bps=0.0,
        max_drawdown_points=0.0,
        same_direction_count=0,
        opposite_direction_count=0,
        flat_count=0,
        same_direction_weight_share=0.0,
        opposite_direction_weight_share=0.0,
        flat_weight_share=0.0,
    )


def build_potential(
    *,
    current_values: np.ndarray,
    ranking: CandidateRanking,
    full_values_by_signal_ts: Mapping[int, np.ndarray],
    bar_size_seconds: int,
    potential_horizon_minutes: int,
    minimum_candidate_count: int,
    maximum_candidate_count: int,
) -> PotentialResult:
    current = np.asarray(current_values, dtype=float)
    bar_size = int(bar_size_seconds)
    trade_points = (
        int(potential_horizon_minutes) * 60 // bar_size
    )
    minimum = int(minimum_candidate_count)
    maximum = int(maximum_candidate_count)
    if minimum <= 0 or maximum < minimum:
        raise SignalCalculationError(
            "invalid potential candidate count range"
        )
    if current.size < 2 or trade_points <= 0:
        return _empty_potential(
            reason="invalid_current_or_trade_window",
            source_candidate_count=len(ranking.ranked_candidates),
        )

    selected = ranking.ranked_candidates[:maximum]
    future_series: list[np.ndarray] = []
    valid_scores: list[float] = []
    for ranked in selected:
        full = full_values_by_signal_ts.get(
            ranked.candidate.signal_bar_ts
        )
        if full is None:
            continue
        values = np.asarray(full, dtype=float)
        expected = current.size + trade_points
        if (
            values.ndim != 1
            or values.size != expected
            or not np.all(np.isfinite(values))
        ):
            continue
        entry_price = float(values[current.size - 1])
        if entry_price == 0.0:
            continue
        future_values = values[current.size :]
        if future_values.size != trade_points:
            continue
        delta_bps = (
            (future_values - entry_price)
            / abs(entry_price)
            * 10_000.0
        )
        future_series.append(
            np.concatenate(
                (np.zeros((1,), dtype=float), delta_bps.astype(float))
            )
        )
        valid_scores.append(ranked.candidate_score)

    used = len(future_series)
    if used < minimum:
        return _empty_potential(
            reason=f"not_enough_valid_candidates:{used}<{minimum}",
            source_candidate_count=len(ranking.ranked_candidates),
            used_candidate_count=used,
        )

    matrix = np.vstack(future_series).astype(float)
    raw_weights = np.maximum(
        np.asarray(valid_scores, dtype=float),
        0.0,
    )
    total = float(raw_weights.sum())
    weights = (
        raw_weights / total
        if total > 0.0
        else np.full((used,), 1.0 / used, dtype=float)
    )
    weighted_bps = np.average(matrix, axis=0, weights=weights)
    current_entry = float(current[-1])
    weighted_points = (
        weighted_bps / 10_000.0 * abs(current_entry)
    )
    end_bps = float(weighted_bps[-1])
    end_points = float(weighted_points[-1])
    direction = (
        "LONG"
        if end_points > 0.0
        else "SHORT"
        if end_points < 0.0
        else "NONE"
    )

    max_up_index = int(np.argmax(weighted_points))
    max_down_index = int(np.argmin(weighted_points))
    max_up_points = float(weighted_points[max_up_index])
    max_down_points = float(weighted_points[max_down_index])
    max_up_bps = float(weighted_bps[max_up_index])
    max_down_bps = float(weighted_bps[max_down_index])

    if direction == "LONG":
        max_profit_points = max(0.0, max_up_points)
        max_profit_bps = max(0.0, max_up_bps)
        max_drawdown_points = min(0.0, max_down_points)
        max_drawdown_bps = min(0.0, max_down_bps)
    elif direction == "SHORT":
        max_profit_points = max(0.0, -max_down_points)
        max_profit_bps = max(0.0, -max_down_bps)
        max_drawdown_points = -max(0.0, max_up_points)
        max_drawdown_bps = -max(0.0, max_up_bps)
    else:
        max_profit_points = 0.0
        max_profit_bps = 0.0
        max_drawdown_points = 0.0
        max_drawdown_bps = 0.0

    final_deltas = matrix[:, -1]
    if direction == "LONG":
        same_mask = final_deltas > 0.0
        opposite_mask = final_deltas < 0.0
    elif direction == "SHORT":
        same_mask = final_deltas < 0.0
        opposite_mask = final_deltas > 0.0
    else:
        same_mask = np.zeros(final_deltas.shape, dtype=bool)
        opposite_mask = np.zeros(final_deltas.shape, dtype=bool)
    flat_mask = final_deltas == 0.0

    return PotentialResult(
        available=True,
        reason=None,
        direction=direction,
        source_candidate_count=len(ranking.ranked_candidates),
        used_candidate_count=used,
        end_delta_bps=end_bps,
        end_delta_points=end_points,
        max_profit_bps=max_profit_bps,
        max_profit_points=max_profit_points,
        max_drawdown_bps=max_drawdown_bps,
        max_drawdown_points=max_drawdown_points,
        same_direction_count=int(same_mask.sum()),
        opposite_direction_count=int(opposite_mask.sum()),
        flat_count=int(flat_mask.sum()),
        same_direction_weight_share=(
            float(weights[same_mask].sum()) if weights.size else 0.0
        ),
        opposite_direction_weight_share=(
            float(weights[opposite_mask].sum())
            if weights.size
            else 0.0
        ),
        flat_weight_share=(
            float(weights[flat_mask].sum()) if weights.size else 0.0
        ),
    )


def signal_direction(
    *,
    potential: PotentialResult,
    minimum_abs_end_delta_points: float,
) -> str:
    threshold = abs(float(minimum_abs_end_delta_points))
    if (
        potential.available
        and potential.direction in {"LONG", "SHORT"}
        and abs(potential.end_delta_points) > threshold
    ):
        return potential.direction
    return "NONE"
