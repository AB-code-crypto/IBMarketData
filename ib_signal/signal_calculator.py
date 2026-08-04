from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from ib_signal.pearson import calculate_centered_pearson_batch
from ib_signal.signal_candidate_potential import (
    CandidatePotentialResult,
    build_candidate_potential_result,
)
from ib_signal.signal_candidate_rank_features import (
    CandidateMinmaxHardFilterResult,
    CandidateScoreResult,
    filter_candidates_by_minmax_ratio,
    rank_candidates_by_score,
)
from ib_signal.signal_candidates import (
    CandidateSearchResult,
    CandidateWindow,
    find_candidate_windows,
)
from ib_signal.signal_config import SignalConfig
from ib_signal.signal_pattern_matrix import PatternMatrixResult, build_pattern_matrix
from ib_signal.signal_window import SignalWindow, build_current_signal_window


@dataclass(frozen=True)
class SignalCalculationResult:
    instrument_code: str
    signal_bar_ts: int
    signal_window: SignalWindow
    candidate_search: CandidateSearchResult
    pattern_matrix: PatternMatrixResult
    all_pearson_scores: np.ndarray
    best_raw_pearson: float
    pearson_passed_candidates: list[CandidateWindow]
    pearson_passed_matrix: np.ndarray
    pearson_passed_scores: np.ndarray
    minmax_result: CandidateMinmaxHardFilterResult
    score_result: CandidateScoreResult
    potential: CandidatePotentialResult
    threshold_points: float
    signal_direction: str | None
    entry_price: float
    best_signal_pearson: float
    best_candidate_score: float | None

    @property
    def has_signal(self) -> bool:
        return self.signal_direction in {"LONG", "SHORT"}

    @property
    def total_candidates_count(self) -> int:
        return len(self.pattern_matrix.valid_candidates)

    @property
    def pearson_passed_count(self) -> int:
        return len(self.pearson_passed_candidates)

    @property
    def minmax_passed_count(self) -> int:
        return len(self.minmax_result.valid_candidates)


def calculate_signal(
        *,
        instrument_code: str,
        signal_bar_ts: int,
        settings: SignalConfig,
) -> SignalCalculationResult:
    """Calculate one rolling signal without writes, plots, logging or clock access."""
    signal_window = build_current_signal_window(
        signal_bar_ts=int(signal_bar_ts),
        settings=settings,
    )
    candidate_search = find_candidate_windows(
        instrument_code=instrument_code,
        current_window=signal_window,
        settings=settings,
    )
    pattern_matrix = build_pattern_matrix(
        instrument_code=instrument_code,
        window=signal_window,
        candidates=candidate_search.candidates,
    )

    all_pearson_scores = calculate_centered_pearson_batch(
        pattern_matrix.current_values,
        pattern_matrix.candidate_matrix,
    )
    best_raw_pearson = (
        float(all_pearson_scores.max()) if all_pearson_scores.size else 0.0
    )

    passed_indices = np.flatnonzero(
        all_pearson_scores >= float(settings.pearson_min)
    )
    passed_candidates = [
        pattern_matrix.valid_candidates[int(index)]
        for index in passed_indices
    ]
    passed_matrix = pattern_matrix.candidate_matrix[passed_indices, :]
    passed_pearson = all_pearson_scores[passed_indices]

    minmax_result = filter_candidates_by_minmax_ratio(
        current_values=pattern_matrix.current_values,
        candidates=passed_candidates,
        candidate_matrix=passed_matrix,
        pearson_scores=passed_pearson,
        max_ratio=settings.candidate_minmax_hard_filter_max_ratio,
    )
    score_result = rank_candidates_by_score(
        current_values=pattern_matrix.current_values,
        candidates=minmax_result.valid_candidates,
        candidate_matrix=minmax_result.candidate_matrix,
        pearson_scores=minmax_result.pearson_scores,
        pearson_weight=settings.candidate_score_pearson_weight,
        end_delta_weight=settings.candidate_score_end_delta_weight,
        minmax_weight=settings.candidate_score_minmax_weight,
    )
    potential = build_candidate_potential_result(
        instrument_code=instrument_code,
        signal_window=signal_window,
        current_values=pattern_matrix.current_values,
        candidates=score_result.valid_candidates,
        candidate_scores=score_result.candidate_scores,
        min_count=settings.candidate_potential_min_count,
        max_count=settings.candidate_potential_max_count,
    )

    threshold = abs(float(settings.candidate_potential_min_abs_end_delta_points))
    signal_direction: str | None = None
    if (
        potential.is_available
        and potential.direction in {"LONG", "SHORT"}
        and abs(float(potential.end_delta_points)) > threshold
    ):
        signal_direction = str(potential.direction)

    best_signal_pearson = (
        float(score_result.pearson_scores.max())
        if score_result.pearson_scores.size
        else 0.0
    )
    best_candidate_score = (
        float(score_result.candidate_scores.max())
        if score_result.candidate_scores.size
        else None
    )

    return SignalCalculationResult(
        instrument_code=str(instrument_code),
        signal_bar_ts=int(signal_bar_ts),
        signal_window=signal_window,
        candidate_search=candidate_search,
        pattern_matrix=pattern_matrix,
        all_pearson_scores=all_pearson_scores,
        best_raw_pearson=best_raw_pearson,
        pearson_passed_candidates=passed_candidates,
        pearson_passed_matrix=passed_matrix,
        pearson_passed_scores=passed_pearson,
        minmax_result=minmax_result,
        score_result=score_result,
        potential=potential,
        threshold_points=threshold,
        signal_direction=signal_direction,
        entry_price=float(pattern_matrix.current_values[-1]),
        best_signal_pearson=best_signal_pearson,
        best_candidate_score=best_candidate_score,
    )


__all__ = ["SignalCalculationResult", "calculate_signal"]
