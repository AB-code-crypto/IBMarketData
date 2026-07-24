"""Pure deterministic rolling-signal calculations."""

from .calculation import (
    CandidateRanking,
    CandidateWindow,
    PatternPathFeatures,
    PotentialResult,
    RankedCandidate,
    SignalCalculationError,
    SignalWindow,
    build_potential,
    build_signal_window,
    calculate_centered_pearson_batch,
    get_due_signal_bar_ts,
    rank_candidates,
    signal_direction,
)
from .inputs import SignalPatternInputs, SignalSourceBar

__all__ = [
    "CandidateRanking",
    "CandidateWindow",
    "PatternPathFeatures",
    "PotentialResult",
    "RankedCandidate",
    "SignalCalculationError",
    "SignalPatternInputs",
    "SignalSourceBar",
    "SignalWindow",
    "build_potential",
    "build_signal_window",
    "calculate_centered_pearson_batch",
    "get_due_signal_bar_ts",
    "rank_candidates",
    "signal_direction",
]
