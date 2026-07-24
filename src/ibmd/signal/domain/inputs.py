from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .calculation import CandidateWindow


@dataclass(frozen=True)
class SignalSourceBar:
    bar_id: str
    revision: int
    instrument_id: str
    con_id: int
    local_symbol: str
    bar_start_ts: int
    bar_end_ts: int
    bar_start_utc: str
    bar_end_utc: str
    published_at_utc: str
    entry_price: float


@dataclass(frozen=True)
class SignalPatternInputs:
    source_bar: SignalSourceBar
    current_values: np.ndarray
    candidates: tuple[CandidateWindow, ...]
    candidate_matrix: np.ndarray
    raw_candidate_count: int
    skipped_candidate_count: int
