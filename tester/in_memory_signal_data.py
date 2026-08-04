from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from ib_signal.signal_candidates import (
    CandidateSearchResult,
    CandidateWindow,
    build_candidate_window,
    get_max_candidate_signal_ts,
    get_min_candidate_signal_ts,
)
from ib_signal.signal_config import SignalConfig
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.signal_pattern_matrix import PatternMatrixResult, get_expected_points
from ib_signal.signal_time import resolve_allowed_hours
from ib_signal.signal_window import SignalWindow
from tester.models import PriceBar


SECONDS_PER_DAY = 24 * 60 * 60


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


@dataclass(frozen=True)
class InMemoryLoadStats:
    signal_rows: int
    candidate_signal_rows: int
    execution_bars: int
    signal_memory_bytes: int
    elapsed_seconds: float
    loaded_start_ts: int
    loaded_end_ts: int


@dataclass(frozen=True)
class LoadedTesterData:
    signal_source: "InMemorySignalDataSource"
    execution_bars: list[PriceBar]
    price_db_metadata: dict[str, int | str | None]
    stats: InMemoryLoadStats


class InMemorySignalDataSource:
    """Read-only MNQ signal data loaded once from SQLite into NumPy arrays."""

    def __init__(
            self,
            *,
            instrument_code: str,
            bar_size_seconds: int,
            bar_time_ts: np.ndarray,
            mid_close: np.ndarray,
            candidate_signal_ts: np.ndarray,
            candidate_signal_time_ct: np.ndarray,
            candidate_hour_ct: np.ndarray,
    ) -> None:
        self.instrument_code = str(instrument_code)
        self.bar_size_seconds = int(bar_size_seconds)
        self.bar_time_ts = np.asarray(bar_time_ts, dtype=np.int64)
        self.mid_close = np.asarray(mid_close, dtype=float)
        self.candidate_signal_ts = np.asarray(candidate_signal_ts, dtype=np.int64)
        self.candidate_signal_time_ct = np.asarray(candidate_signal_time_ct)
        self.candidate_hour_ct = np.asarray(candidate_hour_ct, dtype=np.int8)

        if self.bar_time_ts.ndim != 1 or self.mid_close.ndim != 1:
            raise ValueError("bar_time_ts и mid_close должны быть одномерными")
        if self.bar_time_ts.size != self.mid_close.size:
            raise ValueError(
                "bar_time_ts и mid_close имеют разную длину: "
                f"ts={self.bar_time_ts.size}, mid={self.mid_close.size}"
            )
        if self.candidate_signal_ts.size != self.candidate_signal_time_ct.size:
            raise ValueError("candidate signal timestamp/time arrays disagree")
        if self.candidate_signal_ts.size != self.candidate_hour_ct.size:
            raise ValueError("candidate signal timestamp/hour arrays disagree")
        if self.bar_time_ts.size == 0:
            raise ValueError("В памяти нет price-баров")
        if np.any(np.diff(self.bar_time_ts) <= 0):
            raise ValueError("bar_time_ts должны строго возрастать")

        self._contiguous_run_lengths = self._build_contiguous_run_lengths()
        self._window_views: dict[int, np.ndarray] = {}
        self._phase_groups = self._build_phase_groups()

    @property
    def memory_bytes(self) -> int:
        total = (
            self.bar_time_ts.nbytes
            + self.mid_close.nbytes
            + self.candidate_signal_ts.nbytes
            + self.candidate_signal_time_ct.nbytes
            + self.candidate_hour_ct.nbytes
            + self._contiguous_run_lengths.nbytes
        )
        total += sum(indices.nbytes for indices in self._phase_groups.values())
        return int(total)

    def _build_contiguous_run_lengths(self) -> np.ndarray:
        count = int(self.bar_time_ts.size)
        row_indices = np.arange(count, dtype=np.int64)
        run_starts = np.zeros((count,), dtype=np.int64)
        if count > 1:
            breaks = np.empty((count,), dtype=bool)
            breaks[0] = True
            breaks[1:] = (
                np.diff(self.bar_time_ts) != int(self.bar_size_seconds)
            )
            run_starts[breaks] = row_indices[breaks]
            np.maximum.accumulate(run_starts, out=run_starts)
        return (row_indices - run_starts + 1).astype(np.int32)

    def _build_phase_groups(self) -> dict[int, np.ndarray]:
        phases = self.candidate_signal_ts % 3600
        return {
            int(phase): np.flatnonzero(phases == int(phase)).astype(np.int32)
            for phase in np.unique(phases)
        }

    def _get_window_view(self, expected_points: int) -> np.ndarray:
        points = int(expected_points)
        if points <= 0:
            raise ValueError(f"expected_points должен быть > 0: {points}")
        cached = self._window_views.get(points)
        if cached is not None:
            return cached
        if points > self.mid_close.size:
            return np.empty((0, points), dtype=float)
        view = np.lib.stride_tricks.sliding_window_view(self.mid_close, points)
        self._window_views[points] = view
        return view

    def _find_exact_bar_index(self, bar_ts: int) -> int | None:
        index = int(np.searchsorted(self.bar_time_ts, int(bar_ts), side="left"))
        if index >= self.bar_time_ts.size:
            return None
        if int(self.bar_time_ts[index]) != int(bar_ts):
            return None
        return index

    def _find_exact_candidate_signal_index(self, signal_ts: int) -> int | None:
        index = int(
            np.searchsorted(self.candidate_signal_ts, int(signal_ts), side="left")
        )
        if index >= self.candidate_signal_ts.size:
            return None
        if int(self.candidate_signal_ts[index]) != int(signal_ts):
            return None
        return index

    def find_candidate_windows(
            self,
            *,
            instrument_code: str,
            current_window: SignalWindow,
            settings: SignalConfig,
    ) -> CandidateSearchResult:
        if str(instrument_code) != self.instrument_code:
            raise ValueError(
                f"In-memory source загружен для {self.instrument_code}, "
                f"получено {instrument_code}"
            )

        current_signal_ts = int(current_window.signal_bar_ts)
        current_index = self._find_exact_candidate_signal_index(current_signal_ts)
        if current_index is None:
            raise SignalDataNotReadyError(
                f"Не найден полный price-бар для signal_bar_ts={current_signal_ts}: "
                f"instrument={instrument_code}"
            )

        current_time_ct = str(self.candidate_signal_time_ct[current_index])
        current_hour_ct = int(self.candidate_hour_ct[current_index])
        instrument_row = Instrument[str(instrument_code)]
        allowed_hours = resolve_allowed_hours(
            current_hour_ct=current_hour_ct,
            sec_type=str(instrument_row["secType"]),
        )
        min_signal_ts = get_min_candidate_signal_ts(
            current_signal_bar_ts=current_signal_ts,
            history_lookback_days=settings.history_lookback_days,
        )
        max_signal_ts = get_max_candidate_signal_ts(current_window)
        phase = current_signal_ts % 3600

        if max_signal_ts <= 0 or not allowed_hours:
            return CandidateSearchResult(
                current_signal_bar_time_ct=current_time_ct,
                current_hour_ct=current_hour_ct,
                allowed_hours_ct=allowed_hours,
                candidates=[],
                raw_candidate_rows_count=0,
                min_candidate_signal_ts=min_signal_ts,
                max_candidate_signal_ts=max_signal_ts,
                signal_phase_seconds=phase,
            )

        phase_indices = self._phase_groups.get(int(phase))
        if phase_indices is None or phase_indices.size == 0:
            selected_indices = np.empty((0,), dtype=np.int32)
        else:
            phase_signal_ts = self.candidate_signal_ts[phase_indices]
            left = 0
            if min_signal_ts is not None:
                left = int(
                    np.searchsorted(
                        phase_signal_ts,
                        int(min_signal_ts),
                        side="left",
                    )
                )
            right = int(
                np.searchsorted(
                    phase_signal_ts,
                    int(max_signal_ts),
                    side="right",
                )
            )
            selected_indices = phase_indices[left:right]
            if selected_indices.size:
                allowed_mask = np.isin(
                    self.candidate_hour_ct[selected_indices],
                    np.asarray(allowed_hours, dtype=np.int8),
                )
                selected_indices = selected_indices[allowed_mask]

        candidates = [
            build_candidate_window(
                signal_bar_ts=int(self.candidate_signal_ts[index]),
                signal_bar_time_ct=str(self.candidate_signal_time_ct[index]),
                hour_ct=int(self.candidate_hour_ct[index]),
                current_window=current_window,
            )
            for index in selected_indices
        ]
        return CandidateSearchResult(
            current_signal_bar_time_ct=current_time_ct,
            current_hour_ct=current_hour_ct,
            allowed_hours_ct=allowed_hours,
            candidates=candidates,
            raw_candidate_rows_count=len(candidates),
            min_candidate_signal_ts=min_signal_ts,
            max_candidate_signal_ts=max_signal_ts,
            signal_phase_seconds=phase,
        )

    def build_pattern_matrix(
            self,
            *,
            instrument_code: str,
            window: SignalWindow,
            candidates: list[CandidateWindow],
    ) -> PatternMatrixResult:
        if str(instrument_code) != self.instrument_code:
            raise ValueError(
                f"In-memory source загружен для {self.instrument_code}, "
                f"получено {instrument_code}"
            )

        expected_points = get_expected_points(window, self.bar_size_seconds)
        window_view = self._get_window_view(expected_points)
        current_end_index = self._find_exact_bar_index(
            int(window.pattern_end_ts) - self.bar_size_seconds
        )
        if (
            current_end_index is None
            or int(self._contiguous_run_lengths[current_end_index]) < expected_points
        ):
            raise SignalDataNotReadyError(
                "current pattern: неправильное количество точек или дырка: "
                f"start={window.pattern_start_ts}, end={window.pattern_end_ts}, "
                f"expected={expected_points}"
            )

        current_start_index = current_end_index - expected_points + 1
        current_values = np.asarray(
            window_view[current_start_index],
            dtype=float,
        ).copy()

        if not candidates:
            return PatternMatrixResult(
                current_values=current_values,
                candidate_matrix=np.empty((0, expected_points), dtype=float),
                valid_candidates=[],
                skipped_candidates_count=0,
                expected_points=expected_points,
            )

        candidate_end_ts = np.fromiter(
            (
                int(candidate.pattern_end_ts) - self.bar_size_seconds
                for candidate in candidates
            ),
            dtype=np.int64,
            count=len(candidates),
        )
        candidate_end_indices = np.searchsorted(
            self.bar_time_ts,
            candidate_end_ts,
            side="left",
        )
        in_bounds = candidate_end_indices < self.bar_time_ts.size
        exact = np.zeros((len(candidates),), dtype=bool)
        exact[in_bounds] = (
            self.bar_time_ts[candidate_end_indices[in_bounds]]
            == candidate_end_ts[in_bounds]
        )
        contiguous = np.zeros((len(candidates),), dtype=bool)
        valid_exact_indices = candidate_end_indices[exact]
        contiguous[exact] = (
            self._contiguous_run_lengths[valid_exact_indices]
            >= expected_points
        )
        valid_mask = exact & contiguous
        valid_positions = np.flatnonzero(valid_mask)
        valid_candidates = [candidates[int(index)] for index in valid_positions]

        if valid_positions.size:
            valid_end_indices = candidate_end_indices[valid_positions]
            start_indices = valid_end_indices - expected_points + 1
            matrix = np.asarray(window_view[start_indices], dtype=float).copy()
        else:
            matrix = np.empty((0, expected_points), dtype=float)

        return PatternMatrixResult(
            current_values=current_values,
            candidate_matrix=matrix,
            valid_candidates=valid_candidates,
            skipped_candidates_count=len(candidates) - len(valid_candidates),
            expected_points=expected_points,
        )

    def read_candidate_full_values(
            self,
            *,
            instrument_code: str,
            candidate: CandidateWindow,
            expected_points: int,
            bar_size_seconds: int,
    ) -> np.ndarray | None:
        if str(instrument_code) != self.instrument_code:
            return None
        if int(bar_size_seconds) != self.bar_size_seconds:
            return None

        start_index = self._find_exact_bar_index(int(candidate.pattern_start_ts))
        if start_index is None:
            return None
        end_index = start_index + int(expected_points) - 1
        if end_index >= self.bar_time_ts.size:
            return None
        if int(self._contiguous_run_lengths[end_index]) < int(expected_points):
            return None
        expected_end_ts = (
            int(candidate.pattern_start_ts)
            + (int(expected_points) - 1) * self.bar_size_seconds
        )
        if int(self.bar_time_ts[end_index]) != expected_end_ts:
            return None

        window_view = self._get_window_view(int(expected_points))
        return np.asarray(window_view[start_index], dtype=float).copy()


def _load_signal_arrays(
        *,
        conn: sqlite3.Connection,
        table_name: str,
        load_start_ts: int,
        load_end_ts: int,
        bar_size_seconds: int,
        mid_price_digits: int,
) -> InMemorySignalDataSource:
    table_ref = quote_identifier(table_name)
    midpoint = (
        f"ROUND((bid_close + ask_close) / 2.0, {int(mid_price_digits)})"
    )
    complete = "bid_close IS NOT NULL AND ask_close IS NOT NULL"

    numeric_rows = conn.execute(
        f"""
        SELECT bar_time_ts, {midpoint} AS mid_close
        FROM {table_ref}
        WHERE bar_time_ts >= ?
          AND bar_time_ts < ?
          AND {complete}
        ORDER BY bar_time_ts
        """,
        (int(load_start_ts), int(load_end_ts)),
    )
    numeric = np.fromiter(
        numeric_rows,
        dtype=[("bar_time_ts", "<i8"), ("mid_close", "<f8")],
    )

    modifier = f"+{int(bar_size_seconds)} seconds"
    signal_rows = conn.execute(
        f"""
        SELECT
            bar_time_ts + ? AS signal_bar_ts,
            datetime(bar_time_ct, ?) AS signal_time_ct,
            CAST(substr(datetime(bar_time_ct, ?), 12, 2) AS INTEGER) AS hour_ct
        FROM {table_ref}
        WHERE bar_time_ts >= ?
          AND bar_time_ts < ?
          AND ((bar_time_ts + ?) % 60) = 0
          AND {complete}
        ORDER BY bar_time_ts
        """,
        (
            int(bar_size_seconds),
            modifier,
            modifier,
            int(load_start_ts),
            int(load_end_ts),
            int(bar_size_seconds),
        ),
    )
    signal_meta = np.fromiter(
        signal_rows,
        dtype=[
            ("signal_bar_ts", "<i8"),
            ("signal_time_ct", "U19"),
            ("hour_ct", "i1"),
        ],
    )

    return InMemorySignalDataSource(
        instrument_code="MNQ",
        bar_size_seconds=bar_size_seconds,
        bar_time_ts=numeric["bar_time_ts"],
        mid_close=numeric["mid_close"],
        candidate_signal_ts=signal_meta["signal_bar_ts"],
        candidate_signal_time_ct=signal_meta["signal_time_ct"],
        candidate_hour_ct=signal_meta["hour_ct"],
    )


def _load_execution_bars(
        *,
        conn: sqlite3.Connection,
        table_name: str,
        start_ts: int,
        end_ts: int,
) -> list[PriceBar]:
    rows = conn.execute(
        f"""
        SELECT
            bar_time_ts,
            bid_open,
            bid_high,
            bid_low,
            bid_close,
            ask_open,
            ask_high,
            ask_low,
            ask_close
        FROM {quote_identifier(table_name)}
        WHERE bar_time_ts >= ?
          AND bar_time_ts <= ?
          AND bid_open IS NOT NULL
          AND bid_high IS NOT NULL
          AND bid_low IS NOT NULL
          AND bid_close IS NOT NULL
          AND ask_open IS NOT NULL
          AND ask_high IS NOT NULL
          AND ask_low IS NOT NULL
          AND ask_close IS NOT NULL
        ORDER BY bar_time_ts
        """,
        (int(start_ts), int(end_ts)),
    )
    return [
        PriceBar(
            bar_time_ts=int(row[0]),
            bid_open=float(row[1]),
            bid_high=float(row[2]),
            bid_low=float(row[3]),
            bid_close=float(row[4]),
            ask_open=float(row[5]),
            ask_high=float(row[6]),
            ask_low=float(row[7]),
            ask_close=float(row[8]),
        )
        for row in rows
    ]


def load_tester_data(
        *,
        db_path: Path,
        table_name: str,
        start_ts: int,
        end_ts: int,
        history_lookback_days: int,
        max_rolling_back_minutes: int,
) -> LoadedTesterData:
    started = perf_counter()
    instrument_row = Instrument["MNQ"]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])
    load_start_ts = (
        int(start_ts)
        - int(history_lookback_days) * SECONDS_PER_DAY
        - int(max_rolling_back_minutes) * 60
    )
    load_end_ts = int(end_ts) + 1

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA query_only=ON")
        signal_source = _load_signal_arrays(
            conn=conn,
            table_name=table_name,
            load_start_ts=load_start_ts,
            load_end_ts=load_end_ts,
            bar_size_seconds=bar_size_seconds,
            mid_price_digits=int(instrument_row["mid_price_digits"]),
        )
        execution_bars = _load_execution_bars(
            conn=conn,
            table_name=table_name,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metadata_row = conn.execute(
            f"SELECT COUNT(*), MIN(bar_time_ts), MAX(bar_time_ts) "
            f"FROM {quote_identifier(table_name)}"
        ).fetchone()
    finally:
        conn.close()

    elapsed = perf_counter() - started
    stat = Path(db_path).stat()
    return LoadedTesterData(
        signal_source=signal_source,
        execution_bars=execution_bars,
        price_db_metadata={
            "path": str(Path(db_path).resolve()),
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
            "rows_count": int(metadata_row[0] or 0),
            "min_ts": None if metadata_row[1] is None else int(metadata_row[1]),
            "max_ts": None if metadata_row[2] is None else int(metadata_row[2]),
        },
        stats=InMemoryLoadStats(
            signal_rows=int(signal_source.bar_time_ts.size),
            candidate_signal_rows=int(signal_source.candidate_signal_ts.size),
            execution_bars=len(execution_bars),
            signal_memory_bytes=signal_source.memory_bytes,
            elapsed_seconds=float(elapsed),
            loaded_start_ts=load_start_ts,
            loaded_end_ts=load_end_ts,
        ),
    )


__all__ = [
    "InMemoryLoadStats",
    "LoadedTesterData",
    "InMemorySignalDataSource",
    "load_tester_data",
]
