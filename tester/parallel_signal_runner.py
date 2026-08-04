from __future__ import annotations

import atexit
import gc
import math
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from multiprocessing import shared_memory
from time import perf_counter
from zoneinfo import ZoneInfo

# Каждый worker является отдельным процессом. Внутренние BLAS-пулы NumPy
# должны оставаться однопоточными, иначе процессы начнут конкурировать друг с
# другом десятками скрытых потоков.
for _thread_env_name in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
):
    os.environ[_thread_env_name] = "1"
os.environ["OMP_DYNAMIC"] = "FALSE"
os.environ["MKL_DYNAMIC"] = "FALSE"

import numpy as np

from ib_signal.signal_calculator import calculate_signal
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, SignalConfig
from ib_signal.signal_errors import SignalDataNotReadyError
from tester.in_memory_signal_data import InMemorySignalDataSource
from tester.models import SignalBatchResult, SignalVariant, TesterSignal


SIGNAL_STEP_SECONDS = 60
HISTORY_LOOKBACK_DAYS = 365
CHUNKS_PER_WORKER = 4
MSK_TIMEZONE = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class SharedArrayDescriptor:
    name: str
    shape: tuple[int, ...]
    dtype: str
    nbytes: int


@dataclass(frozen=True)
class SharedSignalDataDescriptor:
    instrument_code: str
    bar_size_seconds: int
    bar_time_ts: SharedArrayDescriptor
    mid_close: SharedArrayDescriptor
    candidate_signal_ts: SharedArrayDescriptor
    candidate_signal_time_ct: SharedArrayDescriptor
    candidate_hour_ct: SharedArrayDescriptor

    @property
    def memory_bytes(self) -> int:
        return int(
            self.bar_time_ts.nbytes
            + self.mid_close.nbytes
            + self.candidate_signal_ts.nbytes
            + self.candidate_signal_time_ct.nbytes
            + self.candidate_hour_ct.nbytes
        )


@dataclass(frozen=True)
class SignalChunkTask:
    variant_index: int
    chunk_index: int
    start_ts: int
    end_ts: int
    variant: SignalVariant

    @property
    def calculation_points(self) -> int:
        if self.end_ts < self.start_ts:
            return 0
        return (int(self.end_ts) - int(self.start_ts)) // SIGNAL_STEP_SECONDS + 1


@dataclass(frozen=True)
class SignalChunkResult:
    variant_index: int
    chunk_index: int
    signals: list[TesterSignal]
    calculation_points: int
    skipped_points: int
    no_signal_points: int
    stage_timings_seconds: dict[str, float]
    elapsed_seconds: float


@dataclass(frozen=True)
class ParallelSignalBatchResult:
    signal_batch: SignalBatchResult
    stage_timings_seconds: dict[str, float]
    worker_elapsed_seconds: float
    chunk_count: int


class SharedSignalDataOwner:
    """Owns parent-created shared-memory blocks until all workers stop."""

    def __init__(self, source: InMemorySignalDataSource) -> None:
        self._segments: list[shared_memory.SharedMemory] = []
        self.descriptor = SharedSignalDataDescriptor(
            instrument_code=source.instrument_code,
            bar_size_seconds=source.bar_size_seconds,
            bar_time_ts=self._share_array(source.bar_time_ts),
            mid_close=self._share_array(source.mid_close),
            candidate_signal_ts=self._share_array(source.candidate_signal_ts),
            candidate_signal_time_ct=self._share_array(
                source.candidate_signal_time_ct
            ),
            candidate_hour_ct=self._share_array(source.candidate_hour_ct),
        )
        self._closed = False

    def _share_array(self, values: np.ndarray) -> SharedArrayDescriptor:
        array = np.ascontiguousarray(values)
        segment = shared_memory.SharedMemory(
            create=True,
            size=max(1, int(array.nbytes)),
        )
        self._segments.append(segment)
        shared_array = np.ndarray(
            array.shape,
            dtype=array.dtype,
            buffer=segment.buf,
        )
        if array.size:
            shared_array[...] = array
        return SharedArrayDescriptor(
            name=segment.name,
            shape=tuple(int(value) for value in array.shape),
            dtype=array.dtype.str,
            nbytes=int(array.nbytes),
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for segment in self._segments:
            try:
                segment.close()
            except BufferError:
                pass
            try:
                segment.unlink()
            except FileNotFoundError:
                pass
        self._segments.clear()

    def __enter__(self) -> "SharedSignalDataOwner":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


_WORKER_SIGNAL_SOURCE: InMemorySignalDataSource | None = None
_WORKER_SHARED_SEGMENTS: list[shared_memory.SharedMemory] = []


def _attach_array(
        descriptor: SharedArrayDescriptor,
) -> tuple[np.ndarray, shared_memory.SharedMemory]:
    segment = shared_memory.SharedMemory(name=descriptor.name)
    array = np.ndarray(
        descriptor.shape,
        dtype=np.dtype(descriptor.dtype),
        buffer=segment.buf,
    )
    array.flags.writeable = False
    return array, segment


def attach_shared_signal_source(
        descriptor: SharedSignalDataDescriptor,
) -> tuple[InMemorySignalDataSource, list[shared_memory.SharedMemory]]:
    arrays: dict[str, np.ndarray] = {}
    segments: list[shared_memory.SharedMemory] = []
    for name in (
        "bar_time_ts",
        "mid_close",
        "candidate_signal_ts",
        "candidate_signal_time_ct",
        "candidate_hour_ct",
    ):
        array, segment = _attach_array(getattr(descriptor, name))
        arrays[name] = array
        segments.append(segment)

    source = InMemorySignalDataSource(
        instrument_code=descriptor.instrument_code,
        bar_size_seconds=descriptor.bar_size_seconds,
        bar_time_ts=arrays["bar_time_ts"],
        mid_close=arrays["mid_close"],
        candidate_signal_ts=arrays["candidate_signal_ts"],
        candidate_signal_time_ct=arrays["candidate_signal_time_ct"],
        candidate_hour_ct=arrays["candidate_hour_ct"],
    )
    return source, segments


def close_signal_worker() -> None:
    global _WORKER_SIGNAL_SOURCE
    global _WORKER_SHARED_SEGMENTS

    _WORKER_SIGNAL_SOURCE = None
    gc.collect()
    for segment in _WORKER_SHARED_SEGMENTS:
        try:
            segment.close()
        except BufferError:
            pass
    _WORKER_SHARED_SEGMENTS = []


def initialize_signal_worker(descriptor: SharedSignalDataDescriptor) -> None:
    global _WORKER_SIGNAL_SOURCE
    global _WORKER_SHARED_SEGMENTS

    close_signal_worker()
    source, segments = attach_shared_signal_source(descriptor)
    _WORKER_SIGNAL_SOURCE = source
    _WORKER_SHARED_SEGMENTS = segments


atexit.register(close_signal_worker)


def build_signal_config(variant: SignalVariant) -> SignalConfig:
    return replace(
        DEFAULT_SIGNAL_CONFIG,
        rolling_signal_step_seconds=SIGNAL_STEP_SECONDS,
        rolling_back_minutes=variant.rolling_back_minutes,
        rolling_trade_minutes=variant.rolling_trade_minutes,
        pearson_min=variant.pearson_min,
        history_lookback_days=HISTORY_LOOKBACK_DAYS,
        candidate_minmax_hard_filter_max_ratio=(
            variant.minmax_hard_filter_max_ratio
        ),
        candidate_potential_min_count=variant.candidate_min_count,
        candidate_potential_max_count=variant.candidate_max_count,
        candidate_potential_min_abs_end_delta_points=(
            variant.potential_min_abs_end_delta_points
        ),
    )


def format_ts_msk(ts: int) -> str:
    return (
        datetime.fromtimestamp(int(ts), tz=timezone.utc)
        .astimezone(MSK_TIMEZONE)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def ceil_to_signal_step(ts: int) -> int:
    remainder = int(ts) % SIGNAL_STEP_SECONDS
    if remainder == 0:
        return int(ts)
    return int(ts) + SIGNAL_STEP_SECONDS - remainder


def count_signal_points(*, start_ts: int, end_ts: int) -> int:
    first_ts = ceil_to_signal_step(start_ts)
    if first_ts > int(end_ts):
        return 0
    return (int(end_ts) - first_ts) // SIGNAL_STEP_SECONDS + 1


def build_signal_chunk_tasks(
        *,
        variant_index: int,
        variant: SignalVariant,
        start_ts: int,
        end_ts: int,
        worker_count: int,
) -> list[SignalChunkTask]:
    first_ts = ceil_to_signal_step(start_ts)
    point_count = count_signal_points(start_ts=start_ts, end_ts=end_ts)
    if point_count <= 0:
        return []

    target_chunks = min(
        point_count,
        max(1, int(worker_count)) * CHUNKS_PER_WORKER,
    )
    points_per_chunk = max(1, math.ceil(point_count / target_chunks))

    tasks: list[SignalChunkTask] = []
    point_offset = 0
    chunk_index = 0
    while point_offset < point_count:
        chunk_points = min(points_per_chunk, point_count - point_offset)
        chunk_start_ts = first_ts + point_offset * SIGNAL_STEP_SECONDS
        chunk_end_ts = (
            chunk_start_ts + (chunk_points - 1) * SIGNAL_STEP_SECONDS
        )
        tasks.append(
            SignalChunkTask(
                variant_index=int(variant_index),
                chunk_index=chunk_index,
                start_ts=chunk_start_ts,
                end_ts=chunk_end_ts,
                variant=variant,
            )
        )
        point_offset += chunk_points
        chunk_index += 1
    return tasks


def _build_tester_signal(result) -> TesterSignal:
    return TesterSignal(
        signal_bar_ts=result.signal_bar_ts,
        signal_time_msk=format_ts_msk(result.signal_bar_ts),
        signal_time_ct=result.candidate_search.current_signal_bar_time_ct,
        direction=str(result.signal_direction),
        reference_price=result.entry_price,
        best_pearson=result.best_signal_pearson,
        best_candidate_score=result.best_candidate_score,
        potential_end_delta_points=result.potential.end_delta_points,
        potential_max_profit_points=result.potential.max_profit_points,
        potential_max_drawdown_points=result.potential.max_drawdown_points,
        potential_used=result.potential.used_candidates_count,
        raw_candidates_count=result.candidate_search.raw_candidate_rows_count,
        valid_candidates_count=result.total_candidates_count,
        pearson_passed_count=result.pearson_passed_count,
        minmax_passed_count=result.minmax_passed_count,
    )


def calculate_signal_chunk_worker(task: SignalChunkTask) -> SignalChunkResult:
    source = _WORKER_SIGNAL_SOURCE
    if source is None:
        raise RuntimeError("Signal worker не подключён к shared-memory данным")

    started = perf_counter()
    settings = build_signal_config(task.variant)
    signals: list[TesterSignal] = []
    calculation_points = 0
    skipped_points = 0
    no_signal_points = 0
    stage_totals: dict[str, float] = defaultdict(float)

    signal_bar_ts = int(task.start_ts)
    while signal_bar_ts <= int(task.end_ts):
        calculation_points += 1
        try:
            result = calculate_signal(
                instrument_code="MNQ",
                signal_bar_ts=signal_bar_ts,
                settings=settings,
                data_source=source,
            )
        except SignalDataNotReadyError:
            skipped_points += 1
            signal_bar_ts += SIGNAL_STEP_SECONDS
            continue

        for name, elapsed in result.stage_timings_seconds.items():
            stage_totals[name] += float(elapsed)

        if result.has_signal:
            signals.append(_build_tester_signal(result))
        else:
            no_signal_points += 1
        signal_bar_ts += SIGNAL_STEP_SECONDS

    return SignalChunkResult(
        variant_index=task.variant_index,
        chunk_index=task.chunk_index,
        signals=signals,
        calculation_points=calculation_points,
        skipped_points=skipped_points,
        no_signal_points=no_signal_points,
        stage_timings_seconds=dict(stage_totals),
        elapsed_seconds=float(perf_counter() - started),
    )


def merge_signal_chunk_results(
        results: list[SignalChunkResult],
) -> ParallelSignalBatchResult:
    ordered = sorted(results, key=lambda row: row.chunk_index)
    signals = [signal for row in ordered for signal in row.signals]
    signals.sort(key=lambda signal: signal.signal_bar_ts)

    stage_totals: dict[str, float] = defaultdict(float)
    for row in ordered:
        for name, elapsed in row.stage_timings_seconds.items():
            stage_totals[name] += float(elapsed)

    return ParallelSignalBatchResult(
        signal_batch=SignalBatchResult(
            signals=signals,
            calculation_points=sum(row.calculation_points for row in ordered),
            skipped_points=sum(row.skipped_points for row in ordered),
            no_signal_points=sum(row.no_signal_points for row in ordered),
        ),
        stage_timings_seconds=dict(stage_totals),
        worker_elapsed_seconds=float(
            sum(row.elapsed_seconds for row in ordered)
        ),
        chunk_count=len(ordered),
    )


def calculate_signal_batch_parallel(
        *,
        executor: ProcessPoolExecutor,
        variant_index: int,
        variant: SignalVariant,
        start_ts: int,
        end_ts: int,
        worker_count: int,
) -> ParallelSignalBatchResult:
    tasks = build_signal_chunk_tasks(
        variant_index=variant_index,
        variant=variant,
        start_ts=start_ts,
        end_ts=end_ts,
        worker_count=worker_count,
    )
    if not tasks:
        return merge_signal_chunk_results([])

    futures = {
        executor.submit(calculate_signal_chunk_worker, task): task
        for task in tasks
    }
    results: list[SignalChunkResult] = []
    total_chunks = len(tasks)
    progress_step = max(1, total_chunks // 20)

    for completed_chunks, future in enumerate(as_completed(futures), start=1):
        task = futures[future]
        try:
            results.append(future.result())
        except Exception as exc:
            raise RuntimeError(
                "Ошибка worker-процесса: "
                f"variant={task.variant_index}, chunk={task.chunk_index}, "
                f"range={task.start_ts}->{task.end_ts}"
            ) from exc

        if (
            completed_chunks == total_chunks
            or completed_chunks % progress_step == 0
        ):
            percent = completed_chunks / total_chunks * 100.0
            print(
                f"    chunks: {completed_chunks}/{total_chunks} "
                f"({percent:.0f}%)",
                flush=True,
            )

    return merge_signal_chunk_results(results)


def format_stage_timings(
        totals: dict[str, float],
        calculation_points: int,
) -> str:
    if calculation_points <= 0:
        return "no timing data"
    order = [
        "candidate_search",
        "pattern_matrix",
        "pearson",
        "filter_and_score",
        "potential",
        "total",
    ]
    parts = []
    for name in order:
        elapsed = float(totals.get(name, 0.0))
        parts.append(
            f"{name}={elapsed:.2f}s/"
            f"{elapsed * 1000.0 / calculation_points:.2f}ms"
        )
    return ", ".join(parts)


__all__ = [
    "ParallelSignalBatchResult",
    "SharedArrayDescriptor",
    "SharedSignalDataDescriptor",
    "SharedSignalDataOwner",
    "SignalChunkResult",
    "SignalChunkTask",
    "attach_shared_signal_source",
    "build_signal_chunk_tasks",
    "calculate_signal_batch_parallel",
    "close_signal_worker",
    "count_signal_points",
    "format_stage_timings",
    "initialize_signal_worker",
    "merge_signal_chunk_results",
]
