from __future__ import annotations

import os

# Должно выполняться до первого import NumPy. Каждый worker использует один
# вычислительный поток, а параллелизм создаётся отдельными процессами.
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

import gc
import itertools
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from tester.in_memory_signal_data import load_tester_data
from tester.models import ExecutionVariant, SignalVariant
from tester.parallel_signal_runner import (
    SharedSignalDataOwner,
    calculate_signal_batch_parallel,
    format_stage_timings,
    initialize_signal_worker,
)
from tester.result_store import ResultStore
from tester.trading_simulator import simulate_trading


# =============================================================================
# НАСТРОЙКИ ТЕСТЕРА. Аргументов командной строки нет.
# =============================================================================

BASE_DIR = Path(__file__).resolve().parents[1]

PRICE_DB_PATH = BASE_DIR / "data" / "prices" / "MNQ.sqlite3"
PRICE_TABLE_NAME = "MNQ_5s"

START_DATETIME_MSK = "2026-08-04 00:00:00"
END_DATETIME_MSK = "2026-08-04 23:59:59"

# Для i7-14700 используем 20 worker-процессов: по одному на физическое ядро.
# Если параллельно на компьютере выполняется тяжёлая работа, значение можно
# временно уменьшить.
WORKER_PROCESSES = 20

# Для перебора добавь значения в соответствующий список.
ROLLING_BACK_MINUTES_VALUES = [90]          # пример: [30, 60, 90]
ROLLING_TRADE_MINUTES_VALUES = [30]         # пример: [10, 15, 20]
PEARSON_MIN_VALUES = [0.70]
MINMAX_HARD_FILTER_MAX_RATIO_VALUES = [1.50]
CANDIDATE_MIN_COUNT_VALUES = [3]
CANDIDATE_MAX_COUNT_VALUES = [9]
POTENTIAL_MIN_ABS_END_DELTA_POINTS_VALUES = [10.0]

DELAY_SECONDS_VALUES = [10]                 # только значения, кратные 5
TAKE_PROFIT_POINTS_VALUES = [50.0]          # 0 отключает TP
STOP_LOSS_POINTS_VALUES = [150.0]           # 0 отключает SL
DAILY_TAKE_PROFIT_USD_VALUES = [0.0]        # 0 отключает дневной take-profit

COMMISSION_PER_CONTRACT_SIDE_USD = 0.62

RESULTS_ROOT = BASE_DIR / "tester" / "results"


MSK_TIMEZONE = ZoneInfo("Europe/Moscow")


def parse_msk_datetime(value: str) -> int:
    local_dt = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=MSK_TIMEZONE
    )
    return int(local_dt.astimezone(timezone.utc).timestamp())


def format_bytes(value: int) -> str:
    size = float(value)
    for suffix in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024.0 or suffix == "TiB":
            return f"{size:.2f} {suffix}"
        size /= 1024.0
    return f"{size:.2f} TiB"


def build_signal_variants() -> list[SignalVariant]:
    return [
        SignalVariant(
            rolling_back_minutes=int(rolling_back),
            rolling_trade_minutes=int(rolling_trade),
            pearson_min=float(pearson_min),
            minmax_hard_filter_max_ratio=float(minmax_ratio),
            candidate_min_count=int(candidate_min),
            candidate_max_count=int(candidate_max),
            potential_min_abs_end_delta_points=float(potential_threshold),
        )
        for (
            rolling_back,
            rolling_trade,
            pearson_min,
            minmax_ratio,
            candidate_min,
            candidate_max,
            potential_threshold,
        ) in itertools.product(
            ROLLING_BACK_MINUTES_VALUES,
            ROLLING_TRADE_MINUTES_VALUES,
            PEARSON_MIN_VALUES,
            MINMAX_HARD_FILTER_MAX_RATIO_VALUES,
            CANDIDATE_MIN_COUNT_VALUES,
            CANDIDATE_MAX_COUNT_VALUES,
            POTENTIAL_MIN_ABS_END_DELTA_POINTS_VALUES,
        )
    ]


def build_execution_variants() -> list[ExecutionVariant]:
    return [
        ExecutionVariant(
            delay_seconds=int(delay_seconds),
            take_profit_points=float(take_profit),
            stop_loss_points=float(stop_loss),
            daily_take_profit_usd=float(daily_take_profit),
        )
        for delay_seconds, take_profit, stop_loss, daily_take_profit in itertools.product(
            DELAY_SECONDS_VALUES,
            TAKE_PROFIT_POINTS_VALUES,
            STOP_LOSS_POINTS_VALUES,
            DAILY_TAKE_PROFIT_USD_VALUES,
        )
    ]


def main() -> None:
    start_ts = parse_msk_datetime(START_DATETIME_MSK)
    end_ts = parse_msk_datetime(END_DATETIME_MSK)

    signal_variants = build_signal_variants()
    execution_variants = build_execution_variants()
    run_count = len(signal_variants) * len(execution_variants)
    logical_cpu_count = os.cpu_count() or 1
    worker_count = max(1, min(int(WORKER_PROCESSES), logical_cpu_count))

    result_dir = RESULTS_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S")
    store = ResultStore(result_dir)

    print(
        f"MNQ tester: {START_DATETIME_MSK} -> {END_DATETIME_MSK} MSK\n"
        f"price DB: {PRICE_DB_PATH}\n"
        f"signal variants: {len(signal_variants)}\n"
        f"execution variants: {len(execution_variants)}\n"
        f"total runs: {run_count}\n"
        f"worker processes: {worker_count} "
        f"(logical CPUs detected: {logical_cpu_count})\n"
        f"results: {result_dir}\n"
        "Loading price history into RAM through one SQLite connection...",
        flush=True,
    )

    completed_runs = 0
    try:
        loaded = load_tester_data(
            db_path=PRICE_DB_PATH,
            table_name=PRICE_TABLE_NAME,
            start_ts=start_ts,
            end_ts=end_ts,
            history_lookback_days=365,
            max_rolling_back_minutes=max(ROLLING_BACK_MINUTES_VALUES),
        )
        bars = loaded.execution_bars
        load_stats = loaded.stats
        signal_source = loaded.signal_source
        print(
            f"RAM load complete: signal_rows={load_stats.signal_rows}, "
            f"candidate_signal_rows={load_stats.candidate_signal_rows}, "
            f"execution_bars={load_stats.execution_bars}, "
            f"signal_memory={format_bytes(load_stats.signal_memory_bytes)}, "
            f"elapsed={load_stats.elapsed_seconds:.2f}s\n"
            "Copying immutable signal arrays to shared memory...",
            flush=True,
        )

        with SharedSignalDataOwner(signal_source) as shared_owner:
            print(
                "Shared memory ready: "
                f"{format_bytes(shared_owner.descriptor.memory_bytes)}",
                flush=True,
            )

            # После копирования исходный набор signal-массивов в главном
            # процессе больше не нужен. Worker-процессы используют общие блоки.
            del signal_source
            del loaded
            gc.collect()

            process_context = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(
                max_workers=worker_count,
                mp_context=process_context,
                initializer=initialize_signal_worker,
                initargs=(shared_owner.descriptor,),
            ) as executor:
                for signal_index, signal_variant in enumerate(
                    signal_variants,
                    start=1,
                ):
                    print(
                        f"\nSignal variant {signal_index}/"
                        f"{len(signal_variants)}: {signal_variant}",
                        flush=True,
                    )
                    signal_started = time.perf_counter()
                    parallel_result = calculate_signal_batch_parallel(
                        executor=executor,
                        variant_index=signal_index,
                        variant=signal_variant,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        worker_count=worker_count,
                    )
                    signal_elapsed = time.perf_counter() - signal_started
                    signal_batch = parallel_result.signal_batch
                    wall_average_ms = (
                        signal_elapsed * 1000.0 / signal_batch.calculation_points
                        if signal_batch.calculation_points
                        else 0.0
                    )
                    print(
                        "  worker stage totals: "
                        + format_stage_timings(
                            parallel_result.stage_timings_seconds,
                            signal_batch.calculation_points,
                        ),
                        flush=True,
                    )
                    print(
                        f"  chunks={parallel_result.chunk_count}, "
                        f"signals={len(signal_batch.signals)}, "
                        f"skipped={signal_batch.skipped_points}, "
                        f"parallel wall={signal_elapsed:.2f}s, "
                        f"worker time sum="
                        f"{parallel_result.worker_elapsed_seconds:.2f}s, "
                        f"wall average={wall_average_ms:.2f}ms/point",
                        flush=True,
                    )

                    for execution_variant in execution_variants:
                        simulation = simulate_trading(
                            bars=bars,
                            signals=signal_batch.signals,
                            execution=execution_variant,
                            commission_per_contract_side_usd=(
                                COMMISSION_PER_CONTRACT_SIDE_USD
                            ),
                        )
                        run_id = store.save_run(
                            signal_variant=signal_variant,
                            execution_variant=execution_variant,
                            signal_batch=signal_batch,
                            simulation=simulation,
                        )
                        completed_runs += 1
                        print(
                            f"  run {completed_runs}/{run_count}, id={run_id}, "
                            f"execution={execution_variant}, "
                            f"trades={simulation.metrics['trades_count']}, "
                            f"net={simulation.metrics['net_profit_usd']:+.2f} USD",
                            flush=True,
                        )
    finally:
        store.close()

    print(
        f"\nГотово. Runs: {completed_runs}. Results: {result_dir}",
        flush=True,
    )


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
