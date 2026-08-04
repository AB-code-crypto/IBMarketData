from __future__ import annotations

import itertools
import subprocess
import time
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from ib_signal.signal_calculator import calculate_signal
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, SignalConfig
from ib_signal.signal_errors import SignalDataNotReadyError
from tester.in_memory_signal_data import InMemorySignalDataSource, load_tester_data
from tester.models import (
    ExecutionVariant,
    SignalBatchResult,
    SignalVariant,
    TesterSignal,
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
PROGRESS_EVERY_CALCULATIONS = 100


MSK_TIMEZONE = ZoneInfo("Europe/Moscow")


def parse_msk_datetime(value: str) -> int:
    local_dt = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=MSK_TIMEZONE
    )
    return int(local_dt.astimezone(timezone.utc).timestamp())


def format_ts_msk(ts: int) -> str:
    return (
        datetime.fromtimestamp(int(ts), tz=timezone.utc)
        .astimezone(MSK_TIMEZONE)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def format_bytes(value: int) -> str:
    size = float(value)
    for suffix in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024.0 or suffix == "TiB":
            return f"{size:.2f} {suffix}"
        size /= 1024.0
    return f"{size:.2f} TiB"


def ceil_to_step(ts: int, step_seconds: int) -> int:
    remainder = int(ts) % int(step_seconds)
    if remainder == 0:
        return int(ts)
    return int(ts) + int(step_seconds) - remainder


def iter_signal_bar_timestamps(
        *,
        start_ts: int,
        end_ts: int,
        step_seconds: int,
) -> Iterable[int]:
    current = ceil_to_step(start_ts, step_seconds)
    while current <= int(end_ts):
        yield current
        current += int(step_seconds)


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


def build_signal_config(variant: SignalVariant) -> SignalConfig:
    return replace(
        DEFAULT_SIGNAL_CONFIG,
        rolling_signal_step_seconds=60,
        rolling_back_minutes=variant.rolling_back_minutes,
        rolling_trade_minutes=variant.rolling_trade_minutes,
        pearson_min=variant.pearson_min,
        history_lookback_days=365,
        candidate_minmax_hard_filter_max_ratio=(
            variant.minmax_hard_filter_max_ratio
        ),
        candidate_potential_min_count=variant.candidate_min_count,
        candidate_potential_max_count=variant.candidate_max_count,
        candidate_potential_min_abs_end_delta_points=(
            variant.potential_min_abs_end_delta_points
        ),
    )


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
            f"{name}={elapsed:.2f}s/{elapsed * 1000.0 / calculation_points:.2f}ms"
        )
    return ", ".join(parts)


def calculate_signal_batch(
        *,
        variant: SignalVariant,
        start_ts: int,
        end_ts: int,
        data_source: InMemorySignalDataSource,
) -> SignalBatchResult:
    settings = build_signal_config(variant)
    signals: list[TesterSignal] = []
    calculation_points = 0
    skipped_points = 0
    no_signal_points = 0
    stage_totals: dict[str, float] = defaultdict(float)

    for signal_bar_ts in iter_signal_bar_timestamps(
        start_ts=start_ts,
        end_ts=end_ts,
        step_seconds=60,
    ):
        calculation_points += 1
        if (
            PROGRESS_EVERY_CALCULATIONS > 0
            and calculation_points % PROGRESS_EVERY_CALCULATIONS == 0
        ):
            print(
                f"    signal points: {calculation_points}, "
                f"current={format_ts_msk(signal_bar_ts)} MSK",
                flush=True,
            )

        try:
            result = calculate_signal(
                instrument_code="MNQ",
                signal_bar_ts=signal_bar_ts,
                settings=settings,
                data_source=data_source,
            )
        except SignalDataNotReadyError:
            skipped_points += 1
            continue

        for name, elapsed in result.stage_timings_seconds.items():
            stage_totals[name] += float(elapsed)

        if not result.has_signal:
            no_signal_points += 1
            continue

        signals.append(
            TesterSignal(
                signal_bar_ts=result.signal_bar_ts,
                signal_time_msk=format_ts_msk(result.signal_bar_ts),
                signal_time_ct=result.candidate_search.current_signal_bar_time_ct,
                direction=str(result.signal_direction),
                reference_price=result.entry_price,
                best_pearson=result.best_signal_pearson,
                best_candidate_score=result.best_candidate_score,
                potential_end_delta_points=result.potential.end_delta_points,
                potential_max_profit_points=result.potential.max_profit_points,
                potential_max_drawdown_points=(
                    result.potential.max_drawdown_points
                ),
                potential_used=result.potential.used_candidates_count,
                raw_candidates_count=(
                    result.candidate_search.raw_candidate_rows_count
                ),
                valid_candidates_count=result.total_candidates_count,
                pearson_passed_count=result.pearson_passed_count,
                minmax_passed_count=result.minmax_passed_count,
            )
        )

    print(
        "  signal stage profile: "
        + format_stage_timings(stage_totals, calculation_points),
        flush=True,
    )
    return SignalBatchResult(
        signals=signals,
        calculation_points=calculation_points,
        skipped_points=skipped_points,
        no_signal_points=no_signal_points,
    )


def read_git_commit() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=BASE_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def main() -> None:
    start_ts = parse_msk_datetime(START_DATETIME_MSK)
    end_ts = parse_msk_datetime(END_DATETIME_MSK)

    signal_variants = build_signal_variants()
    execution_variants = build_execution_variants()
    run_count = len(signal_variants) * len(execution_variants)

    result_dir = RESULTS_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S")
    store = ResultStore(result_dir)
    git_commit = read_git_commit()

    print(
        f"MNQ tester: {START_DATETIME_MSK} -> {END_DATETIME_MSK} MSK\n"
        f"price DB: {PRICE_DB_PATH}\n"
        f"signal variants: {len(signal_variants)}\n"
        f"execution variants: {len(execution_variants)}\n"
        f"total runs: {run_count}\n"
        f"results: {result_dir}\n"
        "Loading price history into RAM through one SQLite connection...",
        flush=True,
    )

    loaded = load_tester_data(
        db_path=PRICE_DB_PATH,
        table_name=PRICE_TABLE_NAME,
        start_ts=start_ts,
        end_ts=end_ts,
        history_lookback_days=365,
        max_rolling_back_minutes=max(ROLLING_BACK_MINUTES_VALUES),
    )
    bars = loaded.execution_bars
    price_db_metadata = loaded.price_db_metadata
    signal_source = loaded.signal_source
    print(
        f"RAM load complete: signal_rows={loaded.stats.signal_rows}, "
        f"candidate_signal_rows={loaded.stats.candidate_signal_rows}, "
        f"execution_bars={loaded.stats.execution_bars}, "
        f"signal_memory={format_bytes(loaded.stats.signal_memory_bytes)}, "
        f"elapsed={loaded.stats.elapsed_seconds:.2f}s",
        flush=True,
    )

    completed_runs = 0
    try:
        for signal_index, signal_variant in enumerate(signal_variants, start=1):
            print(
                f"\nSignal variant {signal_index}/{len(signal_variants)}: "
                f"{signal_variant}",
                flush=True,
            )
            signal_started = time.perf_counter()
            signal_batch = calculate_signal_batch(
                variant=signal_variant,
                start_ts=start_ts,
                end_ts=end_ts,
                data_source=signal_source,
            )
            signal_elapsed = time.perf_counter() - signal_started
            average_ms = (
                signal_elapsed * 1000.0 / signal_batch.calculation_points
                if signal_batch.calculation_points
                else 0.0
            )
            print(
                f"  signals={len(signal_batch.signals)}, "
                f"skipped={signal_batch.skipped_points}, "
                f"signal calculation={signal_elapsed:.2f}s, "
                f"average={average_ms:.2f}ms/point",
                flush=True,
            )

            for execution_variant in execution_variants:
                run_started = time.perf_counter()
                simulation = simulate_trading(
                    bars=bars,
                    signals=signal_batch.signals,
                    execution=execution_variant,
                    commission_per_contract_side_usd=(
                        COMMISSION_PER_CONTRACT_SIDE_USD
                    ),
                )
                run_elapsed = time.perf_counter() - run_started
                run_id = store.save_run(
                    git_commit=git_commit,
                    price_db_metadata=price_db_metadata,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    signal_variant=signal_variant,
                    execution_variant=execution_variant,
                    commission_per_contract_side_usd=(
                        COMMISSION_PER_CONTRACT_SIDE_USD
                    ),
                    signal_batch=signal_batch,
                    simulation=simulation,
                    elapsed_seconds=signal_elapsed + run_elapsed,
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
    main()
