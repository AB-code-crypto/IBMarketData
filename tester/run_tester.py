from __future__ import annotations

import itertools
import sqlite3
import subprocess
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from core.price_source import set_price_db_path_override
from ib_signal.signal_calculator import calculate_signal
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, SignalConfig
from ib_signal.signal_errors import SignalDataNotReadyError
from tester.models import (
    ExecutionVariant,
    SignalBatchResult,
    SignalVariant,
    TesterSignal,
)
from tester.result_store import ResultStore
from tester.trading_simulator import load_price_bars, simulate_trading


# =============================================================================
# НАСТРОЙКИ ТЕСТЕРА. Аргументов командной строки нет.
# =============================================================================

BASE_DIR = Path(__file__).resolve().parents[1]

PRICE_DB_PATH = BASE_DIR / "data" / "prices" / "MNQ.sqlite3"
PRICE_TABLE_NAME = "MNQ_5s"

START_DATETIME_MSK = "2026-01-01 00:00:00"
END_DATETIME_MSK = "2026-01-01 23:59:59"

# Для перебора добавь значения в соответствующий список.
ROLLING_BACK_MINUTES_VALUES = [90]          # пример: [30, 60, 90]
ROLLING_TRADE_MINUTES_VALUES = [30]         # пример: [10, 15, 20]
PEARSON_MIN_VALUES = [0.70]
MINMAX_HARD_FILTER_MAX_RATIO_VALUES = [1.50]
CANDIDATE_MIN_COUNT_VALUES = [3]
CANDIDATE_MAX_COUNT_VALUES = [9]
POTENTIAL_MIN_ABS_END_DELTA_POINTS_VALUES = [10.0]

DELAY_SECONDS_VALUES = [5]                  # только значения, кратные 5
TAKE_PROFIT_POINTS_VALUES = [50.0]          # 0 отключает TP
STOP_LOSS_POINTS_VALUES = [150.0]           # 0 отключает SL
DAILY_TAKE_PROFIT_USD_VALUES = [0.0]        # 0 отключает дневной take-profit

COMMISSION_PER_CONTRACT_SIDE_USD = 0.0
MULTIPLIER_USD_PER_POINT = 2.0
SIGNAL_STEP_SECONDS = 60
HISTORY_LOOKBACK_DAYS = 365

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


def calculate_signal_batch(
        *,
        variant: SignalVariant,
        start_ts: int,
        end_ts: int,
) -> SignalBatchResult:
    settings = build_signal_config(variant)
    signals: list[TesterSignal] = []
    calculation_points = 0
    skipped_points = 0
    no_signal_points = 0

    for signal_bar_ts in iter_signal_bar_timestamps(
        start_ts=start_ts,
        end_ts=end_ts,
        step_seconds=SIGNAL_STEP_SECONDS,
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
            )
        except SignalDataNotReadyError:
            skipped_points += 1
            continue

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


def read_price_db_metadata(db_path: Path, table_name: str) -> dict:
    stat = db_path.stat()
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            f"SELECT COUNT(*), MIN(bar_time_ts), MAX(bar_time_ts) "
            f"FROM \"{table_name}\""
        ).fetchone()
    finally:
        conn.close()
    return {
        "path": str(db_path.resolve()),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "rows_count": int(row[0] or 0),
        "min_ts": None if row[1] is None else int(row[1]),
        "max_ts": None if row[2] is None else int(row[2]),
    }


def main() -> None:
    start_ts = parse_msk_datetime(START_DATETIME_MSK)
    end_ts = parse_msk_datetime(END_DATETIME_MSK)
    set_price_db_path_override("MNQ", PRICE_DB_PATH)

    signal_variants = build_signal_variants()
    execution_variants = build_execution_variants()
    run_count = len(signal_variants) * len(execution_variants)

    result_dir = RESULTS_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S")
    store = ResultStore(result_dir)
    git_commit = read_git_commit()
    price_db_metadata = read_price_db_metadata(PRICE_DB_PATH, PRICE_TABLE_NAME)
    bars = load_price_bars(
        db_path=PRICE_DB_PATH,
        table_name=PRICE_TABLE_NAME,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    print(
        f"MNQ tester: {START_DATETIME_MSK} -> {END_DATETIME_MSK} MSK\n"
        f"price DB: {PRICE_DB_PATH}\n"
        f"price bars in test interval: {len(bars)}\n"
        f"signal variants: {len(signal_variants)}\n"
        f"execution variants: {len(execution_variants)}\n"
        f"total runs: {run_count}\n"
        f"results: {result_dir}",
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
            )
            signal_elapsed = time.perf_counter() - signal_started
            print(
                f"  signals={len(signal_batch.signals)}, "
                f"skipped={signal_batch.skipped_points}, "
                f"signal calculation={signal_elapsed:.2f}s",
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
                    multiplier_usd_per_point=MULTIPLIER_USD_PER_POINT,
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
                    multiplier_usd_per_point=MULTIPLIER_USD_PER_POINT,
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
