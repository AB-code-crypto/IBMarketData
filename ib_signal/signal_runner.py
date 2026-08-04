from __future__ import annotations

import asyncio

from core.logger import get_logger, log_info, setup_logging
from core.price_source import (
    FreshPriceBarStatus,
    get_fresh_price_bar_status,
    read_price_bar_time_ct,
)
from ib_signal.signal_calculator import calculate_signal
from ib_signal.signal_candidate_potential import (
    build_candidate_final_outcome_result,
    format_candidate_potential_result,
)
from ib_signal.signal_candidate_rank_features import (
    format_candidate_minmax_hard_filter_result,
    format_candidate_score_result,
)
from ib_signal.signal_candidates import format_candidate_search_result
from ib_signal.signal_config import SignalConfig
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.signal_event import build_signal_event
from ib_signal.signal_event_store import write_signal_event
from ib_signal.signal_pattern_matrix import format_pattern_matrix_result
from ib_signal.signal_plot import save_signal_candidate_plot
from ib_signal.signal_schedule import get_due_signal_bar_ts
from ib_signal.signal_window import format_signal_window_for_log

setup_logging()
logger = get_logger(__name__)

SIGNAL_LOOP_SLEEP_SECONDS = 1
PRICE_DB_WAIT_SECONDS = 5


def format_fresh_price_bar_status(status: FreshPriceBarStatus) -> str:
    return (
        f"ready={status.is_ready}, "
        f"reason={status.reason}, "
        f"last_bar={status.last_bar_time_ct}, "
        f"close_ts={status.last_bar_close_ts}, "
        f"lag={status.last_bar_lag_seconds}, "
        f"mid_close={status.mid_close}"
    )


async def wait_for_fresh_price_bars(
        instrument_codes: list[str],
        settings: SignalConfig,
) -> list[str]:
    pending = set(str(code) for code in instrument_codes)
    ready: list[str] = []
    log_info(
        logger,
        f"Жду свежие полные BID/ASK price-бары: {sorted(pending)}",
        to_telegram=True,
    )

    while pending:
        for instrument_code in list(pending):
            status = get_fresh_price_bar_status(
                instrument_code,
                settings.max_price_bar_lag_seconds,
            )
            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: price-бар пока не готов: "
                    f"{format_fresh_price_bar_status(status)}",
                    to_telegram=False,
                )
                continue
            ready.append(instrument_code)
            pending.remove(instrument_code)
            log_info(
                logger,
                f"{instrument_code}: price-бар готов: "
                f"{format_fresh_price_bar_status(status)}",
                to_telegram=False,
            )

        if pending:
            await asyncio.sleep(PRICE_DB_WAIT_SECONDS)

    return ready


def _calculate_signal_once(
        *,
        instrument_code: str,
        due_signal_bar_ts: int,
        status: FreshPriceBarStatus,
        settings: SignalConfig,
) -> None:
    calculation = calculate_signal(
        instrument_code=instrument_code,
        signal_bar_ts=due_signal_bar_ts,
        settings=settings,
    )

    minmax_final_outcomes = build_candidate_final_outcome_result(
        instrument_code=instrument_code,
        candidates=calculation.score_result.valid_candidates,
    )
    saved_plot_path = save_signal_candidate_plot(
        instrument_code=instrument_code,
        signal_bar_time_ct=calculation.candidate_search.current_signal_bar_time_ct,
        signal_window=calculation.signal_window,
        current_values=calculation.pattern_matrix.current_values,
        valid_candidates=calculation.score_result.valid_candidates,
        candidate_matrix=calculation.score_result.candidate_matrix,
        pearson_scores=calculation.score_result.pearson_scores,
        candidate_scores=calculation.score_result.candidate_scores,
        candidate_potential_result=calculation.potential,
        minmax_final_outcome_result=minmax_final_outcomes,
        total_candidates_count=calculation.total_candidates_count,
        pearson_passed_count=calculation.pearson_passed_count,
        minmax_passed_count=calculation.minmax_passed_count,
        pearson_best_initial=(
            float(calculation.pearson_passed_scores.max())
            if calculation.pearson_passed_scores.size
            else None
        ),
        pearson_worst_initial=(
            float(calculation.pearson_passed_scores.min())
            if calculation.pearson_passed_scores.size
            else None
        ),
        pearson_best_after_minmax=(
            float(calculation.score_result.pearson_scores.max())
            if calculation.score_result.pearson_scores.size
            else None
        ),
        pearson_worst_after_minmax=(
            float(calculation.score_result.pearson_scores.min())
            if calculation.score_result.pearson_scores.size
            else None
        ),
    )

    signal_id: int | None = None
    if calculation.has_signal:
        event = build_signal_event(
            instrument_code=instrument_code,
            signal_bar_ts=due_signal_bar_ts,
            signal_time_ct=calculation.candidate_search.current_signal_bar_time_ct,
            direction=str(calculation.signal_direction),
            entry_price=calculation.entry_price,
            settings=settings,
            best_pearson=calculation.best_signal_pearson,
            candidate_score_best=calculation.best_candidate_score,
            potential_end_delta_points=calculation.potential.end_delta_points,
            potential_max_profit_points=calculation.potential.max_profit_points,
            potential_max_drawdown_points=calculation.potential.max_drawdown_points,
            potential_used=calculation.potential.used_candidates_count,
        )
        signal_id = write_signal_event(event)

    window_text = format_signal_window_for_log(
        calculation.signal_window,
        lambda ts: read_price_bar_time_ct(instrument_code, ts),
    )
    log_info(
        logger,
        (
            f"{instrument_code}: rolling signal calculation\n"
            f"  latest_price_bar={status.last_bar_time_ct} CT, "
            f"lag={status.last_bar_lag_seconds}s, mid_close={status.mid_close}\n"
            f"  window: {window_text}\n"
            f"  candidates: "
            f"{format_candidate_search_result(calculation.candidate_search)}\n"
            f"  matrix: "
            f"{format_pattern_matrix_result(calculation.pattern_matrix)}\n"
            f"  pearson: min={settings.pearson_min:.3f}, "
            f"best_raw={calculation.best_raw_pearson:.4f}, "
            f"passed={calculation.pearson_passed_count}/"
            f"{calculation.total_candidates_count}\n"
            f"  minmax: "
            f"{format_candidate_minmax_hard_filter_result(calculation.minmax_result)}\n"
            f"  score: "
            f"{format_candidate_score_result(calculation.score_result, top_limit=3)}\n"
            f"  potential: {format_candidate_potential_result(calculation.potential)}\n"
            f"  signal_id={signal_id}, plot={saved_plot_path}"
        ),
        to_telegram=False,
    )


async def run_signal_loop(
        instrument_codes: list[str],
        settings: SignalConfig,
) -> None:
    codes = [str(code) for code in instrument_codes]
    last_seen_bar_ts: dict[str, int | None] = {code: None for code in codes}
    last_calculated_bar_ts: dict[str, int | None] = {code: None for code in codes}

    log_info(
        logger,
        f"Запускаю rolling-only signal-loop: {codes}",
        to_telegram=True,
    )

    while True:
        for instrument_code in codes:
            status = get_fresh_price_bar_status(
                instrument_code,
                settings.max_price_bar_lag_seconds,
            )
            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: расчёт пропущен, price-бар не готов: "
                    f"{format_fresh_price_bar_status(status)}",
                    to_telegram=False,
                )
                continue

            current_bar_ts = status.last_bar_time_ts
            closed_bar_ts = status.last_bar_close_ts
            if current_bar_ts is None or closed_bar_ts is None:
                continue

            due_signal_bar_ts = get_due_signal_bar_ts(
                current_bar_ts=closed_bar_ts,
                settings=settings,
                last_calculated_bar_ts=last_calculated_bar_ts[instrument_code],
            )

            if last_seen_bar_ts[instrument_code] is None:
                last_seen_bar_ts[instrument_code] = current_bar_ts
                last_calculated_bar_ts[instrument_code] = due_signal_bar_ts
                log_info(
                    logger,
                    f"{instrument_code}: начальный price-бар принят: "
                    f"{status.last_bar_time_ct} CT; расчёт начнётся со следующей due-точки",
                    to_telegram=False,
                )
                continue

            if current_bar_ts > int(last_seen_bar_ts[instrument_code] or 0):
                last_seen_bar_ts[instrument_code] = current_bar_ts

            if due_signal_bar_ts is None:
                continue

            try:
                _calculate_signal_once(
                    instrument_code=instrument_code,
                    due_signal_bar_ts=due_signal_bar_ts,
                    status=status,
                    settings=settings,
                )
            except SignalDataNotReadyError as exc:
                log_info(
                    logger,
                    f"{instrument_code}: rolling-расчёт пропущен, данных недостаточно: {exc}",
                    to_telegram=False,
                )
            finally:
                # A due point is attempted once. Repeating it every second would
                # create log storms and cannot repair a historical gap.
                last_calculated_bar_ts[instrument_code] = due_signal_bar_ts

        await asyncio.sleep(SIGNAL_LOOP_SLEEP_SECONDS)


__all__ = [
    "SIGNAL_LOOP_SLEEP_SECONDS",
    "PRICE_DB_WAIT_SECONDS",
    "format_fresh_price_bar_status",
    "wait_for_fresh_price_bars",
    "run_signal_loop",
]
