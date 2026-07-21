from __future__ import annotations

import asyncio

import numpy as np

from core.logger import get_logger, log_info, setup_logging
from core.price_source import (
    FreshPriceBarStatus,
    get_fresh_price_bar_status,
    read_price_bar_time_ct,
)
from ib_signal.pearson import calculate_centered_pearson_batch
from ib_signal.signal_candidate_potential import (
    build_candidate_potential_result,
    format_candidate_potential_result,
)
from ib_signal.signal_candidate_rank_features import (
    filter_candidates_by_minmax_ratio,
    format_candidate_minmax_hard_filter_result,
    format_candidate_score_result,
    rank_candidates_by_score,
)
from ib_signal.signal_candidates import (
    find_candidate_windows,
    format_candidate_search_result,
)
from ib_signal.signal_config import SignalConfig
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.signal_event import build_signal_event
from ib_signal.signal_event_store import write_signal_event
from ib_signal.signal_pattern_matrix import (
    build_pattern_matrix,
    format_pattern_matrix_result,
)
from ib_signal.signal_plot import save_signal_candidate_plot
from ib_signal.signal_schedule import get_due_signal_bar_ts
from ib_signal.signal_window import (
    build_current_signal_window,
    format_signal_window_for_log,
)

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
    signal_window = build_current_signal_window(
        signal_bar_ts=due_signal_bar_ts,
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

    saved_plot_path = save_signal_candidate_plot(
        instrument_code=instrument_code,
        signal_bar_time_ct=candidate_search.current_signal_bar_time_ct,
        signal_window=signal_window,
        current_values=pattern_matrix.current_values,
        valid_candidates=score_result.valid_candidates,
        candidate_matrix=score_result.candidate_matrix,
        pearson_scores=score_result.pearson_scores,
        candidate_scores=score_result.candidate_scores,
        candidate_potential_result=potential,
    )

    signal_id: int | None = None
    threshold = abs(float(settings.candidate_potential_min_abs_end_delta_points))
    if (
        potential.is_available
        and potential.direction in {"LONG", "SHORT"}
        and abs(float(potential.end_delta_points)) > threshold
    ):
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
        event = build_signal_event(
            instrument_code=instrument_code,
            signal_bar_ts=due_signal_bar_ts,
            signal_time_ct=candidate_search.current_signal_bar_time_ct,
            direction=potential.direction,
            entry_price=float(pattern_matrix.current_values[-1]),
            settings=settings,
            best_pearson=best_signal_pearson,
            candidate_score_best=best_candidate_score,
            potential_end_delta_points=potential.end_delta_points,
            potential_max_profit_points=potential.max_profit_points,
            potential_max_drawdown_points=potential.max_drawdown_points,
            potential_used=potential.used_candidates_count,
        )
        signal_id = write_signal_event(event)

    window_text = format_signal_window_for_log(
        signal_window,
        lambda ts: read_price_bar_time_ct(instrument_code, ts),
    )
    log_info(
        logger,
        (
            f"{instrument_code}: rolling signal calculation\n"
            f"  latest_price_bar={status.last_bar_time_ct} CT, "
            f"lag={status.last_bar_lag_seconds}s, mid_close={status.mid_close}\n"
            f"  window: {window_text}\n"
            f"  candidates: {format_candidate_search_result(candidate_search)}\n"
            f"  matrix: {format_pattern_matrix_result(pattern_matrix)}\n"
            f"  pearson: min={settings.pearson_min:.3f}, "
            f"best_raw={best_raw_pearson:.4f}, "
            f"passed={len(passed_candidates)}/{len(pattern_matrix.valid_candidates)}\n"
            f"  minmax: {format_candidate_minmax_hard_filter_result(minmax_result)}\n"
            f"  score: {format_candidate_score_result(score_result, top_limit=3)}\n"
            f"  potential: {format_candidate_potential_result(potential)}\n"
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
