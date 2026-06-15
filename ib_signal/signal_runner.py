import asyncio

import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.logger import get_logger, log_info, setup_logging
from ib_signal.job_reader import get_fresh_job_bar_status, read_job_bar_time_ct
from ib_signal.signal_schedule import get_due_signal_bar_ts
from ib_signal.signal_config import MarketRegimeFilterMode, SignalConfig
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.candidate_funnel_store import record_candidate_funnel_event
from ib_signal.signal_event import build_signal_event
from ib_signal.signal_event_store import write_signal_event
from ib_signal.signal_interpretation import interpret_signal_event
from ib_signal.pearson import calculate_centered_pearson_batch
from ib_signal.signal_candidate_regime_filter import (
    filter_candidates_by_market_regime,
    format_candidate_regime_filter_result,
)
from ib_signal.signal_candidate_rank_features import (
    filter_candidates_by_minmax_ratio,
    format_candidate_minmax_hard_filter_result,
    format_candidate_score_result,
    rank_candidates_by_score,
)
from ib_signal.signal_candidate_potential import (
    build_candidate_potential_result,
    format_candidate_potential_result,
)
from ib_signal.signal_candidates import find_candidate_windows
from ib_signal.signal_pattern_matrix import build_pattern_matrix
from ib_signal.signal_plot import save_signal_candidate_plot
from ib_signal.signal_regression import (
    build_linear_regression,
    calculate_regression_delta_bps,
    classify_regression_direction,
)
from ib_signal.signal_regression_threshold import get_regression_flat_delta_threshold_bps
from ib_signal.signal_regression_relation import build_regression_relation
from ib_signal.signal_sma_reader import read_current_sma_values
from ib_signal.signal_window import build_current_signal_window, format_signal_window_for_log

setup_logging()
logger = get_logger(__name__)

SIGNAL_LOOP_SLEEP_SECONDS = 1
JOB_DB_WAIT_SECONDS = 5


def format_fresh_job_bar_status(status) -> str:
    """Что делает: форматирует статус свежести последнего job-бара в одну строку лога.
    Зачем нужна: в ожидании и runtime-пропусках видно, почему signal-сервис не считает сигнал."""
    return (
        f"ready={status.is_ready}, "
        f"reason={status.reason}, "
        f"last_bar={status.last_bar_time_ct}, "
        f"lag={status.last_bar_lag_seconds}"
    )


def format_candidate_search_summary(result) -> str:
    """Что делает: компактно форматирует первичный поиск кандидатов.
    Зачем нужна: финальный runtime-лог должен читаться человеком, а не превращаться в кашу."""
    prefix = (
        f"hour={result.current_hour_slot_ct}, "
        f"allowed={result.allowed_hour_slots_ct}, "
        f"raw={result.raw_candidate_rows_count}, "
        f"slot_kept={result.slot_offset_kept_count}, "
        f"slot_dropped={result.slot_offset_dropped_count}, "
    )

    if not result.candidates:
        return prefix + "found=0"

    first_candidate = result.candidates[0]
    last_candidate = result.candidates[-1]

    return (
        prefix
        + f"found={len(result.candidates)}, "
        + f"first={first_candidate.signal_bar_time_ct} CT, "
        + f"last={last_candidate.signal_bar_time_ct} CT"
    )


def relation_to_text(value) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


def build_candidate_funnel_rows(
        *,
        candidate_search_result,
        pattern_matrix_result=None,
        pearson_min: float | None = None,
        pearson_passed_count: int | None = None,
        best_pearson: float | None = None,
        price_sma_600_relation=None,
        current_price_direction=None,
        current_sma600_direction=None,
        candidate_regime_filter_result=None,
        regime_skip_reason: str | None = None,
        candidate_minmax_filter_result=None,
        candidate_score_result=None,
        candidate_potential_result=None,
        final_plot_candidates_count: int | None = None,
) -> list[tuple[str, str | None]]:
    rows: list[tuple[str, str | None]] = [
        (
            f"time/hr/phase: {candidate_search_result.raw_candidate_rows_count}",
            None,
        ),
        (
            f"slot offset  : {candidate_search_result.slot_offset_kept_count} "
            f"(-{candidate_search_result.slot_offset_dropped_count})",
            None,
        ),
    ]

    if pattern_matrix_result is not None:
        rows.append((
            f"matrix valid : {len(pattern_matrix_result.valid_candidates)} "
            f"(-{pattern_matrix_result.skipped_candidates_count})",
            None,
        ))

    if pearson_passed_count is not None:
        rows.append((
            f"pearson      : {pearson_passed_count}"
            f" / {pearson_min:.2f}; best={best_pearson:.4f}"
            if pearson_min is not None and best_pearson is not None
            else f"pearson      : {pearson_passed_count}",
            None,
        ))

    relation_text = relation_to_text(getattr(price_sma_600_relation, "relation", None))
    if relation_text is not None:
        rows.append((f"relation     : {relation_text}", None))

    if current_price_direction is not None or current_sma600_direction is not None:
        rows.append((
            f"dirs p/sma   : {relation_to_text(current_price_direction)}/{relation_to_text(current_sma600_direction)}",
            None,
        ))

    if candidate_regime_filter_result is not None:
        rows.extend([
            (f"regime final : {candidate_regime_filter_result.final_kept_count}", None),
            (f"regime soft  : {candidate_regime_filter_result.soft_kept_count}", None),
            (f"regime hard  : {candidate_regime_filter_result.hard_kept_count}", None),
            (
                f"regime drop  : sma={candidate_regime_filter_result.skipped_sma_count} "
                f"rel={candidate_regime_filter_result.relation_mismatch_count} "
                f"dir={candidate_regime_filter_result.direction_mismatch_count}",
                None,
            ),
        ])
    elif regime_skip_reason is not None:
        rows.append((f"regime skip  : {regime_skip_reason}", None))

    if candidate_minmax_filter_result is not None:
        rows.append((
            f"minmax       : {candidate_minmax_filter_result.kept_candidates_count} "
            f"(-{candidate_minmax_filter_result.dropped_candidates_count})",
            None,
        ))

    if candidate_score_result is not None:
        rows.append((f"score        : {len(candidate_score_result.valid_candidates)}", None))

    if candidate_potential_result is not None:
        if candidate_potential_result.is_available:
            rows.append((
                f"potential    : used={candidate_potential_result.used_candidates_count}/"
                f"{candidate_potential_result.source_candidates_count}",
                None,
            ))
        else:
            rows.append((f"potential    : {candidate_potential_result.unavailable_reason}", None))

    if final_plot_candidates_count is not None:
        rows.append((f"plot candidates: {final_plot_candidates_count}", None))

    phase_text = "None" if candidate_search_result.signal_phase_seconds is None else str(candidate_search_result.signal_phase_seconds)
    offset_text = "None" if candidate_search_result.slot_offset_seconds is None else str(candidate_search_result.slot_offset_seconds)
    rows.append((f"phase/offset : {phase_text}/{offset_text}", None))
    return rows


def format_candidate_funnel_summary(rows: list[tuple[str, str | None]]) -> str:
    return "; ".join(text for text, _ in rows)


def record_candidate_funnel_snapshot(
        *,
        instrument_code: str,
        due_signal_bar_ts: int,
        settings: SignalConfig,
        candidate_search_result,
        pattern_matrix_result=None,
        pearson_passed_count: int | None = None,
        best_pearson: float | None = None,
        price_sma_600_relation=None,
        current_price_direction=None,
        current_sma600_direction=None,
        candidate_regime_filter_result=None,
        regime_skip_reason: str | None = None,
        candidate_minmax_filter_result=None,
        candidate_score_result=None,
        candidate_potential_result=None,
        final_plot_candidates_count: int | None = None,
        png_saved: bool | None = None,
        skip_reason: str | None = None,
) -> None:
    try:
        record_candidate_funnel_event(
            instrument_code=instrument_code,
            signal_bar_ts=due_signal_bar_ts,
            signal_time_ct=candidate_search_result.current_signal_bar_time_ct,
            signal_window_mode=settings.signal_window_mode.value,
            market_regime_filter_mode=settings.market_regime_filter_mode.value,
            current_hour_ct=candidate_search_result.current_hour_slot_ct,
            allowed_hours_ct=candidate_search_result.allowed_hour_slots_ct,
            min_candidate_signal_ts=candidate_search_result.min_candidate_signal_ts,
            max_candidate_signal_ts=candidate_search_result.max_candidate_signal_ts,
            signal_phase_seconds=candidate_search_result.signal_phase_seconds,
            slot_offset_seconds=candidate_search_result.slot_offset_seconds,
            raw_candidate_rows_count=candidate_search_result.raw_candidate_rows_count,
            slot_offset_kept_count=candidate_search_result.slot_offset_kept_count,
            slot_offset_dropped_count=candidate_search_result.slot_offset_dropped_count,
            matrix_source_candidates_count=len(candidate_search_result.candidates) if pattern_matrix_result is not None else None,
            matrix_valid_candidates_count=len(pattern_matrix_result.valid_candidates) if pattern_matrix_result is not None else None,
            matrix_skipped_candidates_count=getattr(pattern_matrix_result, "skipped_candidates_count", None),
            pearson_min=settings.pearson_min,
            pearson_passed_count=pearson_passed_count,
            best_pearson=best_pearson,
            current_relation=relation_to_text(getattr(price_sma_600_relation, "relation", None)),
            current_price_direction=relation_to_text(current_price_direction),
            current_sma600_direction=relation_to_text(current_sma600_direction),
            regime_source_candidates_count=getattr(candidate_regime_filter_result, "source_candidates_count", None),
            regime_pearson_passed_count=getattr(candidate_regime_filter_result, "pearson_passed_count", None),
            regime_soft_kept_count=getattr(candidate_regime_filter_result, "soft_kept_count", None),
            regime_hard_kept_count=getattr(candidate_regime_filter_result, "hard_kept_count", None),
            regime_final_kept_count=getattr(candidate_regime_filter_result, "final_kept_count", None),
            regime_skipped_sma_count=getattr(candidate_regime_filter_result, "skipped_sma_count", None),
            regime_relation_mismatch_count=getattr(candidate_regime_filter_result, "relation_mismatch_count", None),
            regime_direction_mismatch_count=getattr(candidate_regime_filter_result, "direction_mismatch_count", None),
            regime_skip_reason=regime_skip_reason,
            minmax_source_candidates_count=getattr(candidate_minmax_filter_result, "source_candidates_count", None),
            minmax_kept_candidates_count=getattr(candidate_minmax_filter_result, "kept_candidates_count", None),
            minmax_dropped_candidates_count=getattr(candidate_minmax_filter_result, "dropped_candidates_count", None),
            minmax_disabled_reason=getattr(candidate_minmax_filter_result, "disabled_reason", None),
            score_candidates_count=len(candidate_score_result.valid_candidates) if candidate_score_result is not None else None,
            potential_available=getattr(candidate_potential_result, "is_available", None),
            potential_used_candidates_count=getattr(candidate_potential_result, "used_candidates_count", None),
            potential_source_candidates_count=getattr(candidate_potential_result, "source_candidates_count", None),
            potential_direction=getattr(candidate_potential_result, "direction", None),
            potential_unavailable_reason=getattr(candidate_potential_result, "unavailable_reason", None),
            potential_end_delta_points=getattr(candidate_potential_result, "end_delta_points", None),
            final_plot_candidates_count=final_plot_candidates_count,
            png_saved=png_saved,
            skip_reason=skip_reason,
        )
    except Exception as exc:
        log_info(
            logger,
            f"{instrument_code}: candidate_funnel запись в state DB не удалась: {type(exc).__name__}: {exc}",
            to_telegram=False,
        )


def format_regression_summary(
        label: str,
        regression,
        *,
        flat_delta_threshold_bps: float,
) -> str:
    """Что делает: форматирует regression delta и direction в человекочитаемом виде.
    Зачем нужна: bps и пункты должны быть рядом, без длинных внутренних имён."""
    if regression is None:
        return f"{label}=None"

    delta_bps = calculate_regression_delta_bps(regression)
    direction = classify_regression_direction(
        regression,
        flat_delta_threshold_bps=flat_delta_threshold_bps,
    )

    return (
        f"{label}: "
        f"delta={delta_bps:.2f} bps / {regression.fitted_delta:.2f} pt, "
        f"dir={direction}"
    )


def format_regression_relation_summary(label: str, relation) -> str:
    """Что делает: форматирует взаимное расположение regression lines.
    Зачем нужна: relation должен быть виден одной компактной строкой."""
    if relation is None:
        return f"{label}=None"

    return (
        f"{label}: {relation.relation}, "
        f"start={relation.diff_start_bps:.2f} bps / {relation.diff_start_points:.2f} pt, "
        f"end={relation.diff_end_bps:.2f} bps / {relation.diff_end_points:.2f} pt"
    )



async def wait_for_fresh_job_bars(
        instrument_codes: list[str],
        settings: SignalConfig,
) -> list[str]:
    """Что делает: ждёт свежий последний job-бар по каждому instrument_code.
    Зачем нужна: signal-сервис стартует расчёт только после того, как job-data начал обновлять рабочие данные."""
    pending = set(instrument_codes)
    ready = []

    log_info(
        logger,
        f"Жду свежих job-баров для signal-сервиса: {sorted(pending)}",
        to_telegram=True,
    )

    while pending:
        for instrument_code in list(pending):
            status = get_fresh_job_bar_status(
                instrument_code=instrument_code,
                max_job_bar_lag_seconds=settings.max_job_bar_lag_seconds,
            )

            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: свежий job-бар пока не готов: "
                    f"{format_fresh_job_bar_status(status)}",
                    to_telegram=False,
                )
                continue

            log_info(
                logger,
                f"{instrument_code}: свежий job-бар готов: "
                f"{format_fresh_job_bar_status(status)}",
                to_telegram=False,
            )
            ready.append(instrument_code)
            pending.remove(instrument_code)

        if pending:
            await asyncio.sleep(JOB_DB_WAIT_SECONDS)

    return ready


async def run_signal_loop(
        instrument_codes: list[str],
        settings: SignalConfig,
) -> None:
    """Что делает: отслеживает новые job-бары, считает raw-сигнал и пишет interpreted signal_event.
    Зачем нужна: это основной runtime-цикл signal-сервиса."""

    # last_seen_ts_by_instrument - память для логирования появления нового job-бара.
    last_seen_ts_by_instrument: dict[str, int | None] = {
        instrument_code: None
        for instrument_code in instrument_codes
    }

    # last_calculated защищает от повторного расчёта одного и того же signal bar.
    last_calculated_ts_by_instrument: dict[str, int | None] = {
        instrument_code: None
        for instrument_code in instrument_codes
    }

    log_info(
        logger,
        f"Запускаю signal-loop для инструментов: {instrument_codes}",
        to_telegram=True,
    )

    while True:
        for instrument_code in instrument_codes:
            # status - лёгкая проверка свежести job DB
            status = get_fresh_job_bar_status(
                instrument_code=instrument_code,
                max_job_bar_lag_seconds=settings.max_job_bar_lag_seconds,
            )

            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: пропускаю расчёт, job-бар не свежий: "
                    f"{format_fresh_job_bar_status(status)}",
                    to_telegram=False,
                )
                continue

            # current_last_ts - последний доступный bar_time_ts в job DB.
            current_last_ts = status.last_bar_time_ts
            # previous_last_ts - прошлое значение current_last_ts для логирования.
            previous_last_ts = last_seen_ts_by_instrument[instrument_code]

            if current_last_ts is None:
                continue

            # bar_time_ts в job DB — это время начала бара.
            # Точка принятия решения должна быть на границе закрытия этого бара.
            # Размер бара берём из contracts.py, а не дублируем в signal config.
            bar_size_seconds = get_bar_size_seconds(Instrument[instrument_code]["barSizeSetting"])
            closed_bar_ts = current_last_ts + bar_size_seconds

            # due_signal_bar_ts - реальная точка принятия решения, для которой надо считать сигнал.
            due_signal_bar_ts = get_due_signal_bar_ts(
                current_bar_ts=closed_bar_ts,
                settings=settings,
                last_calculated_bar_ts=last_calculated_ts_by_instrument[instrument_code],
            )

            if previous_last_ts is None:
                last_seen_ts_by_instrument[instrument_code] = current_last_ts
                last_calculated_ts_by_instrument[instrument_code] = due_signal_bar_ts

                log_info(
                    logger,
                    f"{instrument_code}: начальный job-bar принят: "
                    f"latest_job_bar={status.last_bar_time_ct} CT. "
                    f"Первый расчёт будет со следующей due-точки.",
                    to_telegram=False,
                )
                continue

            elif current_last_ts > previous_last_ts:
                last_seen_ts_by_instrument[instrument_code] = current_last_ts
                log_info(
                    logger,
                    f"{instrument_code}: появился новый job bar: "
                    f"{status.last_bar_time_ct} CT",
                    to_telegram=False,
                )

            if due_signal_bar_ts is None:
                continue

            # signal_window - объект с границами. Он нужен как вход в следующий слой:
            # сбор текущего паттерна, поиск кандидатов и дальнейший расчёт сигнала.
            signal_window = build_current_signal_window(
                signal_bar_ts=due_signal_bar_ts,
                settings=settings,
            )

            try:
                candidate_search_result = find_candidate_windows(
                    instrument_code=instrument_code,
                    current_window=signal_window,
                    settings=settings,
                )

                pattern_matrix_result = build_pattern_matrix(
                    instrument_code=instrument_code,
                    window=signal_window,
                    candidates=candidate_search_result.candidates,
                    price_source=settings.price_source,
                )

                pearson_scores = calculate_centered_pearson_batch(
                    pattern_matrix_result.current_values,
                    pattern_matrix_result.candidate_matrix,
                )

                pearson_passed_count = int((pearson_scores >= settings.pearson_min).sum())
                best_pearson = (
                    float(pearson_scores.max())
                    if pearson_scores.size > 0
                    else 0.0
                )

                regression_flat_delta_threshold_bps = get_regression_flat_delta_threshold_bps(
                    instrument_code,
                )

                price_regression = build_linear_regression(
                    pattern_matrix_result.current_values,
                )
                sma_600_values = read_current_sma_values(
                    instrument_code=instrument_code,
                    signal_window=signal_window,
                    period_bars=600,
                )
                sma_600_regression = (
                    build_linear_regression(sma_600_values)
                    if sma_600_values is not None
                    else None
                )
                price_sma_600_relation = (
                    build_regression_relation(
                        base_regression=price_regression,
                        reference_regression=sma_600_regression,
                        near_threshold_bps=regression_flat_delta_threshold_bps,
                    )
                    if sma_600_regression is not None
                    else None
                )

                plot_valid_candidates = pattern_matrix_result.valid_candidates
                plot_candidate_matrix = pattern_matrix_result.candidate_matrix
                plot_pearson_scores = pearson_scores
                candidate_regime_filter_result = None
                current_price_direction = None
                current_sma600_direction = None

                if (
                        price_sma_600_relation is not None
                        and price_sma_600_relation.relation != "mixed_sma"
                        and sma_600_regression is not None
                ):
                    current_price_direction = classify_regression_direction(
                        price_regression,
                        flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
                    )
                    current_sma600_direction = classify_regression_direction(
                        sma_600_regression,
                        flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
                    )

                    candidate_regime_filter_result = filter_candidates_by_market_regime(
                        instrument_code=instrument_code,
                        candidates=pattern_matrix_result.valid_candidates,
                        candidate_matrix=pattern_matrix_result.candidate_matrix,
                        pearson_scores=pearson_scores,
                        pearson_min=settings.pearson_min,
                        current_relation=price_sma_600_relation.relation,
                        current_price_direction=current_price_direction,
                        current_sma600_direction=current_sma600_direction,
                        near_threshold_bps=regression_flat_delta_threshold_bps,
                        flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
                        mode=settings.market_regime_filter_mode,
                        sma_period_bars=600,
                    )
                    plot_valid_candidates = candidate_regime_filter_result.valid_candidates
                    plot_candidate_matrix = candidate_regime_filter_result.candidate_matrix
                    plot_pearson_scores = candidate_regime_filter_result.pearson_scores

                elif settings.market_regime_filter_mode != MarketRegimeFilterMode.OFF:
                    last_calculated_ts_by_instrument[instrument_code] = due_signal_bar_ts
                    skip_reason = (
                        "current_relation=mixed_sma"
                        if price_sma_600_relation is not None
                        and price_sma_600_relation.relation == "mixed_sma"
                        else "SMA 600 relation не рассчитан"
                    )
                    funnel_rows = build_candidate_funnel_rows(
                        candidate_search_result=candidate_search_result,
                        pattern_matrix_result=pattern_matrix_result,
                        pearson_min=settings.pearson_min,
                        pearson_passed_count=pearson_passed_count,
                        best_pearson=best_pearson,
                        price_sma_600_relation=price_sma_600_relation,
                        regime_skip_reason=skip_reason,
                    )
                    record_candidate_funnel_snapshot(
                        instrument_code=instrument_code,
                        due_signal_bar_ts=due_signal_bar_ts,
                        settings=settings,
                        candidate_search_result=candidate_search_result,
                        pattern_matrix_result=pattern_matrix_result,
                        pearson_passed_count=pearson_passed_count,
                        best_pearson=best_pearson,
                        price_sma_600_relation=price_sma_600_relation,
                        regime_skip_reason=skip_reason,
                        skip_reason=skip_reason,
                    )
                    log_info(
                        logger,
                        f"{instrument_code}: пропускаю расчёт, "
                        f"signal_window_mode={settings.signal_window_mode.value}, "
                        f"market_regime_filter_mode={settings.market_regime_filter_mode.value}, "
                        f"{skip_reason}; "
                        f"latest_job_row={status.last_bar_time_ct} CT, "
                        f"pearson_passed={pearson_passed_count}; "
                        f"candidate_funnel: {format_candidate_funnel_summary(funnel_rows)}",
                        to_telegram=False,
                    )
                    continue

                if candidate_regime_filter_result is None:
                    passed_indices = np.flatnonzero(plot_pearson_scores >= settings.pearson_min)
                    plot_valid_candidates = [
                        plot_valid_candidates[int(index)]
                        for index in passed_indices
                    ]
                    plot_candidate_matrix = plot_candidate_matrix[passed_indices, :]
                    plot_pearson_scores = plot_pearson_scores[passed_indices]

                candidate_minmax_filter_result = filter_candidates_by_minmax_ratio(
                    current_values=pattern_matrix_result.current_values,
                    candidates=plot_valid_candidates,
                    candidate_matrix=plot_candidate_matrix,
                    pearson_scores=plot_pearson_scores,
                    max_ratio=settings.candidate_minmax_hard_filter_max_ratio,
                )
                plot_valid_candidates = candidate_minmax_filter_result.valid_candidates
                plot_candidate_matrix = candidate_minmax_filter_result.candidate_matrix
                plot_pearson_scores = candidate_minmax_filter_result.pearson_scores

                candidate_score_result = rank_candidates_by_score(
                    current_values=pattern_matrix_result.current_values,
                    candidates=plot_valid_candidates,
                    candidate_matrix=plot_candidate_matrix,
                    pearson_scores=plot_pearson_scores,
                    pearson_weight=settings.candidate_score_pearson_weight,
                    end_delta_weight=settings.candidate_score_end_delta_weight,
                    minmax_weight=settings.candidate_score_minmax_weight,
                )
                plot_valid_candidates = candidate_score_result.valid_candidates
                plot_pearson_scores = candidate_score_result.pearson_scores
                plot_candidate_scores = candidate_score_result.candidate_scores

                candidate_potential_result = build_candidate_potential_result(
                    instrument_code=instrument_code,
                    signal_window=signal_window,
                    current_values=pattern_matrix_result.current_values,
                    candidates=plot_valid_candidates,
                    candidate_scores=plot_candidate_scores,
                    price_source=settings.price_source,
                    min_count=settings.candidate_potential_min_count,
                    max_count=settings.candidate_potential_max_count,
                )

                potential_min_abs_end_delta_points = abs(
                    float(settings.candidate_potential_min_abs_end_delta_points),
                )
                signal_event_best_pearson = (
                    float(plot_pearson_scores.max())
                    if plot_pearson_scores.size > 0
                    else 0.0
                )
                signal_event_candidate_score_best = (
                    float(plot_candidate_scores.max())
                    if plot_candidate_scores.size > 0
                    else None
                )

                if (
                        candidate_potential_result.is_available
                        and candidate_potential_result.direction in ("LONG", "SHORT")
                        and abs(candidate_potential_result.end_delta_points) > potential_min_abs_end_delta_points
                ):
                    signal_interpretation = interpret_signal_event(
                        instrument_code=instrument_code,
                        signal_bar_ts=due_signal_bar_ts,
                        signal_time_ct=candidate_search_result.current_signal_bar_time_ct,
                        direction=candidate_potential_result.direction,
                        entry_price=float(pattern_matrix_result.current_values[-1]),
                    )
                    signal_event = build_signal_event(
                        instrument_code=instrument_code,
                        signal_bar_ts=due_signal_bar_ts,
                        signal_time_ct=candidate_search_result.current_signal_bar_time_ct,
                        direction=candidate_potential_result.direction,
                        entry_price=float(pattern_matrix_result.current_values[-1]),
                        settings=settings,
                        best_pearson=signal_event_best_pearson,
                        candidate_score_best=signal_event_candidate_score_best,
                        potential_end_delta_points=candidate_potential_result.end_delta_points,
                        potential_max_profit_points=candidate_potential_result.max_profit_points,
                        potential_max_drawdown_points=candidate_potential_result.max_drawdown_points,
                        potential_used=candidate_potential_result.used_candidates_count,
                        feature_bar_ts=signal_interpretation.feature_bar_ts,
                        regime=signal_interpretation.regime,
                        ma_zone=signal_interpretation.ma_zone,
                        signal_allowed=signal_interpretation.signal_allowed,
                        signal_reject_reason=signal_interpretation.signal_reject_reason,
                        signal_strength=signal_interpretation.signal_strength,
                        order_type=signal_interpretation.order_type,
                        order_policy_reason=signal_interpretation.order_policy_reason,
                        limit_offset_points=signal_interpretation.limit_offset_points,
                        limit_price=signal_interpretation.limit_price,
                        ttl_seconds=signal_interpretation.ttl_seconds,
                        signal_rules_json=signal_interpretation.signal_rules_json,
                    )
                    signal_id = write_signal_event(signal_event)
                    log_info(
                        logger,
                        f"{instrument_code}: signal_event записан: "
                        f"signal_id={signal_id}, "
                        f"direction={signal_event.direction}, "
                        f"entry={signal_event.entry_price:.2f}, "
                        f"potential_end={signal_event.potential_end_delta_points:+.2f}",
                        to_telegram=False,
                    )

                funnel_rows = build_candidate_funnel_rows(
                    candidate_search_result=candidate_search_result,
                    pattern_matrix_result=pattern_matrix_result,
                    pearson_min=settings.pearson_min,
                    pearson_passed_count=pearson_passed_count,
                    best_pearson=best_pearson,
                    price_sma_600_relation=price_sma_600_relation,
                    current_price_direction=current_price_direction,
                    current_sma600_direction=current_sma600_direction,
                    candidate_regime_filter_result=candidate_regime_filter_result,
                    candidate_minmax_filter_result=candidate_minmax_filter_result,
                    candidate_score_result=candidate_score_result,
                    candidate_potential_result=candidate_potential_result,
                    final_plot_candidates_count=len(plot_valid_candidates),
                )

                saved_plot_path = save_signal_candidate_plot(
                    instrument_code=instrument_code,
                    signal_bar_time_ct=candidate_search_result.current_signal_bar_time_ct,
                    signal_window=signal_window,
                    current_values=pattern_matrix_result.current_values,
                    valid_candidates=plot_valid_candidates,
                    pearson_scores=plot_pearson_scores,
                    price_source=settings.price_source,
                    pearson_min=settings.pearson_min,
                    regression_flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
                    signal_window_mode=settings.signal_window_mode.value,
                    market_regime_filter_mode=settings.market_regime_filter_mode.value,
                    candidate_scores=plot_candidate_scores,
                    candidate_potential_result=candidate_potential_result,
                    candidate_funnel_rows=funnel_rows,
                )

                record_candidate_funnel_snapshot(
                    instrument_code=instrument_code,
                    due_signal_bar_ts=due_signal_bar_ts,
                    settings=settings,
                    candidate_search_result=candidate_search_result,
                    pattern_matrix_result=pattern_matrix_result,
                    pearson_passed_count=pearson_passed_count,
                    best_pearson=best_pearson,
                    price_sma_600_relation=price_sma_600_relation,
                    current_price_direction=current_price_direction,
                    current_sma600_direction=current_sma600_direction,
                    candidate_regime_filter_result=candidate_regime_filter_result,
                    candidate_minmax_filter_result=candidate_minmax_filter_result,
                    candidate_score_result=candidate_score_result,
                    candidate_potential_result=candidate_potential_result,
                    final_plot_candidates_count=len(plot_valid_candidates),
                    png_saved=saved_plot_path is not None,
                )

                if saved_plot_path is not None:
                    log_info(
                        logger,
                        f"{instrument_code}: сохранён PNG с кандидатами: {saved_plot_path}",
                        to_telegram=False,
                    )


            except SignalDataNotReadyError as exc:
                # Например, после клиринга первая строка 17:00:00 закрывается в 17:00:05,
                # а due-точка 17:00:00 требует строку 16:59:55, которой нет.
                # Это не авария signal-сервиса, а штатный пропуск расчёта.
                last_calculated_ts_by_instrument[instrument_code] = due_signal_bar_ts

                window_text = format_signal_window_for_log(
                    signal_window,
                    lambda ts: read_job_bar_time_ct(instrument_code, ts),
                )

                log_info(
                    logger,
                    f"{instrument_code}: пропускаю расчёт, данных для окна недостаточно: "
                    f"{exc}; latest_job_row={status.last_bar_time_ct} CT, "
                    f"window={window_text}",
                    to_telegram=False,
                )
                continue

            last_calculated_ts_by_instrument[instrument_code] = due_signal_bar_ts

            window_text = format_signal_window_for_log(
                signal_window,
                lambda ts: read_job_bar_time_ct(instrument_code, ts),
            )
            candidate_search_text = format_candidate_search_summary(candidate_search_result)

            price_regression_text = format_regression_summary(
                "price",
                price_regression,
                flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
            )
            sma_600_regression_text = format_regression_summary(
                "sma600",
                sma_600_regression,
                flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
            )
            price_sma_600_relation_text = format_regression_relation_summary(
                "price_vs_sma600",
                price_sma_600_relation,
            )
            candidate_regime_filter_text = format_candidate_regime_filter_result(
                candidate_regime_filter_result,
                mode=settings.market_regime_filter_mode,
                pearson_passed_count=pearson_passed_count,
            )
            candidate_minmax_filter_text = format_candidate_minmax_hard_filter_result(
                candidate_minmax_filter_result,
            )
            candidate_score_text = format_candidate_score_result(
                candidate_score_result,
                top_limit=3,
            )
            candidate_potential_text = format_candidate_potential_result(
                candidate_potential_result,
            )

            log_info(
                logger,
                (
                    f"{instrument_code}: signal_calc\n"
                    f"  time: latest_job_row={status.last_bar_time_ct} CT\n"
                    f"  modes: signal_window={settings.signal_window_mode.value}, "
                    f"market_regime_filter={settings.market_regime_filter_mode.value}\n"
                    f"  window: {window_text}\n"
                    f"  candidates: {candidate_search_text}\n"
                    f"  candidate_funnel: {format_candidate_funnel_summary(funnel_rows)}\n"
                    f"  pearson: min={settings.pearson_min:.2f}, "
                    f"best={best_pearson:.4f}, "
                    f"passed={pearson_passed_count}/{len(pattern_matrix_result.valid_candidates)}\n"
                    f"  regression: threshold={regression_flat_delta_threshold_bps:.2f} bps; "
                    f"{price_regression_text}; {sma_600_regression_text}\n"
                    f"  relation: {price_sma_600_relation_text}\n"
                    f"  regime_filter: {candidate_regime_filter_text}\n"
                    f"  minmax_filter: {candidate_minmax_filter_text}\n"
                    f"  candidate_score: {candidate_score_text}\n"
                    f"  potential: {candidate_potential_text}"
                ),
                to_telegram=False,
            )

            # Дальше здесь будет полный расчёт сигнала:
            # 1. построение текущего окна от signal_bar_time_ts;
            # 2. поиск исторических кандидатов;
            # 3. расчёт Pearson;
            # 4. фильтры;
            # 5. принятие торгового решения.

        await asyncio.sleep(SIGNAL_LOOP_SLEEP_SECONDS)
