"""
Разовый тестер signal-пайплайна на исторической точке времени.

Назначение
----------
Скрипт эмулирует один проход signal_runner для заданного времени:

1. Берёт TEST_CURRENT_BAR_UTC как "текущую закрытую точку" current_bar_ts.
2. Через get_due_signal_bar_ts() проверяет, надо ли считать сигнал в текущем SignalWindowMode.
3. Строит SignalWindow.
4. Запускает тот же pipeline:
   - поиск кандидатов;
   - pattern matrix;
   - Pearson;
   - regression / relation;
   - market-regime filter OFF/SOFT/HARD;
   - minmax hard filter;
   - candidate score;
   - PNG.

Важно
-----
Это не live-цикл и не подключение к IB. Скрипт только читает локальные БД и строит PNG.
Если в GRID-режиме TEST_CURRENT_BAR_UTC не попадает в разрешённую точку расчёта,
скрипт честно пропускает расчёт.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

# Чтобы скрипт можно было запускать как:
# python tester/signal_pipeline_tester.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, MarketRegimeFilterMode, SignalConfig
from ib_signal.signal_schedule import get_due_signal_bar_ts
from ib_signal.signal_window import build_current_signal_window, format_signal_window_for_log
from ib_signal.job_reader import read_job_bar_time_ct
from ib_signal.signal_candidates import find_candidate_windows
from ib_signal.signal_pattern_matrix import build_pattern_matrix
from ib_signal.pearson import calculate_centered_pearson_batch
from ib_signal.signal_regression import (
    build_linear_regression,
    calculate_regression_delta_bps,
    classify_regression_direction,
)
from ib_signal.signal_regression_threshold import get_regression_flat_delta_threshold_bps
from ib_signal.signal_regression_relation import build_regression_relation
from ib_signal.signal_sma_reader import read_current_sma_values
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
from ib_signal.signal_errors import SignalDataNotReadyError
from ib_signal.signal_plot import save_signal_candidate_plot
from core.logger import get_logger, log_info, setup_logging

# ============================================================
# НАСТРОЙКИ ОДНОРАЗОВОГО ТЕСТА
# ============================================================

# Логический инструмент из contracts.py.
TEST_INSTRUMENT_CODE = "MNQ"

# Время в UTC.
# Это именно current_bar_ts: точка, как будто очередной бар уже закрыт,
# и signal_runner передал это время в get_due_signal_bar_ts().
#
# Пример:
# latest_job_bar в live = 2026-05-15 09:22:55 CT при 5-секундном баре
# current_bar_ts / closed_bar_ts = 2026-05-15 09:23:00 CT
#
# Здесь указываем UTC-время, не CT.
TEST_CURRENT_BAR_UTC = "2026-05-22 06:10:00"

# Текущие настройки signal-сервиса.
# Скрипт намеренно берёт DEFAULT_SIGNAL_CONFIG, чтобы тестировать тот же режим,
# который сейчас задан в проекте.
SETTINGS: SignalConfig = DEFAULT_SIGNAL_CONFIG

# Сколько top-кандидатов выводить в длинной строке candidate_score в логе.
LOG_TOP_CANDIDATES = 3

setup_logging()
logger = get_logger(__name__)


def parse_utc_text(value: str) -> int:
    """Что делает: парсит UTC-время из строки.
    Зачем нужна: tester должен запускаться одной переменной вверху файла."""
    text = str(value).strip()

    if not text:
        raise ValueError("TEST_CURRENT_BAR_UTC должен быть задан")

    if text.endswith("Z"):
        text = text[:-1]

    if len(text) == 10:
        dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            pass

    raise ValueError(
        "Неподдерживаемый формат TEST_CURRENT_BAR_UTC. "
        "Используй YYYY-MM-DD HH:MM:SS или YYYY-MM-DDTHH:MM:SSZ"
    )


def format_candidate_search_summary(result) -> str:
    """Что делает: компактно форматирует первичный поиск кандидатов.
    Зачем нужна: tester должен давать такой же человекочитаемый лог, как signal_runner."""
    if not result.candidates:
        return (
            f"hour={result.current_hour_slot_ct}, "
            f"allowed={result.allowed_hour_slots_ct}, "
            f"found=0"
        )

    first_candidate = result.candidates[0]
    last_candidate = result.candidates[-1]

    return (
        f"hour={result.current_hour_slot_ct}, "
        f"allowed={result.allowed_hour_slots_ct}, "
        f"found={len(result.candidates)}, "
        f"first={first_candidate.signal_bar_time_ct} CT, "
        f"last={last_candidate.signal_bar_time_ct} CT"
    )


def format_regression_summary(
        label: str,
        regression,
        *,
        flat_delta_threshold_bps: float,
) -> str:
    """Что делает: форматирует regression delta и direction.
    Зачем нужна: лог должен показывать bps/points и итоговое направление."""
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
    """Что делает: форматирует relation price-regression vs SMA-regression.
    Зачем нужна: в tester-логе должно быть видно, какой regime получился."""
    if relation is None:
        return f"{label}=None"

    return (
        f"{label}: {relation.relation}, "
        f"start={relation.diff_start_bps:.2f} bps / {relation.diff_start_points:.2f} pt, "
        f"end={relation.diff_end_bps:.2f} bps / {relation.diff_end_points:.2f} pt"
    )


def get_time_ct_for_log(instrument_code: str, ts: int) -> str | None:
    """Что делает: возвращает CT-время бара из job DB, если оно там есть.
    Зачем нужна: format_signal_window_for_log принимает callback для красивых CT-границ."""
    return read_job_bar_time_ct(instrument_code, ts)


def apply_pearson_only_selection(
        *,
        candidates: list,
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
        pearson_min: float,
) -> tuple[list, np.ndarray, np.ndarray]:
    """Что делает: оставляет только Pearson-прошедших кандидатов.
    Зачем нужна: при OFF или невозможности regime-диагностики всё равно надо передавать дальше только passed."""
    passed_indices = np.flatnonzero(pearson_scores >= pearson_min)

    selected_candidates = [
        candidates[int(index)]
        for index in passed_indices
    ]
    selected_matrix = candidate_matrix[passed_indices, :]
    selected_scores = pearson_scores[passed_indices]

    return selected_candidates, selected_matrix, selected_scores


def run_single_signal_test() -> None:
    """Что делает: запускает один тестовый проход signal-пайплайна.
    Зачем нужна: можно смотреть картинки и score в выходной день без live-рынка."""
    instrument_code = TEST_INSTRUMENT_CODE
    settings = SETTINGS
    current_bar_ts = parse_utc_text(TEST_CURRENT_BAR_UTC)

    due_signal_bar_ts = get_due_signal_bar_ts(
        current_bar_ts=current_bar_ts,
        settings=settings,
        last_calculated_bar_ts=None,
    )

    if due_signal_bar_ts is None:
        log_info(
            logger,
            (
                f"{instrument_code}: tester_skip\n"
                f"  reason=no_due_signal_bar\n"
                f"  input_current_bar_utc={TEST_CURRENT_BAR_UTC}\n"
                f"  modes: signal_window={settings.signal_window_mode.value}, "
                f"market_regime_filter={settings.market_regime_filter_mode.value}"
            ),
            to_telegram=False,
        )
        return

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
            skip_reason = (
                "current_relation=mixed_sma"
                if price_sma_600_relation is not None
                and price_sma_600_relation.relation == "mixed_sma"
                else "SMA 600 relation не рассчитан"
            )
            log_info(
                logger,
                (
                    f"{instrument_code}: tester_skip\n"
                    f"  reason={skip_reason}\n"
                    f"  input_current_bar_utc={TEST_CURRENT_BAR_UTC}\n"
                    f"  pearson_passed={pearson_passed_count}\n"
                    f"  modes: signal_window={settings.signal_window_mode.value}, "
                    f"market_regime_filter={settings.market_regime_filter_mode.value}"
                ),
                to_telegram=False,
            )
            return

        if candidate_regime_filter_result is None:
            (
                plot_valid_candidates,
                plot_candidate_matrix,
                plot_pearson_scores,
            ) = apply_pearson_only_selection(
                candidates=plot_valid_candidates,
                candidate_matrix=plot_candidate_matrix,
                pearson_scores=plot_pearson_scores,
                pearson_min=settings.pearson_min,
            )

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
            output_dir=PROJECT_ROOT / "png" / "test",
        )

    except SignalDataNotReadyError as exc:
        window_text = format_signal_window_for_log(
            signal_window,
            lambda ts: get_time_ct_for_log(instrument_code, ts),
        )

        log_info(
            logger,
            (
                f"{instrument_code}: tester_skip\n"
                f"  reason=data_not_ready\n"
                f"  error={exc}\n"
                f"  input_current_bar_utc={TEST_CURRENT_BAR_UTC}\n"
                f"  window={window_text}"
            ),
            to_telegram=False,
        )
        return

    window_text = format_signal_window_for_log(
        signal_window,
        lambda ts: get_time_ct_for_log(instrument_code, ts),
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
        top_limit=LOG_TOP_CANDIDATES,
    )
    candidate_potential_text = format_candidate_potential_result(
        candidate_potential_result,
    )

    plot_text = str(saved_plot_path) if saved_plot_path is not None else "not_saved"

    log_info(
        logger,
        (
            f"{instrument_code}: tester_signal_calc\n"
            f"  input_current_bar_utc={TEST_CURRENT_BAR_UTC}\n"
            f"  png={plot_text}\n"
            f"  time: due_signal_bar_ts={due_signal_bar_ts}\n"
            f"  modes: signal_window={settings.signal_window_mode.value}, "
            f"market_regime_filter={settings.market_regime_filter_mode.value}\n"
            f"  window: {window_text}\n"
            f"  candidates: {candidate_search_text}\n"
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


if __name__ == "__main__":
    run_single_signal_test()
