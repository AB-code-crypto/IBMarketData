import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np


def find_project_root() -> Path:
    candidates = [Path.cwd().resolve(), Path(__file__).resolve().parent, *Path(__file__).resolve().parents]
    for candidate in candidates:
        if (candidate / "config.py").is_file() and (candidate / "ib_signal").is_dir():
            return candidate
    raise RuntimeError("Не найден корень проекта IBMarketData")


PROJECT_ROOT = find_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings_live as app_settings
from core.telegram_sender import TelegramSender
from core.time_utils import CT_TIMEZONE, SQLITE_DATETIME_FORMAT
from ib_signal.pearson import calculate_centered_pearson_batch
from ib_signal.signal_candidate_potential import build_candidate_potential_result
from ib_signal.signal_candidate_rank_features import filter_candidates_by_minmax_ratio, rank_candidates_by_score
from ib_signal.signal_candidates import find_candidate_windows
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
from ib_signal.signal_pattern_matrix import build_pattern_matrix
from ib_signal.signal_plot import save_signal_candidate_plot
from ib_signal.signal_window import build_current_signal_window

INSTRUMENT_CODE = "MNQ"
SIGNAL_TIME_CT = "2026-07-29 21:47:00"
SEND_TO_TELEGRAM = False
OUTPUT_DIR = PROJECT_ROOT / "png" / "preview"


def parse_signal_time_ct(value: str) -> tuple[int, datetime]:
    dt_ct = datetime.strptime(value, SQLITE_DATETIME_FORMAT).replace(tzinfo=CT_TIMEZONE)
    return int(dt_ct.astimezone(timezone.utc).timestamp()), dt_ct


def render_preview() -> tuple[Path, str]:
    settings = DEFAULT_SIGNAL_CONFIG
    instrument_code = INSTRUMENT_CODE.strip().upper()
    signal_ts, requested_dt_ct = parse_signal_time_ct(SIGNAL_TIME_CT.strip())

    signal_window = build_current_signal_window(signal_bar_ts=signal_ts, settings=settings)
    candidate_search = find_candidate_windows(instrument_code=instrument_code, current_window=signal_window, settings=settings)
    pattern_matrix = build_pattern_matrix(instrument_code=instrument_code, window=signal_window, candidates=candidate_search.candidates)

    all_pearson_scores = calculate_centered_pearson_batch(pattern_matrix.current_values, pattern_matrix.candidate_matrix)
    passed_indices = np.flatnonzero(all_pearson_scores >= float(settings.pearson_min))
    passed_candidates = [pattern_matrix.valid_candidates[int(index)] for index in passed_indices]
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

    plot_path = save_signal_candidate_plot(
        instrument_code=instrument_code,
        signal_bar_time_ct=candidate_search.current_signal_bar_time_ct,
        signal_window=signal_window,
        current_values=pattern_matrix.current_values,
        valid_candidates=score_result.valid_candidates,
        candidate_matrix=score_result.candidate_matrix,
        pearson_scores=score_result.pearson_scores,
        candidate_scores=score_result.candidate_scores,
        candidate_potential_result=potential,
        output_dir=OUTPUT_DIR,
    )

    threshold = abs(float(settings.candidate_potential_min_abs_end_delta_points))
    would_create_signal = potential.is_available and potential.direction in {"LONG", "SHORT"} and abs(float(potential.end_delta_points)) > threshold
    potential_end_text = f"{potential.end_delta_points:+.2f} pt" if potential.is_available else "unavailable"
    caption = (
        "🧪 Тестовый replay графика сигнала\n"
        f"instrument: {instrument_code}\n"
        f"signal_time_ct: {candidate_search.current_signal_bar_time_ct}\n"
        f"raw_candidates: {candidate_search.raw_candidate_rows_count}\n"
        f"pearson_passed: {len(passed_candidates)}\n"
        f"ranked_candidates: {len(score_result.valid_candidates)}\n"
        f"potential: {potential.direction or 'n/a'}, {potential_end_text}\n"
        f"would_create_signal: {would_create_signal}"
    )

    print("=" * 80)
    print("SIGNAL PLOT REPLAY")
    print(f"requested_time_ct: {requested_dt_ct.strftime(SQLITE_DATETIME_FORMAT)}")
    print(f"resolved_time_ct: {candidate_search.current_signal_bar_time_ct}")
    print(f"signal_bar_ts: {signal_ts}")
    print(f"pattern_minutes: {settings.rolling_back_minutes}")
    print(f"trade_minutes: {settings.rolling_trade_minutes}")
    print(f"raw_candidates: {candidate_search.raw_candidate_rows_count}")
    print(f"pearson_passed: {len(passed_candidates)}")
    print(f"ranked_candidates: {len(score_result.valid_candidates)}")
    print(f"potential_ready: {potential.is_available}")
    print(f"potential_direction: {potential.direction}")
    print(f"potential_end: {potential_end_text}")
    print(f"would_create_signal: {would_create_signal}")
    print(f"plot_path: {plot_path}")
    print("=" * 80)
    return plot_path, caption


async def send_to_telegram(plot_path: Path, caption: str) -> None:
    sender = TelegramSender(app_settings, robot_name="signal_plot_replay")
    try:
        queued = await sender.send_photo(plot_path, caption=caption, message_thread_id=app_settings.telegram_message_thread_id_deal)
        if not queued:
            raise RuntimeError("Telegram не принял PNG в очередь отправки")
    finally:
        await sender.close()
    print("TELEGRAM_REPLAY_SENT")


def main() -> None:
    plot_path, caption = render_preview()
    if SEND_TO_TELEGRAM:
        asyncio.run(send_to_telegram(plot_path, caption))


if __name__ == "__main__":
    main()
