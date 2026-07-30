from __future__ import annotations

from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.time_utils import CT_TIMEZONE, MSK_TIMEZONE, SQLITE_DATETIME_FORMAT
from ib_signal.signal_candidate_potential import CandidatePotentialResult
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
from ib_signal.signal_window import SignalWindow


CURRENT_PATTERN_COLOR = "#d62728"
FORECAST_COLOR = "#d62728"
FIXED_CANDIDATE_COLORS = (
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
    "#393b79",
)


def get_signal_png_dir() -> Path:
    png_dir = Path(__file__).resolve().parent.parent / "png"
    png_dir.mkdir(parents=True, exist_ok=True)
    return png_dir


def sanitize_filename_part(value: str) -> str:
    return str(value).replace(" ", "_").replace(":", "-").replace("/", "-")


def build_plot_path(instrument_code: str, signal_bar_time_ct: str, output_dir: Path | None = None) -> Path:
    filename = f"signal_candidates_{str(instrument_code).lower()}_{sanitize_filename_part(signal_bar_time_ct)}_CT.png"
    base_dir = Path(output_dir) if output_dir is not None else get_signal_png_dir()
    target_dir = base_dir / str(instrument_code).lower()
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / filename


def normalize_series_for_plot(values: np.ndarray) -> np.ndarray:
    series = np.asarray(values, dtype=float)
    return series if series.size == 0 else series - float(series[0])


def build_candidate_colors(count: int) -> list[str]:
    colors = list(FIXED_CANDIDATE_COLORS[:count])
    rng = np.random.default_rng()

    while len(colors) < count:
        rgb = rng.integers(35, 221, size=3)
        color = f"#{int(rgb[0]):02x}{int(rgb[1]):02x}{int(rgb[2]):02x}"
        if color.lower() != CURRENT_PATTERN_COLOR and color not in colors:
            colors.append(color)

    return colors


def format_signal_time_msk(signal_bar_time_ct: str) -> str:
    dt_ct = datetime.strptime(str(signal_bar_time_ct), SQLITE_DATETIME_FORMAT).replace(tzinfo=CT_TIMEZONE)
    return dt_ct.astimezone(MSK_TIMEZONE).strftime(SQLITE_DATETIME_FORMAT)


def format_candidate_legend_label(index: int, candidate: CandidateWindow, pearson: float, score: float) -> str:
    compact_time = candidate.signal_bar_time_ct[5:16]
    return f"#{index + 1} {compact_time} CT  r={pearson:.3f}  score={score:.3f}"


def validate_candidate_arrays(candidates: list[CandidateWindow], candidate_matrix: np.ndarray, pearson_scores: np.ndarray, candidate_scores: np.ndarray) -> None:
    matrix = np.asarray(candidate_matrix)
    pearson = np.asarray(pearson_scores)
    scores = np.asarray(candidate_scores)
    expected = len(candidates)

    if matrix.ndim != 2:
        raise ValueError(f"candidate_matrix must be 2D, shape={matrix.shape}")
    if matrix.shape[0] != expected:
        raise ValueError(f"candidate_matrix rows do not match candidates: rows={matrix.shape[0]}, candidates={expected}")
    if pearson.shape != (expected,):
        raise ValueError(f"pearson_scores do not match candidates: shape={pearson.shape}, candidates={expected}")
    if scores.shape != (expected,):
        raise ValueError(f"candidate_scores do not match candidates: shape={scores.shape}, candidates={expected}")


def validate_potential_arrays(potential: CandidatePotentialResult) -> None:
    used_ts = np.asarray(potential.used_candidate_signal_bar_ts)
    future_matrix = np.asarray(potential.candidate_future_delta_points)
    x_minutes = np.asarray(potential.x_minutes)

    if used_ts.ndim != 1:
        raise ValueError(f"used_candidate_signal_bar_ts must be 1D, shape={used_ts.shape}")
    if future_matrix.ndim != 2:
        raise ValueError(f"candidate_future_delta_points must be 2D, shape={future_matrix.shape}")
    if future_matrix.shape[0] != used_ts.size:
        raise ValueError(f"future rows do not match used candidates: rows={future_matrix.shape[0]}, used={used_ts.size}")
    if future_matrix.shape[1] != x_minutes.size:
        raise ValueError(f"future columns do not match x_minutes: columns={future_matrix.shape[1]}, x={x_minutes.size}")


def build_display_indices(valid_candidates: list[CandidateWindow], potential: CandidatePotentialResult) -> list[int]:
    max_count = min(int(potential.max_count), len(valid_candidates))
    used_ts = [int(value) for value in np.asarray(potential.used_candidate_signal_bar_ts, dtype=np.int64)]

    if not used_ts:
        return list(range(max_count))

    index_by_ts = {int(candidate.signal_bar_ts): index for index, candidate in enumerate(valid_candidates)}
    indices = [index_by_ts[signal_ts] for signal_ts in used_ts if signal_ts in index_by_ts]
    return indices if indices else list(range(max_count))


def _format_opt(value: float | None, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def _count_final_outcomes(candidate_future_delta_points: np.ndarray) -> tuple[int, int, int]:
    future = np.asarray(candidate_future_delta_points, dtype=float)

    if future.size == 0 or future.shape[0] == 0 or future.shape[1] == 0:
        return 0, 0, 0

    final_values = future[:, -1]
    up = int(np.count_nonzero(final_values > 0))
    down = int(np.count_nonzero(final_values < 0))
    flat = int(final_values.size - up - down)
    return up, down, flat


def _format_title(instrument_code: str, signal_bar_time_ct: str) -> str:
    settings = DEFAULT_SIGNAL_CONFIG
    first_line = f"{instrument_code} — {format_signal_time_msk(signal_bar_time_ct)} МСК"
    second_line = (
        f"back={int(settings.rolling_back_minutes)}m  "
        f"trade={int(settings.rolling_trade_minutes)}m  "
        f"pot_n={int(settings.candidate_potential_min_count)}..{int(settings.candidate_potential_max_count)}  "
        f"abs_end≥{float(settings.candidate_potential_min_abs_end_delta_points):.1f}  "
        f"mm_ratio≤{float(settings.candidate_minmax_hard_filter_max_ratio):.2f}"
    )
    return first_line + "\n" + second_line


def save_signal_candidate_plot(
    *,
    instrument_code: str,
    signal_bar_time_ct: str,
    signal_window: SignalWindow,
    current_values: np.ndarray,
    valid_candidates: list[CandidateWindow],
    candidate_matrix: np.ndarray,
    pearson_scores: np.ndarray,
    candidate_scores: np.ndarray,
    candidate_potential_result: CandidatePotentialResult,
    total_candidates_count: int,
    pearson_passed_count: int,
    minmax_passed_count: int,
    pearson_best_initial: float | None = None,
    pearson_worst_initial: float | None = None,
    pearson_best_after_minmax: float | None = None,
    pearson_worst_after_minmax: float | None = None,
    output_dir: Path | None = None,
) -> Path:
    current = np.asarray(current_values, dtype=float)
    matrix = np.asarray(candidate_matrix, dtype=float)
    pearson = np.asarray(pearson_scores, dtype=float)
    scores = np.asarray(candidate_scores, dtype=float)
    potential = candidate_potential_result

    validate_candidate_arrays(valid_candidates, matrix, pearson, scores)
    validate_potential_arrays(potential)

    if current.size == 0:
        raise ValueError("current_values is empty")

    instrument_row = Instrument[str(instrument_code)]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])
    pattern_x_minutes = (np.arange(current.size, dtype=float) - float(current.size - 1)) * bar_size_seconds / 60.0

    display_indices = build_display_indices(valid_candidates, potential)
    colors = build_candidate_colors(len(display_indices))
    color_by_signal_ts: dict[int, str] = {}

    fig, (pattern_ax, potential_ax) = plt.subplots(2, 1, figsize=(16, 9.5))
    fig.subplots_adjust(left=0.08, right=0.69, top=0.89, bottom=0.08, hspace=0.26)
    fig.suptitle(_format_title(str(instrument_code), str(signal_bar_time_ct)), fontsize=13.5, fontweight="bold")

    candidate_legend_handles: list[Line2D] = []
    candidate_legend_labels: list[str] = []

    for display_position in range(len(display_indices) - 1, -1, -1):
        candidate_index = display_indices[display_position]
        candidate = valid_candidates[candidate_index]
        color = colors[display_position]
        color_by_signal_ts[int(candidate.signal_bar_ts)] = color
        pattern_ax.plot(pattern_x_minutes, normalize_series_for_plot(matrix[candidate_index]), color=color, linewidth=1.5, alpha=0.80, zorder=2)

    for display_position, candidate_index in enumerate(display_indices):
        candidate = valid_candidates[candidate_index]
        color = colors[display_position]
        candidate_legend_handles.append(Line2D([0], [0], color=color, linewidth=2.0))
        candidate_legend_labels.append(format_candidate_legend_label(candidate_index, candidate, float(pearson[candidate_index]), float(scores[candidate_index])))

    pattern_ax.plot(pattern_x_minutes, normalize_series_for_plot(current), color=CURRENT_PATTERN_COLOR, linewidth=3.2, alpha=1.0, zorder=10)
    pattern_ax.axvline(0.0, color="#555555", linewidth=1.0, linestyle="--")
    pattern_ax.set_xlim(float(pattern_x_minutes[0]), 0.0)
    pattern_ax.margins(x=0.0)
    pattern_ax.set_xlabel("Минуты до сигнала")
    pattern_ax.set_ylabel("Изменение цены, пункты")
    pattern_ax.grid(True, alpha=0.25)

    future_matrix = np.asarray(potential.candidate_future_delta_points, dtype=float)
    future_x = np.asarray(potential.x_minutes, dtype=float)
    used_ts = np.asarray(potential.used_candidate_signal_bar_ts, dtype=np.int64)
    extra_colors = build_candidate_colors(len(used_ts))

    for row_index, signal_ts in enumerate(used_ts):
        color = color_by_signal_ts.get(int(signal_ts), extra_colors[row_index])
        potential_ax.plot(future_x, future_matrix[row_index], color=color, linewidth=1.5, alpha=0.80, zorder=2)

    forecast_label = "Прогноз недоступен"

    if potential.is_available:
        potential_ax.plot(future_x, potential.weighted_future_delta_points, color=FORECAST_COLOR, linewidth=3.2, linestyle="--", alpha=1.0, zorder=10)
        potential_ax.set_xlim(float(future_x[0]), float(future_x[-1]))
        forecast_label = f"Взвешенный прогноз: {potential.direction}, end={potential.end_delta_points:+.2f} pt, used={potential.used_candidates_count}"
    else:
        potential_ax.set_xlim(0.0, float(signal_window.trade_seconds) / 60.0)

    potential_ax.axhline(0.0, color="#555555", linewidth=1.0, linestyle="--")
    potential_ax.margins(x=0.0)
    potential_ax.set_xlabel("Минуты после сигнала")
    potential_ax.set_ylabel("Изменение цены, пункты")
    potential_ax.grid(True, alpha=0.25)

    end_up_count, end_down_count, end_flat_count = _count_final_outcomes(potential.candidate_future_delta_points)

    weighted_points = np.asarray(potential.weighted_future_delta_points, dtype=float)
    if weighted_points.size:
        weighted_max = float(np.max(weighted_points))
        weighted_min = float(np.min(weighted_points))
    else:
        weighted_max = 0.0
        weighted_min = 0.0

    stats_lines = [
        "КАНДИДАТЫ",
        f"всего: {int(total_candidates_count)}",
        f"Pearson: {int(pearson_passed_count)}",
        f"min/max: {int(minmax_passed_count)}",
        f"в прогнозе: {int(potential.used_candidates_count)} / {int(potential.max_count)}",
        "",
        "ФИНАЛ КАНДИДАТОВ",
        f"выросло: {end_up_count}",
        f"упало: {end_down_count}",
        f"флэт: {end_flat_count}",
    ]

    if potential.is_available:
        stats_lines.extend([
            "",
            "ПОТЕНЦИАЛ",
            f"dir: {potential.direction}",
            f"end: {float(potential.end_delta_points):+.2f} pt",
            f"max: {weighted_max:+.2f} pt",
            f"min: {weighted_min:+.2f} pt",
            f"best_dir: {float(potential.max_profit_points):+.2f} pt",
            f"worst_dir: {float(potential.max_drawdown_points):+.2f} pt",
            f"same/opposite/flat: {int(potential.same_direction_count)}/{int(potential.opposite_direction_count)}/{int(potential.flat_count)}",
            f"w_same/opposite/flat: {float(potential.same_direction_weight_share):.2f}/{float(potential.opposite_direction_weight_share):.2f}/{float(potential.flat_weight_share):.2f}",
        ])

    fig.text(
        0.72,
        0.90,
        "\n".join(stats_lines),
        ha="left",
        va="top",
        fontsize=10.4,
        linespacing=1.38,
        bbox={"boxstyle": "round,pad=0.6", "facecolor": "white", "edgecolor": "#cccccc", "alpha": 0.95},
    )

    summary_handles = [
        Line2D([0], [0], color=FORECAST_COLOR, linewidth=3.2, linestyle="--"),
        Line2D([], [], color="none"),
        Line2D([], [], color="none"),
    ]
    summary_labels = [
        forecast_label,
        f"Pearson до min/max: best={_format_opt(pearson_best_initial)}  worst={_format_opt(pearson_worst_initial)}",
        f"Pearson после min/max: best={_format_opt(pearson_best_after_minmax)}  worst={_format_opt(pearson_worst_after_minmax)}",
    ]
    legend_handles = [*summary_handles, *candidate_legend_handles]
    legend_labels = [*summary_labels, *candidate_legend_labels]

    fig.legend(
        legend_handles,
        legend_labels,
        loc="upper left",
        bbox_to_anchor=(0.72, 0.62),
        fontsize=8.3,
        frameon=True,
        borderaxespad=0.0,
        handlelength=3.0,
        labelspacing=0.75,
    )

    target = build_plot_path(str(instrument_code), str(signal_bar_time_ct), output_dir)
    fig.savefig(target, dpi=140, facecolor="white")
    plt.close(fig)
    return target


__all__ = [
    "get_signal_png_dir",
    "sanitize_filename_part",
    "build_plot_path",
    "normalize_series_for_plot",
    "build_candidate_colors",
    "format_signal_time_msk",
    "save_signal_candidate_plot",
]
