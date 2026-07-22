from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from ib_signal.signal_candidate_potential import CandidatePotentialResult
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_window import SignalWindow


def get_signal_png_dir() -> Path:
    png_dir = Path(__file__).resolve().parent.parent / "png"
    png_dir.mkdir(parents=True, exist_ok=True)
    return png_dir


def sanitize_filename_part(value: str) -> str:
    return (
        str(value)
        .replace(" ", "_")
        .replace(":", "-")
        .replace("/", "-")
    )


def build_plot_path(
        instrument_code: str,
        signal_bar_time_ct: str,
        output_dir: Path | None = None,
) -> Path:
    filename = (
        f"signal_candidates_{str(instrument_code).lower()}_"
        f"{sanitize_filename_part(signal_bar_time_ct)}_CT.png"
    )
    base_dir = Path(output_dir) if output_dir is not None else get_signal_png_dir()
    target_dir = base_dir / str(instrument_code).lower()
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / filename


def normalize_series_for_plot(values: np.ndarray) -> np.ndarray:
    series = np.asarray(values, dtype=float)
    if series.size == 0:
        return series
    return series - float(series[0])


def _validate_candidate_arrays(
        *,
        candidates: list[CandidateWindow],
        candidate_matrix: np.ndarray,
        pearson_scores: np.ndarray,
        candidate_scores: np.ndarray,
) -> None:
    matrix = np.asarray(candidate_matrix)
    pearson = np.asarray(pearson_scores)
    scores = np.asarray(candidate_scores)
    if matrix.ndim != 2:
        raise ValueError(f"candidate_matrix must be 2D, shape={matrix.shape}")
    expected = len(candidates)
    if matrix.shape[0] != expected:
        raise ValueError(
            "candidate_matrix rows do not match candidates: "
            f"rows={matrix.shape[0]}, candidates={expected}"
        )
    if pearson.shape != (expected,):
        raise ValueError(
            "pearson_scores do not match candidates: "
            f"shape={pearson.shape}, candidates={expected}"
        )
    if scores.shape != (expected,):
        raise ValueError(
            "candidate_scores do not match candidates: "
            f"shape={scores.shape}, candidates={expected}"
        )


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
        output_dir: Path | None = None,
) -> Path:
    """Save a compact rolling-pattern diagnostic chart.

    The old indicator panels were deliberately removed.  The image now shows
    only the current midpoint path, the best historical patterns and the
    weighted historical future used to make the signal decision.
    """
    current = np.asarray(current_values, dtype=float)
    matrix = np.asarray(candidate_matrix, dtype=float)
    pearson = np.asarray(pearson_scores, dtype=float)
    scores = np.asarray(candidate_scores, dtype=float)
    _validate_candidate_arrays(
        candidates=valid_candidates,
        candidate_matrix=matrix,
        pearson_scores=pearson,
        candidate_scores=scores,
    )

    instrument_row = Instrument[str(instrument_code)]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])
    pattern_x_minutes = (
        np.arange(current.size, dtype=float) * bar_size_seconds / 60.0
        - float(signal_window.pattern_seconds) / 60.0
    )

    fig, axes = plt.subplots(2, 1, figsize=(13, 9))
    pattern_ax, potential_ax = axes

    top_count = min(
        int(candidate_potential_result.max_count),
        len(valid_candidates),
    )
    for index in range(top_count - 1, -1, -1):
        candidate = valid_candidates[index]
        label = (
            f"#{index + 1} {candidate.signal_bar_time_ct[0:16]} CT "
            f"score={scores[index]:.3f}, r={pearson[index]:.3f}"
        )
        pattern_ax.plot(
            pattern_x_minutes,
            normalize_series_for_plot(matrix[index]),
            linewidth=1.0,
            alpha=0.55,
            label=label,
        )

    pattern_ax.plot(
        pattern_x_minutes,
        normalize_series_for_plot(current),
        linewidth=2.2,
        label="current rolling pattern",
    )
    pattern_ax.axvline(0.0, linewidth=1.0, linestyle="--")
    pattern_ax.set_title(
        f"{instrument_code} rolling midpoint pattern — {signal_bar_time_ct} CT"
    )
    pattern_ax.set_xlabel("minutes from signal bar")
    pattern_ax.set_ylabel("midpoint change, points")
    pattern_ax.grid(True, alpha=0.25)
    pattern_ax.legend(loc="best", fontsize=8)

    potential = candidate_potential_result
    if potential.is_available:
        potential_ax.plot(
            potential.x_minutes,
            potential.weighted_future_delta_points,
            linewidth=2.2,
            label=(
                f"weighted potential: {potential.direction}, "
                f"end={potential.end_delta_points:+.2f} pt"
            ),
        )
        potential_ax.axhline(0.0, linewidth=1.0, linestyle="--")
        potential_ax.set_title(
            "Historical weighted future "
            f"({potential.used_candidates_count}/{potential.source_candidates_count} candidates)"
        )
        potential_ax.set_xlabel("minutes after signal bar")
        potential_ax.set_ylabel("expected change, points")
        potential_ax.grid(True, alpha=0.25)
        potential_ax.legend(loc="best")
    else:
        potential_ax.axis("off")
        potential_ax.text(
            0.5,
            0.5,
            "Potential unavailable\n"
            f"{potential.unavailable_reason}\n"
            f"valid={potential.used_candidates_count}, required={potential.min_count}",
            ha="center",
            va="center",
            transform=potential_ax.transAxes,
            fontsize=13,
        )

    target = build_plot_path(
        instrument_code=str(instrument_code),
        signal_bar_time_ct=str(signal_bar_time_ct),
        output_dir=output_dir,
    )
    fig.tight_layout()
    fig.savefig(target, dpi=140)
    plt.close(fig)
    return target


__all__ = [
    "get_signal_png_dir",
    "sanitize_filename_part",
    "build_plot_path",
    "normalize_series_for_plot",
    "save_signal_candidate_plot",
]
