from __future__ import annotations

import re
import subprocess
from pathlib import Path

EXPECTED_HEAD = "2d109321aabf4190f91a4ae1303d4c8e5459abaa"


NEW_SIGNAL_PLOT = """from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np

from core.time_utils import CT_TIMEZONE, SQLITE_DATETIME_FORMAT


_FIXED_CANDIDATE_COLORS = [
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
]


def _build_plot_path(output_dir: Path, instrument_code: str, signal_bar_time_ct: str) -> Path:
    instrument_dir = output_dir / instrument_code.lower()
    instrument_dir.mkdir(parents=True, exist_ok=True)
    safe_time = signal_bar_time_ct.replace(":", "-").replace(" ", "_")
    return instrument_dir / f"signal_candidates_{instrument_code.lower()}_{safe_time}_CT.png"


def _normalize_to_zero(values: np.ndarray) -> np.ndarray:
    series = np.asarray(values, dtype=float)
    if series.size == 0:
        return series
    return series - float(series[0])


def _extract_future_values(candidate: object) -> np.ndarray | None:
    candidate_dict = candidate.__dict__ if hasattr(candidate, "__dict__") else {}
    names = [
        "future_values",
        "continuation_values",
        "trade_values",
        "future_price_values",
        "future_close_values",
    ]

    for name in names:
        if hasattr(candidate, name):
            data = getattr(candidate, name)
            if data is not None:
                arr = np.asarray(data, dtype=float)
                if arr.size > 0:
                    return arr

        if name in candidate_dict and candidate_dict[name] is not None:
            arr = np.asarray(candidate_dict[name], dtype=float)
            if arr.size > 0:
                return arr

    return None


def _build_candidate_label(index: int, pearson: float, score: float) -> str:
    return f"C{index + 1} ρ={pearson:.2f} s={score:.2f}"


def _candidate_color(index: int) -> str:
    if index < len(_FIXED_CANDIDATE_COLORS):
        return _FIXED_CANDIDATE_COLORS[index]

    rgb = np.random.default_rng().random(3)
    return "#{:02x}{:02x}{:02x}".format(*(np.clip(rgb * 255, 0, 255).astype(int)))


def _build_stats_text(total_candidates_count: int | None, pearson_passed_count: int | None, minmax_passed_count: int | None) -> str:
    total_text = str(total_candidates_count) if total_candidates_count is not None else "n/a"
    pearson_text = str(pearson_passed_count) if pearson_passed_count is not None else "n/a"
    minmax_text = str(minmax_passed_count) if minmax_passed_count is not None else "n/a"
    return (
        "Статистика\\n"
        f"Всего кандидатов: {total_text}\\n"
        f"Прошли Pearson: {pearson_text}\\n"
        f"Прошли min/max: {minmax_text}"
    )


def _build_title(instrument_code: str, signal_bar_time_ct: str) -> str:
    dt_ct = datetime.strptime(signal_bar_time_ct, SQLITE_DATETIME_FORMAT).replace(tzinfo=CT_TIMEZONE)
    dt_msk = dt_ct.astimezone(ZoneInfo("Europe/Moscow"))
    return f"{instrument_code} • сигнал {dt_msk.strftime('%Y-%m-%d %H:%M:%S')} МСК"


def save_signal_candidate_plot(
    *,
    instrument_code: str,
    signal_bar_time_ct: str,
    signal_window,
    current_values: np.ndarray,
    valid_candidates: list,
    candidate_matrix: np.ndarray,
    pearson_scores: np.ndarray,
    candidate_scores: np.ndarray,
    candidate_potential_result,
    output_dir: Path,
    total_candidates_count: int | None = None,
    pearson_passed_count: int | None = None,
    minmax_passed_count: int | None = None,
) -> Path:
    current_series = _normalize_to_zero(np.asarray(current_values, dtype=float))
    hist_len = int(current_series.size)
    hist_x = np.arange(-(hist_len - 1), 1)

    fig, (ax_top, ax_bottom) = plt.subplots(2, 1, figsize=(13, 9), dpi=140)
    fig.subplots_adjust(left=0.08, right=0.78, top=0.92, bottom=0.08, hspace=0.28)
    fig.suptitle(_build_title(instrument_code, signal_bar_time_ct), fontsize=14, fontweight="bold")

    current_handle = ax_top.plot(hist_x, current_series, color="red", linewidth=3.0, label="Текущий паттерн")[0]

    candidate_handles: list = []
    candidate_labels: list[str] = []
    future_delta_series: list[np.ndarray] = []

    pearson_array = np.asarray(pearson_scores, dtype=float)
    score_array = np.asarray(candidate_scores, dtype=float)

    for idx, candidate in enumerate(valid_candidates):
        color = _candidate_color(idx)
        hist_candidate = _normalize_to_zero(np.asarray(candidate_matrix[idx], dtype=float))
        label = _build_candidate_label(idx, float(pearson_array[idx]), float(score_array[idx]))
        handle = ax_top.plot(hist_x, hist_candidate, color=color, linewidth=1.6, alpha=0.95, label=label)[0]
        candidate_handles.append(handle)
        candidate_labels.append(label)

        future_values = _extract_future_values(candidate)
        if future_values is None or future_values.size == 0:
            continue

        anchor = float(np.asarray(candidate_matrix[idx], dtype=float)[-1])
        future_deltas = np.asarray(future_values, dtype=float) - anchor
        future_delta_series.append(future_deltas)

    ax_top.set_ylabel("Δ points")
    ax_top.set_xlabel("Минуты до сигнала")
    ax_top.grid(True, alpha=0.30)
    ax_top.axvline(0, color="black", linewidth=1.0, alpha=0.50)
    ax_top.margins(x=0.0)
    ax_top.set_xlim(hist_x[0], 0)

    weighted_handle = None

    if future_delta_series:
        min_future_len = min(len(series) for series in future_delta_series)
        if min_future_len > 0:
            future_x = np.arange(0, min_future_len + 1)
            aligned = []

            for idx, deltas in enumerate(future_delta_series):
                trimmed = np.asarray(deltas[:min_future_len], dtype=float)
                path = np.concatenate(([0.0], trimmed))
                aligned.append(trimmed)
                ax_bottom.plot(future_x, path, color=_candidate_color(idx), linewidth=1.6, alpha=0.95)

            aligned_matrix = np.vstack(aligned)
            if score_array.size >= aligned_matrix.shape[0]:
                raw_weights = np.asarray(score_array[:aligned_matrix.shape[0]], dtype=float)
                min_weight = float(raw_weights.min())
                if min_weight <= 0:
                    raw_weights = raw_weights - min_weight + 1e-6
                weight_sum = float(raw_weights.sum())
                weights = raw_weights / weight_sum if weight_sum > 0 else np.full(raw_weights.shape, 1.0 / raw_weights.size)
            else:
                weights = np.full((aligned_matrix.shape[0],), 1.0 / aligned_matrix.shape[0])

            weighted_future = np.average(aligned_matrix, axis=0, weights=weights)
            weighted_path = np.concatenate(([0.0], weighted_future))
            weighted_handle = ax_bottom.plot(future_x, weighted_path, color="red", linewidth=3.0, label="Прогноз текущего паттерна")[0]

    ax_bottom.set_ylabel("Δ points")
    ax_bottom.set_xlabel("Минуты после сигнала")
    ax_bottom.grid(True, alpha=0.30)
    ax_bottom.axvline(0, color="black", linewidth=1.0, alpha=0.50)
    ax_bottom.margins(x=0.0)

    stats_text = _build_stats_text(total_candidates_count, pearson_passed_count, minmax_passed_count)
    fig.text(
        0.805,
        0.92,
        stats_text,
        ha="left",
        va="top",
        fontsize=10,
        bbox=dict(boxstyle="round", facecolor="white", edgecolor="lightgray", alpha=0.95),
    )

    legend_handles = [current_handle]
    legend_labels = ["Текущий паттерн"]

    if weighted_handle is not None:
        legend_handles.append(weighted_handle)
        legend_labels.append("Прогноз текущего паттерна")

    legend_handles.extend(candidate_handles)
    legend_labels.extend(candidate_labels)

    fig.legend(
        legend_handles,
        legend_labels,
        loc="center left",
        bbox_to_anchor=(0.805, 0.45),
        frameon=True,
        fontsize=9,
        borderaxespad=0.0,
    )

    plot_path = _build_plot_path(output_dir, instrument_code, signal_bar_time_ct)
    fig.savefig(plot_path, bbox_inches="tight")
    plt.close(fig)
    return plot_path
"""


def find_project_root() -> Path:
    for candidate in [Path.cwd().resolve(), Path(__file__).resolve().parent]:
        if (candidate / ".git").is_dir() and (candidate / "config.py").is_file() and (candidate / "ib_signal").is_dir():
            return candidate
    raise RuntimeError("Запусти apply-скрипт из корня проекта IBMarketData")


def run_git(root: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def patch_render_preview(text: str) -> str:
    if "total_candidates_count=" in text:
        return text

    target = "        candidate_potential_result=potential,\n        output_dir=OUTPUT_DIR,\n"
    replacement = "        candidate_potential_result=potential,\n        total_candidates_count=candidate_search.raw_candidate_rows_count,\n        pearson_passed_count=len(passed_candidates),\n        minmax_passed_count=len(score_result.valid_candidates),\n        output_dir=OUTPUT_DIR,\n"
    if target not in text:
        raise RuntimeError("Не найден блок save_signal_candidate_plot() в scripts/render_signal_plot_preview.py")
    return text.replace(target, replacement, 1)


def patch_production_caller(root: Path) -> list[Path]:
    updated = []
    pattern = re.compile(r"(candidate_potential_result\s*=\s*[^,\n]+,\n)(\s*)(output_dir\s*=)", flags=re.MULTILINE)

    for path in sorted((root / "ib_signal").rglob("*.py")):
        if path.name == "signal_plot.py":
            continue

        text = path.read_text(encoding="utf-8")
        if "save_signal_candidate_plot(" not in text:
            continue
        if "total_candidates_count=" in text:
            continue
        if "raw_candidate_rows_count" not in text:
            continue
        if "passed_candidates" not in text or "score_result.valid_candidates" not in text:
            continue

        new_text, count = pattern.subn(
            r"\1\2total_candidates_count=candidate_search.raw_candidate_rows_count,\n\2pearson_passed_count=len(passed_candidates),\n\2minmax_passed_count=len(score_result.valid_candidates),\n\2\3",
            text,
            count=1,
        )
        if count != 1:
            raise RuntimeError(f"Не удалось пропатчить вызов save_signal_candidate_plot() в {path}")

        path.write_text(new_text, encoding="utf-8", newline="\n")
        updated.append(path)

    if not updated:
        raise RuntimeError("Не найден production-вызов save_signal_candidate_plot() для добавления статистики")
    return updated


def validate_python_text(source: str, name: str) -> None:
    compile(source, name, "exec")


def main() -> None:
    root = find_project_root()
    actual_head = run_git(root, "rev-parse", "HEAD")
    if actual_head != EXPECTED_HEAD:
        raise RuntimeError(f"Неожиданный HEAD: expected={EXPECTED_HEAD}, actual={actual_head}")

    target_files = [
        root / "ib_signal" / "signal_plot.py",
        root / "scripts" / "render_signal_plot_preview.py",
    ]

    backups: dict[Path, str] = {}
    for path in target_files:
        if path.exists():
            backups[path] = path.read_text(encoding="utf-8")

    for path in (root / "ib_signal").rglob("*.py"):
        if path.name != "signal_plot.py":
            backups.setdefault(path, path.read_text(encoding="utf-8"))

    try:
        signal_plot_path = root / "ib_signal" / "signal_plot.py"
        validate_python_text(NEW_SIGNAL_PLOT, "ib_signal/signal_plot.py")
        signal_plot_path.write_text(NEW_SIGNAL_PLOT, encoding="utf-8", newline="\n")

        preview_path = root / "scripts" / "render_signal_plot_preview.py"
        if preview_path.exists():
            preview_text = preview_path.read_text(encoding="utf-8")
            preview_text = patch_render_preview(preview_text)
            validate_python_text(preview_text, "scripts/render_signal_plot_preview.py")
            preview_path.write_text(preview_text, encoding="utf-8", newline="\n")

        production_updated = patch_production_caller(root)

        for path in production_updated:
            validate_python_text(path.read_text(encoding="utf-8"), str(path))

    except Exception:
        for path, text in backups.items():
            path.write_text(text, encoding="utf-8", newline="\n")
        raise

    print("APPLY_SIGNAL_PLOT_LAYOUT_V2_OK")
    print("updated files:")
    print("  ib_signal/signal_plot.py")
    if (root / "scripts" / "render_signal_plot_preview.py").exists():
        print("  scripts/render_signal_plot_preview.py")
    for path in production_updated:
        print(f"  {path.relative_to(root).as_posix()}")
    print("changes:")
    print("  - current pattern is fixed bold red")
    print("  - first 10 candidate colors are fixed")
    print("  - candidate continuations are shown on lower chart")
    print("  - top chart right gap after zero is removed")
    print("  - legends moved to right field")
    print("  - title uses Moscow time")
    print("  - stats box added on the right")


if __name__ == "__main__":
    main()
