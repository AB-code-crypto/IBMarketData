from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_regression import (
    build_linear_regression,
    classify_regression_direction,
)
from ib_signal.signal_sma_reader import read_current_sma_lines
from ib_signal.signal_window import SignalWindow

PLOT_TOP_CANDIDATES = 10

CURRENT_PATTERN_COLOR = "red"
SMA_LINE_COLORS: dict[int, str] = {
    120: "tab:orange",
    600: "tab:blue",
    1200: "tab:green",
}


def get_signal_png_dir() -> Path:
    """Что делает: возвращает каталог для PNG signal-сервиса и создаёт его при необходимости.
    Зачем нужна: пользователь хочет складывать графики в корневой каталог png, а не отправлять их в Telegram."""
    png_dir = Path(__file__).resolve().parent.parent / "png"
    png_dir.mkdir(parents=True, exist_ok=True)
    return png_dir


def sanitize_filename_part(value: str) -> str:
    """Что делает: очищает часть имени файла от символов, неудобных для Windows/Linux.
    Зачем нужна: signal_bar_time_ct содержит пробелы и двоеточия, которые лучше не оставлять в имени PNG."""
    return (
        value.replace(" ", "_")
        .replace(":", "-")
        .replace("/", "-")
    )


def normalize_series_for_plot(values: np.ndarray) -> np.ndarray:
    """Что делает: нормализует ряд для визуального сравнения, вычитая первую точку.
    Зачем нужна: исторические кандидаты и текущий паттерн могут быть на разных ценовых уровнях,
    а для картинки важна форма движения, а не абсолютный уровень."""
    if values.size == 0:
        return values
    return values - values[0]


def get_top_passed_candidate_indices(
        pearson_scores: np.ndarray,
        pearson_min: float,
        limit: int,
) -> np.ndarray:
    """Что делает: выбирает лучшие candidate-индексы только среди тех, кто прошёл pearson_min.
    Зачем нужна: на картинке нужны только реально допущенные кандидаты, а не просто top-N вообще."""
    if pearson_scores.size == 0 or limit <= 0:
        return np.empty((0,), dtype=int)

    passed_indices = np.flatnonzero(pearson_scores >= pearson_min)

    if passed_indices.size == 0:
        return np.empty((0,), dtype=int)

    passed_scores = pearson_scores[passed_indices]
    passed_order = np.argsort(passed_scores)[::-1]
    top_passed = passed_indices[passed_order[:limit]]

    return top_passed.astype(int)


def build_plot_path(
        instrument_code: str,
        signal_bar_time_ct: str,
) -> Path:
    """Что делает: строит путь к итоговому PNG-файлу.
    Зачем нужна: каждый расчёт должен сохранять отдельную картинку с понятным именем."""
    filename = (
        f"signal_candidates_"
        f"{instrument_code.lower()}_"
        f"{sanitize_filename_part(signal_bar_time_ct)}_CT.png"
    )
    return get_signal_png_dir() / filename


def format_plot_regression_value(value: float) -> str:
    """Что делает: компактно форматирует число regression-диагностики для PNG.
    Зачем нужна: fixed-формат плох для EURUSD, а слишком длинные числа забивают картинку."""
    return f"{value:.6g}"


def read_candidate_full_values(
        *,
        instrument_code: str,
        candidate: CandidateWindow,
        price_source: str,
        expected_points: int,
        bar_size_seconds: int,
) -> np.ndarray | None:
    """Что делает: читает полный historical window кандидата: pattern + дальнейшее движение trade-window.
    Зачем нужна: на картинке нужно показать не только похожий паттерн до входа, но и то,
    как исторический кандидат вёл себя после точки входа."""
    instrument_row = Instrument[instrument_code]
    job_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        rows = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                {quote_identifier(price_source)}
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            WHERE bar_time_ts >= ?
              AND bar_time_ts < ?
            ORDER BY bar_time_ts
            """,
            (candidate.pattern_start_ts, candidate.trade_end_ts),
        ).fetchall()

    finally:
        conn.close()

    if len(rows) != expected_points:
        return None

    values = np.empty((expected_points,), dtype=float)

    for index, row in enumerate(rows):
        bar_time_ts = int(row[0])
        value = row[1]
        expected_ts = candidate.pattern_start_ts + index * bar_size_seconds

        if bar_time_ts != expected_ts or value is None:
            return None

        values[index] = float(value)

    return values


def draw_info_section(
        ax_info,
        *,
        title: str,
        rows: list[tuple[str, str | None]],
        y: float,
        line_height: float,
) -> float:
    """Что делает: рисует одну секцию в правой панели построчно.
    Зачем нужна: разные строки, особенно TOP CANDIDATES, должны иметь собственный цвет."""
    ax_info.text(
        0.01,
        y,
        title,
        transform=ax_info.transAxes,
        fontsize=8.3,
        fontweight="bold",
        verticalalignment="top",
        horizontalalignment="left",
        family="monospace",
        color="black",
    )
    y -= line_height

    ax_info.text(
        0.01,
        y,
        "-" * len(title),
        transform=ax_info.transAxes,
        fontsize=8.0,
        verticalalignment="top",
        horizontalalignment="left",
        family="monospace",
        color="black",
    )
    y -= line_height

    for text, color in rows:
        ax_info.text(
            0.01,
            y,
            text,
            transform=ax_info.transAxes,
            fontsize=8.0,
            verticalalignment="top",
            horizontalalignment="left",
            family="monospace",
            color=(color or "black"),
        )
        y -= line_height

    return y - line_height * 0.22


def save_signal_candidate_plot(
        *,
        instrument_code: str,
        signal_bar_time_ct: str,
        signal_window: SignalWindow,
        current_values: np.ndarray,
        valid_candidates: list[CandidateWindow],
        pearson_scores: np.ndarray,
        price_source: str,
        pearson_min: float,
        regression_flat_delta_threshold: float,
) -> Path | None:
    """Что делает: сохраняет PNG с текущим паттерном и лучшими историческими кандидатами.
    Зачем нужна: удобно визуально проверить, что signal-сервис нашёл похожие участки,
    и сразу увидеть дальнейшее историческое движение после точки входа."""
    if current_values.size == 0:
        return None

    top_indices = get_top_passed_candidate_indices(
        pearson_scores=pearson_scores,
        pearson_min=pearson_min,
        limit=min(PLOT_TOP_CANDIDATES, len(valid_candidates)),
    )

    if top_indices.size == 0:
        return None

    instrument_row = Instrument[instrument_code]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])

    current_x_minutes = (
        np.arange(current_values.size, dtype=float) * bar_size_seconds / 60.0
        - signal_window.pattern_seconds / 60.0
    )

    current_sma_lines = read_current_sma_lines(
        instrument_code=instrument_code,
        signal_window=signal_window,
    )
    current_line = normalize_series_for_plot(np.asarray(current_values, dtype=float))

    current_regression = build_linear_regression(current_values)
    current_regression_direction = classify_regression_direction(
        current_regression,
        flat_delta_threshold=regression_flat_delta_threshold,
    )
    current_regression_line = current_regression.line_values - current_values[0]

    sma_600_values = current_sma_lines.get(600)
    sma_600_regression = (
        build_linear_regression(sma_600_values)
        if sma_600_values is not None
        else None
    )
    sma_600_regression_direction = (
        classify_regression_direction(
            sma_600_regression,
            flat_delta_threshold=regression_flat_delta_threshold,
        )
        if sma_600_regression is not None
        else None
    )
    sma_600_regression_line = (
        sma_600_regression.line_values - current_values[0]
        if sma_600_regression is not None
        else None
    )

    trade_points = signal_window.trade_seconds // bar_size_seconds
    full_points = current_values.size + trade_points

    fig = plt.figure(figsize=(18, 9))
    grid = fig.add_gridspec(
        nrows=1,
        ncols=2,
        width_ratios=[4, 1],
        wspace=0.04,
    )
    ax = fig.add_subplot(grid[0, 0])
    ax_info = fig.add_subplot(grid[0, 1])
    ax_info.axis("off")

    shown_candidates: list[tuple[int, CandidateWindow, float, str]] = []

    for sma_period_bars, sma_values in current_sma_lines.items():
        ax.plot(
            current_x_minutes,
            sma_values - current_values[0],
            linestyle="--",
            linewidth=2.2,
            alpha=0.95,
            zorder=4,
            color=SMA_LINE_COLORS.get(sma_period_bars),
        )

    if top_indices.size > 0:
        for rank, candidate_index in enumerate(top_indices, start=1):
            candidate = valid_candidates[int(candidate_index)]
            pearson_value = float(pearson_scores[int(candidate_index)])

            candidate_full_values = read_candidate_full_values(
                instrument_code=instrument_code,
                candidate=candidate,
                price_source=price_source,
                expected_points=full_points,
                bar_size_seconds=bar_size_seconds,
            )

            if candidate_full_values is None:
                continue

            candidate_line = normalize_series_for_plot(candidate_full_values)
            candidate_x_minutes = (
                np.arange(candidate_line.size, dtype=float) * bar_size_seconds / 60.0
                - signal_window.pattern_seconds / 60.0
            )

            line = ax.plot(
                candidate_x_minutes,
                candidate_line,
                linewidth=1.1,
                alpha=0.4,
            )[0]

            shown_candidates.append((rank, candidate, pearson_value, line.get_color()))

    ax.plot(
        current_x_minutes,
        current_line,
        linewidth=2.0,
        alpha=1.0,
        zorder=7,
        color=CURRENT_PATTERN_COLOR,
    )

    ax.plot(
        current_x_minutes,
        current_regression_line,
        linewidth=2.0,
        alpha=0.95,
        zorder=8,
        color=CURRENT_PATTERN_COLOR,
    )

    if sma_600_regression_line is not None:
        ax.plot(
            current_x_minutes,
            sma_600_regression_line,
            linewidth=2.0,
            alpha=0.95,
            zorder=6,
            color=SMA_LINE_COLORS[600],
        )

    ax.axvline(0.0, linestyle="--", linewidth=1.2)

    ax.set_title(
        f"{instrument_code} | signal_bar={signal_bar_time_ct} CT | "
        f"price_source={price_source}\n"
        f"pattern_minutes={signal_window.pattern_seconds / 60:g} | "
        f"trade_minutes={signal_window.trade_seconds / 60:g} | "
        f"valid_candidates={len(valid_candidates)} | "
        f"passed_threshold={(pearson_scores >= pearson_min).sum()} | "
        f"shown_top={len(shown_candidates)}"
    )
    ax.grid(True)

    regression_rows: list[tuple[str, str | None]] = [
        (
            f"flat_threshold : {format_plot_regression_value(regression_flat_delta_threshold)}",
            None,
        ),
        (
            f"price  slope   : {format_plot_regression_value(current_regression.slope)}",
            None,
        ),
        (
            f"price  delta   : {format_plot_regression_value(current_regression.fitted_delta)}",
            None,
        ),
        (f"price  dir     : {current_regression_direction}", None),
    ]

    if sma_600_regression is not None:
        regression_rows.extend([
            (
                f"sma600 slope   : {format_plot_regression_value(sma_600_regression.slope)}",
                None,
            ),
            (
                f"sma600 delta   : {format_plot_regression_value(sma_600_regression.fitted_delta)}",
                None,
            ),
            (f"sma600 dir     : {sma_600_regression_direction}", None),
        ])
    else:
        regression_rows.append(("sma600         : regression=None", None))

    lines_rows: list[tuple[str, str | None]] = [
        (f"pearson_min    : {pearson_min:.4f}", None),
        (
            "sma_present    : "
            + (", ".join(str(period) for period in sorted(current_sma_lines)) if current_sma_lines else "none"),
            None,
        ),
        ("current        : red", CURRENT_PATTERN_COLOR),
        ("sma 120         : orange", SMA_LINE_COLORS[120]),
        ("sma 600         : blue", SMA_LINE_COLORS[600]),
        ("sma 1200        : green", SMA_LINE_COLORS[1200]),
    ]

    candidate_rows: list[tuple[str, str | None]] = []
    if shown_candidates:
        for rank, candidate, pearson_value, candidate_color in shown_candidates:
            candidate_rows.append(
                (
                    f"{rank:02d} | r={pearson_value:.4f} | {candidate.signal_bar_time_ct}",
                    candidate_color,
                )
            )
    else:
        candidate_rows.append(("No shown candidates", None))

    y = 0.99
    line_height = 0.024
    y = draw_info_section(
        ax_info,
        title="REGRESSION",
        rows=regression_rows,
        y=y,
        line_height=line_height,
    )
    y = draw_info_section(
        ax_info,
        title="LINES",
        rows=lines_rows,
        y=y,
        line_height=line_height,
    )
    draw_info_section(
        ax_info,
        title="TOP CANDIDATES",
        rows=candidate_rows,
        y=y,
        line_height=line_height,
    )

    fig.subplots_adjust(
        left=0.05,
        right=0.985,
        top=0.90,
        bottom=0.06,
        wspace=0.035,
    )

    plot_path = build_plot_path(
        instrument_code=instrument_code,
        signal_bar_time_ct=signal_bar_time_ct,
    )
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)

    return plot_path
