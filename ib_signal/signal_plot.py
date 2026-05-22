from pathlib import Path

import matplotlib

from ib_signal.signal_config import PLOT_TOP_CANDIDATES

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from ib_job_data.job_features_config import MA_ZONE_LEVEL1_PERCENT, MA_ZONE_LEVEL2_PERCENT
from ib_signal.signal_candidate_potential import CandidatePotentialResult, read_candidate_full_values
from ib_signal.signal_candidate_rank_features import calculate_pattern_path_features
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_regression import (
    build_linear_regression,
    calculate_regression_delta_bps,
    calculate_regression_threshold_points,
    classify_regression_direction,
)
from ib_signal.signal_regression_relation import build_regression_relation
from ib_signal.signal_sma_reader import read_current_sma_lines
from ib_signal.signal_ma_zone_reader import read_current_ma_zone_ranges
from ib_signal.signal_regime_reader import read_signal_regime_values
from ib_signal.signal_window import SignalWindow

CURRENT_PATTERN_COLOR = "red"
SMA_LINE_COLORS: dict[int, str] = {
    120: "tab:orange",
    600: "tab:blue",
    1200: "tab:green",
}
REGIME_COLORS: dict[int | None, str] = {
    -1: "#d62728",
    0: "#6b6b6b",
    1: "#2ca02c",
    None: "#6b6b6b",
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
        candidate_scores: np.ndarray | None = None,
) -> np.ndarray:
    """Что делает: выбирает top candidate-индексы для PNG.
    Зачем нужна: после scoring top должен идти по candidate_score, а не только по Pearson."""
    if pearson_scores.size == 0 or limit <= 0:
        return np.empty((0,), dtype=int)

    passed_indices = np.flatnonzero(pearson_scores >= pearson_min)

    if passed_indices.size == 0:
        return np.empty((0,), dtype=int)

    if candidate_scores is not None:
        if candidate_scores.shape[0] != pearson_scores.shape[0]:
            raise ValueError(
                f"candidate_scores и pearson_scores не совпадают по длине: "
                f"scores={candidate_scores.shape[0]}, pearson={pearson_scores.shape[0]}"
            )
        sort_values = candidate_scores[passed_indices]
    else:
        sort_values = pearson_scores[passed_indices]

    passed_order = np.argsort(sort_values)[::-1]
    top_passed = passed_indices[passed_order[:limit]]

    return top_passed.astype(int)


def build_plot_path(
        instrument_code: str,
        signal_bar_time_ct: str,
        output_dir: Path | None = None,
) -> Path:
    """Что делает: строит путь к итоговому PNG-файлу.
    Зачем нужна: live-картинки сохраняются в png, а тестовые можно сразу писать в отдельный каталог."""
    filename = (
        f"signal_candidates_"
        f"{instrument_code.lower()}_"
        f"{sanitize_filename_part(signal_bar_time_ct)}_CT.png"
    )

    png_dir = output_dir or get_signal_png_dir()
    png_dir.mkdir(parents=True, exist_ok=True)

    return png_dir / filename


def format_plot_regression_value(value: float) -> str:
    """Что делает: форматирует regression-диагностику для PNG с двумя знаками после запятой.
    Зачем нужна: картинка должна быть читаемой, а расчёты остаются без округления."""
    return f"{value:.2f}"

def format_potential_time_for_plot(value: str) -> str:
    """Что делает: сокращает CT timestamp до времени для PNG.
    Зачем нужна: полный timestamp слишком длинный для правой панели."""
    text = str(value)

    if len(text) >= 19:
        return text[11:19]

    return text


def get_regime_color(regime_value: int | None) -> str:
    """Что делает: возвращает цвет режима для нижней полосы на PNG.
    Зачем нужна: текущий regime должен читаться визуально по цвету."""
    return REGIME_COLORS.get(regime_value, REGIME_COLORS[None])


def draw_regime_panel(
        ax_regime,
        *,
        x_values: np.ndarray,
        regime_values: list[int | None],
        bar_size_seconds: int,
) -> None:
    """Что делает: рисует снизу отдельную гистограмму режима по всем барам текущего окна.
    Зачем нужна: пользователь хочет видеть режим визуально под всей шкалой времени."""
    if x_values.size == 0 or not regime_values:
        ax_regime.axis("off")
        return

    regime_values = list(regime_values)

    if len(regime_values) > x_values.size:
        regime_values = regime_values[-x_values.size:]
    elif len(regime_values) < x_values.size:
        regime_values = [None] * (x_values.size - len(regime_values)) + regime_values

    bar_width = bar_size_seconds / 60.0 * 1.06

    ax_regime.set_facecolor("white")
    ax_regime.bar(
        x_values,
        [1.0] * len(regime_values),
        width=bar_width,
        bottom=0.0,
        align="center",
        color=[get_regime_color(value) for value in regime_values],
        edgecolor="none",
        linewidth=0.0,
        alpha=1.0,
        antialiased=False,
        zorder=3,
    )

    ax_regime.axvline(0.0, linestyle="--", linewidth=1.0, color="black", alpha=0.8, zorder=4)
    ax_regime.set_ylim(0.0, 1.0)
    ax_regime.set_yticks([])
    ax_regime.grid(False)
    ax_regime.margins(x=0.0)
    ax_regime.spines["top"].set_visible(False)
    ax_regime.spines["right"].set_visible(False)
    ax_regime.spines["left"].set_visible(False)
    ax_regime.tick_params(axis="x", labelsize=9)


def convert_optional_float_list_to_array(values: list[float | None]) -> np.ndarray:
    """Что делает: переводит optional float list в numpy array с NaN.
    Зачем нужна: matplotlib корректно пропускает NaN-сегменты на линиях зон."""
    return np.asarray([np.nan if value is None else float(value) for value in values], dtype=float)


def draw_ma_zone_lines(
        ax,
        *,
        x_values: np.ndarray,
        sma_600_values: np.ndarray | None,
        upper_range_values: list[float | None],
        lower_range_values: list[float | None],
        base_value: float,
) -> None:
    """Что делает: рисует границы near/middle/far вокруг SMA600.
    Зачем нужна: ma_zone integer показывает текущую зону, а линии показывают сами границы зон."""
    if sma_600_values is None or x_values.size == 0:
        return

    sma = np.asarray(sma_600_values, dtype=float)

    if sma.size != x_values.size:
        return

    upper_range = convert_optional_float_list_to_array(upper_range_values)
    lower_range = convert_optional_float_list_to_array(lower_range_values)

    if upper_range.size != x_values.size or lower_range.size != x_values.size:
        return

    level1 = float(MA_ZONE_LEVEL1_PERCENT) / 100.0
    level2 = float(MA_ZONE_LEVEL2_PERCENT) / 100.0

    zone_lines = [
        (sma + upper_range * level1 - base_value, ":", 1.15, 0.78),
        (sma + upper_range * level2 - base_value, "--", 1.15, 0.78),
        (sma - lower_range * level1 - base_value, ":", 1.15, 0.78),
        (sma - lower_range * level2 - base_value, "--", 1.15, 0.78),
    ]

    for line_values, linestyle, linewidth, alpha in zone_lines:
        ax.plot(
            x_values,
            line_values,
            linestyle=linestyle,
            linewidth=linewidth,
            alpha=alpha,
            color="#555555",
            zorder=3,
        )


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
        regression_flat_delta_threshold_bps: float,
        signal_window_mode: str,
        market_regime_filter_mode: str,
        candidate_scores: np.ndarray | None = None,
        candidate_potential_result: CandidatePotentialResult | None = None,
        output_dir: Path | None = None,
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
        candidate_scores=candidate_scores,
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
    current_ma_zone_ranges = read_current_ma_zone_ranges(
        instrument_code=instrument_code,
        signal_window=signal_window,
        expected_points=current_values.size,
    )

    current_line = normalize_series_for_plot(np.asarray(current_values, dtype=float))
    current_path_features = calculate_pattern_path_features(current_values)
    current_regime_values = read_signal_regime_values(
        instrument_code=instrument_code,
        signal_window=signal_window,
        expected_points=current_values.size,
    )
    current_regression = build_linear_regression(current_values)
    current_regression_direction = classify_regression_direction(
        current_regression,
        flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
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
            flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
        )
        if sma_600_regression is not None
        else None
    )
    sma_600_regression_line = (
        sma_600_regression.line_values - current_values[0]
        if sma_600_regression is not None
        else None
    )
    price_sma_600_relation = (
        build_regression_relation(
            base_regression=current_regression,
            reference_regression=sma_600_regression,
            near_threshold_bps=regression_flat_delta_threshold_bps,
        )
        if sma_600_regression is not None
        else None
    )

    trade_points = signal_window.trade_seconds // bar_size_seconds
    full_points = current_values.size + trade_points
    candidate_label_padding_minutes = max(bar_size_seconds / 60.0 * 2.0, 0.35)

    fig = plt.figure(figsize=(18, 9))
    grid = fig.add_gridspec(
        nrows=2,
        ncols=2,
        width_ratios=[3, 1],
        height_ratios=[16, 1.15],
        hspace=0.06,
        wspace=0.04,
    )
    ax = fig.add_subplot(grid[0, 0])
    ax_regime = fig.add_subplot(grid[1, 0], sharex=ax)
    ax_info = fig.add_subplot(grid[:, 1])
    ax_info.axis("off")
    ax.tick_params(axis="x", labelbottom=False)

    shown_candidates: list[tuple[int, CandidateWindow, float, str, float | None, object, float, float]] = []

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

    draw_ma_zone_lines(
        ax,
        x_values=current_x_minutes,
        sma_600_values=current_sma_lines.get(600),
        upper_range_values=current_ma_zone_ranges.upper_range_points,
        lower_range_values=current_ma_zone_ranges.lower_range_points,
        base_value=float(current_values[0]),
    )

    if top_indices.size > 0:
        for rank, candidate_index in enumerate(top_indices, start=1):
            candidate = valid_candidates[int(candidate_index)]
            pearson_value = float(pearson_scores[int(candidate_index)])
            candidate_score = (
                float(candidate_scores[int(candidate_index)])
                if candidate_scores is not None
                else None
            )

            candidate_full_values = read_candidate_full_values(
                instrument_code=instrument_code,
                candidate=candidate,
                price_source=price_source,
                expected_points=full_points,
                bar_size_seconds=bar_size_seconds,
            )

            if candidate_full_values is None:
                continue

            candidate_pattern_values = candidate_full_values[:current_values.size]
            candidate_path_features = calculate_pattern_path_features(candidate_pattern_values)

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

            shown_candidates.append((
                rank,
                candidate,
                pearson_value,
                line.get_color(),
                candidate_score,
                candidate_path_features,
                float(candidate_x_minutes[-1]) + candidate_label_padding_minutes,
                float(candidate_line[-1]),
            ))

    for (
            rank,
            candidate,
            pearson_value,
            candidate_color,
            candidate_score,
            candidate_path_features,
            candidate_label_x,
            candidate_label_y,
    ) in reversed(shown_candidates):
        ax.text(
            candidate_label_x,
            candidate_label_y,
            f"{rank}",
            color=candidate_color,
            fontsize=8.5,
            fontweight="bold",
            verticalalignment="center",
            horizontalalignment="left",
            bbox={
                "facecolor": "white",
                "edgecolor": candidate_color,
                "boxstyle": "round,pad=0.18",
                "alpha": 0.95,
            },
            zorder=9,
        )

    if (
            candidate_potential_result is not None
            and candidate_potential_result.is_available
            and candidate_potential_result.weighted_future_delta_points.size > 0
    ):
        potential_x_minutes = candidate_potential_result.x_minutes
        potential_line = (
                current_line[-1]
                + candidate_potential_result.weighted_future_delta_points
        )

        ax.plot(
            potential_x_minutes,
            potential_line,
            linewidth=2.8,
            alpha=0.95,
            zorder=10,
            color="black",
        )

        ax.text(
            float(potential_x_minutes[-1]) + candidate_label_padding_minutes,
            float(potential_line[-1]),
            "POT",
            color="black",
            fontsize=8.5,
            fontweight="bold",
            verticalalignment="center",
            horizontalalignment="left",
            bbox={
                "facecolor": "white",
                "edgecolor": "black",
                "boxstyle": "round,pad=0.18",
                "alpha": 0.95,
            },
            zorder=11,
        )

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
        f"window_mode={signal_window_mode} | "
        f"regime_mode={market_regime_filter_mode}\n"
        f"pattern_minutes={signal_window.pattern_seconds / 60:g} | "
        f"trade_minutes={signal_window.trade_seconds / 60:g} | "
        f"valid_candidates={len(valid_candidates)} | "
        f"passed_threshold={(pearson_scores >= pearson_min).sum()} | "
        f"shown_top={len(shown_candidates)}"
    )
    if shown_candidates or (
            candidate_potential_result is not None
            and candidate_potential_result.is_available
    ):
        ax.set_xlim(
            float(current_x_minutes[0]),
            float(signal_window.trade_seconds / 60.0) + candidate_label_padding_minutes * 4.0,
        )

    ax.grid(True)

    draw_regime_panel(
        ax_regime,
        x_values=current_x_minutes,
        regime_values=current_regime_values,
        bar_size_seconds=bar_size_seconds,
    )

    current_regression_delta_bps = calculate_regression_delta_bps(current_regression)
    current_regression_threshold_points = calculate_regression_threshold_points(
        current_regression,
        flat_delta_threshold_bps=regression_flat_delta_threshold_bps,
    )

    regression_rows: list[tuple[str, str | None]] = [
        (
            f"flat_threshold : "
            f"{format_plot_regression_value(regression_flat_delta_threshold_bps)} / "
            f"{format_plot_regression_value(current_regression_threshold_points)}",
            None,
        ),
        (
            f"price  delta   : "
            f"{format_plot_regression_value(current_regression_delta_bps)} / "
            f"{format_plot_regression_value(current_regression.fitted_delta)}",
            None,
        ),
        (f"price  dir     : {current_regression_direction}", None),
    ]

    if sma_600_regression is not None:
        sma_600_regression_delta_bps = calculate_regression_delta_bps(sma_600_regression)
        regression_rows.extend([
            (
                f"sma 600 delta : "
                f"{format_plot_regression_value(sma_600_regression_delta_bps)} / "
                f"{format_plot_regression_value(sma_600_regression.fitted_delta)}",
                None,
            ),
            (f"sma 600 dir   : {sma_600_regression_direction}", None),
        ])
    else:
        regression_rows.append(("sma 600       : regression=None", None))

    relation_rows: list[tuple[str, str | None]] = []
    if price_sma_600_relation is not None:
        relation_rows.extend([
            (f"price/sma 600  : {price_sma_600_relation.relation}", None),
            (
                f"start_diff     : "
                f"{format_plot_regression_value(price_sma_600_relation.diff_start_bps)} / "
                f"{format_plot_regression_value(price_sma_600_relation.diff_start_points)}",
                None,
            ),
            (
                f"end_diff       : "
                f"{format_plot_regression_value(price_sma_600_relation.diff_end_bps)} / "
                f"{format_plot_regression_value(price_sma_600_relation.diff_end_points)}",
                None,
            ),
        ])
    else:
        relation_rows.append(("price/sma 600  : None", None))

    path_rows: list[tuple[str, str | None]] = [
        (
            f"end_delta    : "
            f"{format_plot_regression_value(current_path_features.end_delta_bps)} / "
            f"{format_plot_regression_value(current_path_features.end_delta_points)}",
            None,
        ),
        (
            f"minmax       : "
            f"{format_plot_regression_value(current_path_features.minmax_bps)} / "
            f"{format_plot_regression_value(current_path_features.minmax_points)}",
            None,
        ),
    ]

    potential_rows: list[tuple[str, str | None]] = []
    if candidate_potential_result is not None and candidate_potential_result.is_available:
        potential_rows.extend([
            (f"dir          : {candidate_potential_result.direction}", None),
            (
                f"used         : "
                f"{candidate_potential_result.used_candidates_count}/"
                f"{candidate_potential_result.max_count}",
                None,
            ),
            (
                f"end          : "
                f"{candidate_potential_result.end_delta_points:+.2f}",
                None,
            ),
            (
                f"profit       : "
                f"+{candidate_potential_result.max_profit_points:.2f} @ "
                f"{format_potential_time_for_plot(candidate_potential_result.max_profit_time_ct)}",
                None,
            ),
            (
                f"drawdown     : "
                f"{candidate_potential_result.max_drawdown_points:.2f} @ "
                f"{format_potential_time_for_plot(candidate_potential_result.max_drawdown_time_ct)}",
                None,
            ),
            (
                f"same/opp/fl  : "
                f"{candidate_potential_result.same_direction_count}/"
                f"{candidate_potential_result.opposite_direction_count}/"
                f"{candidate_potential_result.flat_count}",
                None,
            ),
            (
                f"w same/opp   : "
                f"{candidate_potential_result.same_direction_weight_share:.2f}/"
                f"{candidate_potential_result.opposite_direction_weight_share:.2f}",
                None,
            ),
        ])
    elif candidate_potential_result is not None:
        potential_rows.append((f"status       : {candidate_potential_result.unavailable_reason}", None))
    else:
        potential_rows.append(("status       : None", None))

    lines_rows: list[tuple[str, str | None]] = [
        ("sma 120       : orange", SMA_LINE_COLORS[120]),
        ("sma 600       : blue", SMA_LINE_COLORS[600]),
        ("sma 1200      : green", SMA_LINE_COLORS[1200]),
    ]
    candidate_rows: list[tuple[str, str | None]] = []
    if shown_candidates:
        for rank, candidate, pearson_value, candidate_color, candidate_score, candidate_path_features, candidate_label_x, candidate_label_y in shown_candidates:
            score_text = (
                f"s={candidate_score:.2f} "
                if candidate_score is not None
                else ""
            )
            candidate_rows.append(
                (
                    f"{rank:02d} {score_text}r={pearson_value:.2f} "
                    f"ed={format_plot_regression_value(candidate_path_features.end_delta_points)} "
                    f"mm={format_plot_regression_value(candidate_path_features.minmax_points)} "
                    f"{candidate.signal_bar_time_ct}",
                    candidate_color,
                )
            )
    else:
        candidate_rows.append(("No shown candidates", None))

    y = 0.99
    line_height = 0.0215
    y = draw_info_section(
        ax_info,
        title="REGRESSION (bps / pt)",
        rows=regression_rows,
        y=y,
        line_height=line_height,
    )
    y = draw_info_section(
        ax_info,
        title="RELATION (bps / pt)",
        rows=relation_rows,
        y=y,
        line_height=line_height,
    )
    y = draw_info_section(
        ax_info,
        title="PATH (bps / pt)",
        rows=path_rows,
        y=y,
        line_height=line_height,
    )
    y = draw_info_section(
        ax_info,
        title="POTENTIAL (pt)",
        rows=potential_rows,
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
        output_dir=output_dir,
    )
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)

    return plot_path
