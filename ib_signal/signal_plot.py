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
from ib_signal.signal_window import SignalWindow

PLOT_TOP_CANDIDATES = 10


def get_signal_png_dir() -> Path:
    """Что делает: возвращает каталог для PNG signal-сервиса и создаёт его при необходимости.
    Зачем нужна: пользователь хочет складывать графики в data/png, а не отправлять их в Telegram."""
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
    current_line = normalize_series_for_plot(np.asarray(current_values, dtype=float))

    trade_points = signal_window.trade_seconds // bar_size_seconds
    full_points = current_values.size + trade_points

    fig, ax = plt.subplots(figsize=(16, 9))

    shown_count = 0

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

            shown_count += 1
            candidate_line = normalize_series_for_plot(candidate_full_values)
            candidate_x_minutes = (
                    np.arange(candidate_line.size, dtype=float) * bar_size_seconds / 60.0
                    - signal_window.pattern_seconds / 60.0
            )

            ax.plot(
                candidate_x_minutes,
                candidate_line,
                linewidth=1.1,
                alpha=0.8,
                label=(
                    f"{rank}. "
                    f"{candidate.signal_bar_time_ct} CT | "
                    f"r={pearson_value:.4f}"
                ),
            )

    ax.plot(
        current_x_minutes,
        current_line,
        linewidth=2.8,
        label="Current pattern",
    )

    ax.axvline(0.0, linestyle="--", linewidth=1.2)

    ax.set_title(
        f"{instrument_code} | signal_bar={signal_bar_time_ct} CT | "
        f"price_source={price_source}\n"
        f"pattern_seconds={signal_window.pattern_seconds} | "
        f"trade_seconds={signal_window.trade_seconds} | "
        f"valid_candidates={len(valid_candidates)} | "
        f"passed_threshold={(pearson_scores >= pearson_min).sum()} | "
        f"shown_top={shown_count}"
    )
    ax.set_xlabel("Minutes relative to signal point")
    ax.set_ylabel("Delta from first point")
    ax.grid(True)
    ax.legend(loc="best", fontsize=8)
    fig.tight_layout()

    plot_path = build_plot_path(
        instrument_code=instrument_code,
        signal_bar_time_ct=signal_bar_time_ct,
    )
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)

    return plot_path
