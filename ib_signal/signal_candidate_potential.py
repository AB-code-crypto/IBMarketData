from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, TypeAlias

import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.price_source import (
    complete_mid_close_predicate,
    get_price_db_target,
    mid_close_sql,
    quote_identifier,
)
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import build_bar_time_fields_from_utc_dt
from ib_signal.signal_candidates import CandidateWindow
from ib_signal.signal_window import SignalWindow

PotentialDirection: TypeAlias = Literal["LONG", "SHORT", "NONE"]


@dataclass(frozen=True)
class CandidatePotentialResult:
    # True, если потенциал реально посчитан.
    is_available: bool

    # Причина недоступности, если is_available=False.
    unavailable_reason: str | None

    # Направление по финальной точке weighted path.
    direction: PotentialDirection

    # Сколько кандидатов требовалось минимум.
    min_count: int

    # Сколько кандидатов максимум брали в расчёт.
    max_count: int

    # Сколько кандидатов было на входе.
    source_candidates_count: int

    # Сколько кандидатов взяли в расчёт после проверки future-window.
    used_candidates_count: int

    # Временная шкала future path в минутах от signal_bar.
    x_minutes: np.ndarray

    # Weighted future path в bps.
    weighted_future_delta_bps: np.ndarray

    # Weighted future path в пунктах текущего инструмента.
    weighted_future_delta_points: np.ndarray

    # Финальная точка weighted future path.
    end_delta_bps: float
    end_delta_points: float

    # Максимальный profit в сторону direction. Положительное число.
    max_profit_bps: float
    max_profit_points: float
    max_profit_time_ct: str

    # Максимальная просадка против direction. Отрицательное число.
    max_drawdown_bps: float
    max_drawdown_points: float
    max_drawdown_time_ct: str

    # Сколько кандидатов закончили в сторону direction / против / flat.
    same_direction_count: int
    opposite_direction_count: int
    flat_count: int

    # Доли веса кандидатов в сторону direction / против / flat.
    same_direction_weight_share: float
    opposite_direction_weight_share: float
    flat_weight_share: float


def read_candidate_full_values(
        *,
        instrument_code: str,
        candidate: CandidateWindow,
        expected_points: int,
        bar_size_seconds: int,
) -> np.ndarray | None:
    """Reads a candidate pattern and its historical future from price DB."""
    target = get_price_db_target(instrument_code)
    conn = open_sqlite_connection(
        str(target.db_path),
        require_existing_file=True,
        use_wal=False,
    )
    try:
        rows = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                {mid_close_sql(instrument_code)} AS mid_close
            FROM {quote_identifier(target.table_name)}
            WHERE bar_time_ts >= ?
              AND bar_time_ts < ?
              AND {complete_mid_close_predicate()}
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


def calculate_future_delta_bps(
        *,
        full_values: np.ndarray,
        pattern_points: int,
        trade_points: int,
) -> np.ndarray | None:
    """Что делает: строит future delta path кандидата в bps от entry.
    Зачем нужна: кандидаты могут быть на разных ценовых уровнях, поэтому усреднять надо относительное движение."""
    values = np.asarray(full_values, dtype=float)

    if values.ndim != 1:
        raise ValueError(
            f"full_values должен быть одномерным, получено shape={values.shape}"
        )

    expected_points = pattern_points + trade_points
    if values.size != expected_points:
        return None

    if pattern_points < 2 or trade_points <= 0:
        return None

    entry_price = float(values[pattern_points - 1])

    if abs(entry_price) == 0.0:
        return None

    future_values = values[pattern_points:]

    if future_values.size != trade_points:
        return None

    future_delta_bps = (future_values - entry_price) / abs(entry_price) * 10000.0

    return np.concatenate([
        np.zeros((1,), dtype=float),
        future_delta_bps.astype(float),
    ])


def normalize_candidate_weights(raw_scores: np.ndarray) -> np.ndarray:
    """Что делает: превращает candidate score в веса.
    Зачем нужна: weighted potential должен давать больший вес более сильным кандидатам."""
    scores = np.asarray(raw_scores, dtype=float)

    if scores.ndim != 1:
        raise ValueError(f"candidate scores должны быть одномерными, shape={scores.shape}")

    positive_scores = np.maximum(scores, 0.0)
    total_score = float(positive_scores.sum())

    if total_score > 0.0:
        return positive_scores / total_score

    if scores.size == 0:
        return scores

    return np.full((scores.size,), 1.0 / scores.size, dtype=float)


def get_ct_time_text(ts: int) -> str:
    """Что делает: форматирует UTC timestamp в CT-время.
    Зачем нужна: в POTENTIAL нужны времена max profit / drawdown."""
    dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    return str(build_bar_time_fields_from_utc_dt(dt_utc)["bar_time_ct"])


def get_direction(value: float) -> PotentialDirection:
    """Что делает: определяет направление по знаку значения.
    Зачем нужна: direction задаёт, что считается profit и drawdown."""
    number = float(value)

    if number > 0.0:
        return "LONG"

    if number < 0.0:
        return "SHORT"

    return "NONE"


def build_empty_potential_result(
        *,
        reason: str,
        min_count: int,
        max_count: int,
        source_candidates_count: int,
        used_candidates_count: int = 0,
) -> CandidatePotentialResult:
    """Что делает: возвращает пустой potential result.
    Зачем нужна: runner/plot/log получают единый объект даже когда потенциал считать нельзя."""
    return CandidatePotentialResult(
        is_available=False,
        unavailable_reason=reason,
        direction="NONE",
        min_count=int(min_count),
        max_count=int(max_count),
        source_candidates_count=int(source_candidates_count),
        used_candidates_count=int(used_candidates_count),
        x_minutes=np.empty((0,), dtype=float),
        weighted_future_delta_bps=np.empty((0,), dtype=float),
        weighted_future_delta_points=np.empty((0,), dtype=float),
        end_delta_bps=0.0,
        end_delta_points=0.0,
        max_profit_bps=0.0,
        max_profit_points=0.0,
        max_profit_time_ct="-",
        max_drawdown_bps=0.0,
        max_drawdown_points=0.0,
        max_drawdown_time_ct="-",
        same_direction_count=0,
        opposite_direction_count=0,
        flat_count=0,
        same_direction_weight_share=0.0,
        opposite_direction_weight_share=0.0,
        flat_weight_share=0.0,
    )


def calculate_direction_stats(
        *,
        direction: PotentialDirection,
        final_delta_bps: np.ndarray,
        weights: np.ndarray,
) -> tuple[int, int, int, float, float, float]:
    """Что делает: считает count/weight-share кандидатов в сторону потенциала и против.
    Зачем нужна: кроме weighted path надо видеть согласованность кандидатов."""
    if final_delta_bps.shape[0] != weights.shape[0]:
        raise ValueError(
            f"final_delta_bps и weights не совпадают: "
            f"final={final_delta_bps.shape[0]}, weights={weights.shape[0]}"
        )

    if direction == "LONG":
        same_mask = final_delta_bps > 0.0
        opposite_mask = final_delta_bps < 0.0

    elif direction == "SHORT":
        same_mask = final_delta_bps < 0.0
        opposite_mask = final_delta_bps > 0.0

    else:
        same_mask = np.zeros(final_delta_bps.shape, dtype=bool)
        opposite_mask = np.zeros(final_delta_bps.shape, dtype=bool)

    flat_mask = final_delta_bps == 0.0

    same_count = int(same_mask.sum())
    opposite_count = int(opposite_mask.sum())
    flat_count = int(flat_mask.sum())

    same_weight = float(weights[same_mask].sum()) if weights.size else 0.0
    opposite_weight = float(weights[opposite_mask].sum()) if weights.size else 0.0
    flat_weight = float(weights[flat_mask].sum()) if weights.size else 0.0

    return same_count, opposite_count, flat_count, same_weight, opposite_weight, flat_weight


def build_candidate_potential_result(
        *,
        instrument_code: str,
        signal_window: SignalWindow,
        current_values: np.ndarray,
        candidates: list[CandidateWindow],
        candidate_scores: np.ndarray,
        min_count: int,
        max_count: int,
) -> CandidatePotentialResult:
    """Что делает: строит weighted future path по top-score кандидатам.
    Зачем нужна: potential показывает ожидаемое развитие future-window по уже отобранным кандидатам."""
    min_count = int(min_count)
    max_count = int(max_count)

    if min_count <= 0:
        raise ValueError(f"min_count должен быть > 0, получено: {min_count}")

    if max_count <= 0:
        raise ValueError(f"max_count должен быть > 0, получено: {max_count}")

    if max_count < min_count:
        raise ValueError(
            f"max_count не может быть меньше min_count: min={min_count}, max={max_count}"
        )

    if current_values.size == 0:
        return build_empty_potential_result(
            reason="current_values_empty",
            min_count=min_count,
            max_count=max_count,
            source_candidates_count=len(candidates),
        )

    scores = np.asarray(candidate_scores, dtype=float)

    if scores.ndim != 1:
        raise ValueError(f"candidate_scores должен быть одномерным, shape={scores.shape}")

    if scores.shape[0] != len(candidates):
        raise ValueError(
            f"candidate_scores и candidates не совпадают по длине: "
            f"scores={scores.shape[0]}, candidates={len(candidates)}"
        )

    if not candidates:
        return build_empty_potential_result(
            reason="no_candidates",
            min_count=min_count,
            max_count=max_count,
            source_candidates_count=0,
        )

    instrument_row = Instrument[instrument_code]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])
    pattern_points = int(current_values.size)
    trade_points = int(signal_window.trade_seconds // bar_size_seconds)
    expected_points = pattern_points + trade_points

    if trade_points <= 0:
        return build_empty_potential_result(
            reason="trade_window_empty",
            min_count=min_count,
            max_count=max_count,
            source_candidates_count=len(candidates),
        )

    selected_count = min(max_count, len(candidates))
    selected_candidates = candidates[:selected_count]
    selected_scores = scores[:selected_count]

    future_series: list[np.ndarray] = []
    valid_candidates: list[CandidateWindow] = []
    valid_scores: list[float] = []

    for candidate, candidate_score in zip(selected_candidates, selected_scores):
        full_values = read_candidate_full_values(
            instrument_code=instrument_code,
            candidate=candidate,
            expected_points=expected_points,
            bar_size_seconds=bar_size_seconds,
        )

        if full_values is None:
            continue

        future_delta_bps = calculate_future_delta_bps(
            full_values=full_values,
            pattern_points=pattern_points,
            trade_points=trade_points,
        )

        if future_delta_bps is None:
            continue

        future_series.append(future_delta_bps)
        valid_candidates.append(candidate)
        valid_scores.append(float(candidate_score))

    used_count = len(valid_candidates)

    if used_count < min_count:
        return build_empty_potential_result(
            reason=f"not_enough_valid_candidates:{used_count}<{min_count}",
            min_count=min_count,
            max_count=max_count,
            source_candidates_count=len(candidates),
            used_candidates_count=used_count,
        )

    future_matrix = np.vstack(future_series).astype(float)
    score_array = np.asarray(valid_scores, dtype=float)
    weights = normalize_candidate_weights(score_array)

    weighted_future_bps = np.average(
        future_matrix,
        axis=0,
        weights=weights,
    )

    current_entry_price = float(current_values[-1])
    weighted_future_points = weighted_future_bps / 10000.0 * abs(current_entry_price)

    x_minutes = np.arange(weighted_future_points.size, dtype=float) * bar_size_seconds / 60.0

    end_delta_bps = float(weighted_future_bps[-1])
    end_delta_points = float(weighted_future_points[-1])
    direction = get_direction(end_delta_points)

    max_up_index = int(np.argmax(weighted_future_points))
    max_down_index = int(np.argmin(weighted_future_points))

    max_up_points = float(weighted_future_points[max_up_index])
    max_down_points = float(weighted_future_points[max_down_index])
    max_up_bps = float(weighted_future_bps[max_up_index])
    max_down_bps = float(weighted_future_bps[max_down_index])

    if direction == "LONG":
        max_profit_points = max(0.0, max_up_points)
        max_profit_bps = max(0.0, max_up_bps)
        max_profit_index = max_up_index

        max_drawdown_points = min(0.0, max_down_points)
        max_drawdown_bps = min(0.0, max_down_bps)
        max_drawdown_index = max_down_index

    elif direction == "SHORT":
        max_profit_points = max(0.0, -max_down_points)
        max_profit_bps = max(0.0, -max_down_bps)
        max_profit_index = max_down_index

        max_drawdown_points = -max(0.0, max_up_points)
        max_drawdown_bps = -max(0.0, max_up_bps)
        max_drawdown_index = max_up_index

    else:
        max_profit_points = 0.0
        max_profit_bps = 0.0
        max_profit_index = 0
        max_drawdown_points = 0.0
        max_drawdown_bps = 0.0
        max_drawdown_index = 0

    final_candidate_delta_bps = future_matrix[:, -1]
    (
        same_count,
        opposite_count,
        flat_count,
        same_weight,
        opposite_weight,
        flat_weight,
    ) = calculate_direction_stats(
        direction=direction,
        final_delta_bps=final_candidate_delta_bps,
        weights=weights,
    )

    max_profit_ts = signal_window.signal_bar_ts + max_profit_index * bar_size_seconds
    max_drawdown_ts = signal_window.signal_bar_ts + max_drawdown_index * bar_size_seconds

    return CandidatePotentialResult(
        is_available=True,
        unavailable_reason=None,
        direction=direction,
        min_count=min_count,
        max_count=max_count,
        source_candidates_count=len(candidates),
        used_candidates_count=used_count,
        x_minutes=x_minutes,
        weighted_future_delta_bps=weighted_future_bps.astype(float),
        weighted_future_delta_points=weighted_future_points.astype(float),
        end_delta_bps=end_delta_bps,
        end_delta_points=end_delta_points,
        max_profit_bps=float(max_profit_bps),
        max_profit_points=float(max_profit_points),
        max_profit_time_ct=get_ct_time_text(max_profit_ts),
        max_drawdown_bps=float(max_drawdown_bps),
        max_drawdown_points=float(max_drawdown_points),
        max_drawdown_time_ct=get_ct_time_text(max_drawdown_ts),
        same_direction_count=same_count,
        opposite_direction_count=opposite_count,
        flat_count=flat_count,
        same_direction_weight_share=same_weight,
        opposite_direction_weight_share=opposite_weight,
        flat_weight_share=flat_weight,
    )


def format_candidate_potential_result(result: CandidatePotentialResult) -> str:
    """Что делает: форматирует potential result для runtime-лога.
    Зачем нужна: лог показывает weighted future path без необходимости открывать PNG."""
    if not result.is_available:
        return (
            f"status=off, reason={result.unavailable_reason}, "
            f"used={result.used_candidates_count}, "
            f"min={result.min_count}, max={result.max_count}, "
            f"source={result.source_candidates_count}"
        )

    return (
        f"dir={result.direction}, "
        f"used={result.used_candidates_count}/{result.max_count}, "
        f"end={result.end_delta_points:+.2f} pt / {result.end_delta_bps:+.2f} bps, "
        f"profit={result.max_profit_points:+.2f} pt / {result.max_profit_bps:+.2f} bps "
        f"@ {result.max_profit_time_ct} CT, "
        f"drawdown={result.max_drawdown_points:+.2f} pt / {result.max_drawdown_bps:+.2f} bps "
        f"@ {result.max_drawdown_time_ct} CT, "
        f"same/opposite/flat={result.same_direction_count}/"
        f"{result.opposite_direction_count}/{result.flat_count}, "
        f"w_same/opposite/flat={result.same_direction_weight_share:.2f}/"
        f"{result.opposite_direction_weight_share:.2f}/"
        f"{result.flat_weight_share:.2f}"
    )
