
from bisect import bisect_left, insort
from collections import deque
from dataclasses import dataclass
from typing import Deque

from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.job_features_config import (
    MA_ZONE_COLUMN_NAME,
    MA_ZONE_LEVEL1_PERCENT,
    MA_ZONE_LEVEL2_PERCENT,
    MA_ZONE_LOWER_RANGE_COLUMN_NAME,
    MA_ZONE_RANGE_LOOKBACK_BARS,
    MA_ZONE_RANGE_PERCENTILE,
    MA_ZONE_RANGE_SOURCE,
    MA_ZONE_UPPER_RANGE_COLUMN_NAME,
)
from ib_job_data.sma_features import (
    SMA_TABLE_NAME,
    create_sma_table_sql,
    get_sma_column_name,
    get_sma_distance_points_column_name,
)


MA_ZONE_BASE_SMA_PERIOD_BARS = 600


@dataclass(frozen=True)
class MaZoneSourceRow:
    """Что делает: хранит одну строку данных для расчёта MA-zone.
    Зачем нужна: расчёт работает в Python, а не раздувает SQL оконными подзапросами."""
    bar_time_ts: int
    sma_value: float | None
    delta_close_points: float | None
    mid_high: float
    mid_low: float
    mid_close: float


class RollingPositiveValues:
    """Что делает: хранит rolling-окно положительных отклонений в отсортированном виде.
    Зачем нужна: percentile по предыдущим барам должен считаться без сортировки всего окна на каждом баре."""

    def __init__(self, *, max_rows: int) -> None:
        self.max_rows = int(max_rows)
        self._queue: Deque[float | None] = deque()
        self._sorted_values: list[float] = []

    @property
    def rows_count(self) -> int:
        return len(self._queue)

    def append(self, value: float | None) -> None:
        if self.max_rows <= 0:
            return

        self._queue.append(value)

        if value is not None:
            insort(self._sorted_values, float(value))

        if len(self._queue) <= self.max_rows:
            return

        old_value = self._queue.popleft()

        if old_value is None:
            return

        old_value = float(old_value)
        index = bisect_left(self._sorted_values, old_value)

        if index >= len(self._sorted_values):
            raise RuntimeError(f"Не найдено значение для удаления из rolling percentile: {old_value}")

        if self._sorted_values[index] != old_value:
            found_index = None
            left = max(0, index - 3)
            right = min(len(self._sorted_values), index + 4)

            for candidate_index in range(left, right):
                if self._sorted_values[candidate_index] == old_value:
                    found_index = candidate_index
                    break

            if found_index is None:
                raise RuntimeError(f"Не найдено значение для удаления из rolling percentile: {old_value}")

            index = found_index

        self._sorted_values.pop(index)

    def percentile(self, percentile: float) -> float | None:
        return calculate_percentile_value(sorted_values=self._sorted_values, percentile=percentile)


def calculate_percentile_value(*, sorted_values: list[float], percentile: float) -> float | None:
    count = len(sorted_values)

    if count <= 0:
        return None

    if percentile <= 0.0:
        return float(sorted_values[0])

    if percentile >= 100.0:
        return float(sorted_values[count - 1])

    position = (float(percentile) / 100.0) * (count - 1)
    index_low = int(position)
    index_high = index_low if position == index_low else index_low + 1

    if index_low == index_high:
        return float(sorted_values[index_low])

    weight = position - index_low

    return float(sorted_values[index_low] * (1.0 - weight) + sorted_values[index_high] * weight)


def get_positive_deviations_for_row(row: MaZoneSourceRow) -> tuple[float | None, float | None]:
    if row.sma_value is None or row.sma_value <= 0.0:
        return None, None

    if MA_ZONE_RANGE_SOURCE == "HIGH_LOW":
        upper_price = row.mid_high
        lower_price = row.mid_low
    elif MA_ZONE_RANGE_SOURCE == "CLOSE":
        upper_price = row.mid_close
        lower_price = row.mid_close
    else:
        raise ValueError(f"Неизвестный MA_ZONE_RANGE_SOURCE: {MA_ZONE_RANGE_SOURCE!r}")

    upper_deviation = float(upper_price) - float(row.sma_value)
    lower_deviation = float(row.sma_value) - float(lower_price)

    return (
        upper_deviation if upper_deviation > 0.0 else None,
        lower_deviation if lower_deviation > 0.0 else None,
    )


def classify_ma_zone(
        *,
        delta_close_points: float | None,
        upper_range_points: float | None,
        lower_range_points: float | None,
) -> int:
    if delta_close_points is None:
        return 0

    delta = float(delta_close_points)

    if delta == 0.0:
        return 0

    level1_part = float(MA_ZONE_LEVEL1_PERCENT) / 100.0
    level2_part = float(MA_ZONE_LEVEL2_PERCENT) / 100.0

    if delta > 0.0:
        if upper_range_points is None or upper_range_points <= 0.0:
            return 0

        level1 = float(upper_range_points) * level1_part
        level2 = float(upper_range_points) * level2_part

        if delta <= level1:
            return 1

        if delta <= level2:
            return 2

        if delta <= float(upper_range_points):
            return 3

        return 4

    abs_delta = abs(delta)

    if lower_range_points is None or lower_range_points <= 0.0:
        return 0

    level1 = float(lower_range_points) * level1_part
    level2 = float(lower_range_points) * level2_part

    if abs_delta <= level1:
        return -1

    if abs_delta <= level2:
        return -2

    if abs_delta <= float(lower_range_points):
        return -3

    return -4


def get_last_ma_zone_bar_ts(conn) -> int:
    conn.execute(create_sma_table_sql())

    row = conn.execute(
        f"""
        SELECT MAX(bar_time_ts)
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE {quote_identifier(MA_ZONE_COLUMN_NAME)} IS NOT NULL
        """
    ).fetchone()

    if row is None or row[0] is None:
        return 0

    return int(row[0])


def get_first_ma_zone_target_ts(conn, *, after_bar_ts: int) -> int | None:
    row = conn.execute(
        f"""
        SELECT MIN(bar_time_ts)
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE bar_time_ts > ?
        """,
        (int(after_bar_ts),),
    ).fetchone()

    if row is None or row[0] is None:
        return None

    return int(row[0])


def get_ma_zone_context_start_ts(conn, *, first_target_ts: int) -> int:
    rows = conn.execute(
        f"""
        SELECT bar_time_ts
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE bar_time_ts < ?
        ORDER BY bar_time_ts DESC
        LIMIT ?
        """,
        (int(first_target_ts), int(MA_ZONE_RANGE_LOOKBACK_BARS)),
    ).fetchall()

    if not rows:
        return int(first_target_ts)

    return min(int(row[0]) for row in rows)


def read_ma_zone_source_rows(conn, *, context_start_ts: int) -> list[MaZoneSourceRow]:
    sma_column_name = get_sma_column_name(MA_ZONE_BASE_SMA_PERIOD_BARS)
    distance_column_name = get_sma_distance_points_column_name(MA_ZONE_BASE_SMA_PERIOD_BARS)

    rows = conn.execute(
        f"""
        SELECT
            s.bar_time_ts,
            s.{quote_identifier(sma_column_name)},
            s.{quote_identifier(distance_column_name)},
            m.mid_high,
            m.mid_low,
            m.mid_close
        FROM {quote_identifier(SMA_TABLE_NAME)} AS s
        INNER JOIN {quote_identifier(MID_PRICE_TABLE_NAME)} AS m
          ON m.bar_time_ts = s.bar_time_ts
        WHERE s.bar_time_ts >= ?
        ORDER BY s.bar_time_ts
        """,
        (int(context_start_ts),),
    ).fetchall()

    return [
        MaZoneSourceRow(
            bar_time_ts=int(row[0]),
            sma_value=None if row[1] is None else float(row[1]),
            delta_close_points=None if row[2] is None else float(row[2]),
            mid_high=float(row[3]),
            mid_low=float(row[4]),
            mid_close=float(row[5]),
        )
        for row in rows
    ]


def calculate_ma_zone_updates(
        *,
        rows: list[MaZoneSourceRow],
        last_ma_zone_bar_ts: int,
) -> list[tuple[int, float | None, float | None, int]]:
    upper_window = RollingPositiveValues(max_rows=MA_ZONE_RANGE_LOOKBACK_BARS)
    lower_window = RollingPositiveValues(max_rows=MA_ZONE_RANGE_LOOKBACK_BARS)

    updates: list[tuple[int, float | None, float | None, int]] = []

    for row in rows:
        if row.bar_time_ts > last_ma_zone_bar_ts:
            if upper_window.rows_count >= MA_ZONE_RANGE_LOOKBACK_BARS:
                upper_range = upper_window.percentile(MA_ZONE_RANGE_PERCENTILE)
                lower_range = lower_window.percentile(MA_ZONE_RANGE_PERCENTILE)
                zone = classify_ma_zone(
                    delta_close_points=row.delta_close_points,
                    upper_range_points=upper_range,
                    lower_range_points=lower_range,
                )
            else:
                upper_range = None
                lower_range = None
                zone = 0

            updates.append(
                (
                    int(zone),
                    None if upper_range is None else float(upper_range),
                    None if lower_range is None else float(lower_range),
                    int(row.bar_time_ts),
                )
            )

        upper_deviation, lower_deviation = get_positive_deviations_for_row(row)
        upper_window.append(upper_deviation)
        lower_window.append(lower_deviation)

    return updates


def update_ma_zone_features(conn) -> int:
    conn.execute(create_sma_table_sql())

    last_ma_zone_bar_ts = get_last_ma_zone_bar_ts(conn)
    first_target_ts = get_first_ma_zone_target_ts(conn, after_bar_ts=last_ma_zone_bar_ts)

    if first_target_ts is None:
        return 0

    context_start_ts = get_ma_zone_context_start_ts(conn, first_target_ts=first_target_ts)
    rows = read_ma_zone_source_rows(conn, context_start_ts=context_start_ts)
    updates = calculate_ma_zone_updates(rows=rows, last_ma_zone_bar_ts=last_ma_zone_bar_ts)

    if not updates:
        return 0

    conn.executemany(
        f"""
        UPDATE {quote_identifier(SMA_TABLE_NAME)}
        SET
            {quote_identifier(MA_ZONE_COLUMN_NAME)} = ?,
            {quote_identifier(MA_ZONE_UPPER_RANGE_COLUMN_NAME)} = ?,
            {quote_identifier(MA_ZONE_LOWER_RANGE_COLUMN_NAME)} = ?
        WHERE bar_time_ts = ?
        """,
        updates,
    )

    return len(updates)


def rebuild_ma_zone_features(conn) -> int:
    conn.execute(create_sma_table_sql())

    conn.execute(
        f"""
        UPDATE {quote_identifier(SMA_TABLE_NAME)}
        SET
            {quote_identifier(MA_ZONE_COLUMN_NAME)} = NULL,
            {quote_identifier(MA_ZONE_UPPER_RANGE_COLUMN_NAME)} = NULL,
            {quote_identifier(MA_ZONE_LOWER_RANGE_COLUMN_NAME)} = NULL
        """
    )

    return update_ma_zone_features(conn)
