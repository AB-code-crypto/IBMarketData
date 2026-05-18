from datetime import datetime, timezone

from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.sma_features import (
    SMA_DISTANCE_PERIOD_BARS,
    SMA_TABLE_NAME,
    get_sma_distance_points_column_name,
)

PROFILE_TABLE_NAME = "profile"
PROFILE_VOL_PLUS_NAME = "ewm_avg_vol_plus"
PROFILE_VOL_MINUS_NAME = "ewm_avg_vol_minus"


def create_profile_table_sql() -> str:
    """Что делает: возвращает SQL создания таблицы profile.
    Зачем нужна: profile хранит редкие характеристики инструмента, которые не нужно дублировать в каждой 5-секундной строке."""
    return f"""
    CREATE TABLE IF NOT EXISTS {quote_identifier(PROFILE_TABLE_NAME)} (
        name TEXT PRIMARY KEY,
        value REAL,
        updated_at_ts INTEGER NOT NULL,
        updated_at TEXT NOT NULL
    );
    """


def drop_profile_table(conn) -> None:
    """Что делает: удаляет таблицу profile.
    Зачем нужна: полный rebuild job DB пересчитывает редкие характеристики с нуля."""
    conn.execute(f"DROP TABLE IF EXISTS {quote_identifier(PROFILE_TABLE_NAME)}")


def get_last_profile_updated_at_ts(conn) -> int | None:
    """Что делает: читает время последнего обновления profile.
    Зачем нужна: live job-data обновляет profile не чаще заданного интервала."""
    conn.execute(create_profile_table_sql())

    row = conn.execute(
        f"""
        SELECT MAX(updated_at_ts)
        FROM {quote_identifier(PROFILE_TABLE_NAME)}
        """
    ).fetchone()

    if row is None or row[0] is None:
        return None

    return int(row[0])


def read_recent_sma_distance_values(conn, *, lookback_bars: int) -> list[float]:
    """Что делает: читает последние готовые signed-дельты mid_close - SMA600 от новых к старым.
    Зачем нужна: EWM-веса проще считать от последнего бара к более старым."""
    distance_column_name = get_sma_distance_points_column_name(SMA_DISTANCE_PERIOD_BARS)

    rows = conn.execute(
        f"""
        SELECT {quote_identifier(distance_column_name)}
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE {quote_identifier(distance_column_name)} IS NOT NULL
        ORDER BY bar_time_ts DESC
        LIMIT ?
        """,
        (int(lookback_bars),),
    ).fetchall()

    return [float(row[0]) for row in rows]


def calculate_directional_ewm_avg(
        values_desc: list[float],
        *,
        lookback_bars: int,
) -> tuple[float | None, float | None]:
    """Что делает: считает EWM-средние отдельно для положительных и отрицательных отклонений.
    Зачем нужна: типичное отклонение выше SMA600 и ниже SMA600 может быть разным."""
    if not values_desc:
        return None, None

    alpha = 2.0 / (float(lookback_bars) + 1.0)
    decay = 1.0 - alpha
    weight = 1.0

    plus_sum = 0.0
    plus_weight = 0.0
    minus_sum = 0.0
    minus_weight = 0.0

    for value in values_desc:
        if value > 0.0:
            plus_sum += value * weight
            plus_weight += weight
        elif value < 0.0:
            minus_sum += value * weight
            minus_weight += weight

        weight *= decay

    plus_value = plus_sum / plus_weight if plus_weight > 0.0 else None
    minus_value = minus_sum / minus_weight if minus_weight > 0.0 else None

    return plus_value, minus_value


def round_optional_value(value: float | None, digits: int) -> float | None:
    """Что делает: округляет значение profile, если оно есть.
    Зачем нужна: profile хранит значения в тех же пунктах цены, что и mid/SMA."""
    if value is None:
        return None

    return round(float(value), int(digits))


def refresh_profile_features(
        conn,
        *,
        lookback_bars: int,
        mid_price_digits: int,
        updated_at: datetime | None = None,
) -> int:
    """Что делает: пересчитывает текущие значения profile.
    Зачем нужна: редкие характеристики инструмента считаются отдельно от 5-секундных признаков."""
    lookback_bars = int(lookback_bars)
    mid_price_digits = int(mid_price_digits)
    updated_at_utc = updated_at or datetime.now(timezone.utc)

    if updated_at_utc.tzinfo is None:
        updated_at_utc = updated_at_utc.replace(tzinfo=timezone.utc)

    updated_at_utc = updated_at_utc.astimezone(timezone.utc)
    updated_at_ts = int(updated_at_utc.timestamp())
    updated_at_text = updated_at_utc.isoformat()

    conn.execute(create_profile_table_sql())

    values_desc = read_recent_sma_distance_values(
        conn,
        lookback_bars=lookback_bars,
    )
    plus_value, minus_value = calculate_directional_ewm_avg(
        values_desc,
        lookback_bars=lookback_bars,
    )

    profile_rows = [
        (PROFILE_VOL_PLUS_NAME, round_optional_value(plus_value, mid_price_digits)),
        (PROFILE_VOL_MINUS_NAME, round_optional_value(minus_value, mid_price_digits)),
    ]

    changes_before = conn.total_changes

    for profile_name, profile_value in profile_rows:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {quote_identifier(PROFILE_TABLE_NAME)} (
                name,
                value,
                updated_at_ts,
                updated_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                profile_name,
                profile_value,
                updated_at_ts,
                updated_at_text,
            ),
        )

    return conn.total_changes - changes_before


def refresh_profile_features_if_needed(
        conn,
        *,
        lookback_bars: int,
        mid_price_digits: int,
        update_interval_seconds: int,
) -> int:
    """Что делает: пересчитывает profile, если с прошлого обновления прошёл заданный интервал.
    Зачем нужна: run_job_data может вызывать проверку часто, а тяжёлый расчёт будет выполняться только раз в час."""
    last_updated_at_ts = get_last_profile_updated_at_ts(conn)
    now_utc = datetime.now(timezone.utc)
    now_ts = int(now_utc.timestamp())

    if (
            last_updated_at_ts is not None
            and now_ts - last_updated_at_ts < int(update_interval_seconds)
    ):
        return 0

    return refresh_profile_features(
        conn,
        lookback_bars=lookback_bars,
        mid_price_digits=mid_price_digits,
        updated_at=now_utc,
    )


def rebuild_profile_features(
        conn,
        *,
        lookback_bars: int,
        mid_price_digits: int,
) -> int:
    """Что делает: полностью пересоздаёт profile.
    Зачем нужна: rebuild job DB должен получить свежие редкие характеристики после пересчёта sma_5s."""
    drop_profile_table(conn)
    conn.execute(create_profile_table_sql())

    return refresh_profile_features(
        conn,
        lookback_bars=lookback_bars,
        mid_price_digits=mid_price_digits,
    )
