from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.sma_features import (
    SMA_DISTANCE_PERIOD_BARS,
    SMA_TABLE_NAME,
    get_sma_distance_points_column_name,
)

PROFILE_TABLE_NAME = "profile"
PROFILE_POSITIVE_SMA_DISTANCE_NAME = (
    f"ewm_avg_pos_{get_sma_distance_points_column_name(SMA_DISTANCE_PERIOD_BARS)}"
)
PROFILE_NEGATIVE_SMA_DISTANCE_NAME = (
    f"ewm_avg_neg_{get_sma_distance_points_column_name(SMA_DISTANCE_PERIOD_BARS)}"
)


def create_profile_table_sql() -> str:
    """Что делает: возвращает SQL создания таблицы profile.
    Зачем нужна: profile хранит редкие характеристики инструмента, которые не должны дублироваться в каждой 5-секундной строке."""
    return f"""
    CREATE TABLE IF NOT EXISTS {quote_identifier(PROFILE_TABLE_NAME)} (
        name TEXT PRIMARY KEY,
        value REAL,
        updated_at_ts INTEGER NOT NULL,
        updated_at TEXT NOT NULL,
        updated_at_ct TEXT NOT NULL,
        source_from_ts INTEGER,
        source_to_ts INTEGER,
        source_bar_count INTEGER NOT NULL,
        lookback_bars INTEGER NOT NULL
    );
    """


def drop_profile_table(conn) -> None:
    """Что делает: удаляет таблицу profile.
    Зачем нужна: полный rebuild job DB должен пересчитать редкие характеристики с нуля."""
    conn.execute(
        f"DROP TABLE IF EXISTS {quote_identifier(PROFILE_TABLE_NAME)}"
    )


def get_last_profile_updated_at_ts(conn) -> int | None:
    """Что делает: читает последний timestamp обновления profile.
    Зачем нужна: live job-data не должен пересчитывать profile чаще одного раза за дневное окно клиринга."""
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


def get_profile_source_to_ts(conn) -> int | None:
    """Что делает: читает последний source_to_ts из profile.
    Зачем нужна: диагностика понимает, до какого бара были рассчитаны текущие значения profile."""
    conn.execute(create_profile_table_sql())

    row = conn.execute(
        f"""
        SELECT MAX(source_to_ts)
        FROM {quote_identifier(PROFILE_TABLE_NAME)}
        """
    ).fetchone()

    if row is None or row[0] is None:
        return None

    return int(row[0])


def read_recent_sma_distance_rows(conn, *, lookback_bars: int) -> list[tuple[int, float]]:
    """Что делает: читает последние готовые signed-дельты mid_close - SMA600.
    Зачем нужна: profile считается по уже подготовленной sma_5s, без повторного расчёта SMA."""
    distance_column_name = get_sma_distance_points_column_name(SMA_DISTANCE_PERIOD_BARS)

    rows_desc = conn.execute(
        f"""
        SELECT
            bar_time_ts,
            {quote_identifier(distance_column_name)}
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE {quote_identifier(distance_column_name)} IS NOT NULL
        ORDER BY bar_time_ts DESC
        LIMIT ?
        """,
        (int(lookback_bars),),
    ).fetchall()

    return [
        (int(row[0]), float(row[1]))
        for row in reversed(rows_desc)
    ]


def calculate_directional_exponential_weighted_means(
        rows: list[tuple[int, float]],
        *,
        lookback_bars: int,
) -> tuple[float | None, float | None]:
    """Что делает: считает EWM-средние отдельно для положительных и отрицательных отклонений.
    Зачем нужна: цена выше и ниже SMA600 имеет разные типичные амплитуды, а последние бары должны весить больше старых."""
    if not rows:
        return None, None

    alpha = 2.0 / (float(lookback_bars) + 1.0)
    decay = 1.0 - alpha
    total_rows = len(rows)

    pos_weighted_sum = 0.0
    pos_weight_sum = 0.0
    neg_weighted_sum = 0.0
    neg_weight_sum = 0.0

    for index, (_, distance_value) in enumerate(rows):
        age_bars = total_rows - index - 1
        weight = decay ** age_bars

        if distance_value > 0.0:
            pos_weighted_sum += distance_value * weight
            pos_weight_sum += weight
        elif distance_value < 0.0:
            neg_weighted_sum += distance_value * weight
            neg_weight_sum += weight

    pos_value = (
        pos_weighted_sum / pos_weight_sum
        if pos_weight_sum > 0.0
        else None
    )
    neg_value = (
        neg_weighted_sum / neg_weight_sum
        if neg_weight_sum > 0.0
        else None
    )

    return pos_value, neg_value


def normalize_updated_at(
        updated_at: datetime | None,
        *,
        timezone_name: str,
) -> tuple[int, str, str]:
    """Что делает: готовит UTC/CT timestamp обновления profile.
    Зачем нужна: таблица должна хранить машинный timestamp и читаемое CT-время обновления."""
    updated_at_utc = updated_at or datetime.now(timezone.utc)

    if updated_at_utc.tzinfo is None:
        updated_at_utc = updated_at_utc.replace(tzinfo=timezone.utc)

    updated_at_utc = updated_at_utc.astimezone(timezone.utc)

    try:
        profile_timezone = ZoneInfo(timezone_name)
    except Exception:
        profile_timezone = timezone.utc

    updated_at_ct = updated_at_utc.astimezone(profile_timezone)

    return (
        int(updated_at_utc.timestamp()),
        updated_at_utc.isoformat(),
        updated_at_ct.isoformat(),
    )


def round_optional_value(value: float | None, digits: int) -> float | None:
    """Что делает: округляет profile-значение, если оно есть.
    Зачем нужна: profile хранит значения в тех же пунктах цены, что и mid/SMA."""
    if value is None:
        return None

    return round(float(value), int(digits))


def refresh_profile_features(
        conn,
        *,
        lookback_bars: int,
        mid_price_digits: int,
        timezone_name: str,
        updated_at: datetime | None = None,
) -> int:
    """Что делает: пересчитывает текущие строки profile.
    Зачем нужна: редкие характеристики инструмента обновляются один раз за заданное окно, а не на каждом 5-секундном баре."""
    lookback_bars = int(lookback_bars)
    mid_price_digits = int(mid_price_digits)

    conn.execute(create_profile_table_sql())

    rows = read_recent_sma_distance_rows(
        conn,
        lookback_bars=lookback_bars,
    )

    pos_value, neg_value = calculate_directional_exponential_weighted_means(
        rows,
        lookback_bars=lookback_bars,
    )

    updated_at_ts, updated_at_iso, updated_at_ct = normalize_updated_at(
        updated_at,
        timezone_name=timezone_name,
    )

    source_from_ts = rows[0][0] if rows else None
    source_to_ts = rows[-1][0] if rows else None
    source_bar_count = len(rows)

    profile_rows = [
        (
            PROFILE_POSITIVE_SMA_DISTANCE_NAME,
            round_optional_value(pos_value, mid_price_digits),
        ),
        (
            PROFILE_NEGATIVE_SMA_DISTANCE_NAME,
            round_optional_value(neg_value, mid_price_digits),
        ),
    ]

    changes_before = conn.total_changes

    for profile_name, profile_value in profile_rows:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {quote_identifier(PROFILE_TABLE_NAME)} (
                name,
                value,
                updated_at_ts,
                updated_at,
                updated_at_ct,
                source_from_ts,
                source_to_ts,
                source_bar_count,
                lookback_bars
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile_name,
                profile_value,
                updated_at_ts,
                updated_at_iso,
                updated_at_ct,
                source_from_ts,
                source_to_ts,
                source_bar_count,
                lookback_bars,
            ),
        )

    return conn.total_changes - changes_before


def rebuild_profile_features(
        conn,
        *,
        lookback_bars: int,
        mid_price_digits: int,
        timezone_name: str,
) -> int:
    """Что делает: полностью пересоздаёт profile.
    Зачем нужна: rebuild job DB должен получить свежую таблицу редких характеристик после пересчёта sma_5s."""
    drop_profile_table(conn)
    conn.execute(create_profile_table_sql())

    return refresh_profile_features(
        conn,
        lookback_bars=lookback_bars,
        mid_price_digits=mid_price_digits,
        timezone_name=timezone_name,
    )
