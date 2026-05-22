from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.job_features_config import MA_ZONE_COLUMN_NAME, REGIME_COLUMN_NAME

SMA_TABLE_NAME = "sma_5s"
SMA_PRICE_SOURCE = "mid_close"
SMA_PERIODS = (120, 600, 1200)
SMA_DISTANCE_PERIOD_BARS = 600


def get_sma_column_name(period_bars: int) -> str:
    """Что делает: возвращает имя SMA-колонки по периоду в барах.
    Зачем нужна: имя признака явно содержит период, чтобы не путать SMA разных длин."""
    return f"sma_{int(period_bars)}"


def get_sma_distance_points_column_name(period_bars: int) -> str:
    """Что делает: возвращает имя колонки signed-расстояния mid_close до SMA в пунктах.
    Зачем нужна: downstream-сервисы должны явно понимать знак: плюс выше SMA, минус ниже SMA."""
    return f"mid_minus_sma_{int(period_bars)}"


def create_sma_table_sql() -> str:
    """Что делает: возвращает SQL создания таблицы SMA-признаков.
    Зачем нужна: job DB хранит готовые MA-признаки отдельно от базовой таблицы mid/spread."""
    sma_columns_sql = ",\n        ".join(
        f"{quote_identifier(get_sma_column_name(period_bars))} REAL"
        for period_bars in SMA_PERIODS
    )

    distance_column_sql = (
        f"{quote_identifier(get_sma_distance_points_column_name(SMA_DISTANCE_PERIOD_BARS))} REAL"
    )

    return f"""
    CREATE TABLE IF NOT EXISTS {quote_identifier(SMA_TABLE_NAME)} (
        bar_time_ts INTEGER PRIMARY KEY,
        {sma_columns_sql},
        {distance_column_sql},
        {quote_identifier(REGIME_COLUMN_NAME)} INTEGER,
        {quote_identifier(MA_ZONE_COLUMN_NAME)} INTEGER
    );
    """


def drop_sma_table(conn) -> None:
    """Что делает: удаляет таблицу SMA-признаков.
    Зачем нужна: полный rebuild job DB должен пересчитывать SMA с нуля по актуальной mid_price_5s."""
    conn.execute(
        f"DROP TABLE IF EXISTS {quote_identifier(SMA_TABLE_NAME)}"
    )


def build_full_sma_insert_sql(mid_price_digits: int) -> str:
    """Что делает: строит SQL полного расчёта SMA по всей mid_price_5s.
    Зачем нужна: после rebuild mid_price_5s получаем готовые SMA-признаки одним INSERT SELECT."""
    mid_price_digits = int(mid_price_digits)

    window_columns = []
    output_columns = []

    for period_bars in SMA_PERIODS:
        column_name = get_sma_column_name(period_bars)
        window_columns.append(
            f"""
            AVG({quote_identifier(SMA_PRICE_SOURCE)}) OVER (
                ORDER BY bar_time_ts
                ROWS BETWEEN {period_bars - 1} PRECEDING AND CURRENT ROW
            ) AS {quote_identifier(column_name)}
            """
        )
        output_columns.append(
            f"""
            CASE
                WHEN row_number_value >= {period_bars}
                THEN ROUND({quote_identifier(column_name)}, {mid_price_digits})
                ELSE NULL
            END AS {quote_identifier(column_name)}
            """
        )

    distance_sma_column_name = get_sma_column_name(SMA_DISTANCE_PERIOD_BARS)
    distance_points_column_name = get_sma_distance_points_column_name(SMA_DISTANCE_PERIOD_BARS)

    distance_output_column = f"""
            CASE
                WHEN row_number_value >= {SMA_DISTANCE_PERIOD_BARS}
                THEN ROUND(
                    {quote_identifier(SMA_PRICE_SOURCE)}
                    - ROUND({quote_identifier(distance_sma_column_name)}, {mid_price_digits}),
                    {mid_price_digits}
                )
                ELSE NULL
            END AS {quote_identifier(distance_points_column_name)}
            """

    return f"""
    INSERT INTO {quote_identifier(SMA_TABLE_NAME)} (
        bar_time_ts,
        {", ".join(quote_identifier(get_sma_column_name(period_bars)) for period_bars in SMA_PERIODS)},
        {quote_identifier(distance_points_column_name)}
    )
    WITH calculated AS (
        SELECT
            bar_time_ts,
            {quote_identifier(SMA_PRICE_SOURCE)},
            ROW_NUMBER() OVER (ORDER BY bar_time_ts) AS row_number_value,
            {", ".join(window_columns)}
        FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
    )
    SELECT
        bar_time_ts,
        {", ".join(output_columns)},
        {distance_output_column}
    FROM calculated
    ORDER BY bar_time_ts;
    """


def build_incremental_sma_insert_sql(mid_price_digits: int) -> str:
    """Что делает: строит SQL инкрементального расчёта SMA для новых строк mid_price_5s.
    Зачем нужна: live job-data досчитывает SMA только для баров, которых ещё нет в sma_5s."""
    mid_price_digits = int(mid_price_digits)

    select_columns = []

    for period_bars in SMA_PERIODS:
        column_name = get_sma_column_name(period_bars)
        select_columns.append(
            f"""
            CASE
                WHEN (
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1
                        FROM {quote_identifier(MID_PRICE_TABLE_NAME)} AS source
                        WHERE source.bar_time_ts <= target.bar_time_ts
                        ORDER BY source.bar_time_ts DESC
                        LIMIT {period_bars}
                    )
                ) = {period_bars}
                THEN ROUND((
                    SELECT AVG({quote_identifier(SMA_PRICE_SOURCE)})
                    FROM (
                        SELECT {quote_identifier(SMA_PRICE_SOURCE)}
                        FROM {quote_identifier(MID_PRICE_TABLE_NAME)} AS source
                        WHERE source.bar_time_ts <= target.bar_time_ts
                        ORDER BY source.bar_time_ts DESC
                        LIMIT {period_bars}
                    )
                ), {mid_price_digits})
                ELSE NULL
            END AS {quote_identifier(column_name)}
            """
        )

    distance_points_column_name = get_sma_distance_points_column_name(SMA_DISTANCE_PERIOD_BARS)
    select_columns.append(
        f"""
            CASE
                WHEN (
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1
                        FROM {quote_identifier(MID_PRICE_TABLE_NAME)} AS source
                        WHERE source.bar_time_ts <= target.bar_time_ts
                        ORDER BY source.bar_time_ts DESC
                        LIMIT {SMA_DISTANCE_PERIOD_BARS}
                    )
                ) = {SMA_DISTANCE_PERIOD_BARS}
                THEN ROUND(
                    target.{quote_identifier(SMA_PRICE_SOURCE)}
                    - ROUND((
                        SELECT AVG({quote_identifier(SMA_PRICE_SOURCE)})
                        FROM (
                            SELECT {quote_identifier(SMA_PRICE_SOURCE)}
                            FROM {quote_identifier(MID_PRICE_TABLE_NAME)} AS source
                            WHERE source.bar_time_ts <= target.bar_time_ts
                            ORDER BY source.bar_time_ts DESC
                            LIMIT {SMA_DISTANCE_PERIOD_BARS}
                        )
                    ), {mid_price_digits}),
                    {mid_price_digits}
                )
                ELSE NULL
            END AS {quote_identifier(distance_points_column_name)}
            """
    )

    return f"""
    INSERT OR REPLACE INTO {quote_identifier(SMA_TABLE_NAME)} (
        bar_time_ts,
        {", ".join(quote_identifier(get_sma_column_name(period_bars)) for period_bars in SMA_PERIODS)},
        {quote_identifier(distance_points_column_name)}
    )
    SELECT
        target.bar_time_ts,
        {", ".join(select_columns)}
    FROM {quote_identifier(MID_PRICE_TABLE_NAME)} AS target
    WHERE target.bar_time_ts > ?
    ORDER BY target.bar_time_ts;
    """


def get_last_sma_bar_ts(conn) -> int:
    """Что делает: читает последний bar_time_ts в таблице SMA.
    Зачем нужна: инкрементальный расчёт знает, с какой границы досчитывать новые MA-признаки."""
    conn.execute(create_sma_table_sql())

    row = conn.execute(
        f"""
        SELECT MAX(bar_time_ts)
        FROM {quote_identifier(SMA_TABLE_NAME)}
        """
    ).fetchone()

    if row is None or row[0] is None:
        return 0

    return int(row[0])


def rebuild_sma_features(
        conn,
        *,
        mid_price_digits: int,
) -> int:
    """Что делает: полностью пересчитывает таблицу sma_5s по всей mid_price_5s.
    Зачем нужна: после rebuild job DB SMA должна соответствовать актуальной рабочей истории."""
    drop_sma_table(conn)
    conn.execute(create_sma_table_sql())

    changes_before = conn.total_changes
    conn.execute(
        build_full_sma_insert_sql(
            mid_price_digits=mid_price_digits,
        )
    )

    return conn.total_changes - changes_before


def append_new_sma_rows(
        conn,
        *,
        mid_price_digits: int,
) -> int:
    """Что делает: досчитывает SMA-признаки для новых mid_price_5s строк.
    Зачем нужна: live job-data поддерживает sma_5s синхронной с mid_price_5s."""
    conn.execute(create_sma_table_sql())

    last_sma_bar_ts = get_last_sma_bar_ts(conn)

    changes_before = conn.total_changes
    conn.execute(
        build_incremental_sma_insert_sql(
            mid_price_digits=mid_price_digits,
        ),
        (last_sma_bar_ts,),
    )

    return conn.total_changes - changes_before
