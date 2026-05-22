from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.job_features_config import (
    REGIME_COLUMN_NAME,
    REGIME_FLAT_DELTA_THRESHOLD_POINTS_KEY,
    REGIME_REGRESSION_BARS,
    REGIME_SMA_PERIOD_BARS,
)
from ib_job_data.sma_features import (
    SMA_TABLE_NAME,
    create_sma_table_sql,
    get_sma_column_name,
)


def calculate_regression_delta_points(values: list[float]) -> float:
    """Что делает: считает изменение fitted regression line за окно значений.
    Зачем нужна: regime определяет направление сглаженной SMA без требования роста каждого 5-секундного бара."""
    n = len(values)

    if n < 2:
        raise ValueError(f"Для регрессии нужно минимум 2 значения, получено: {n}")

    sum_x = n * (n - 1) / 2.0
    sum_x2 = (n - 1) * n * (2 * n - 1) / 6.0
    sum_y = sum(float(value) for value in values)
    sum_xy = sum(index * float(value) for index, value in enumerate(values))

    denominator = n * sum_x2 - sum_x * sum_x

    if denominator == 0.0:
        return 0.0

    slope = (n * sum_xy - sum_x * sum_y) / denominator

    return float(slope * (n - 1))


def classify_regime_by_regression_delta(
        *,
        delta_points: float,
        flat_delta_threshold_points: float,
) -> int:
    """Что делает: переводит regression delta в regime code -1/0/1.
    Зачем нужна: в sma_5s храним только итоговый режим, а не промежуточные значения."""
    threshold = abs(float(flat_delta_threshold_points))

    if float(delta_points) > threshold:
        return 1

    if float(delta_points) < -threshold:
        return -1

    return 0


def calculate_regime_code(
        *,
        sma_values: list[float | None],
        flat_delta_threshold_points: float,
) -> int | None:
    """Что делает: считает итоговый regime для одного rolling-window по SMA.
    Зачем нужна: скрывает механику regression/threshold от SQL-слоя."""
    if len(sma_values) < REGIME_REGRESSION_BARS:
        return None

    if any(value is None for value in sma_values):
        return None

    delta_points = calculate_regression_delta_points(
        [float(value) for value in sma_values]
    )

    return classify_regime_by_regression_delta(
        delta_points=delta_points,
        flat_delta_threshold_points=flat_delta_threshold_points,
    )


def get_last_regime_bar_ts(conn) -> int:
    """Что делает: читает последний bar_time_ts, где regime уже рассчитан.
    Зачем нужна: live job-data досчитывает режим только для новых строк."""
    conn.execute(create_sma_table_sql())

    row = conn.execute(
        f"""
        SELECT MAX(bar_time_ts)
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE {quote_identifier(REGIME_COLUMN_NAME)} IS NOT NULL
        """
    ).fetchone()

    if row is None or row[0] is None:
        return 0

    return int(row[0])


def get_first_regime_target_ts(conn, *, after_bar_ts: int) -> int | None:
    """Что делает: находит первую строку sma_5s после already-calculated зоны.
    Зачем нужна: перед расчётом берём только новые строки и контекст перед ними."""
    sma_column_name = get_sma_column_name(REGIME_SMA_PERIOD_BARS)

    row = conn.execute(
        f"""
        SELECT MIN(bar_time_ts)
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE bar_time_ts > ?
          AND {quote_identifier(sma_column_name)} IS NOT NULL
        """,
        (int(after_bar_ts),),
    ).fetchone()

    if row is None or row[0] is None:
        return None

    return int(row[0])


def get_regime_context_start_ts(conn, *, first_target_ts: int) -> int:
    """Что делает: выбирает левую границу контекста перед первой новой строкой.
    Зачем нужна: для rolling-регрессии нужны предыдущие REGIME_REGRESSION_BARS строк."""
    rows = conn.execute(
        f"""
        SELECT bar_time_ts
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE bar_time_ts <= ?
        ORDER BY bar_time_ts DESC
        LIMIT ?
        """,
        (
            int(first_target_ts),
            int(REGIME_REGRESSION_BARS),
        ),
    ).fetchall()

    if not rows:
        return int(first_target_ts)

    return min(int(row[0]) for row in rows)


def read_sma_rows_for_regime(conn, *, context_start_ts: int) -> list[tuple[int, float | None]]:
    """Что делает: читает sma_600 строки, нужные для расчёта regime.
    Зачем нужна: режим считается в Python, а БД хранит только результат."""
    sma_column_name = get_sma_column_name(REGIME_SMA_PERIOD_BARS)

    rows = conn.execute(
        f"""
        SELECT
            bar_time_ts,
            {quote_identifier(sma_column_name)}
        FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE bar_time_ts >= ?
        ORDER BY bar_time_ts
        """,
        (int(context_start_ts),),
    ).fetchall()

    return [
        (
            int(row[0]),
            None if row[1] is None else float(row[1]),
        )
        for row in rows
    ]


def update_regime_features(
        conn,
        *,
        instrument_row: dict,
) -> int:
    """Что делает: досчитывает regime для новых строк sma_5s.
    Зачем нужна: live job-data поддерживает режим синхронным с SMA без пересчёта всей таблицы."""
    conn.execute(create_sma_table_sql())

    threshold_points = float(
        instrument_row.get(
            REGIME_FLAT_DELTA_THRESHOLD_POINTS_KEY,
            0.0,
        )
    )

    last_regime_bar_ts = get_last_regime_bar_ts(conn)
    first_target_ts = get_first_regime_target_ts(
        conn,
        after_bar_ts=last_regime_bar_ts,
    )

    if first_target_ts is None:
        return 0

    context_start_ts = get_regime_context_start_ts(
        conn,
        first_target_ts=first_target_ts,
    )

    rows = read_sma_rows_for_regime(
        conn,
        context_start_ts=context_start_ts,
    )

    window: list[float | None] = []
    updates: list[tuple[int, int]] = []

    for bar_time_ts, sma_value in rows:
        window.append(sma_value)

        if len(window) > REGIME_REGRESSION_BARS:
            window.pop(0)

        if bar_time_ts <= last_regime_bar_ts:
            continue

        regime_code = calculate_regime_code(
            sma_values=window,
            flat_delta_threshold_points=threshold_points,
        )

        if regime_code is None:
            continue

        updates.append(
            (
                int(regime_code),
                int(bar_time_ts),
            )
        )

    if not updates:
        return 0

    conn.executemany(
        f"""
        UPDATE {quote_identifier(SMA_TABLE_NAME)}
        SET {quote_identifier(REGIME_COLUMN_NAME)} = ?
        WHERE bar_time_ts = ?
        """,
        updates,
    )

    return len(updates)


def rebuild_regime_features(
        conn,
        *,
        instrument_row: dict,
) -> int:
    """Что делает: полностью пересчитывает regime внутри sma_5s.
    Зачем нужна: полный rebuild job DB должен получать regime с нуля по актуальной SMA."""
    conn.execute(create_sma_table_sql())

    conn.execute(
        f"""
        UPDATE {quote_identifier(SMA_TABLE_NAME)}
        SET {quote_identifier(REGIME_COLUMN_NAME)} = NULL
        """
    )

    return update_regime_features(
        conn,
        instrument_row=instrument_row,
    )
