MID_PRICE_TABLE_NAME = "mid_price_5s"


def create_mid_price_table_sql(table_name: str = MID_PRICE_TABLE_NAME) -> str:
    # Таблица подготовленных mid/spread-цен для IBSignal и тестера.
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        bar_time_ts INTEGER PRIMARY KEY,
        bar_time TEXT NOT NULL,
        bar_time_ct TEXT NOT NULL,
        bar_time_msk TEXT NOT NULL,

        mid_open REAL NOT NULL,
        mid_high REAL NOT NULL,
        mid_low REAL NOT NULL,
        mid_close REAL NOT NULL,

        spread_open REAL NOT NULL,
        spread_high REAL NOT NULL,
        spread_low REAL NOT NULL,
        spread_close REAL NOT NULL
    );
    """


def insert_mid_price_from_attached_price_db_sql(
        *,
        target_table_name: str = MID_PRICE_TABLE_NAME,
        attached_schema_name: str,
        source_table_name: str,
) -> str:
    # Полностью заполняет feature-таблицу из attached price DB.
    #
    # В feature-БД попадают только полностью валидные бары,
    # где есть все нужные BID/ASK OHLC-значения.
    source_table_ref = (
        f"{quote_identifier(attached_schema_name)}."
        f"{quote_identifier(source_table_name)}"
    )

    return f"""
    INSERT INTO {target_table_name} (
        bar_time_ts,
        bar_time,
        bar_time_ct,
        bar_time_msk,

        mid_open,
        mid_high,
        mid_low,
        mid_close,

        spread_open,
        spread_high,
        spread_low,
        spread_close
    )
    SELECT
        bar_time_ts,
        bar_time,
        bar_time_ct,
        bar_time_msk,

        (bid_open + ask_open) / 2.0 AS mid_open,
        (bid_high + ask_high) / 2.0 AS mid_high,
        (bid_low + ask_low) / 2.0 AS mid_low,
        (bid_close + ask_close) / 2.0 AS mid_close,

        ask_open - bid_open AS spread_open,
        ask_high - bid_high AS spread_high,
        ask_low - bid_low AS spread_low,
        ask_close - bid_close AS spread_close

    FROM {source_table_ref}
    WHERE bid_open IS NOT NULL
      AND ask_open IS NOT NULL
      AND bid_high IS NOT NULL
      AND ask_high IS NOT NULL
      AND bid_low IS NOT NULL
      AND ask_low IS NOT NULL
      AND bid_close IS NOT NULL
      AND ask_close IS NOT NULL
    ORDER BY bar_time_ts ASC;
    """


def quote_identifier(value: str) -> str:
    # Безопасно экранирует имя таблицы/схемы SQLite.
    return '"' + str(value).replace('"', '""') + '"'
