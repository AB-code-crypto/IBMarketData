MID_PRICE_TABLE_NAME = "mid_price_5s"


def create_mid_price_table_sql() -> str:
    # Таблица подготовленных mid/spread-цен для job-data сервиса и тестера.
    """Что делает: возвращает SQL создания таблицы mid_price_5s. Зачем нужна: job DB хранит подготовленные mid/spread OHLC для signal и tester."""
    return f"""
    CREATE TABLE IF NOT EXISTS {quote_identifier(MID_PRICE_TABLE_NAME)} (
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
        attached_schema_name: str,
        source_table_name: str,
        price_digits: int,
        mid_price_digits: int,
) -> str:
    # Полностью заполняет mid_price_5s из attached price DB.
    #
    # mid_* округляем по mid_price_digits, потому что там есть деление на 2.
    # spread_* округляем по price_digits, потому что spread — это разница цен
    # в той же исходной разрядности.
    """Что делает: возвращает SQL полной загрузки mid/spread из attached price DB. Зачем нужна: rebuild job DB пересоздаёт весь рабочий набор признаков одним INSERT SELECT."""
    price_digits = int(price_digits)
    mid_price_digits = int(mid_price_digits)

    target_table_ref = quote_identifier(MID_PRICE_TABLE_NAME)
    source_table_ref = (
        f"{quote_identifier(attached_schema_name)}."
        f"{quote_identifier(source_table_name)}"
    )

    return f"""
    INSERT INTO {target_table_ref} (
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

        ROUND((bid_open + ask_open) / 2.0, {mid_price_digits}) AS mid_open,
        ROUND((bid_high + ask_high) / 2.0, {mid_price_digits}) AS mid_high,
        ROUND((bid_low + ask_low) / 2.0, {mid_price_digits}) AS mid_low,
        ROUND((bid_close + ask_close) / 2.0, {mid_price_digits}) AS mid_close,

        ROUND(ask_open - bid_open, {price_digits}) AS spread_open,
        ROUND(ask_high - bid_high, {price_digits}) AS spread_high,
        ROUND(ask_low - bid_low, {price_digits}) AS spread_low,
        ROUND(ask_close - bid_close, {price_digits}) AS spread_close

    FROM {source_table_ref}
    WHERE bid_open IS NOT NULL
      AND ask_open IS NOT NULL
      AND bid_high IS NOT NULL
      AND ask_high IS NOT NULL
      AND bid_low IS NOT NULL
      AND ask_low IS NOT NULL
      AND bid_close IS NOT NULL
      AND ask_close IS NOT NULL;
    """


def insert_new_mid_price_from_attached_price_db_sql(
        *,
        attached_schema_name: str,
        source_table_name: str,
        price_digits: int,
        mid_price_digits: int,
) -> str:
    # Инкрементально дописывает новые mid/spread-строки из attached price DB.
    #
    # Граница берётся из параметра SQL:
    #     bar_time_ts > ?
    #
    # Вставляем только полностью валидные строки, где есть весь BID/ASK OHLC.
    # Если realtime успел записать только BID или только ASK, строка будет
    # пропущена и попадёт в job DB на следующем проходе.
    """Что делает: возвращает SQL инкрементальной вставки новых mid/spread строк. Зачем нужна: live job-data обновляет только бары новее последнего рассчитанного."""
    price_digits = int(price_digits)
    mid_price_digits = int(mid_price_digits)

    target_table_ref = quote_identifier(MID_PRICE_TABLE_NAME)
    source_table_ref = (
        f"{quote_identifier(attached_schema_name)}."
        f"{quote_identifier(source_table_name)}"
    )

    return f"""
    INSERT OR IGNORE INTO {target_table_ref} (
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

        ROUND((bid_open + ask_open) / 2.0, {mid_price_digits}) AS mid_open,
        ROUND((bid_high + ask_high) / 2.0, {mid_price_digits}) AS mid_high,
        ROUND((bid_low + ask_low) / 2.0, {mid_price_digits}) AS mid_low,
        ROUND((bid_close + ask_close) / 2.0, {mid_price_digits}) AS mid_close,

        ROUND(ask_open - bid_open, {price_digits}) AS spread_open,
        ROUND(ask_high - bid_high, {price_digits}) AS spread_high,
        ROUND(ask_low - bid_low, {price_digits}) AS spread_low,
        ROUND(ask_close - bid_close, {price_digits}) AS spread_close

    FROM {source_table_ref}
    WHERE bar_time_ts > ?
      AND bid_open IS NOT NULL
      AND ask_open IS NOT NULL
      AND bid_high IS NOT NULL
      AND ask_high IS NOT NULL
      AND bid_low IS NOT NULL
      AND ask_low IS NOT NULL
      AND bid_close IS NOT NULL
      AND ask_close IS NOT NULL;
    """


def quote_identifier(value: str) -> str:
    # Безопасно экранирует имя таблицы/схемы SQLite.
    """Что делает: экранирует SQLite identifier двойными кавычками. Зачем нужна: имена таблиц и схем безопасно вставляются в SQL."""
    return '"' + str(value).replace('"', '""') + '"'
