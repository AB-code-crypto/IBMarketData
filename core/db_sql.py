def create_quotes_table_sql(table_name):
    # Таблица для BID/ASK-баров.
    """Что делает: возвращает SQL создания BID/ASK price-таблицы. Зачем нужна: централизует схему хранения исторических и realtime-баров."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        bar_time_ts INTEGER PRIMARY KEY,
        bar_time TEXT NOT NULL,
        bar_time_ct TEXT NOT NULL,
        bar_time_msk TEXT NOT NULL,
        contract TEXT NOT NULL,

        ask_open REAL,
        bid_open REAL,

        ask_high REAL,
        bid_high REAL,

        ask_low REAL,
        bid_low REAL,

        ask_close REAL,
        bid_close REAL,

        volume REAL,
        average REAL,
        bar_count INTEGER
    );
    """


def upsert_quotes_sql(table_name):
    # UPSERT для BID/ASK-таблицы.
    #
    # Этот вариант подходит, когда вся строка бара уже полностью собрана,
    # например при исторической загрузке BID + ASK за один и тот же интервал.
    """Что делает: возвращает SQL UPSERT полной строки BID+ASK. Зачем нужна: historical loader пишет уже собранные пары BID/ASK одним запросом."""
    return f"""
    INSERT INTO {table_name} (
        bar_time_ts,
        bar_time,
        bar_time_ct,
        bar_time_msk,
        contract,

        ask_open,
        bid_open,

        ask_high,
        bid_high,

        ask_low,
        bid_low,

        ask_close,
        bid_close,

        volume,
        average,
        bar_count
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,
        bar_time_ct = excluded.bar_time_ct,
        bar_time_msk = excluded.bar_time_msk,
        contract = excluded.contract,

        ask_open = excluded.ask_open,
        bid_open = excluded.bid_open,

        ask_high = excluded.ask_high,
        bid_high = excluded.bid_high,

        ask_low = excluded.ask_low,
        bid_low = excluded.bid_low,

        ask_close = excluded.ask_close,
        bid_close = excluded.bid_close,

        volume = excluded.volume,
        average = excluded.average,
        bar_count = excluded.bar_count
    ;
    """


def upsert_quotes_ask_sql(table_name):
    # UPSERT только для ASK-стороны realtime-бара.
    #
    # Обновляем только ask_* поля и не трогаем bid_*.
    # Это важно, потому что BID и ASK в realtime приходят отдельными потоками,
    # и более поздний UPSERT одной стороны не должен затирать другую сторону в NULL.
    """Что делает: возвращает SQL UPSERT только ASK-стороны realtime-бара. Зачем нужна: realtime ASK приходит отдельно и не должен затирать BID-поля."""
    return f"""
    INSERT INTO {table_name} (
        bar_time_ts,
        bar_time,
        bar_time_ct,
        bar_time_msk,
        contract,

        ask_open,
        ask_high,
        ask_low,
        ask_close
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,
        bar_time_ct = excluded.bar_time_ct,
        bar_time_msk = excluded.bar_time_msk,
        contract = excluded.contract,

        ask_open = excluded.ask_open,
        ask_high = excluded.ask_high,
        ask_low = excluded.ask_low,
        ask_close = excluded.ask_close
    ;
    """


def upsert_quotes_bid_sql(table_name):
    # UPSERT только для BID-стороны realtime-бара.
    #
    # Обновляем только bid_* поля и не трогаем ask_*.
    """Что делает: возвращает SQL UPSERT только BID-стороны realtime-бара. Зачем нужна: realtime BID приходит отдельно и не должен затирать ASK-поля."""
    return f"""
    INSERT INTO {table_name} (
        bar_time_ts,
        bar_time,
        bar_time_ct,
        bar_time_msk,
        contract,

        bid_open,
        bid_high,
        bid_low,
        bid_close
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

    ON CONFLICT(bar_time_ts) DO UPDATE SET
        bar_time = excluded.bar_time,
        bar_time_ct = excluded.bar_time_ct,
        bar_time_msk = excluded.bar_time_msk,
        contract = excluded.contract,

        bid_open = excluded.bid_open,
        bid_high = excluded.bid_high,
        bid_low = excluded.bid_low,
        bid_close = excluded.bid_close
    ;
    """
