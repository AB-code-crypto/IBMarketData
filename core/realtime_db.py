from core.db_sql import upsert_quotes_ask_sql, upsert_quotes_bid_sql
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import build_bar_time_fields_from_utc_dt


def open_quotes_db(db_path):
    # Realtime loader открывает только уже существующую price DB.
    # Первый старт и создание БД выполняются заранее через initialize_databases_sync().
    return open_sqlite_connection(
        db_path,
        require_existing_file=True,
    )


def write_realtime_bar_to_sqlite(conn, table_name, contract_name, what_to_show, bar):
    # Записываем одну сторону realtime-бара в SQLite.
    #
    # BID и ASK приходят раздельно, поэтому и пишем их раздельными UPSERT-ами,
    # которые обновляют только свою сторону строки.
    time_fields = build_bar_time_fields_from_utc_dt(bar.time)

    bar_time_ts = time_fields["bar_time_ts"]
    bar_time = time_fields["bar_time"]
    bar_time_ct = time_fields["bar_time_ct"]
    bar_time_msk = time_fields["bar_time_msk"]

    if what_to_show == "ASK":
        sql = upsert_quotes_ask_sql(table_name)
        params = (
            bar_time_ts,
            bar_time,
            bar_time_ct,
            bar_time_msk,
            contract_name,

            bar.open_,
            bar.high,
            bar.low,
            bar.close,
        )

    elif what_to_show == "BID":
        sql = upsert_quotes_bid_sql(table_name)
        params = (
            bar_time_ts,
            bar_time,
            bar_time_ct,
            bar_time_msk,
            contract_name,

            bar.open_,
            bar.high,
            bar.low,
            bar.close,
        )

    else:
        raise ValueError(f"Неподдерживаемый realtime stream: {what_to_show}")

    conn.execute(sql, params)
    conn.commit()
