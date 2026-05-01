import sqlite3

from core.db_sql import create_quotes_table_sql, upsert_quotes_sql
from core.sqlite_utils import open_sqlite_connection


def write_quote_rows_to_sqlite(db_path, table_name, rows):
    create_sql = create_quotes_table_sql(table_name)
    upsert_sql = upsert_quotes_sql(table_name)

    conn = open_sqlite_connection(db_path)

    try:
        conn.execute(create_sql)
        conn.executemany(upsert_sql, rows)
        conn.commit()

    finally:
        conn.close()


def get_contract_history_bounds(db_path, table_name, contract_name):
    conn = open_sqlite_connection(db_path, use_wal=False)

    try:
        cursor = conn.execute(
            f"""
            SELECT
                MIN(bar_time_ts) AS min_bar_time_ts,
                MAX(bar_time_ts) AS max_bar_time_ts
            FROM {table_name}
            WHERE contract = ?
            """,
            (contract_name,),
        )
        row = cursor.fetchone()

        if row is None:
            return None, None

        if row[0] is None or row[1] is None:
            return None, None

        return int(row[0]), int(row[1])

    except sqlite3.OperationalError:
        return None, None

    finally:
        conn.close()
