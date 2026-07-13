import time
from typing import Any

from ib_execution.execution_store import get_trade_db_connection, initialize_execution_db


PROTECTIVE_ORDERS_TABLE_NAME = "protective_orders"

PROTECTIVE_ORDER_ROLE_TAKE_PROFIT = "TAKE_PROFIT"
PROTECTIVE_ORDER_ROLE_STOP_LOSS = "STOP_LOSS"

PROTECTIVE_ORDER_STATUS_ACTIVE = "ACTIVE"
PROTECTIVE_ORDER_STATUS_CANCELLED = "CANCELLED"
PROTECTIVE_ORDER_STATUS_FILLED = "FILLED"
PROTECTIVE_ORDER_STATUS_FAILED = "FAILED"
PROTECTIVE_ORDER_STATUS_UNPROTECTED = "UNPROTECTED"

PROTECTIVE_ORDER_ACTIONABLE_STATUSES = {
    PROTECTIVE_ORDER_STATUS_ACTIVE,
    PROTECTIVE_ORDER_STATUS_UNPROTECTED,
}


PROTECTIVE_ORDER_ROLES = {
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
    PROTECTIVE_ORDER_ROLE_STOP_LOSS,
}


def create_protective_orders_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {PROTECTIVE_ORDERS_TABLE_NAME} (
        protective_order_id INTEGER PRIMARY KEY AUTOINCREMENT,

        instrument_code TEXT NOT NULL,
        parent_trade_intent_id INTEGER NOT NULL,
        role TEXT NOT NULL,
        oca_group TEXT,

        order_ref TEXT NOT NULL,
        order_id INTEGER NOT NULL UNIQUE,
        order_action TEXT NOT NULL,
        order_quantity INTEGER NOT NULL,
        order_type TEXT NOT NULL,

        limit_price REAL,
        stop_price REAL,

        status TEXT NOT NULL,
        error_text TEXT,

        filled_qty REAL,
        avg_fill_price REAL,
        total_commission REAL,
        realized_pnl REAL,
        filled_at_ts INTEGER,
        synthetic_trade_intent_id INTEGER,

        created_at_ts INTEGER NOT NULL,
        updated_at_ts INTEGER NOT NULL,
        finished_at_ts INTEGER
    );
    """


def table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def ensure_table_column(conn, *, table_name: str, column_name: str, column_sql: str) -> None:
    if column_name in table_columns(conn, table_name):
        return

    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def ensure_protective_order_runtime_columns(conn) -> None:
    # Защита на случай, если таблица уже была создана старой версией кода.
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="oca_group",
        column_sql="oca_group TEXT",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="order_type",
        column_sql="order_type TEXT NOT NULL DEFAULT 'LMT'",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="limit_price",
        column_sql="limit_price REAL",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="stop_price",
        column_sql="stop_price REAL",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="filled_qty",
        column_sql="filled_qty REAL",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="avg_fill_price",
        column_sql="avg_fill_price REAL",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="total_commission",
        column_sql="total_commission REAL",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="realized_pnl",
        column_sql="realized_pnl REAL",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="filled_at_ts",
        column_sql="filled_at_ts INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=PROTECTIVE_ORDERS_TABLE_NAME,
        column_name="synthetic_trade_intent_id",
        column_sql="synthetic_trade_intent_id INTEGER",
    )


def initialize_protective_order_db(conn) -> None:
    initialize_execution_db(conn)
    conn.execute(create_protective_orders_table_sql())
    ensure_protective_order_runtime_columns(conn)
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_protective_orders_active_instrument
        ON {PROTECTIVE_ORDERS_TABLE_NAME}(instrument_code, status, created_at_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_protective_orders_parent_role
        ON {PROTECTIVE_ORDERS_TABLE_NAME}(parent_trade_intent_id, role, status);
        """
    )

    from ib_execution.trade_db_migrations import run_trade_db_migrations
    run_trade_db_migrations(conn)


def validate_protective_order_role(role: str) -> str:
    role_value = str(role).upper()

    if role_value not in PROTECTIVE_ORDER_ROLES:
        raise ValueError(f"Unknown protective order role: {role!r}")

    return role_value


def record_protective_order(
        conn,
        *,
        instrument_code: str,
        parent_trade_intent_id: int,
        role: str,
        order_ref: str,
        order_id: int,
        order_action: str,
        order_quantity: int,
        order_type: str,
        limit_price: float | None = None,
        stop_price: float | None = None,
        oca_group: str | None = None,
) -> None:
    initialize_protective_order_db(conn)
    role_value = validate_protective_order_role(role)
    now_ts = int(time.time())

    conn.execute(
        f"""
        INSERT INTO {PROTECTIVE_ORDERS_TABLE_NAME} (
            instrument_code,
            parent_trade_intent_id,
            role,
            oca_group,
            order_ref,
            order_id,
            order_action,
            order_quantity,
            order_type,
            limit_price,
            stop_price,
            status,
            error_text,
            filled_qty,
            avg_fill_price,
            total_commission,
            realized_pnl,
            filled_at_ts,
            synthetic_trade_intent_id,
            created_at_ts,
            updated_at_ts,
            finished_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL)

        ON CONFLICT(order_id) DO UPDATE SET
            instrument_code = excluded.instrument_code,
            parent_trade_intent_id = excluded.parent_trade_intent_id,
            role = excluded.role,
            oca_group = excluded.oca_group,
            order_ref = excluded.order_ref,
            order_action = excluded.order_action,
            order_quantity = excluded.order_quantity,
            order_type = excluded.order_type,
            limit_price = excluded.limit_price,
            stop_price = excluded.stop_price,
            status = excluded.status,
            error_text = NULL,
            filled_qty = NULL,
            avg_fill_price = NULL,
            total_commission = NULL,
            realized_pnl = NULL,
            filled_at_ts = NULL,
            synthetic_trade_intent_id = NULL,
            created_at_ts = excluded.created_at_ts,
            updated_at_ts = excluded.updated_at_ts,
            finished_at_ts = NULL
        """,
        (
            str(instrument_code),
            int(parent_trade_intent_id),
            role_value,
            None if oca_group is None else str(oca_group),
            str(order_ref),
            int(order_id),
            str(order_action).upper(),
            int(order_quantity),
            str(order_type).upper(),
            None if limit_price is None else float(limit_price),
            None if stop_price is None else float(stop_price),
            PROTECTIVE_ORDER_STATUS_ACTIVE,
            now_ts,
            now_ts,
        ),
    )


def row_to_protective_order(row) -> dict[str, Any]:
    return {
        "protective_order_id": int(row[0]),
        "instrument_code": str(row[1]),
        "parent_trade_intent_id": int(row[2]),
        "role": str(row[3]),
        "oca_group": None if row[4] is None else str(row[4]),
        "order_ref": str(row[5]),
        "order_id": int(row[6]),
        "order_action": str(row[7]),
        "order_quantity": int(row[8]),
        "order_type": str(row[9]),
        "limit_price": None if row[10] is None else float(row[10]),
        "stop_price": None if row[11] is None else float(row[11]),
        "status": str(row[12]),
        "error_text": None if row[13] is None else str(row[13]),
        "filled_qty": None if row[14] is None else float(row[14]),
        "avg_fill_price": None if row[15] is None else float(row[15]),
        "total_commission": None if row[16] is None else float(row[16]),
        "realized_pnl": None if row[17] is None else float(row[17]),
        "filled_at_ts": None if row[18] is None else int(row[18]),
        "synthetic_trade_intent_id": None if row[19] is None else int(row[19]),
        "created_at_ts": int(row[20]),
        "updated_at_ts": int(row[21]),
        "finished_at_ts": None if row[22] is None else int(row[22]),
    }


def read_active_protective_orders(conn, *, instrument_code: str | None = None) -> list[dict[str, Any]]:
    initialize_protective_order_db(conn)

    params: tuple[Any, ...]
    where_instrument = ""

    actionable_statuses = sorted(PROTECTIVE_ORDER_ACTIONABLE_STATUSES)
    status_placeholders = ", ".join("?" for _ in actionable_statuses)

    if instrument_code is None:
        params = tuple(actionable_statuses)
    else:
        where_instrument = "AND instrument_code = ?"
        params = (*actionable_statuses, str(instrument_code))

    rows = conn.execute(
        f"""
        SELECT
            protective_order_id,
            instrument_code,
            parent_trade_intent_id,
            role,
            oca_group,
            order_ref,
            order_id,
            order_action,
            order_quantity,
            order_type,
            limit_price,
            stop_price,
            status,
            error_text,
            filled_qty,
            avg_fill_price,
            total_commission,
            realized_pnl,
            filled_at_ts,
            synthetic_trade_intent_id,
            created_at_ts,
            updated_at_ts,
            finished_at_ts
        FROM {PROTECTIVE_ORDERS_TABLE_NAME}
        WHERE status IN ({status_placeholders})
          {where_instrument}
        ORDER BY created_at_ts ASC, protective_order_id ASC
        """,
        params,
    ).fetchall()

    return [row_to_protective_order(row) for row in rows]


def has_active_protective_stop_for_parent(
        conn,
        *,
        parent_trade_intent_id: int,
) -> bool:
    initialize_protective_order_db(conn)

    row = conn.execute(
        f"""
        SELECT 1
        FROM {PROTECTIVE_ORDERS_TABLE_NAME}
        WHERE parent_trade_intent_id = ?
          AND role = ?
          AND status IN ('ACTIVE', 'UNPROTECTED')
        LIMIT 1
        """,
        (
            int(parent_trade_intent_id),
            PROTECTIVE_ORDER_ROLE_STOP_LOSS,
        ),
    ).fetchone()

    return row is not None


def mark_protective_order_status(
        conn,
        *,
        order_id: int,
        status: str,
        error_text: str | None = None,
) -> None:
    initialize_protective_order_db(conn)
    now_ts = int(time.time())
    status_value = str(status).upper()
    finished_at_ts = (
        None
        if status_value in PROTECTIVE_ORDER_ACTIONABLE_STATUSES
        else now_ts
    )

    conn.execute(
        f"""
        UPDATE {PROTECTIVE_ORDERS_TABLE_NAME}
        SET
            status = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE order_id = ?
        """,
        (
            status_value,
            error_text,
            now_ts,
            finished_at_ts,
            int(order_id),
        ),
    )
