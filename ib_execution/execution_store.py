import time

from ib_trader.trade_store import (
    TRADE_INTENTS_TABLE_NAME,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent


STALE_NEW_INTENT_ERROR_TEXT = "stale trade_intent: older than execution max age"
STALE_ACTIVE_INTENT_ERROR_TEXT = "stale active trade_intent: execution service is no longer tracking it"

TAKE_PROFIT_ORDERS_TABLE_NAME = "take_profit_orders"
TAKE_PROFIT_STATUS_ACTIVE = "ACTIVE"
TAKE_PROFIT_STATUS_CANCELLED = "CANCELLED"


def create_take_profit_orders_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {TAKE_PROFIT_ORDERS_TABLE_NAME} (
        take_profit_order_id INTEGER PRIMARY KEY AUTOINCREMENT,

        instrument_code TEXT NOT NULL,
        parent_trade_intent_id INTEGER NOT NULL,

        order_ref TEXT NOT NULL,
        order_id INTEGER NOT NULL UNIQUE,
        order_action TEXT NOT NULL,
        order_quantity INTEGER NOT NULL,
        take_profit_price REAL NOT NULL,

        status TEXT NOT NULL,
        error_text TEXT,

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


def ensure_trade_intents_cancel_request_columns(conn) -> None:
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_requested",
        column_sql="cancel_requested INTEGER NOT NULL DEFAULT 0",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_reason",
        column_sql="cancel_reason TEXT",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_source_signal_id",
        column_sql="cancel_source_signal_id INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_requested_at_ts",
        column_sql="cancel_requested_at_ts INTEGER",
    )


def initialize_execution_db(conn) -> None:
    initialize_trade_db(conn)
    ensure_trade_intents_cancel_request_columns(conn)
    conn.execute(create_take_profit_orders_table_sql())
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_take_profit_orders_active_instrument
        ON {TAKE_PROFIT_ORDERS_TABLE_NAME}(instrument_code, status, created_at_ts);
        """
    )


def record_take_profit_order(
        conn,
        *,
        instrument_code: str,
        parent_trade_intent_id: int,
        order_ref: str,
        order_id: int,
        order_action: str,
        order_quantity: int,
        take_profit_price: float,
) -> None:
    now_ts = int(time.time())

    conn.execute(
        f"""
        INSERT INTO {TAKE_PROFIT_ORDERS_TABLE_NAME} (
            instrument_code,
            parent_trade_intent_id,
            order_ref,
            order_id,
            order_action,
            order_quantity,
            take_profit_price,
            status,
            error_text,
            created_at_ts,
            updated_at_ts,
            finished_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL)

        ON CONFLICT(order_id) DO UPDATE SET
            instrument_code = excluded.instrument_code,
            parent_trade_intent_id = excluded.parent_trade_intent_id,
            order_ref = excluded.order_ref,
            order_action = excluded.order_action,
            order_quantity = excluded.order_quantity,
            take_profit_price = excluded.take_profit_price,
            status = excluded.status,
            error_text = NULL,
            updated_at_ts = excluded.updated_at_ts,
            finished_at_ts = NULL
        """,
        (
            str(instrument_code),
            int(parent_trade_intent_id),
            str(order_ref),
            int(order_id),
            str(order_action).upper(),
            int(order_quantity),
            float(take_profit_price),
            TAKE_PROFIT_STATUS_ACTIVE,
            now_ts,
            now_ts,
        ),
    )


def read_active_take_profit_orders(conn, *, instrument_code: str) -> list[dict]:
    rows = conn.execute(
        f"""
        SELECT
            take_profit_order_id,
            instrument_code,
            parent_trade_intent_id,
            order_ref,
            order_id,
            order_action,
            order_quantity,
            take_profit_price,
            status,
            error_text,
            created_at_ts,
            updated_at_ts,
            finished_at_ts
        FROM {TAKE_PROFIT_ORDERS_TABLE_NAME}
        WHERE instrument_code = ?
          AND status = ?
        ORDER BY created_at_ts ASC, take_profit_order_id ASC
        """,
        (
            str(instrument_code),
            TAKE_PROFIT_STATUS_ACTIVE,
        ),
    ).fetchall()

    return [
        {
            "take_profit_order_id": int(row[0]),
            "instrument_code": str(row[1]),
            "parent_trade_intent_id": int(row[2]),
            "order_ref": str(row[3]),
            "order_id": int(row[4]),
            "order_action": str(row[5]),
            "order_quantity": int(row[6]),
            "take_profit_price": float(row[7]),
            "status": str(row[8]),
            "error_text": None if row[9] is None else str(row[9]),
            "created_at_ts": int(row[10]),
            "updated_at_ts": int(row[11]),
            "finished_at_ts": None if row[12] is None else int(row[12]),
        }
        for row in rows
    ]


def mark_take_profit_order_status(
        conn,
        *,
        order_id: int,
        status: str,
        error_text: str | None = None,
) -> None:
    now_ts = int(time.time())
    status_value = str(status).upper()
    finished_at_ts = now_ts if status_value != TAKE_PROFIT_STATUS_ACTIVE else None

    conn.execute(
        f"""
        UPDATE {TAKE_PROFIT_ORDERS_TABLE_NAME}
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


def expire_stale_new_trade_intents(
        conn,
        *,
        max_age_seconds: int,
        now_ts: int | None = None,
) -> int:
    max_age_seconds = int(max_age_seconds)

    if max_age_seconds <= 0:
        return 0

    now_ts = int(time.time() if now_ts is None else now_ts)
    min_created_at_ts = now_ts - max_age_seconds

    changes_before = conn.total_changes

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE status = 'NEW'
          AND created_at_ts < ?
        """,
        (
            ExecutionStatus.FAILED.value,
            f"{STALE_NEW_INTENT_ERROR_TEXT}; max_age_seconds={max_age_seconds}",
            now_ts,
            now_ts,
            min_created_at_ts,
        ),
    )

    return int(conn.total_changes - changes_before)


def expire_stale_active_trade_intents(
        conn,
        *,
        now_ts: int | None = None,
        default_limit_ttl_seconds: int = 600,
        limit_grace_seconds: int = 10,
        market_max_age_seconds: int = 90,
) -> int:
    now_ts = int(time.time() if now_ts is None else now_ts)
    default_limit_ttl_seconds = int(default_limit_ttl_seconds)
    limit_grace_seconds = int(limit_grace_seconds)
    market_max_age_seconds = int(market_max_age_seconds)

    changes_before = conn.total_changes

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = CASE
                WHEN order_type = 'LIMIT' THEN ?
                ELSE ?
            END,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE status IN ('SENDING', 'ACCEPTED')
          AND (
              (
                  order_type = 'LIMIT'
                  AND ? - COALESCE(sent_at_ts, updated_at_ts, created_at_ts)
                      > COALESCE(ttl_seconds, ?) + ?
              )
              OR
              (
                  order_type != 'LIMIT'
                  AND ? - COALESCE(sent_at_ts, updated_at_ts, created_at_ts)
                      > ?
              )
          )
        """,
        (
            ExecutionStatus.EXPIRED.value,
            ExecutionStatus.FAILED.value,
            (
                f"{STALE_ACTIVE_INTENT_ERROR_TEXT}; "
                f"default_limit_ttl_seconds={default_limit_ttl_seconds}; "
                f"limit_grace_seconds={limit_grace_seconds}; "
                f"market_max_age_seconds={market_max_age_seconds}"
            ),
            now_ts,
            now_ts,
            now_ts,
            default_limit_ttl_seconds,
            limit_grace_seconds,
            now_ts,
            market_max_age_seconds,
        ),
    )

    return int(conn.total_changes - changes_before)


def read_trade_intent_cancel_request(conn, *, trade_intent_id: int) -> dict | None:
    ensure_trade_intents_cancel_request_columns(conn)

    row = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            cancel_requested,
            cancel_reason,
            cancel_source_signal_id,
            cancel_requested_at_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE trade_intent_id = ?
          AND COALESCE(cancel_requested, 0) = 1
        LIMIT 1
        """,
        (int(trade_intent_id),),
    ).fetchone()

    if row is None:
        return None

    return {
        "trade_intent_id": int(row[0]),
        "cancel_requested": bool(int(row[1] or 0)),
        "cancel_reason": None if row[2] is None else str(row[2]),
        "cancel_source_signal_id": None if row[3] is None else int(row[3]),
        "cancel_requested_at_ts": None if row[4] is None else int(row[4]),
    }


def read_new_trade_intents(*, limit: int = 20, max_age_seconds: int = 10) -> list[TradeIntent]:
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)

        now_ts = int(time.time())
        max_age_seconds = int(max_age_seconds)

        expire_stale_new_trade_intents(
            conn,
            max_age_seconds=max_age_seconds,
            now_ts=now_ts,
        )
        expire_stale_active_trade_intents(
            conn,
            now_ts=now_ts,
        )
        conn.commit()

        min_created_at_ts = now_ts - max_age_seconds if max_age_seconds > 0 else now_ts

        rows = conn.execute(
            f"""
            SELECT
                trade_intent_id,
                source_signal_id,
                instrument_code,
                order_ref,

                action,
                target_side,
                target_qty,

                position_before_side,
                position_before_qty,

                order_type,
                limit_price,
                limit_offset_points,
                ttl_seconds,

                status,
                created_at_ts
            FROM {TRADE_INTENTS_TABLE_NAME}
            WHERE status = 'NEW'
              AND COALESCE(cancel_requested, 0) = 0
              AND created_at_ts >= ?
            ORDER BY created_at_ts ASC, trade_intent_id ASC
            LIMIT ?
            """,
            (min_created_at_ts, int(limit)),
        ).fetchall()

        result: list[TradeIntent] = []

        for row in rows:
            order_ref = "" if row[3] is None else str(row[3]).strip()

            if not order_ref:
                raise RuntimeError(
                    f"TradeIntent without order_ref: "
                    f"trade_intent_id={int(row[0])}, "
                    f"instrument_code={str(row[2])}"
                )

            result.append(
                TradeIntent(
                    trade_intent_id=int(row[0]),
                    source_signal_id=int(row[1]),
                    instrument_code=str(row[2]),
                    order_ref=order_ref,
                    action=str(row[4]),
                    target_side=str(row[5]),
                    target_qty=float(row[6]),
                    position_before_side=str(row[7]),
                    position_before_qty=float(row[8]),
                    order_type=str(row[9]).upper(),
                    limit_price=None if row[10] is None else float(row[10]),
                    limit_offset_points=None if row[11] is None else float(row[11]),
                    ttl_seconds=None if row[12] is None else int(row[12]),
                    status=str(row[13]),
                    created_at_ts=int(row[14]),
                )
            )

        return result

    finally:
        conn.close()


def mark_trade_intent_sending(conn, *, trade_intent_id: int) -> None:
    now_ts = int(time.time())

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = ?,
            sent_at_ts = COALESCE(sent_at_ts, ?),
            updated_at_ts = ?
        WHERE trade_intent_id = ?
        """,
        (
            ExecutionStatus.SENDING.value,
            now_ts,
            now_ts,
            int(trade_intent_id),
        ),
    )


def mark_trade_intent_order_submitted(
        conn,
        *,
        trade_intent_id: int,
        order_id: int,
        order_action: str,
        order_quantity: int,
) -> None:
    now_ts = int(time.time())

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = ?,
            order_id = COALESCE(?, order_id),
            order_action = COALESCE(?, order_action),
            order_quantity = COALESCE(?, order_quantity),
            sent_at_ts = COALESCE(sent_at_ts, ?),
            updated_at_ts = ?
        WHERE trade_intent_id = ?
        """,
        (
            ExecutionStatus.SENDING.value,
            int(order_id),
            str(order_action),
            int(order_quantity),
            now_ts,
            now_ts,
            int(trade_intent_id),
        ),
    )


def write_trade_intent_execution_result(
        conn,
        *,
        result: ExecutionResult,
) -> None:
    now_ts = int(time.time())
    terminal_statuses = {
        ExecutionStatus.EXECUTED.value,
        ExecutionStatus.FAILED.value,
        ExecutionStatus.CANCELLED.value,
        ExecutionStatus.EXPIRED.value,
    }
    finished_at_ts = now_ts if result.status.value in terminal_statuses else None

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = ?,

            order_id = COALESCE(?, order_id),
            order_action = COALESCE(?, order_action),
            order_quantity = COALESCE(?, order_quantity),
            avg_fill_price = ?,
            total_commission = ?,
            realized_pnl = ?,
            error_text = ?,

            finished_at_ts = ?,
            updated_at_ts = ?
        WHERE trade_intent_id = ?
        """,
        (
            result.status.value,
            None if result.order_id is None else int(result.order_id),
            result.order_action,
            None if result.order_quantity is None else int(result.order_quantity),
            result.avg_fill_price,
            result.total_commission,
            result.realized_pnl,
            result.error_text,
            finished_at_ts,
            now_ts,
            int(result.trade_intent_id),
        ),
    )
