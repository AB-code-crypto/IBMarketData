import time

from ib_trader.trade_store import (
    TRADE_INTENTS_TABLE_NAME,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent


def initialize_execution_db(conn) -> None:
    initialize_trade_db(conn)


def read_new_trade_intents(*, limit: int = 20) -> list[TradeIntent]:
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)

        rows = conn.execute(
            f"""
            SELECT
                trade_intent_id,
                decision_id,
                source_signal_id,
                instrument_code,

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
            ORDER BY created_at_ts ASC, trade_intent_id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        return [
            TradeIntent(
                trade_intent_id=int(row[0]),
                decision_id=int(row[1]),
                source_signal_id=int(row[2]),
                instrument_code=str(row[3]),
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
            for row in rows
        ]

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


def write_trade_intent_execution_result(
        conn,
        *,
        result: ExecutionResult,
) -> None:
    now_ts = int(time.time())

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = ?,

            order_id = ?,
            order_action = ?,
            order_quantity = ?,
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
            now_ts,
            now_ts,
            int(result.trade_intent_id),
        ),
    )
