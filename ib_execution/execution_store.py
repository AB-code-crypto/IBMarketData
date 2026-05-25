import time

from ib_trader.trade_store import (
    TRADE_INTENTS_TABLE_NAME,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent


STALE_NEW_INTENT_ERROR_TEXT = "stale trade_intent: older than execution max age"


def initialize_execution_db(conn) -> None:
    initialize_trade_db(conn)



def expire_stale_new_trade_intents(
        conn,
        *,
        max_age_seconds: int,
        now_ts: int | None = None,
) -> int:
    """Что делает: переводит старые NEW trade_intents в FAILED.
    Зачем нужна: execution не должен отправлять брокеру ордер по устаревшему сигналу."""
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



STALE_ACTIVE_INTENT_ERROR_TEXT = "stale active trade_intent: execution service is no longer tracking it"


def expire_stale_active_trade_intents(
        conn,
        *,
        now_ts: int | None = None,
        default_limit_ttl_seconds: int = 600,
        limit_grace_seconds: int = 10,
        market_max_age_seconds: int = 90,
) -> int:
    """Что делает: переводит зависшие SENDING/ACCEPTED trade_intents в terminal status.
    Зачем нужна: после рестарта execution старый active intent не должен вечно блокировать ib_trader."""
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
              AND created_at_ts >= ?
            ORDER BY created_at_ts ASC, trade_intent_id ASC
            LIMIT ?
            """,
            (min_created_at_ts, int(limit)),
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
            finished_at_ts,
            now_ts,
            int(result.trade_intent_id),
        ),
    )
