import time

from core.sqlite_schema import require_table_absent
from ib_trader.trade_schema import (
    TRADE_INTENTS_TABLE_NAME,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent


STALE_NEW_INTENT_ERROR_TEXT = "stale trade_intent: older than execution max age"
STALE_ACTIVE_INTENT_ERROR_TEXT = "stale active trade_intent: execution service is no longer tracking it"

# Database-only cleanup for obsolete auxiliary exits. The old table name is
# retained only so existing installations can be upgraded safely.
LEGACY_AUXILIARY_STATE_TABLE = "slot_loss_extensions"
LEGACY_AUXILIARY_EXIT_SUFFIXES = ("_EXT_TP", "_EXT_SL")


def remove_legacy_auxiliary_exit_state(conn) -> None:
    conn.execute(f'DROP TABLE IF EXISTS "{LEGACY_AUXILIARY_STATE_TABLE}"')
    protective_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        ("protective_orders",),
    ).fetchone()
    if protective_exists is None:
        return
    conn.execute(
        """
        DELETE FROM protective_orders
        WHERE substr(order_ref, -7) IN (?, ?)
        """,
        LEGACY_AUXILIARY_EXIT_SUFFIXES,
    )

def trade_intent_age_anchor_sql() -> str:
    return (
        "CASE WHEN intent_source = 'SIGNAL' "
        "THEN signal_bar_ts ELSE created_at_ts END"
    )




def initialize_execution_db(conn) -> None:
    initialize_trade_db(conn)
    remove_legacy_auxiliary_exit_state(conn)
    require_table_absent(
        conn,
        "take_profit_orders",
        reason="legacy TP storage is unsupported; protective_orders is the only source",
    )
    require_table_absent(
        conn,
        "schema_migrations",
        reason="runtime DB migrations are unsupported; deploy only current schema",
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
    min_anchor_ts = now_ts - max_age_seconds
    age_anchor = trade_intent_age_anchor_sql()
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
          AND {age_anchor} < ?
        """,
        (
            ExecutionStatus.FAILED.value,
            (
                f"{STALE_NEW_INTENT_ERROR_TEXT}; "
                f"one_pipeline_budget_seconds={max_age_seconds}"
            ),
            now_ts,
            now_ts,
            min_anchor_ts,
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
                WHEN status = 'SENDING' OR order_id IS NOT NULL THEN ?
                WHEN order_type = 'LIMIT' THEN ?
                ELSE ?
            END,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = CASE
                WHEN status = 'SENDING' OR order_id IS NOT NULL THEN NULL
                ELSE ?
            END
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
            ExecutionStatus.RECONCILING.value,
            ExecutionStatus.EXPIRED.value,
            ExecutionStatus.FAILED.value,
            (
                f"{STALE_ACTIVE_INTENT_ERROR_TEXT}; "
                f"submitted_orders_move_to_reconciling=1; "
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

        min_anchor_ts = now_ts - max_age_seconds if max_age_seconds > 0 else now_ts
        age_anchor = trade_intent_age_anchor_sql()

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
              AND {age_anchor} >= ?
            ORDER BY {age_anchor} ASC, trade_intent_id ASC
            LIMIT ?
            """,
            (min_anchor_ts, int(limit)),
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


def read_trade_intent_submission_state(
        conn,
        *,
        trade_intent_id: int,
) -> dict | None:
    row = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            status,
            order_id,
            order_action,
            order_quantity,
            error_text
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE trade_intent_id = ?
        LIMIT 1
        """,
        (int(trade_intent_id),),
    ).fetchone()

    if row is None:
        return None

    return {
        "trade_intent_id": int(row[0]),
        "status": str(row[1]).upper(),
        "order_id": None if row[2] is None else int(row[2]),
        "order_action": None if row[3] is None else str(row[3]).upper(),
        "order_quantity": None if row[4] is None else int(row[4]),
        "error_text": None if row[5] is None else str(row[5]),
    }


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
