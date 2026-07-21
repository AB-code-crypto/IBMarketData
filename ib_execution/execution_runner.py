from __future__ import annotations

import sqlite3

from core.state_db import STATE_DB_PATH
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_store import get_trade_db_connection, initialize_execution_db
from ib_signal.signal_event_store import SIGNAL_EVENTS_TABLE_NAME
from ib_signal.signal_plot import build_plot_path

def read_signal_event_snapshot(*, source_signal_id: int) -> dict | None:
    """Что делает: читает signal_event для execution-уведомления.
    Зачем нужна: сделка исполняется по trade_intent, но картинка и signal_time лежат в signal_events."""
    try:
        conn = sqlite3.connect(str(STATE_DB_PATH))
        conn.row_factory = sqlite3.Row

        try:
            row = conn.execute(
                f"""
                SELECT
                    signal_id,
                    instrument_code,
                    signal_bar_ts,
                    signal_time_ct,
                    direction,
                    entry_price,
                    potential_end_delta_points,
                    potential_max_profit_points,
                    potential_max_drawdown_points,
                    potential_used,
                    best_pearson,
                    candidate_score_best
                FROM {SIGNAL_EVENTS_TABLE_NAME}
                WHERE signal_id = ?
                LIMIT 1
                """,
                (int(source_signal_id),),
            ).fetchone()

            if row is None:
                return None

            return dict(row)

        finally:
            conn.close()

    except Exception:
        return None


def read_latest_open_signal_event_for_close_intent(intent) -> dict | None:
    # Служебное CLOSE_POSITION имеет отрицательный source_signal_id.
    # Для PNG на закрытии берём signal_event исходного OPEN_POSITION.
    if str(intent.action).upper() != "CLOSE_POSITION":
        return None

    try:
        conn = get_trade_db_connection()
        try:
            row = conn.execute(
                """
                SELECT source_signal_id
                FROM trade_intents
                WHERE instrument_code = ?
                  AND action = 'OPEN_POSITION'
                  AND status = 'EXECUTED'
                  AND trade_intent_id < ?
                ORDER BY trade_intent_id DESC
                LIMIT 1
                """,
                (
                    str(intent.instrument_code),
                    int(intent.trade_intent_id),
                ),
            ).fetchone()
        finally:
            conn.close()

    except Exception:
        return None

    if row is None or row[0] is None:
        return None

    source_signal_id = int(row[0])

    if source_signal_id <= 0:
        return None

    return read_signal_event_snapshot(source_signal_id=source_signal_id)


def resolve_deal_signal_event(intent) -> dict | None:
    if str(intent.action).upper() == "CLOSE_POSITION":
        open_signal_event = read_latest_open_signal_event_for_close_intent(intent)

        if open_signal_event is not None:
            return open_signal_event

    return read_signal_event_snapshot(
        source_signal_id=int(intent.source_signal_id),
    )


def build_executed_deal_title(intent) -> str:
    action = str(intent.action).upper()

    if action == "OPEN_POSITION":
        return "✅ Сделка открыта"

    if action == "CLOSE_POSITION":
        return "✅ Сделка закрыта"

    if action == "REVERSE_POSITION":
        return "✅ Реверс исполнен"

    return "✅ Сделка исполнена"


def resolve_deal_plot_path(intent, signal_event: dict | None):
    # PNG отправляем при открытии и при реверсе сделки.
    # CLOSE_POSITION и прочие post-trade события идут текстом.
    action = str(intent.action).upper()

    if action not in {"OPEN_POSITION", "REVERSE_POSITION"}:
        return None

    if signal_event is None:
        return None

    return build_plot_path(
        instrument_code=str(signal_event["instrument_code"]),
        signal_bar_time_ct=str(signal_event["signal_time_ct"]),
    )


def format_optional_execution_money(value, *, signed: bool = False) -> str:
    if value is None:
        return "pending"

    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    if signed:
        return f"{number:+.2f}"

    return f"{number:.2f}"


def build_executed_deal_caption(*, intent, result, signal_event: dict | None) -> str:
    signal_time_ct = signal_event.get("signal_time_ct") if signal_event else "n/a"
    signal_direction = signal_event.get("direction") if signal_event else "n/a"
    entry_price = signal_event.get("entry_price") if signal_event else None
    potential_end = signal_event.get("potential_end_delta_points") if signal_event else None

    entry_text = f"{float(entry_price):.2f}" if entry_price is not None else "n/a"
    potential_text = f"{float(potential_end):+.2f} pt" if potential_end is not None else "n/a"

    avg_fill_text = (
        f"{float(result.avg_fill_price):.2f}"
        if result.avg_fill_price is not None
        else "n/a"
    )
    commission_text = format_optional_execution_money(result.total_commission)
    realized_pnl_text = format_optional_execution_money(result.realized_pnl, signed=True)

    return (
        f"{build_executed_deal_title(intent)}\n"
        f"instrument: {intent.instrument_code}\n"
        f"trade_intent_id: {intent.trade_intent_id}\n"
        f"source_signal_id: {intent.source_signal_id}\n"
        f"signal_time_ct: {signal_time_ct}\n"
        f"signal_direction: {signal_direction}\n"
        f"entry_price: {entry_text}\n"
        f"potential_end: {potential_text}\n"
        f"action: {intent.action}\n"
        f"target: {intent.target_side}/{intent.target_qty:g}\n"
        f"order_type: {intent.order_type}\n"
        f"order_id: {result.order_id}\n"
        f"order_action: {result.order_action}\n"
        f"order_qty: {result.order_quantity}\n"
        f"avg_fill: {avg_fill_text}\n"
        f"commission: {commission_text}\n"
        f"realized_pnl: {realized_pnl_text}"
    )


async def send_executed_deal_notification(
        *,
        telegram_sender,
        message_thread_id,
        intent,
        result,
) -> None:
    """Что делает: отправляет deal-уведомление только после EXECUTED.
    Зачем нужна: принятый брокером ордер ещё не является совершённой сделкой."""
    if telegram_sender is None:
        return

    if result.status != ExecutionStatus.EXECUTED:
        return

    signal_event = resolve_deal_signal_event(intent)
    caption = build_executed_deal_caption(
        intent=intent,
        result=result,
        signal_event=signal_event,
    )

    plot_path = resolve_deal_plot_path(intent, signal_event)

    ok = False

    if plot_path is not None and plot_path.is_file():
        ok = await telegram_sender.send_photo(
            plot_path,
            caption=caption,
            message_thread_id=message_thread_id,
        )

    if ok:
        return

    if plot_path is not None and not plot_path.is_file():
        caption += f"\nPNG not found: {plot_path}"

    await telegram_sender.send_text(
        caption,
        message_thread_id=message_thread_id,
    )



def build_execution_status_caption(*, intent, result, signal_event: dict | None) -> str:
    """Что делает: собирает техническое сообщение о неисполненном/завершённом без fill ордере.
    Зачем нужна: deal-status thread должен сразу показывать EXPIRED/CANCELLED/FAILED."""
    signal_time_ct = signal_event.get("signal_time_ct") if signal_event else "n/a"
    signal_direction = signal_event.get("direction") if signal_event else "n/a"
    entry_price = signal_event.get("entry_price") if signal_event else None
    potential_end = signal_event.get("potential_end_delta_points") if signal_event else None

    entry_text = f"{float(entry_price):.2f}" if entry_price is not None else "n/a"
    potential_text = f"{float(potential_end):+.2f} pt" if potential_end is not None else "n/a"

    return (
        "⚠️ Ордер завершён без открытия сделки\n"
        f"status: {result.status.value}\n"
        f"instrument: {intent.instrument_code}\n"
        f"trade_intent_id: {intent.trade_intent_id}\n"
        f"source_signal_id: {intent.source_signal_id}\n"
        f"signal_time_ct: {signal_time_ct}\n"
        f"signal_direction: {signal_direction}\n"
        f"entry_price: {entry_text}\n"
        f"potential_end: {potential_text}\n"
        f"action: {intent.action}\n"
        f"target: {intent.target_side}/{intent.target_qty:g}\n"
        f"order_type: {intent.order_type}\n"
        f"limit_price: {intent.limit_price}\n"
        f"ttl_seconds: {intent.ttl_seconds}\n"
        f"order_id: {result.order_id}\n"
        f"order_action: {result.order_action}\n"
        f"order_qty: {result.order_quantity}\n"
        f"error_text: {result.error_text}"
    )


async def send_deal_status_notification(
        *,
        telegram_sender,
        message_thread_id,
        intent,
        result,
) -> None:
    """Что делает: отправляет технический статус неисполненного ордера в deal-status thread.
    Зачем нужна: EXPIRED/CANCELLED/FAILED не должны теряться в консоли."""
    if telegram_sender is None:
        return

    terminal_problem_statuses = {
        ExecutionStatus.EXPIRED,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.FAILED,
    }

    if result.status not in terminal_problem_statuses:
        return

    signal_event = read_signal_event_snapshot(
        source_signal_id=intent.source_signal_id,
    )
    caption = build_execution_status_caption(
        intent=intent,
        result=result,
        signal_event=signal_event,
    )

    await telegram_sender.send_text(
        caption,
        message_thread_id=message_thread_id,
    )


def read_executed_trade_intent_and_result_for_notification(*, trade_intent_id: int):
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)

        row = conn.execute(
            """
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
                created_at_ts,

                order_id,
                order_action,
                order_quantity,
                avg_fill_price,
                total_commission,
                realized_pnl,
                error_text
            FROM trade_intents
            WHERE trade_intent_id = ?
            LIMIT 1
            """,
            (int(trade_intent_id),),
        ).fetchone()

        if row is None:
            return None, None

        status_value = str(row[13]).upper()

        if status_value != ExecutionStatus.EXECUTED.value:
            return None, None

        intent = TradeIntent(
            trade_intent_id=int(row[0]),
            source_signal_id=int(row[1]),
            instrument_code=str(row[2]),
            order_ref=str(row[3] or ""),
            action=str(row[4]),
            target_side=str(row[5]),
            target_qty=float(row[6]),
            position_before_side=str(row[7]),
            position_before_qty=float(row[8]),
            order_type=str(row[9]).upper(),
            limit_price=None if row[10] is None else float(row[10]),
            limit_offset_points=None if row[11] is None else float(row[11]),
            ttl_seconds=None if row[12] is None else int(row[12]),
            status=status_value,
            created_at_ts=int(row[14]),
        )

        result = ExecutionResult(
            trade_intent_id=int(row[0]),
            order_id=None if row[15] is None else int(row[15]),
            order_action=None if row[16] is None else str(row[16]),
            order_quantity=None if row[17] is None else int(row[17]),
            status=ExecutionStatus.EXECUTED,
            avg_fill_price=None if row[18] is None else float(row[18]),
            total_commission=None if row[19] is None else float(row[19]),
            realized_pnl=None if row[20] is None else float(row[20]),
            error_text=None if row[21] is None else str(row[21]),
        )

        return intent, result

    finally:
        conn.close()


__all__ = [
    "read_signal_event_snapshot",
    "read_latest_open_signal_event_for_close_intent",
    "resolve_deal_signal_event",
    "build_executed_deal_title",
    "resolve_deal_plot_path",
    "format_optional_execution_money",
    "build_executed_deal_caption",
    "send_executed_deal_notification",
    "build_execution_status_caption",
    "send_deal_status_notification",
    "read_executed_trade_intent_and_result_for_notification",
]
