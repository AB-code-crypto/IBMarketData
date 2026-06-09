import asyncio
import sqlite3
import time
import traceback
from decimal import Decimal, ROUND_FLOOR

from core.logger import get_logger, log_info, log_warning, setup_logging
from contracts import Instrument
from core.state_db import STATE_DB_PATH
from ib_execution.contract_resolver import build_execution_contract
from ib_execution.execution_logic import execute_trade_intent
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
    read_new_trade_intents,
    write_trade_intent_execution_result,
    record_take_profit_order,
    read_active_take_profit_orders,
    mark_take_profit_order_status,
)
from ib_execution.order_service import OrderService
from ib_execution.take_profit_reconciliation import reconcile_take_profit_orders_once
from ib_signal.signal_event_store import SIGNAL_EVENTS_TABLE_NAME
from ib_signal.signal_plot import build_plot_path

setup_logging()
logger = get_logger(__name__)

EXECUTION_LOOP_SLEEP_SECONDS = 1
NEW_INTENTS_LIMIT = 20
MAX_NEW_INTENT_AGE_SECONDS = 10
EXECUTION_HEARTBEAT_INTERVAL_SECONDS = 60

TAKE_PROFIT_ORDER_REF_SUFFIX = "_TP"


def get_take_profit_points(instrument_code: str) -> float:
    instrument_row = Instrument.get(str(instrument_code))

    if instrument_row is None:
        return 0.0

    return float(instrument_row.get("take_profit_points", 0.0) or 0.0)


def normalize_price_to_tick_floor(*, price: Decimal, price_tick: Decimal) -> float:
    if price_tick <= Decimal("0"):
        raise ValueError(f"price_tick must be positive: {price_tick!r}")

    steps = (price / price_tick).to_integral_value(rounding=ROUND_FLOOR)
    return float(steps * price_tick)


def calculate_take_profit_price(*, instrument_code: str, position_side: str, avg_fill_price: float) -> float | None:
    instrument_row = Instrument.get(str(instrument_code))

    if instrument_row is None:
        return None

    take_profit_points = Decimal(str(instrument_row.get("take_profit_points", 0.0) or 0.0))

    if take_profit_points <= Decimal("0"):
        return None

    avg_fill_price_decimal = Decimal(str(avg_fill_price))
    position_side = str(position_side).upper()

    if position_side == "LONG":
        raw_take_profit_price = avg_fill_price_decimal + take_profit_points

    elif position_side == "SHORT":
        raw_take_profit_price = avg_fill_price_decimal - take_profit_points

    else:
        return None

    price_tick = Decimal(str(instrument_row.get("price_tick", 0.0) or 0.0))
    return normalize_price_to_tick_floor(
        price=raw_take_profit_price,
        price_tick=price_tick,
    )


def get_take_profit_order_action(position_side: str) -> str | None:
    position_side = str(position_side).upper()

    if position_side == "LONG":
        return "SELL"

    if position_side == "SHORT":
        return "BUY"

    return None


def get_take_profit_order_quantity(intent) -> int | None:
    target_qty = float(intent.target_qty)

    if target_qty <= 0.0:
        return None

    if target_qty != int(target_qty):
        return None

    return int(target_qty)


def build_take_profit_order_ref(intent) -> str:
    return f"{intent.order_ref}{TAKE_PROFIT_ORDER_REF_SUFFIX}"


async def cancel_take_profit_orders_before_position_change(*, conn, order_service: OrderService, intent) -> None:
    action = str(intent.action).upper()

    # Перед OPEN_POSITION тоже чистим старые ACTIVE TP из прошлых запусков/сессий.
    if action not in {"OPEN_POSITION", "CLOSE_POSITION", "REVERSE_POSITION"}:
        return

    instrument_code = str(intent.instrument_code)
    active_orders = read_active_take_profit_orders(
        conn,
        instrument_code=instrument_code,
    )

    if not active_orders:
        return

    for active_order in active_orders:
        order_id = int(active_order["order_id"])

        try:
            await order_service.cancel_order_id(order_id)
            mark_take_profit_order_status(
                conn,
                order_id=order_id,
                status="CANCELLED",
                error_text=f"cancelled before {action}",
            )
            log_info(
                logger,
                f"{instrument_code}: take-profit order cancelled before {action}: order_id={order_id}",
                to_telegram=False,
            )

        except Exception as exc:
            mark_take_profit_order_status(
                conn,
                order_id=order_id,
                status="ACTIVE",
                error_text=f"cancel failed before {action}: {type(exc).__name__}: {exc}",
            )
            log_warning(
                logger,
                (
                    f"{instrument_code}: take-profit cancel failed before {action}: "
                    f"order_id={order_id}, {type(exc).__name__}: {exc}"
                ),
                to_telegram=False,
            )


async def place_take_profit_after_entry(*, conn, order_service: OrderService, intent, result) -> None:
    if result.status != ExecutionStatus.EXECUTED:
        return

    action = str(intent.action).upper()

    if action not in {"OPEN_POSITION", "REVERSE_POSITION"}:
        return

    if result.avg_fill_price is None:
        return

    instrument_code = str(intent.instrument_code)

    if get_take_profit_points(instrument_code) <= 0.0:
        return

    target_side = str(intent.target_side).upper()
    order_action = get_take_profit_order_action(target_side)

    if order_action is None:
        return

    quantity = get_take_profit_order_quantity(intent)

    if quantity is None:
        log_warning(
            logger,
            (
                f"{instrument_code}: take-profit skipped: unsupported target_qty={intent.target_qty!r} "
                f"for trade_intent={intent.trade_intent_id}"
            ),
            to_telegram=False,
        )
        return

    try:
        take_profit_price = calculate_take_profit_price(
            instrument_code=instrument_code,
            position_side=target_side,
            avg_fill_price=float(result.avg_fill_price),
        )

        if take_profit_price is None:
            return

        contract = build_execution_contract(instrument_code=instrument_code)
        order_ref = build_take_profit_order_ref(intent)

        if order_action == "BUY":
            placement = await order_service.buy_limit(
                contract=contract,
                quantity=quantity,
                limit_price=float(take_profit_price),
                order_ref=order_ref,
                ttl_seconds=None,
                wait="none",
            )

        else:
            placement = await order_service.sell_limit(
                contract=contract,
                quantity=quantity,
                limit_price=float(take_profit_price),
                order_ref=order_ref,
                ttl_seconds=None,
                wait="none",
            )

        order_id = int(placement.receipt.order_id)
        record_take_profit_order(
            conn,
            instrument_code=instrument_code,
            parent_trade_intent_id=int(intent.trade_intent_id),
            order_ref=order_ref,
            order_id=order_id,
            order_action=order_action,
            order_quantity=quantity,
            take_profit_price=float(take_profit_price),
        )

        log_info(
            logger,
            (
                f"{instrument_code}: take-profit order submitted: "
                f"parent_trade_intent={intent.trade_intent_id}, "
                f"order_id={order_id}, action={order_action}, qty={quantity}, "
                f"price={take_profit_price}, order_ref={order_ref}"
            ),
            to_telegram=False,
        )

    except Exception as exc:
        log_warning(
            logger,
            (
                f"{instrument_code}: take-profit order submit failed: "
                f"trade_intent={intent.trade_intent_id}, "
                f"{type(exc).__name__}: {exc}"
            ),
            to_telegram=False,
        )




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
    # CLOSE_POSITION может иметь служебный source_signal_id=-1/-2/-3.
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
        f"commission: {result.total_commission}\n"
        f"realized_pnl: {result.realized_pnl}"
    )


async def send_executed_deal_notification(
        *,
        telegram_sender,
        message_thread_id,
        intent,
        result,
) -> None:
    """Что делает: отправляет deal-уведомление только после EXECUTED.
    Зачем нужна: сигнал/лимитный ACCEPTED не являются совершённой сделкой."""
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


async def run_execution_loop(
        order_service: OrderService,
        *,
        deal_telegram_sender=None,
        deal_message_thread_id=None,
        deal_status_message_thread_id=None,
) -> None:
    log_info(
        logger,
        (
            "ib_execution loop started: "
            "order_type=FROM_TRADE_INTENT, "
            f"max_new_intent_age_seconds={MAX_NEW_INTENT_AGE_SECONDS}"
        ),
        to_telegram=False,
    )

    next_heartbeat_ts = int(time.time()) + EXECUTION_HEARTBEAT_INTERVAL_SECONDS

    while True:
        try:
            reconciled_take_profits = await reconcile_take_profit_orders_once(
                order_service=order_service,
            )

            for reconciled_take_profit in reconciled_take_profits:
                log_info(
                    logger,
                    (
                        f"{reconciled_take_profit['instrument_code']}: "
                        f"take-profit {reconciled_take_profit['event'].lower()}: "
                        f"order_id={reconciled_take_profit['order_id']}, "
                        f"parent_trade_intent_id={reconciled_take_profit['parent_trade_intent_id']}, "
                        f"synthetic_trade_intent_id={reconciled_take_profit['synthetic_trade_intent_id']}, "
                        f"filled_qty={reconciled_take_profit['filled_qty']}, "
                        f"avg_fill={reconciled_take_profit['avg_fill_price']}, "
                        f"realized_pnl={reconciled_take_profit['realized_pnl']}"
                    ),
                    to_telegram=False,
                )

                if str(reconciled_take_profit.get("event", "")).upper() != "FILLED":
                    continue

                synthetic_trade_intent_id = reconciled_take_profit.get("synthetic_trade_intent_id")

                if synthetic_trade_intent_id is None:
                    continue

                try:
                    synthetic_intent, synthetic_result = read_executed_trade_intent_and_result_for_notification(
                        trade_intent_id=int(synthetic_trade_intent_id),
                    )

                    if synthetic_intent is None or synthetic_result is None:
                        continue

                    await send_executed_deal_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_message_thread_id,
                        intent=synthetic_intent,
                        result=synthetic_result,
                    )

                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"take-profit deal notification failed "
                            f"synthetic_trade_intent_id={synthetic_trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

        except Exception as exc:
            log_warning(
                logger,
                f"take-profit reconciliation failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                to_telegram=True,
            )

        intents = read_new_trade_intents(
            limit=NEW_INTENTS_LIMIT,
            max_age_seconds=MAX_NEW_INTENT_AGE_SECONDS,
        )

        for intent in intents:
            conn = get_trade_db_connection()
            try:
                initialize_execution_db(conn)

                mark_trade_intent_sending(conn, trade_intent_id=intent.trade_intent_id)
                conn.commit()

                async def on_order_submitted(order_id: int, order_action: str, order_quantity: int) -> None:
                    mark_trade_intent_order_submitted(
                        conn,
                        trade_intent_id=intent.trade_intent_id,
                        order_id=order_id,
                        order_action=order_action,
                        order_quantity=order_quantity,
                    )
                    conn.commit()

                await cancel_take_profit_orders_before_position_change(
                    conn=conn,
                    order_service=order_service,
                    intent=intent,
                )
                conn.commit()

                result = await execute_trade_intent(
                    order_service=order_service,
                    intent=intent,
                    order_submitted_callback=on_order_submitted,
                )

                write_trade_intent_execution_result(conn, result=result)
                conn.commit()

                await place_take_profit_after_entry(
                    conn=conn,
                    order_service=order_service,
                    intent=intent,
                    result=result,
                )
                conn.commit()

                log_info(
                    logger,
                    (
                        f"{intent.instrument_code}: executed trade_intent={intent.trade_intent_id}, "
                        f"action={intent.action}, order_type={intent.order_type}, "
                        f"order_id={result.order_id}, order_action={result.order_action}, "
                        f"qty={result.order_quantity}, avg_fill={result.avg_fill_price}, "
                        f"realized_pnl={result.realized_pnl}, commission={result.total_commission}"
                    ),
                    to_telegram=False,
                )

                try:
                    await send_executed_deal_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_message_thread_id,
                        intent=intent,
                        result=result,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"deal notification failed "
                            f"trade_intent={intent.trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

                try:
                    await send_deal_status_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_status_message_thread_id,
                        intent=intent,
                        result=result,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"deal status notification failed "
                            f"trade_intent={intent.trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}"

                try:
                    failure_result = ExecutionResult(
                        trade_intent_id=intent.trade_intent_id,
                        order_id=getattr(exc, "order_id", None),
                        order_action=None,
                        order_quantity=None,
                        status=ExecutionStatus.FAILED,
                        avg_fill_price=None,
                        total_commission=None,
                        realized_pnl=None,
                        error_text=error_text,
                    )
                    write_trade_intent_execution_result(conn, result=failure_result)
                    conn.commit()

                    await send_deal_status_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_status_message_thread_id,
                        intent=intent,
                        result=failure_result,
                    )
                finally:
                    log_warning(
                        logger,
                        f"ib_execution failed trade_intent={intent.trade_intent_id}: {error_text}\\n"
                        f"{traceback.format_exc()}",
                        to_telegram=True,
                    )

            finally:
                conn.close()

        now_ts = int(time.time())
        if now_ts >= next_heartbeat_ts:
            log_info(
                logger,
                (
                    "ib_execution heartbeat: alive, "
                    f"new_intents_limit={NEW_INTENTS_LIMIT}, "
                    f"max_new_intent_age_seconds={MAX_NEW_INTENT_AGE_SECONDS}"
                ),
                to_telegram=False,
            )
            next_heartbeat_ts = now_ts + EXECUTION_HEARTBEAT_INTERVAL_SECONDS

        await asyncio.sleep(EXECUTION_LOOP_SLEEP_SECONDS)
