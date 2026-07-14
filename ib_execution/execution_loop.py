from __future__ import annotations

import asyncio
import time
import traceback

from core.daily_trading_guard import DAILY_GUARD_STATUS_HALTED
from ib_execution.daily_take_profit import (
    DailyTakeProfitEvent,
    evaluate_and_trigger_daily_take_profit_once,
    get_daily_take_profit_monitor_error,
    is_daily_take_profit_monitor_ready,
    mark_daily_take_profit_monitor_health,
    read_current_daily_halt_for_order_service,
    run_daily_take_profit_cleanup_once,
)
from ib_execution.protective_execution_runner import (
    EXECUTION_HEARTBEAT_INTERVAL_SECONDS,
    EXECUTION_LOOP_SLEEP_SECONDS,
    EXECUTION_STATS_RECONCILE_INTERVAL_SECONDS,
    EXECUTION_STATS_RECONCILE_TIMEOUT_SECONDS,
    ExecutionResult,
    ExecutionStatus,
    MAX_NEW_INTENT_AGE_SECONDS,
    NEW_INTENTS_LIMIT,
    PROTECTIVE_RECONCILE_INTERVAL_SECONDS,
    PROTECTIVE_RECONCILE_TOTAL_TIMEOUT_SECONDS,
    PROTECTIVE_WATCHDOG_TIMEOUT_SECONDS,
    SLOT_CLOSE_RECOVERY_INTERVAL_SECONDS,
    SLOT_CLOSE_RECOVERY_TIMEOUT_SECONDS,
    SLOT_LOSS_EXTENSION_TIMEOUT_SECONDS,
    UNCERTAIN_EXECUTION_RECONCILE_INTERVAL_SECONDS,
    UNCERTAIN_EXECUTION_RECONCILE_TIMEOUT_SECONDS,
    cancel_protective_orders_before_position_change,
    execute_trade_intent,
    format_backfilled_execution_stats_message,
    get_trade_db_connection,
    get_trade_intent_execution_timeout_seconds,
    initialize_execution_db,
    is_ib_api_connected,
    log_info,
    log_warning,
    logger,
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
    place_protective_orders_after_entry,
    read_new_trade_intents,
    read_trade_intent_submission_state,
    reconcile_and_notify_protective_orders,
    reconcile_missing_execution_stats_once,
    reconcile_uncertain_executions_and_restore_protection,
    run_protective_order_price_watchdog_once,
    run_slot_close_recovery_once,
    run_slot_loss_extension_once,
    send_deal_status_notification,
    send_executed_deal_notification,
    write_trade_intent_execution_result,
)

async def handle_daily_take_profit_events(
        events: tuple[DailyTakeProfitEvent, ...],
        *,
        deal_telegram_sender=None,
        deal_message_thread_id=None,
        deal_status_message_thread_id=None,
) -> None:
    for event in events:
        if str(event.log_level).upper() == "WARNING":
            log_warning(logger, event.message, to_telegram=True)
        else:
            log_info(logger, event.message, to_telegram=False)

        if event.intent is None or event.result is None:
            continue

        try:
            await send_executed_deal_notification(
                telegram_sender=deal_telegram_sender,
                message_thread_id=deal_message_thread_id,
                intent=event.intent,
                result=event.result,
            )
        except Exception as exc:
            log_warning(
                logger,
                f"daily take-profit deal notification failed: "
                f"{type(exc).__name__}: {exc}",
                to_telegram=False,
            )

        try:
            await send_deal_status_notification(
                telegram_sender=deal_telegram_sender,
                message_thread_id=deal_status_message_thread_id,
                intent=event.intent,
                result=event.result,
            )
        except Exception as exc:
            log_warning(
                logger,
                f"daily take-profit status notification failed: "
                f"{type(exc).__name__}: {exc}",
                to_telegram=False,
            )


async def process_new_trade_intents_once(
        *,
        order_service: OrderService,
        deal_telegram_sender=None,
        deal_message_thread_id=None,
        deal_status_message_thread_id=None,
) -> None:
    if read_current_daily_halt_for_order_service(order_service) is not None:
        return

    try:
        intents = read_new_trade_intents(
            limit=NEW_INTENTS_LIMIT,
            max_age_seconds=MAX_NEW_INTENT_AGE_SECONDS,
        )

    except Exception as exc:
        log_warning(
            logger,
            f"read_new_trade_intents failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            to_telegram=True,
        )
        intents = []

    if intents and not is_ib_api_connected(order_service):
        log_warning(
            logger,
            (
                f"ib_execution deferred {len(intents)} new trade_intent(s) because IB API is disconnected; "
                f"they will be retried before max age or expired by read_new_trade_intents"
            ),
            to_telegram=False,
        )
        intents = []

    for intent in intents:
        if read_current_daily_halt_for_order_service(order_service) is not None:
            break

        risk_increasing = (
            str(intent.action).upper()
            in {"OPEN_POSITION", "REVERSE_POSITION"}
        )
        if risk_increasing:
            try:
                risk_halt, risk_snapshot, risk_first_trigger = (
                    await evaluate_and_trigger_daily_take_profit_once(
                        order_service
                    )
                )
                mark_daily_take_profit_monitor_health(
                    account_id=order_service.account_id,
                    ready=True,
                    error_text=None,
                )
            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}"
                mark_daily_take_profit_monitor_health(
                    account_id=order_service.account_id,
                    ready=False,
                    error_text=error_text,
                )
                log_warning(
                    logger,
                    "daily take-profit pre-trade evaluation failed; "
                    f"trade_intent={intent.trade_intent_id} remains blocked: "
                    f"{error_text}",
                    to_telegram=False,
                )
                continue

            if risk_halt is not None:
                if (
                        risk_first_trigger
                        and risk_snapshot is not None
                ):
                    log_warning(
                        logger,
                        (
                            "DAILY TAKE PROFIT reached immediately before "
                            "risk-increasing execution; trading halt persisted: "
                            f"account={order_service.account_id}, "
                            f"moscow_day={risk_snapshot.moscow_day}, "
                            f"realized={risk_snapshot.realized_pnl_usd:+.2f}, "
                            f"unrealized={risk_snapshot.unrealized_pnl_usd:+.2f}, "
                            f"total={risk_snapshot.total_pnl_usd:+.2f}"
                        ),
                        to_telegram=True,
                    )
                break

            if not is_daily_take_profit_monitor_ready(order_service):
                continue

        if not is_ib_api_connected(order_service):
            log_warning(
                logger,
                (
                    f"ib_execution stops processing new intents because IB API disconnected before "
                    f"trade_intent={intent.trade_intent_id}"
                ),
                to_telegram=False,
            )
            break

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

            await cancel_protective_orders_before_position_change(
                conn=conn,
                order_service=order_service,
                intent=intent,
            )
            conn.commit()

            result = await asyncio.wait_for(
                execute_trade_intent(
                    order_service=order_service,
                    intent=intent,
                    order_submitted_callback=on_order_submitted,
                ),
                timeout=float(get_trade_intent_execution_timeout_seconds(intent)),
            )

            write_trade_intent_execution_result(conn, result=result)
            conn.commit()

            await place_protective_orders_after_entry(
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
            submitted_order_id = getattr(exc, "order_id", None)
            submitted_order_action = getattr(exc, "order_action", None)
            submitted_order_quantity = getattr(exc, "order_quantity", None)

            try:
                submission_state = read_trade_intent_submission_state(
                    conn,
                    trade_intent_id=intent.trade_intent_id,
                ) or {}

                submitted_order_id = (
                    submission_state.get("order_id")
                    or submitted_order_id
                )
                submitted_order_action = (
                    submission_state.get("order_action")
                    or submitted_order_action
                )
                submitted_order_quantity = (
                    submission_state.get("order_quantity")
                    or submitted_order_quantity
                )

                result_status = (
                    ExecutionStatus.RECONCILING
                    if submitted_order_id is not None
                    else ExecutionStatus.FAILED
                )
                failure_result = ExecutionResult(
                    trade_intent_id=intent.trade_intent_id,
                    order_id=submitted_order_id,
                    order_action=submitted_order_action,
                    order_quantity=submitted_order_quantity,
                    status=result_status,
                    avg_fill_price=None,
                    total_commission=None,
                    realized_pnl=None,
                    error_text=error_text,
                )
                write_trade_intent_execution_result(
                    conn,
                    result=failure_result,
                )
                conn.commit()

                await send_deal_status_notification(
                    telegram_sender=deal_telegram_sender,
                    message_thread_id=deal_status_message_thread_id,
                    intent=intent,
                    result=failure_result,
                )
            finally:
                state_text = (
                    "requires broker reconciliation"
                    if submitted_order_id is not None
                    else "failed before broker submission was recorded"
                )
                log_warning(
                    logger,
                    f"ib_execution trade_intent={intent.trade_intent_id} {state_text}: "
                    f"{error_text}\n{traceback.format_exc()}",
                    to_telegram=True,
                )

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
    next_slot_close_recovery_ts = 0
    next_uncertain_reconcile_ts = 0
    next_protective_reconcile_ts = 0
    next_execution_stats_reconcile_ts = 0
    next_daily_cleanup_error_log_ts = 0

    while True:
        daily_halt = read_current_daily_halt_for_order_service(order_service)
        if (
                daily_halt is not None
                and daily_halt.state.status != DAILY_GUARD_STATUS_HALTED
        ):
            try:
                cleanup_result = await asyncio.wait_for(
                    run_daily_take_profit_cleanup_once(
                        order_service,
                        halt=daily_halt,
                    ),
                    timeout=90.0,
                )
                await handle_daily_take_profit_events(
                    cleanup_result.events,
                    deal_telegram_sender=deal_telegram_sender,
                    deal_message_thread_id=deal_message_thread_id,
                    deal_status_message_thread_id=deal_status_message_thread_id,
                )
            except Exception as exc:
                now_error_ts = int(time.time())
                if now_error_ts >= next_daily_cleanup_error_log_ts:
                    log_warning(
                        logger,
                        f"daily take-profit cleanup failed: "
                        f"{type(exc).__name__}: {exc}\n"
                        f"{traceback.format_exc()}",
                        to_telegram=True,
                    )
                    next_daily_cleanup_error_log_ts = now_error_ts + 60

        if read_current_daily_halt_for_order_service(order_service) is None:
            await process_new_trade_intents_once(
                order_service=order_service,
                deal_telegram_sender=deal_telegram_sender,
                deal_message_thread_id=deal_message_thread_id,
                deal_status_message_thread_id=deal_status_message_thread_id,
            )

        now_ts = int(time.time())
        if now_ts >= next_uncertain_reconcile_ts:
            next_uncertain_reconcile_ts = now_ts + UNCERTAIN_EXECUTION_RECONCILE_INTERVAL_SECONDS
            try:
                await asyncio.wait_for(
                    reconcile_uncertain_executions_and_restore_protection(
                        order_service=order_service,
                    ),
                    timeout=float(UNCERTAIN_EXECUTION_RECONCILE_TIMEOUT_SECONDS),
                )
            except Exception as exc:
                log_warning(
                    logger,
                    f"uncertain execution reconciliation failed: "
                    f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                    to_telegram=True,
                )

        now_ts = int(time.time())
        if now_ts >= next_protective_reconcile_ts:
            next_protective_reconcile_ts = now_ts + PROTECTIVE_RECONCILE_INTERVAL_SECONDS
            try:
                await asyncio.wait_for(
                    reconcile_and_notify_protective_orders(
                        order_service=order_service,
                        deal_telegram_sender=deal_telegram_sender,
                        deal_message_thread_id=deal_message_thread_id,
                    ),
                    timeout=float(PROTECTIVE_RECONCILE_TOTAL_TIMEOUT_SECONDS),
                )

            except Exception as exc:
                log_warning(
                    logger,
                    f"protective-order reconciliation failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                    to_telegram=True,
                )

        try:
            await asyncio.wait_for(
                run_protective_order_price_watchdog_once(
                    order_service=order_service,
                ),
                timeout=float(PROTECTIVE_WATCHDOG_TIMEOUT_SECONDS),
            )

        except Exception as exc:
            log_warning(
                logger,
                f"protective-order watchdog failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                to_telegram=True,
            )

        try:
            slot_loss_extension_events = await asyncio.wait_for(
                run_slot_loss_extension_once(
                    order_service=order_service,
                ),
                timeout=float(SLOT_LOSS_EXTENSION_TIMEOUT_SECONDS),
            )

            for extension_event in slot_loss_extension_events:
                if str(extension_event.log_level).upper() == "WARNING":
                    log_warning(
                        logger,
                        extension_event.message,
                        to_telegram=True,
                    )
                else:
                    log_info(
                        logger,
                        extension_event.message,
                        to_telegram=False,
                    )

                if extension_event.intent is None or extension_event.result is None:
                    continue

                try:
                    await send_executed_deal_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_message_thread_id,
                        intent=extension_event.intent,
                        result=extension_event.result,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"slot-loss extension deal notification failed "
                            f"trade_intent={extension_event.intent.trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

                try:
                    await send_deal_status_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_status_message_thread_id,
                        intent=extension_event.intent,
                        result=extension_event.result,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"slot-loss extension status notification failed "
                            f"trade_intent={extension_event.intent.trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

        except Exception as exc:
            log_warning(
                logger,
                f"slot-loss extension failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                to_telegram=True,
            )

        now_ts = int(time.time())
        if now_ts >= next_slot_close_recovery_ts:
            next_slot_close_recovery_ts = now_ts + SLOT_CLOSE_RECOVERY_INTERVAL_SECONDS

            try:
                recovery_events = await asyncio.wait_for(
                    run_slot_close_recovery_once(
                        order_service=order_service,
                    ),
                    timeout=float(SLOT_CLOSE_RECOVERY_TIMEOUT_SECONDS),
                )

                for recovery_event in recovery_events:
                    if str(recovery_event.log_level).upper() == "WARNING":
                        log_warning(
                            logger,
                            recovery_event.message,
                            to_telegram=True,
                        )
                    else:
                        log_info(
                            logger,
                            recovery_event.message,
                            to_telegram=False,
                        )

                    if recovery_event.intent is None or recovery_event.result is None:
                        continue

                    try:
                        await send_executed_deal_notification(
                            telegram_sender=deal_telegram_sender,
                            message_thread_id=deal_message_thread_id,
                            intent=recovery_event.intent,
                            result=recovery_event.result,
                        )
                    except Exception as notification_exc:
                        log_warning(
                            logger,
                            (
                                f"slot-close recovery deal notification failed "
                                f"trade_intent={recovery_event.intent.trade_intent_id}: "
                                f"{type(notification_exc).__name__}: {notification_exc}"
                            ),
                            to_telegram=True,
                        )

                    try:
                        await send_deal_status_notification(
                            telegram_sender=deal_telegram_sender,
                            message_thread_id=deal_status_message_thread_id,
                            intent=recovery_event.intent,
                            result=recovery_event.result,
                        )
                    except Exception as notification_exc:
                        log_warning(
                            logger,
                            (
                                f"slot-close recovery status notification failed "
                                f"trade_intent={recovery_event.intent.trade_intent_id}: "
                                f"{type(notification_exc).__name__}: {notification_exc}"
                            ),
                            to_telegram=True,
                        )

            except Exception as exc:
                log_warning(
                    logger,
                    f"slot-close recovery failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                    to_telegram=True,
                )

        now_ts = int(time.time())
        if now_ts >= next_execution_stats_reconcile_ts:
            next_execution_stats_reconcile_ts = now_ts + EXECUTION_STATS_RECONCILE_INTERVAL_SECONDS
            try:
                execution_stats_events = await asyncio.wait_for(
                    reconcile_missing_execution_stats_once(
                        order_service=order_service,
                    ),
                    timeout=float(EXECUTION_STATS_RECONCILE_TIMEOUT_SECONDS),
                )

                for execution_stats_event in execution_stats_events:
                    if str(execution_stats_event.get("event", "")).upper() != "BACKFILLED":
                        continue

                    log_info(
                        logger,
                        (
                            f"{execution_stats_event['instrument_code']}: execution stats backfilled: "
                            f"trade_intent_id={execution_stats_event['trade_intent_id']}, "
                            f"order_id={execution_stats_event['order_id']}, "
                            f"avg_fill={execution_stats_event.get('avg_fill_price')}, "
                            f"commission={execution_stats_event.get('total_commission')}, "
                            f"realized_pnl={execution_stats_event.get('realized_pnl')}"
                        ),
                        to_telegram=False,
                    )

                    if deal_telegram_sender is None:
                        continue

                    try:
                        await deal_telegram_sender.send_text(
                            format_backfilled_execution_stats_message(execution_stats_event),
                            message_thread_id=deal_message_thread_id,
                        )
                    except Exception as notification_exc:
                        log_warning(
                            logger,
                            (
                                f"execution-stats backfill notification failed "
                                f"trade_intent={execution_stats_event['trade_intent_id']}: "
                                f"{type(notification_exc).__name__}: {notification_exc}"
                            ),
                            to_telegram=True,
                        )

            except Exception as exc:
                log_warning(
                    logger,
                    f"execution-stats reconciliation failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                    to_telegram=True,
                )

        if read_current_daily_halt_for_order_service(order_service) is None:
            await process_new_trade_intents_once(
                order_service=order_service,
                deal_telegram_sender=deal_telegram_sender,
                deal_message_thread_id=deal_message_thread_id,
                deal_status_message_thread_id=deal_status_message_thread_id,
            )

        now_ts = int(time.time())
        if now_ts >= next_heartbeat_ts:
            heartbeat_halt = read_current_daily_halt_for_order_service(
                order_service
            )
            daily_status = (
                "RUNNING"
                if heartbeat_halt is None
                else (
                    f"{heartbeat_halt.state.status}/"
                    f"{heartbeat_halt.state.moscow_day}"
                )
            )
            daily_monitor_ready = is_daily_take_profit_monitor_ready(
                order_service
            )
            daily_monitor_error = get_daily_take_profit_monitor_error(
                order_service
            )
            daily_monitor_status = (
                "READY"
                if daily_monitor_ready
                else f"BLOCKED:{daily_monitor_error or 'not_initialized'}"
            )
            log_info(
                logger,
                (
                    "ib_execution heartbeat: alive, "
                    f"new_intents_limit={NEW_INTENTS_LIMIT}, "
                    f"max_new_intent_age_seconds={MAX_NEW_INTENT_AGE_SECONDS}, "
                    f"daily_take_profit_status={daily_status}, "
                    f"daily_pnl_monitor={daily_monitor_status}"
                ),
                to_telegram=False,
            )
            next_heartbeat_ts = now_ts + EXECUTION_HEARTBEAT_INTERVAL_SECONDS

        await asyncio.sleep(EXECUTION_LOOP_SLEEP_SECONDS)
