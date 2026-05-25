import asyncio
import time
import traceback

from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_signal.signal_plot import build_plot_path
from ib_trader.trade_store import process_signal_events_once

setup_logging()
logger = get_logger(__name__)

TRADER_LOOP_SLEEP_SECONDS = 1
TRADER_MAX_SIGNAL_AGE_SECONDS = 10
TRADER_HEARTBEAT_INTERVAL_SECONDS = 60



async def send_deal_decision_notification(
        *,
        telegram_sender,
        message_thread_id,
        decision,
) -> None:
    """Что делает: отправляет Telegram-уведомление только по реальному торговому решению.
    Зачем нужна: signal-сервис может создавать повторяющиеся signal_events, а deal-thread должен получать только action != NO_ACTION."""
    if telegram_sender is None:
        return

    if decision.action.value == "NO_ACTION":
        return

    plot_path = build_plot_path(
        instrument_code=decision.instrument_code,
        signal_bar_time_ct=decision.signal_time_ct,
    )

    caption = (
        "🚨 Торговое решение\n"
        f"instrument: {decision.instrument_code}\n"
        f"action: {decision.action.value}\n"
        f"signal_id: {decision.source_signal_id}\n"
        f"time CT: {decision.signal_time_ct}\n"
        f"direction: {decision.signal_direction}\n"
        f"entry: {decision.entry_price:.2f}\n"
        f"order_type: {decision.order_type}\n"
        f"limit_price: {decision.limit_price}\n"
        f"regime: {decision.regime}\n"
        f"ma_zone: {decision.ma_zone}\n"
        f"strength: {decision.signal_strength}\n"
        f"potential_end: {decision.potential_end_delta_points:+.2f} pt\n"
        f"position_before: {decision.position_before_side.value}/{decision.position_before_qty:g}\n"
        f"position_after: {decision.position_after_side.value}/{decision.position_after_qty:g}\n"
        f"reason: {decision.reason}"
    )

    ok = False

    if plot_path.is_file():
        ok = await telegram_sender.send_photo(
            plot_path,
            caption=caption,
            message_thread_id=message_thread_id,
        )

    if ok:
        return

    if not plot_path.is_file():
        caption += f"\nPNG not found: {plot_path}"

    await telegram_sender.send_text(
        caption,
        message_thread_id=message_thread_id,
    )


async def run_trader_loop(
        *,
        deal_telegram_sender=None,
        deal_message_thread_id=None,
) -> None:
    log_info(
        logger,
        (
            "ib_trader loop started: "
            "source=signal_events, "
            f"max_signal_age_seconds={TRADER_MAX_SIGNAL_AGE_SECONDS}"
        ),
        to_telegram=False,
    )

    next_heartbeat_ts = int(time.time()) + TRADER_HEARTBEAT_INTERVAL_SECONDS

    while True:
        try:
            decisions = process_signal_events_once(
                max_signal_age_seconds=TRADER_MAX_SIGNAL_AGE_SECONDS,
            )

            for decision in decisions:
                log_info(
                    logger,
                    (
                        f"{decision.instrument_code}: trader decision: "
                        f"source_signal_id={decision.source_signal_id}, "
                        f"action={decision.action.value}, "
                        f"signal_direction={decision.signal_direction}, "
                        f"potential_end={decision.potential_end_delta_points:+.2f}, "
                        f"regime={decision.regime}, "
                        f"ma_zone={decision.ma_zone}, "
                        f"strength={decision.signal_strength}, "
                        f"order_type={decision.order_type}, "
                        f"limit_price={decision.limit_price}, "
                        f"position_before={decision.position_before_side.value}/{decision.position_before_qty:g}, "
                        f"position_after={decision.position_after_side.value}/{decision.position_after_qty:g}, "
                        f"reason={decision.reason}"
                    ),
                    to_telegram=False,
                )

                await send_deal_decision_notification(
                    telegram_sender=deal_telegram_sender,
                    message_thread_id=deal_message_thread_id,
                    decision=decision,
                )

        except Exception as exc:
            log_warning(
                logger,
                f"ib_trader: ошибка обработки signal_events: {exc}\\n"
                f"{traceback.format_exc()}",
                to_telegram=True,
            )

        now_ts = int(time.time())
        if now_ts >= next_heartbeat_ts:
            log_info(
                logger,
                (
                    "ib_trader heartbeat: alive, "
                    "source=signal_events, "
                    f"max_signal_age_seconds={TRADER_MAX_SIGNAL_AGE_SECONDS}"
                ),
                to_telegram=False,
            )
            next_heartbeat_ts = now_ts + TRADER_HEARTBEAT_INTERVAL_SECONDS

        await asyncio.sleep(TRADER_LOOP_SLEEP_SECONDS)
