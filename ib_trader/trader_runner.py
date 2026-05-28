import asyncio
import time
import traceback

from config import settings_live as app_settings
from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_trader.trade_store import process_signal_events_once

setup_logging()
logger = get_logger(__name__)

TRADER_LOOP_SLEEP_SECONDS = 1
TRADER_MAX_SIGNAL_AGE_SECONDS = 10
TRADER_HEARTBEAT_INTERVAL_SECONDS = 60
REPORTED_REJECTED_TRADE_INTENTS: set[tuple[str, int, str]] = set()


def format_rejected_trade_intent(rejected) -> str:
    age_text = (
        "missing"
        if rejected.positions_latest_age_seconds is None
        else str(rejected.positions_latest_age_seconds)
    )
    updated_at_ts_text = (
        "missing"
        if rejected.positions_latest_updated_at_ts is None
        else str(rejected.positions_latest_updated_at_ts)
    )
    updated_at_utc_text = rejected.positions_latest_updated_at_utc or "missing"

    return (
        "❌ POSITION SNAPSHOT STALE: сделка не будет открыта\n"
        f"instrument: {rejected.instrument_code}\n"
        f"source_signal_id: {rejected.source_signal_id}\n"
        f"signal_time_utc: {rejected.signal_time_utc}\n"
        f"signal_time_ct: {rejected.signal_time_ct}\n"
        f"signal_time_msk: {rejected.signal_time_msk}\n"
        f"direction: {rejected.signal_direction}\n"
        f"order_type: {rejected.order_type}\n"
        f"action: {rejected.action.value}\n"
        f"reason: {rejected.reason}\n"
        f"positions_latest.side: {rejected.position_before_side.value}\n"
        f"positions_latest.qty: {float(rejected.position_before_qty):g}\n"
        f"positions_latest.updated_at_ts: {updated_at_ts_text}\n"
        f"positions_latest.updated_at_utc: {updated_at_utc_text}\n"
        f"positions_latest.age_seconds: {age_text}\n"
        f"max_allowed_age_seconds: {rejected.max_allowed_age_seconds}"
    )


def should_report_rejected_trade_intent(rejected) -> bool:
    key = (
        str(rejected.instrument_code),
        int(rejected.source_signal_id),
        str(rejected.reason),
    )

    if key in REPORTED_REJECTED_TRADE_INTENTS:
        return False

    REPORTED_REJECTED_TRADE_INTENTS.add(key)
    return True


async def run_trader_loop() -> None:
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
            result = process_signal_events_once(
                max_signal_age_seconds=TRADER_MAX_SIGNAL_AGE_SECONDS,
            )

            for rejected in result.rejected:
                if not should_report_rejected_trade_intent(rejected):
                    continue

                log_warning(
                    logger,
                    format_rejected_trade_intent(rejected),
                    to_telegram=True,
                    message_thread_id=getattr(app_settings, "telegram_message_thread_id_error", None),
                )

            for intent in result.created:
                log_info(
                    logger,
                    (
                        f"{intent.instrument_code}: trade_intent created: "
                        f"trade_intent_id={intent.trade_intent_id}, "
                        f"source={intent.intent_source}, "
                        f"source_signal_id={intent.source_signal_id}, "
                        f"action={intent.action.value}, "
                        f"reason={intent.reason}, "
                        f"signal_direction={intent.signal_direction}, "
                        f"potential_end={intent.potential_end_delta_points:+.2f}, "
                        f"regime={intent.regime}, "
                        f"ma_zone={intent.ma_zone}, "
                        f"strength={intent.signal_strength}, "
                        f"order_type={intent.order_type}, "
                        f"limit_price={intent.limit_price}, "
                        f"position_before={intent.position_before_side.value}/{intent.position_before_qty:g}, "
                        f"position_after={intent.position_after_side.value}/{intent.position_after_qty:g}"
                    ),
                    to_telegram=False,
                )

        except Exception as exc:
            log_warning(
                logger,
                f"ib_trader: ошибка обработки signal_events: {exc}\n"
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
