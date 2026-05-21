import asyncio
import traceback

from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_execution.execution_logic import execute_trade_intent_market
from ib_execution.execution_models import ExecutionOrderResult, ExecutionStatus
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    read_new_trade_intents,
    update_positions_latest_after_execution,
    update_trade_intent_status,
    write_execution_order_result,
)
from ib_execution.order_service import OrderService

setup_logging()
logger = get_logger(__name__)

EXECUTION_LOOP_SLEEP_SECONDS = 1
NEW_INTENTS_LIMIT = 20


async def run_execution_loop(order_service: OrderService) -> None:
    """Что делает: читает NEW trade_intents и исполняет их через IB."""
    log_info(
        logger,
        "ib_execution loop started: order_type=MARKET_FROM_INTENT_DELTA",
        to_telegram=False,
    )

    while True:
        intents = read_new_trade_intents(limit=NEW_INTENTS_LIMIT)

        for intent in intents:
            conn = get_trade_db_connection()
            try:
                initialize_execution_db(conn)
                update_trade_intent_status(
                    conn,
                    trade_intent_id=intent.trade_intent_id,
                    status=ExecutionStatus.SENDING,
                )
                conn.commit()

                result = await execute_trade_intent_market(
                    order_service=order_service,
                    intent=intent,
                )

                write_execution_order_result(
                    conn,
                    intent=intent,
                    result=result,
                )
                update_trade_intent_status(
                    conn,
                    trade_intent_id=intent.trade_intent_id,
                    status=result.status,
                )
                update_positions_latest_after_execution(
                    conn,
                    intent=intent,
                )
                conn.commit()

                log_info(
                    logger,
                    (
                        f"{intent.instrument_code}: executed trade_intent={intent.trade_intent_id}, "
                        f"action={intent.action}, order_id={result.order_id}, "
                        f"order_action={result.order_action}, qty={result.order_quantity}, "
                        f"avg_fill={result.avg_fill_price}"
                    ),
                    to_telegram=False,
                )

            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}"

                try:
                    failure_result = ExecutionOrderResult(
                        trade_intent_id=intent.trade_intent_id,
                        order_id=None,
                        order_action="-",
                        order_quantity=0,
                        status=ExecutionStatus.FAILED,
                        avg_fill_price=None,
                        error_text=error_text,
                    )
                    write_execution_order_result(
                        conn,
                        intent=intent,
                        result=failure_result,
                    )
                    update_trade_intent_status(
                        conn,
                        trade_intent_id=intent.trade_intent_id,
                        status=ExecutionStatus.FAILED,
                    )
                    conn.commit()
                finally:
                    log_warning(
                        logger,
                        f"ib_execution failed trade_intent={intent.trade_intent_id}: {error_text}\n"
                        f"{traceback.format_exc()}",
                        to_telegram=True,
                    )

            finally:
                conn.close()

        await asyncio.sleep(EXECUTION_LOOP_SLEEP_SECONDS)
