import asyncio
import traceback

from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_execution.execution_logic import execute_trade_intent
from ib_execution.execution_models import ExecutionResult, ExecutionStatus
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    mark_trade_intent_sending,
    read_new_trade_intents,
    write_trade_intent_execution_result,
)
from ib_execution.order_service import OrderService

setup_logging()
logger = get_logger(__name__)

EXECUTION_LOOP_SLEEP_SECONDS = 1
NEW_INTENTS_LIMIT = 20
MAX_NEW_INTENT_AGE_SECONDS = 10


async def run_execution_loop(order_service: OrderService) -> None:
    log_info(
        logger,
        (
            "ib_execution loop started: "
            "order_type=FROM_TRADE_INTENT, "
            f"max_new_intent_age_seconds={MAX_NEW_INTENT_AGE_SECONDS}"
        ),
        to_telegram=False,
    )

    while True:
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

                result = await execute_trade_intent(
                    order_service=order_service,
                    intent=intent,
                )

                write_trade_intent_execution_result(conn, result=result)
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

            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}"

                try:
                    failure_result = ExecutionResult(
                        trade_intent_id=intent.trade_intent_id,
                        order_id=None,
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
                finally:
                    log_warning(
                        logger,
                        f"ib_execution failed trade_intent={intent.trade_intent_id}: {error_text}\\n"
                        f"{traceback.format_exc()}",
                        to_telegram=True,
                    )

            finally:
                conn.close()

        await asyncio.sleep(EXECUTION_LOOP_SLEEP_SECONDS)
