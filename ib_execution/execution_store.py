import time

from ib_trader.trade_store import (
    TRADE_DB_PATH,
    TRADE_INTENTS_TABLE_NAME,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_execution.execution_models import ExecutionOrderResult, ExecutionStatus, TradeIntent

EXECUTION_ORDERS_TABLE_NAME = "execution_orders"


def create_execution_orders_table_sql() -> str:
    """Что делает: возвращает SQL создания execution_orders.
    Зачем нужна: фиксируем связь trade_intent -> IB orderId / результат исполнения."""
    return f"""
    CREATE TABLE IF NOT EXISTS {EXECUTION_ORDERS_TABLE_NAME} (
        execution_order_id INTEGER PRIMARY KEY AUTOINCREMENT,

        trade_intent_id INTEGER NOT NULL UNIQUE,
        decision_id INTEGER NOT NULL,
        source_signal_id INTEGER NOT NULL,
        instrument_code TEXT NOT NULL,

        order_id INTEGER,
        order_action TEXT NOT NULL,
        order_quantity INTEGER NOT NULL,

        status TEXT NOT NULL,
        avg_fill_price REAL,
        error_text TEXT,

        created_at_ts INTEGER NOT NULL,
        updated_at_ts INTEGER NOT NULL
    );
    """


def initialize_execution_db(conn) -> None:
    """Что делает: создаёт таблицы trade/intents/positions + execution_orders."""
    initialize_trade_db(conn)
    conn.execute(create_execution_orders_table_sql())
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_execution_orders_status
        ON {EXECUTION_ORDERS_TABLE_NAME}(status, updated_at_ts);
        """
    )


def read_new_trade_intents(*, limit: int = 20) -> list[TradeIntent]:
    """Что делает: читает trade_intents со статусом NEW.
    Зачем нужна: execution-сервис исполняет только новые торговые намерения."""
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
                status=str(row[9]),
                created_at_ts=int(row[10]),
            )
            for row in rows
        ]

    finally:
        conn.close()


def update_trade_intent_status(
        conn,
        *,
        trade_intent_id: int,
        status: ExecutionStatus | str,
) -> None:
    """Что делает: меняет status у trade_intents."""
    status_value = status.value if isinstance(status, ExecutionStatus) else str(status)

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET status = ?
        WHERE trade_intent_id = ?
        """,
        (status_value, int(trade_intent_id)),
    )


def write_execution_order_result(
        conn,
        *,
        intent: TradeIntent,
        result: ExecutionOrderResult,
) -> None:
    """Что делает: пишет/обновляет результат исполнения trade_intent."""
    now_ts = int(time.time())

    conn.execute(
        f"""
        INSERT INTO {EXECUTION_ORDERS_TABLE_NAME} (
            trade_intent_id,
            decision_id,
            source_signal_id,
            instrument_code,

            order_id,
            order_action,
            order_quantity,

            status,
            avg_fill_price,
            error_text,

            created_at_ts,
            updated_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT(trade_intent_id) DO UPDATE SET
            order_id = excluded.order_id,
            order_action = excluded.order_action,
            order_quantity = excluded.order_quantity,

            status = excluded.status,
            avg_fill_price = excluded.avg_fill_price,
            error_text = excluded.error_text,

            updated_at_ts = excluded.updated_at_ts
        ;
        """,
        (
            int(intent.trade_intent_id),
            int(intent.decision_id),
            int(intent.source_signal_id),
            intent.instrument_code,
            None if result.order_id is None else int(result.order_id),
            result.order_action,
            int(result.order_quantity),
            result.status.value,
            result.avg_fill_price,
            result.error_text,
            now_ts,
            now_ts,
        ),
    )
