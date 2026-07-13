from __future__ import annotations

from core.sqlite_migrations import (
    SqliteMigration,
    apply_sqlite_migrations,
)


TRADE_DB_MIGRATION_NAMESPACE = "ib_execution_trade_db"
LEGACY_TAKE_PROFIT_TABLE_NAME = "take_profit_orders"
PROTECTIVE_ORDERS_TABLE_NAME = "protective_orders"


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        LIMIT 1
        """,
        (str(table_name),),
    ).fetchone()
    return row is not None


def table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(
        f"PRAGMA table_info({table_name})"
    ).fetchall()
    return {str(row[1]) for row in rows}


def _column_or_null(
        columns: set[str],
        column_name: str,
) -> str:
    if str(column_name) in columns:
        return str(column_name)
    return "NULL"


def migrate_legacy_take_profit_orders(conn) -> None:
    if not table_exists(conn, LEGACY_TAKE_PROFIT_TABLE_NAME):
        return

    if not table_exists(conn, PROTECTIVE_ORDERS_TABLE_NAME):
        raise RuntimeError(
            "protective_orders must be initialized before "
            "legacy take-profit migration"
        )

    columns = table_columns(
        conn,
        LEGACY_TAKE_PROFIT_TABLE_NAME,
    )

    required = {
        "instrument_code",
        "parent_trade_intent_id",
        "order_ref",
        "order_id",
        "order_action",
        "order_quantity",
        "take_profit_price",
        "status",
        "created_at_ts",
        "updated_at_ts",
    }
    missing = sorted(required - columns)
    if missing:
        raise RuntimeError(
            "legacy take_profit_orders has unexpected schema; "
            f"missing columns={missing}"
        )

    error_expr = (
        "CASE "
        "WHEN error_text IS NULL OR TRIM(error_text) = '' "
        "THEN 'migrated from legacy take_profit_orders' "
        "ELSE 'migrated from legacy take_profit_orders; ' || error_text "
        "END"
        if "error_text" in columns
        else "'migrated from legacy take_profit_orders'"
    )

    filled_at_expr = _column_or_null(columns, "filled_at_ts")
    synthetic_intent_expr = _column_or_null(
        columns,
        "synthetic_trade_intent_id",
    )
    finished_at_expr = (
        "finished_at_ts"
        if "finished_at_ts" in columns
        else (
            "CASE WHEN UPPER(status) = 'ACTIVE' "
            "THEN NULL ELSE updated_at_ts END"
        )
    )

    conn.execute(
        f"""
        INSERT OR IGNORE INTO {PROTECTIVE_ORDERS_TABLE_NAME} (
            instrument_code,
            parent_trade_intent_id,
            role,
            oca_group,
            order_ref,
            order_id,
            order_action,
            order_quantity,
            order_type,
            limit_price,
            stop_price,
            status,
            error_text,
            filled_qty,
            avg_fill_price,
            total_commission,
            realized_pnl,
            filled_at_ts,
            synthetic_trade_intent_id,
            created_at_ts,
            updated_at_ts,
            finished_at_ts
        )
        SELECT
            instrument_code,
            parent_trade_intent_id,
            'TAKE_PROFIT',
            NULL,
            order_ref,
            order_id,
            UPPER(order_action),
            order_quantity,
            'LIMIT',
            take_profit_price,
            NULL,
            CASE
                WHEN UPPER(status) = 'ACTIVE' THEN 'ACTIVE'
                WHEN UPPER(status) = 'FILLED' THEN 'FILLED'
                WHEN UPPER(status) = 'FAILED' THEN 'FAILED'
                ELSE 'CANCELLED'
            END,
            {error_expr},
            {_column_or_null(columns, "filled_qty")},
            {_column_or_null(columns, "avg_fill_price")},
            {_column_or_null(columns, "total_commission")},
            {_column_or_null(columns, "realized_pnl")},
            {filled_at_expr},
            {synthetic_intent_expr},
            created_at_ts,
            updated_at_ts,
            {finished_at_expr}
        FROM {LEGACY_TAKE_PROFIT_TABLE_NAME}
        """
    )

    conn.execute(
        f"DROP TABLE {LEGACY_TAKE_PROFIT_TABLE_NAME}"
    )


TRADE_DB_MIGRATIONS = (
    SqliteMigration(
        version=1,
        name="migrate legacy take_profit_orders into protective_orders",
        apply=migrate_legacy_take_profit_orders,
    ),
)


def run_trade_db_migrations(conn):
    return apply_sqlite_migrations(
        conn,
        namespace=TRADE_DB_MIGRATION_NAMESPACE,
        migrations=TRADE_DB_MIGRATIONS,
    )
