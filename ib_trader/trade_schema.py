from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from core.sqlite_utils import open_sqlite_connection
from core.time_utils import CT_TIMEZONE, MSK_TIMEZONE, SQLITE_DATETIME_FORMAT
from ib_trader.trade_models import PositionSide, TradeDecisionAction


TRADE_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trade.sqlite3"
POSITIONS_LATEST_TABLE_NAME = "positions_latest"
TRADE_INTENTS_TABLE_NAME = "trade_intents"
ORDER_REF_PREFIX = "IBMD_INTENT"

@dataclass(frozen=True)
class TradeIntentDraft:
    source_signal_id: int
    instrument_code: str
    signal_bar_ts: int
    signal_time_ct: str | None

    intent_source: str
    action: TradeDecisionAction
    reason: str

    signal_direction: str
    entry_price: float
    potential_end_delta_points: float

    regime: int | None
    ma_zone: int | None
    signal_strength: str

    order_type: str
    limit_price: float | None
    limit_offset_points: float | None
    ttl_seconds: int | None

    position_before_side: PositionSide
    position_before_qty: float

    position_after_side: PositionSide
    position_after_qty: float

def get_trade_db_connection():
    return open_sqlite_connection(
        str(TRADE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

def create_positions_latest_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {POSITIONS_LATEST_TABLE_NAME} (
        instrument_code TEXT PRIMARY KEY,
        side TEXT NOT NULL,
        quantity REAL NOT NULL,
        broker_contract TEXT,
        broker_con_id INTEGER,
        broker_account TEXT,
        contract_is_active INTEGER,
        updated_at_ts INTEGER NOT NULL,
        updated_at_utc TEXT NOT NULL
    );
    """

def create_trade_intents_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {TRADE_INTENTS_TABLE_NAME} (
        trade_intent_id INTEGER PRIMARY KEY AUTOINCREMENT,

        source_signal_id INTEGER NOT NULL,
        instrument_code TEXT NOT NULL,
        signal_bar_ts INTEGER NOT NULL,
        signal_time_utc TEXT NOT NULL,
        signal_time_ct TEXT NOT NULL,
        signal_time_msk TEXT NOT NULL,

        entry_regime INTEGER,
        entry_ma_zone INTEGER,

        intent_source TEXT NOT NULL,

        action TEXT NOT NULL,
        action_reason TEXT NOT NULL,

        target_side TEXT NOT NULL,
        target_qty REAL NOT NULL,

        position_before_side TEXT NOT NULL,
        position_before_qty REAL NOT NULL,

        order_type TEXT NOT NULL,
        limit_price REAL,
        limit_offset_points REAL,
        ttl_seconds INTEGER,

        status TEXT NOT NULL,

        cancel_requested INTEGER NOT NULL DEFAULT 0,
        cancel_reason TEXT,
        cancel_source_signal_id INTEGER,
        cancel_requested_at_ts INTEGER,

        order_ref TEXT,
        order_id INTEGER,
        order_action TEXT,
        order_quantity INTEGER,
        avg_fill_price REAL,
        total_commission REAL,
        realized_pnl REAL,
        error_text TEXT,

        created_at_ts INTEGER NOT NULL,
        updated_at_ts INTEGER NOT NULL,
        sent_at_ts INTEGER,
        finished_at_ts INTEGER,

        UNIQUE (
            instrument_code,
            source_signal_id,
            signal_bar_ts,
            action
        )
    );
    """

def table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}

def ensure_table_column(conn, *, table_name: str, column_name: str, column_sql: str) -> None:
    if column_name in table_columns(conn, table_name):
        return

    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")

def ensure_positions_latest_runtime_columns(conn) -> None:
    ensure_table_column(
        conn,
        table_name=POSITIONS_LATEST_TABLE_NAME,
        column_name="broker_contract",
        column_sql="broker_contract TEXT",
    )
    ensure_table_column(
        conn,
        table_name=POSITIONS_LATEST_TABLE_NAME,
        column_name="broker_con_id",
        column_sql="broker_con_id INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=POSITIONS_LATEST_TABLE_NAME,
        column_name="broker_account",
        column_sql="broker_account TEXT",
    )
    ensure_table_column(
        conn,
        table_name=POSITIONS_LATEST_TABLE_NAME,
        column_name="contract_is_active",
        column_sql="contract_is_active INTEGER",
    )

def ensure_trade_intents_runtime_columns(conn) -> None:
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="entry_regime",
        column_sql="entry_regime INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="entry_ma_zone",
        column_sql="entry_ma_zone INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_requested",
        column_sql="cancel_requested INTEGER NOT NULL DEFAULT 0",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_reason",
        column_sql="cancel_reason TEXT",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_source_signal_id",
        column_sql="cancel_source_signal_id INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_requested_at_ts",
        column_sql="cancel_requested_at_ts INTEGER",
    )

def initialize_trade_db(conn) -> None:
    conn.execute(create_positions_latest_table_sql())
    conn.execute(create_trade_intents_table_sql())
    ensure_positions_latest_runtime_columns(conn)
    ensure_trade_intents_runtime_columns(conn)

    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_intents_status
        ON {TRADE_INTENTS_TABLE_NAME}(status, created_at_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_intents_instrument_time
        ON {TRADE_INTENTS_TABLE_NAME}(instrument_code, created_at_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_intents_source_signal
        ON {TRADE_INTENTS_TABLE_NAME}(instrument_code, source_signal_id, signal_bar_ts);
        """
    )

def build_time_text_fields_from_ts(ts: int) -> tuple[str, str, str]:
    dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    dt_ct = dt_utc.astimezone(CT_TIMEZONE)
    dt_msk = dt_utc.astimezone(MSK_TIMEZONE)

    return (
        dt_utc.strftime(SQLITE_DATETIME_FORMAT),
        dt_ct.strftime(SQLITE_DATETIME_FORMAT),
        dt_msk.strftime(SQLITE_DATETIME_FORMAT),
    )

__all__ = ['TRADE_DB_PATH', 'POSITIONS_LATEST_TABLE_NAME', 'TRADE_INTENTS_TABLE_NAME', 'ORDER_REF_PREFIX', 'TradeIntentDraft', 'get_trade_db_connection', 'create_positions_latest_table_sql', 'create_trade_intents_table_sql', 'table_columns', 'ensure_table_column', 'ensure_positions_latest_runtime_columns', 'ensure_trade_intents_runtime_columns', 'initialize_trade_db', 'build_time_text_fields_from_ts']
