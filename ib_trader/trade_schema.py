from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from core.sqlite_schema import require_exact_table_schema
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import CT_TIMEZONE, MSK_TIMEZONE, SQLITE_DATETIME_FORMAT
from ib_trader.trade_models import PositionSide, TradeDecisionAction


TRADE_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trade.sqlite3"
POSITIONS_LATEST_TABLE_NAME = "positions_latest"
TRADE_INTENTS_TABLE_NAME = "trade_intents"
ORDER_REF_PREFIX = "IBMD_INTENT"

POSITIONS_LATEST_SCHEMA = (
    ('instrument_code', 'TEXT', 0, None, 1),
    ('side', 'TEXT', 1, None, 0),
    ('quantity', 'REAL', 1, None, 0),
    ('broker_contract', 'TEXT', 0, None, 0),
    ('broker_con_id', 'INTEGER', 0, None, 0),
    ('broker_account', 'TEXT', 0, None, 0),
    ('contract_is_active', 'INTEGER', 0, None, 0),
    ('updated_at_ts', 'INTEGER', 1, None, 0),
    ('updated_at_utc', 'TEXT', 1, None, 0),
)

TRADE_INTENTS_SCHEMA = (
    ('trade_intent_id', 'INTEGER', 0, None, 1),
    ('source_signal_id', 'INTEGER', 1, None, 0),
    ('instrument_code', 'TEXT', 1, None, 0),
    ('signal_bar_ts', 'INTEGER', 1, None, 0),
    ('signal_time_utc', 'TEXT', 1, None, 0),
    ('signal_time_ct', 'TEXT', 1, None, 0),
    ('signal_time_msk', 'TEXT', 1, None, 0),
    ('entry_regime', 'INTEGER', 0, None, 0),
    ('entry_ma_zone', 'INTEGER', 0, None, 0),
    ('intent_source', 'TEXT', 1, None, 0),
    ('action', 'TEXT', 1, None, 0),
    ('action_reason', 'TEXT', 1, None, 0),
    ('target_side', 'TEXT', 1, None, 0),
    ('target_qty', 'REAL', 1, None, 0),
    ('position_before_side', 'TEXT', 1, None, 0),
    ('position_before_qty', 'REAL', 1, None, 0),
    ('order_type', 'TEXT', 1, None, 0),
    ('limit_price', 'REAL', 0, None, 0),
    ('limit_offset_points', 'REAL', 0, None, 0),
    ('ttl_seconds', 'INTEGER', 0, None, 0),
    ('status', 'TEXT', 1, None, 0),
    ('cancel_requested', 'INTEGER', 1, '0', 0),
    ('cancel_reason', 'TEXT', 0, None, 0),
    ('cancel_source_signal_id', 'INTEGER', 0, None, 0),
    ('cancel_requested_at_ts', 'INTEGER', 0, None, 0),
    ('order_ref', 'TEXT', 0, None, 0),
    ('order_id', 'INTEGER', 0, None, 0),
    ('order_action', 'TEXT', 0, None, 0),
    ('order_quantity', 'INTEGER', 0, None, 0),
    ('avg_fill_price', 'REAL', 0, None, 0),
    ('total_commission', 'REAL', 0, None, 0),
    ('realized_pnl', 'REAL', 0, None, 0),
    ('error_text', 'TEXT', 0, None, 0),
    ('created_at_ts', 'INTEGER', 1, None, 0),
    ('updated_at_ts', 'INTEGER', 1, None, 0),
    ('sent_at_ts', 'INTEGER', 0, None, 0),
    ('finished_at_ts', 'INTEGER', 0, None, 0),
)


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
        synchronous="FULL",
        foreign_keys=True,
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

def initialize_trade_db(conn) -> None:
    conn.execute(create_positions_latest_table_sql())
    conn.execute(create_trade_intents_table_sql())
    require_exact_table_schema(
        conn,
        table_name=POSITIONS_LATEST_TABLE_NAME,
        expected_schema=POSITIONS_LATEST_SCHEMA,
    )
    require_exact_table_schema(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        expected_schema=TRADE_INTENTS_SCHEMA,
    )
    conn.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_trade_intents_status
        ON {TRADE_INTENTS_TABLE_NAME}(status, created_at_ts);"""
    )
    conn.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_trade_intents_instrument_time
        ON {TRADE_INTENTS_TABLE_NAME}(instrument_code, created_at_ts);"""
    )
    conn.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_trade_intents_source_signal
        ON {TRADE_INTENTS_TABLE_NAME}(instrument_code, source_signal_id, signal_bar_ts);"""
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

__all__ = [
    'TRADE_DB_PATH',
    'POSITIONS_LATEST_TABLE_NAME',
    'TRADE_INTENTS_TABLE_NAME',
    'ORDER_REF_PREFIX',
    'POSITIONS_LATEST_SCHEMA',
    'TRADE_INTENTS_SCHEMA',
    'TradeIntentDraft',
    'get_trade_db_connection',
    'create_positions_latest_table_sql',
    'create_trade_intents_table_sql',
    'initialize_trade_db',
    'build_time_text_fields_from_ts',
]
