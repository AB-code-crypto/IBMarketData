from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import uuid

from core.sqlite_schema import require_exact_table_schema
from core.sqlite_utils import open_sqlite_connection
from core.time_utils import CT_TIMEZONE, MSK_TIMEZONE, SQLITE_DATETIME_FORMAT
from ib_trader.trade_models import PositionSide, TradeDecisionAction

TRADE_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trade.sqlite3"
POSITIONS_LATEST_TABLE_NAME = "positions_latest"
TRADE_INTENTS_TABLE_NAME = "trade_intents"
ORDER_REF_PREFIX = "IBMD_INTENT"

POSITIONS_LATEST_SCHEMA = (
    ("instrument_code", "TEXT", 0, None, 1),
    ("side", "TEXT", 1, None, 0),
    ("quantity", "REAL", 1, None, 0),
    ("broker_contract", "TEXT", 0, None, 0),
    ("broker_con_id", "INTEGER", 0, None, 0),
    ("broker_account", "TEXT", 0, None, 0),
    ("contract_is_active", "INTEGER", 0, None, 0),
    ("updated_at_ts", "INTEGER", 1, None, 0),
    ("updated_at_utc", "TEXT", 1, None, 0),
)

TRADE_INTENTS_SCHEMA = (
    ("trade_intent_id", "INTEGER", 0, None, 1),
    ("source_signal_id", "INTEGER", 1, None, 0),
    ("instrument_code", "TEXT", 1, None, 0),
    ("signal_bar_ts", "INTEGER", 1, None, 0),
    ("signal_time_utc", "TEXT", 1, None, 0),
    ("signal_time_ct", "TEXT", 1, None, 0),
    ("signal_time_msk", "TEXT", 1, None, 0),
    ("intent_source", "TEXT", 1, None, 0),
    ("action", "TEXT", 1, None, 0),
    ("action_reason", "TEXT", 1, None, 0),
    ("target_side", "TEXT", 1, None, 0),
    ("target_qty", "REAL", 1, None, 0),
    ("position_before_side", "TEXT", 1, None, 0),
    ("position_before_qty", "REAL", 1, None, 0),
    ("order_type", "TEXT", 1, None, 0),
    ("limit_price", "REAL", 0, None, 0),
    ("limit_offset_points", "REAL", 0, None, 0),
    ("ttl_seconds", "INTEGER", 0, None, 0),
    ("status", "TEXT", 1, None, 0),
    ("cancel_requested", "INTEGER", 1, "0", 0),
    ("cancel_reason", "TEXT", 0, None, 0),
    ("cancel_source_signal_id", "INTEGER", 0, None, 0),
    ("cancel_requested_at_ts", "INTEGER", 0, None, 0),
    ("order_ref", "TEXT", 0, None, 0),
    ("order_id", "INTEGER", 0, None, 0),
    ("order_action", "TEXT", 0, None, 0),
    ("order_quantity", "INTEGER", 0, None, 0),
    ("avg_fill_price", "REAL", 0, None, 0),
    ("total_commission", "REAL", 0, None, 0),
    ("realized_pnl", "REAL", 0, None, 0),
    ("error_text", "TEXT", 0, None, 0),
    ("created_at_ts", "INTEGER", 1, None, 0),
    ("updated_at_ts", "INTEGER", 1, None, 0),
    ("sent_at_ts", "INTEGER", 0, None, 0),
    ("finished_at_ts", "INTEGER", 0, None, 0),
)
TRADE_INTENT_COLUMN_NAMES = tuple(row[0] for row in TRADE_INTENTS_SCHEMA)


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
    order_type: str
    position_before_side: PositionSide
    position_before_qty: float
    position_after_side: PositionSide
    position_after_qty: float


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


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


def create_trade_intents_table_sql(
        table_name: str = TRADE_INTENTS_TABLE_NAME,
) -> str:
    table_ref = quote_identifier(table_name)
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref} (
        trade_intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_signal_id INTEGER NOT NULL,
        instrument_code TEXT NOT NULL,
        signal_bar_ts INTEGER NOT NULL,
        signal_time_utc TEXT NOT NULL,
        signal_time_ct TEXT NOT NULL,
        signal_time_msk TEXT NOT NULL,
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
        UNIQUE (instrument_code, source_signal_id, signal_bar_ts, action)
    );
    """


def _table_exists(conn, table_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone() is not None


def _read_column_names(conn, table_name: str) -> tuple[str, ...]:
    return tuple(
        str(row[1])
        for row in conn.execute(
            f"PRAGMA table_info({quote_identifier(table_name)})"
        ).fetchall()
    )


def _migrate_trade_intents(conn, actual_columns: tuple[str, ...]) -> None:
    missing = [
        column for column in TRADE_INTENT_COLUMN_NAMES
        if column not in actual_columns
    ]
    if missing:
        raise RuntimeError(
            "Legacy trade_intents cannot be migrated because required columns "
            f"are missing: {missing}; actual={actual_columns}"
        )

    legacy_max_intent_id = int(
        conn.execute(
            f"SELECT COALESCE(MAX(trade_intent_id), 0) "
            f"FROM {quote_identifier(TRADE_INTENTS_TABLE_NAME)}"
        ).fetchone()[0]
    )
    legacy_sequence_row = conn.execute(
        "SELECT seq FROM sqlite_sequence WHERE name = ?",
        (TRADE_INTENTS_TABLE_NAME,),
    ).fetchone()
    legacy_id_high_water = max(
        legacy_max_intent_id,
        0 if legacy_sequence_row is None else int(legacy_sequence_row[0]),
    )
    temporary_name = f"trade_intents_rolling_{uuid.uuid4().hex}"
    conn.execute(create_trade_intents_table_sql(temporary_name))
    columns_sql = ", ".join(
        quote_identifier(column) for column in TRADE_INTENT_COLUMN_NAMES
    )
    conn.execute(
        f"""
        INSERT INTO {quote_identifier(temporary_name)} ({columns_sql})
        SELECT {columns_sql}
        FROM {quote_identifier(TRADE_INTENTS_TABLE_NAME)}
        ORDER BY trade_intent_id
        """
    )
    conn.execute(f"DROP TABLE {quote_identifier(TRADE_INTENTS_TABLE_NAME)}")
    conn.execute(
        f"ALTER TABLE {quote_identifier(temporary_name)} "
        f"RENAME TO {quote_identifier(TRADE_INTENTS_TABLE_NAME)}"
    )
    conn.execute(
        "DELETE FROM sqlite_sequence WHERE name = ?",
        (TRADE_INTENTS_TABLE_NAME,),
    )
    conn.execute(
        "INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)",
        (TRADE_INTENTS_TABLE_NAME, legacy_id_high_water),
    )


def initialize_trade_db(conn) -> None:
    conn.execute(create_positions_latest_table_sql())
    require_exact_table_schema(
        conn,
        table_name=POSITIONS_LATEST_TABLE_NAME,
        expected_schema=POSITIONS_LATEST_SCHEMA,
    )

    if not _table_exists(conn, TRADE_INTENTS_TABLE_NAME):
        conn.execute(create_trade_intents_table_sql())
    elif _read_column_names(conn, TRADE_INTENTS_TABLE_NAME) != TRADE_INTENT_COLUMN_NAMES:
        _migrate_trade_intents(
            conn,
            _read_column_names(conn, TRADE_INTENTS_TABLE_NAME),
        )

    require_exact_table_schema(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        expected_schema=TRADE_INTENTS_SCHEMA,
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_trade_intents_status "
        f"ON {TRADE_INTENTS_TABLE_NAME}(status, created_at_ts)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_trade_intents_instrument_time "
        f"ON {TRADE_INTENTS_TABLE_NAME}(instrument_code, created_at_ts)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_trade_intents_source_signal "
        f"ON {TRADE_INTENTS_TABLE_NAME}"
        f"(instrument_code, source_signal_id, signal_bar_ts)"
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
    "TRADE_DB_PATH",
    "POSITIONS_LATEST_TABLE_NAME",
    "TRADE_INTENTS_TABLE_NAME",
    "ORDER_REF_PREFIX",
    "POSITIONS_LATEST_SCHEMA",
    "TRADE_INTENTS_SCHEMA",
    "TRADE_INTENT_COLUMN_NAMES",
    "TradeIntentDraft",
    "get_trade_db_connection",
    "create_positions_latest_table_sql",
    "create_trade_intents_table_sql",
    "initialize_trade_db",
    "build_time_text_fields_from_ts",
]
