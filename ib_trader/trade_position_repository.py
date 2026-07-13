from __future__ import annotations

from ib_trader.trade_models import (
    PositionSide,
    PositionSnapshot,
    PositionSnapshotFreshness,
    TradeIntentRejected,
    TraderSignalEvent,
)
from ib_trader.trade_schema import (
    POSITIONS_LATEST_TABLE_NAME,
    TradeIntentDraft,
    initialize_trade_db,
)


POSITION_SNAPSHOT_MAX_AGE_SECONDS = 10

def read_position_snapshot(conn, *, instrument_code: str) -> PositionSnapshot:
    initialize_trade_db(conn)

    row = conn.execute(
        f"""
        SELECT
            side,
            quantity,
            broker_contract,
            broker_con_id,
            broker_account,
            contract_is_active
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None:
        return PositionSnapshot(
            instrument_code=str(instrument_code),
            side=PositionSide.UNKNOWN,
            quantity=0.0,
        )

    quantity = float(row[1])
    side = PositionSide(str(row[0]).upper())
    snapshot = PositionSnapshot(
        instrument_code=str(instrument_code),
        side=side,
        quantity=max(0.0, quantity),
        broker_contract=None if row[2] is None else str(row[2]),
        broker_con_id=None if row[3] is None else int(row[3]),
        broker_account=None if row[4] is None else str(row[4]),
        contract_is_active=None if row[5] is None else bool(int(row[5])),
    )

    if side == PositionSide.UNKNOWN:
        return PositionSnapshot(
            instrument_code=str(instrument_code),
            side=PositionSide.UNKNOWN,
            quantity=0.0,
            broker_contract=snapshot.broker_contract,
            broker_con_id=snapshot.broker_con_id,
            broker_account=snapshot.broker_account,
            contract_is_active=snapshot.contract_is_active,
        )

    if quantity <= 0.0:
        return PositionSnapshot(
            instrument_code=str(instrument_code),
            side=PositionSide.FLAT,
            quantity=0.0,
            broker_contract=snapshot.broker_contract,
            broker_con_id=snapshot.broker_con_id,
            broker_account=snapshot.broker_account,
            contract_is_active=snapshot.contract_is_active,
        )

    return snapshot

def read_open_position_snapshots(conn) -> list[PositionSnapshot]:
    initialize_trade_db(conn)

    rows = conn.execute(
        f"""
        SELECT
            instrument_code,
            side,
            quantity,
            broker_contract,
            broker_con_id,
            broker_account,
            contract_is_active
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE side IN ('LONG', 'SHORT')
          AND quantity > 0
        ORDER BY instrument_code
        """
    ).fetchall()

    return [
        PositionSnapshot(
            instrument_code=str(row[0]),
            side=PositionSide(str(row[1]).upper()),
            quantity=float(row[2]),
            broker_contract=None if row[3] is None else str(row[3]),
            broker_con_id=None if row[4] is None else int(row[4]),
            broker_account=None if row[5] is None else str(row[5]),
            contract_is_active=None if row[6] is None else bool(int(row[6])),
        )
        for row in rows
    ]

def read_position_updated_at_ts(conn, *, instrument_code: str) -> int | None:
    row = conn.execute(
        f"""
        SELECT updated_at_ts
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None or row[0] is None:
        return None

    return int(row[0])

def read_position_snapshot_freshness(
        conn,
        *,
        instrument_code: str,
        now_ts: int,
        max_age_seconds: int = POSITION_SNAPSHOT_MAX_AGE_SECONDS,
) -> PositionSnapshotFreshness:
    row = conn.execute(
        f"""
        SELECT updated_at_ts, updated_at_utc
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None or row[0] is None:
        return PositionSnapshotFreshness(
            instrument_code=str(instrument_code),
            updated_at_ts=None,
            updated_at_utc=None,
            age_seconds=None,
            max_age_seconds=int(max_age_seconds),
            is_stale=True,
        )

    updated_at_ts = int(row[0])
    age_seconds = max(0, int(now_ts) - updated_at_ts)

    return PositionSnapshotFreshness(
        instrument_code=str(instrument_code),
        updated_at_ts=updated_at_ts,
        updated_at_utc=None if row[1] is None else str(row[1]),
        age_seconds=age_seconds,
        max_age_seconds=int(max_age_seconds),
        is_stale=age_seconds > int(max_age_seconds),
    )

def build_stale_position_open_rejected_event(
        *,
        signal: TraderSignalEvent,
        position: PositionSnapshot,
        freshness: PositionSnapshotFreshness,
        draft: TradeIntentDraft,
) -> TradeIntentRejected:
    return TradeIntentRejected(
        instrument_code=signal.instrument_code,
        source_signal_id=int(signal.source_signal_id),
        signal_bar_ts=int(signal.signal_bar_ts),
        signal_time_utc=signal.signal_time_utc,
        signal_time_ct=signal.signal_time_ct,
        signal_time_msk=signal.signal_time_msk,
        reason="positions_latest_stale_trade_rejected",
        action=draft.action,
        signal_direction=draft.signal_direction,
        order_type=draft.order_type,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        positions_latest_updated_at_ts=freshness.updated_at_ts,
        positions_latest_updated_at_utc=freshness.updated_at_utc,
        positions_latest_age_seconds=freshness.age_seconds,
        max_allowed_age_seconds=freshness.max_age_seconds,
    )

__all__ = ['POSITION_SNAPSHOT_MAX_AGE_SECONDS', 'read_position_snapshot', 'read_open_position_snapshots', 'read_position_updated_at_ts', 'read_position_snapshot_freshness', 'build_stale_position_open_rejected_event']
