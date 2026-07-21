from __future__ import annotations

import time
from datetime import datetime, timezone

from config import settings_live as app_settings
from contracts import Instrument
from core.daily_trading_guard import (
    format_daily_halt_warning,
    get_moscow_day_context,
    read_effective_daily_trading_halt,
)
from core.ib_clock import read_ib_clock_health
from core.time_utils import CT_TIMEZONE
from ib_trader.trade_intent_repository import (
    has_trade_intent_for_signal,
    has_unresolved_trade_intent_for_instrument,
    write_trade_intent_and_event,
)
from ib_trader.trade_models import (
    PositionSide,
    PositionSnapshot,
    TradeDecisionAction,
    TradeIntentCreated,
    TradeIntentRejected,
    TradeProcessResult,
    TraderSignalEvent,
)
from ib_trader.trade_position_repository import (
    build_stale_position_open_rejected_event,
    read_open_position_snapshots,
    read_position_snapshot,
    read_position_snapshot_freshness,
)
from ib_trader.trade_schema import (
    TRADE_INTENTS_TABLE_NAME,
    TradeIntentDraft,
    build_time_text_fields_from_ts,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_trader.trade_signal_repository import read_latest_signal_events

FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID = -2
FUTURES_DAILY_FLAT_INTENT_SOURCE = "FUTURES_DAILY_FLAT"
FUTURES_DAILY_FLAT_REASON = "futures_daily_flat_before_no_trade_hour"

FUTURES_ROLLOVER_CLOSE_SOURCE_SIGNAL_ID = -11
FUTURES_ROLLOVER_CLOSE_INTENT_SOURCE = "FUTURES_ROLLOVER_CLOSE"
FUTURES_ROLLOVER_CLOSE_REASON = "close_non_active_futures_contract_before_new_trading"

FUTURES_NO_NEW_TRADES_HOUR_CT = 15
FUTURES_CLEARING_HOUR_CT = 16
FUTURES_DAILY_FLAT_CLOSE_BEFORE_SECONDS = 10


def build_signal_trade_intent_draft(
        *,
        signal: TraderSignalEvent,
        position: PositionSnapshot,
) -> TradeIntentDraft | None:
    if position.side == PositionSide.UNKNOWN:
        return None

    direction = str(signal.direction).upper()
    if direction not in {"LONG", "SHORT"}:
        return None

    # An obsolete quarterly contract is handled only by the rollover guard.
    if (
        position.side in {PositionSide.LONG, PositionSide.SHORT}
        and position.quantity > 0.0
        and position.contract_is_active is False
    ):
        return None

    if position.side == PositionSide.FLAT or position.quantity <= 0.0:
        action = TradeDecisionAction.OPEN_POSITION
        reason = "flat_position_open_by_rolling_signal"
        after_side = PositionSide(direction)
        after_qty = 1.0
    elif position.side.value == direction:
        return None
    else:
        action = TradeDecisionAction.REVERSE_POSITION
        reason = "opposite_rolling_signal_reverse_position"
        after_side = PositionSide(direction)
        after_qty = max(1.0, float(position.quantity))

    return TradeIntentDraft(
        source_signal_id=signal.source_signal_id,
        instrument_code=signal.instrument_code,
        signal_bar_ts=signal.signal_bar_ts,
        signal_time_ct=signal.signal_time_ct,
        intent_source="SIGNAL",
        action=action,
        reason=reason,
        signal_direction=direction,
        entry_price=float(signal.entry_price),
        potential_end_delta_points=float(signal.potential_end_delta_points),
        order_type="MARKET",
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=after_side,
        position_after_qty=float(after_qty),
    )


def is_risk_increasing_trade_action(action: TradeDecisionAction) -> bool:
    return action in {
        TradeDecisionAction.OPEN_POSITION,
        TradeDecisionAction.REVERSE_POSITION,
    }


def get_ct_day_ts(
        *,
        now_ts: int,
        hour: int,
        minute: int = 0,
        second: int = 0,
) -> int:
    now_ct = datetime.fromtimestamp(int(now_ts), tz=timezone.utc).astimezone(
        CT_TIMEZONE
    )
    target_ct = now_ct.replace(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=0,
    )
    return int(target_ct.astimezone(timezone.utc).timestamp())


def get_futures_daily_flat_context(*, now_ts: int) -> dict | None:
    no_new_trades_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_NO_NEW_TRADES_HOUR_CT,
    )
    clearing_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_CLEARING_HOUR_CT,
    )
    close_at_ts = no_new_trades_ts - FUTURES_DAILY_FLAT_CLOSE_BEFORE_SECONDS
    if close_at_ts <= int(now_ts) < clearing_ts:
        return {
            "no_new_trades_ts": no_new_trades_ts,
            "clearing_ts": clearing_ts,
            "close_at_ts": close_at_ts,
            "close_before_seconds": FUTURES_DAILY_FLAT_CLOSE_BEFORE_SECONDS,
        }
    return None


def is_futures_instrument(instrument_code: str) -> bool:
    row = Instrument.get(str(instrument_code))
    return row is not None and str(row.get("secType", "")).upper() == "FUT"


def build_futures_daily_flat_trade_intent_draft(
        *,
        position: PositionSnapshot,
        close_context: dict,
        now_ts: int,
) -> TradeIntentDraft:
    _ = close_context
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)
    return TradeIntentDraft(
        source_signal_id=FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=FUTURES_DAILY_FLAT_INTENT_SOURCE,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=FUTURES_DAILY_FLAT_REASON,
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        order_type="MARKET",
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


def build_futures_rollover_close_trade_intent_draft(
        *,
        position: PositionSnapshot,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)
    return TradeIntentDraft(
        source_signal_id=FUTURES_ROLLOVER_CLOSE_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=FUTURES_ROLLOVER_CLOSE_INTENT_SOURCE,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=(
            f"{FUTURES_ROLLOVER_CLOSE_REASON}; "
            f"broker_contract={position.broker_contract}; "
            f"broker_con_id={position.broker_con_id}"
        ),
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        order_type="MARKET",
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


def process_futures_rollover_close_once(
        conn,
        *,
        now_ts: int | None = None,
) -> list[TradeIntentCreated]:
    now_value = int(time.time() if now_ts is None else now_ts)
    created: list[TradeIntentCreated] = []
    for position in read_open_position_snapshots(conn):
        if not is_futures_instrument(position.instrument_code):
            continue
        if position.contract_is_active is not False:
            continue
        if has_unresolved_trade_intent_for_instrument(
            conn,
            instrument_code=position.instrument_code,
        ):
            continue
        created.append(
            write_trade_intent_and_event(
                conn,
                build_futures_rollover_close_trade_intent_draft(
                    position=position,
                    now_ts=now_value,
                ),
            )
        )
    return created


def process_futures_daily_flat_once(
        conn,
        *,
        now_ts: int | None = None,
) -> list[TradeIntentCreated]:
    now_value = int(time.time() if now_ts is None else now_ts)
    close_context = get_futures_daily_flat_context(now_ts=now_value)
    if close_context is None:
        return []

    created: list[TradeIntentCreated] = []
    for position in read_open_position_snapshots(conn):
        if not is_futures_instrument(position.instrument_code):
            continue
        if has_unresolved_trade_intent_for_instrument(
            conn,
            instrument_code=position.instrument_code,
        ):
            continue
        created.append(
            write_trade_intent_and_event(
                conn,
                build_futures_daily_flat_trade_intent_draft(
                    position=position,
                    close_context=close_context,
                    now_ts=now_value,
                ),
            )
        )
    return created


def has_pending_daily_execution_stats(*, now_ts: int) -> bool:
    day = get_moscow_day_context(now_ts)
    conn = get_trade_db_connection()
    try:
        initialize_trade_db(conn)
        event_ts = "COALESCE(finished_at_ts, sent_at_ts, updated_at_ts, created_at_ts)"
        row = conn.execute(
            f"""
            SELECT 1
            FROM {TRADE_INTENTS_TABLE_NAME}
            WHERE status = 'EXECUTED'
              AND order_id IS NOT NULL
              AND {event_ts} >= ?
              AND {event_ts} < ?
              AND (
                    realized_pnl IS NULL
                    OR (action = 'OPEN_POSITION' AND total_commission IS NULL)
              )
            LIMIT 1
            """,
            (day.start_ts, day.end_ts),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def process_signal_events_once(*, max_signal_age_seconds: int) -> TradeProcessResult:
    daily_halt = read_effective_daily_trading_halt(
        account_id=app_settings.ib_account_id,
    )
    if daily_halt is not None:
        return TradeProcessResult(
            created=[],
            rejected=[],
            guard_warnings=[format_daily_halt_warning(daily_halt)],
        )

    signals = read_latest_signal_events(
        max_signal_age_seconds=max_signal_age_seconds
    )
    clock_health = read_ib_clock_health(
        max_abs_offset_seconds=app_settings.ib_clock_max_abs_offset_seconds,
        max_sample_age_seconds=app_settings.ib_clock_health_max_age_seconds,
    )
    now_ts = (
        clock_health.corrected_now_ts
        if clock_health.sample is not None
        else int(time.time())
    )
    pending_stats = has_pending_daily_execution_stats(now_ts=now_ts)
    guard_warnings: list[str] = []
    created: list[TradeIntentCreated] = []
    rejected: list[TradeIntentRejected] = []

    conn = get_trade_db_connection()
    try:
        initialize_trade_db(conn)
        created.extend(process_futures_rollover_close_once(conn, now_ts=now_ts))
        created.extend(process_futures_daily_flat_once(conn, now_ts=now_ts))

        for signal in signals:
            if has_trade_intent_for_signal(
                conn,
                instrument_code=signal.instrument_code,
                source_signal_id=signal.source_signal_id,
                signal_bar_ts=signal.signal_bar_ts,
            ):
                continue

            position = read_position_snapshot(
                conn,
                instrument_code=signal.instrument_code,
            )
            freshness = read_position_snapshot_freshness(
                conn,
                instrument_code=signal.instrument_code,
                now_ts=now_ts,
            )
            if has_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=signal.instrument_code,
            ):
                continue

            draft = build_signal_trade_intent_draft(
                signal=signal,
                position=position,
            )
            if draft is None:
                continue

            if freshness.is_stale:
                rejected.append(
                    build_stale_position_open_rejected_event(
                        signal=signal,
                        position=position,
                        freshness=freshness,
                        draft=draft,
                    )
                )
                continue

            if is_risk_increasing_trade_action(draft.action):
                if pending_stats:
                    warning = (
                        f"{signal.instrument_code}: risk-increasing trade blocked "
                        "while current Moscow-day execution PnL/commission is "
                        "still pending reconciliation"
                    )
                    if warning not in guard_warnings:
                        guard_warnings.append(warning)
                    continue
                if not clock_health.is_healthy:
                    warning = (
                        f"{signal.instrument_code}: risk-increasing trade blocked "
                        f"by IB clock guard; reason={clock_health.reason}"
                    )
                    if warning not in guard_warnings:
                        guard_warnings.append(warning)
                    continue

            created.append(write_trade_intent_and_event(conn, draft))

        conn.commit()
        return TradeProcessResult(
            created=created,
            rejected=rejected,
            guard_warnings=guard_warnings,
        )
    finally:
        conn.close()


__all__ = [
    "FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID",
    "FUTURES_DAILY_FLAT_INTENT_SOURCE",
    "FUTURES_DAILY_FLAT_REASON",
    "FUTURES_ROLLOVER_CLOSE_SOURCE_SIGNAL_ID",
    "FUTURES_ROLLOVER_CLOSE_INTENT_SOURCE",
    "FUTURES_ROLLOVER_CLOSE_REASON",
    "FUTURES_NO_NEW_TRADES_HOUR_CT",
    "FUTURES_CLEARING_HOUR_CT",
    "FUTURES_DAILY_FLAT_CLOSE_BEFORE_SECONDS",
    "build_signal_trade_intent_draft",
    "is_risk_increasing_trade_action",
    "get_ct_day_ts",
    "get_futures_daily_flat_context",
    "is_futures_instrument",
    "build_futures_daily_flat_trade_intent_draft",
    "build_futures_rollover_close_trade_intent_draft",
    "process_futures_rollover_close_once",
    "process_futures_daily_flat_once",
    "has_pending_daily_execution_stats",
    "process_signal_events_once",
]
