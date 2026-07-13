from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from config import settings_live as app_settings
from contracts import Instrument
from core.ib_clock import read_ib_clock_health
from core.time_utils import CT_TIMEZONE
from ib_execution.slot_loss_extension_store import (
    has_active_slot_loss_extension_for_instrument,
)
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, SignalWindowMode
from ib_signal.signal_interpretation import MarketSnapshot, read_market_snapshot
from ib_signal.signal_rules_config import SIGNAL_RULES, SIGNAL_RULE_SETTINGS
from ib_signal.signal_schedule import get_slot_start_ts
from ib_trader.trade_intent_repository import (
    has_trade_intent_for_signal,
    has_unresolved_trade_intent_for_instrument,
    request_cancel_pending_entry_limit_for_opposite_signal,
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
    TradeIntentDraft,
    build_time_text_fields_from_ts,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_trader.trade_signal_repository import read_latest_signal_events


SLOT_CLOSE_SOURCE_SIGNAL_ID = -1
SLOT_CLOSE_INTENT_SOURCE = "SLOT_CLOSE"
SLOT_CLOSE_REASON = "slot_close_before_slot_end"

FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID = -2
FUTURES_DAILY_FLAT_INTENT_SOURCE = "FUTURES_DAILY_FLAT"
FUTURES_DAILY_FLAT_REASON = "futures_daily_flat_before_no_trade_hour"

EXTREME_MA_ZONE_CLOSE_SOURCE_SIGNAL_ID = -3
EXTREME_MA_ZONE_CLOSE_INTENT_SOURCE = "EXTREME_MA_ZONE_CLOSE"
EXTREME_MA_ZONE_CLOSE_REASON = "extreme_ma_zone_close_position"

FUTURES_ROLLOVER_CLOSE_SOURCE_SIGNAL_ID = -11
FUTURES_ROLLOVER_CLOSE_INTENT_SOURCE = "FUTURES_ROLLOVER_CLOSE"
FUTURES_ROLLOVER_CLOSE_REASON = "close_non_active_futures_contract_before_new_trading"

FUTURES_NO_NEW_TRADES_HOUR_CT = 15
FUTURES_CLEARING_HOUR_CT = 16

def build_signal_trade_intent_draft(
        *,
        signal: TraderSignalEvent,
        position: PositionSnapshot,
) -> TradeIntentDraft | None:
    if not signal.signal_allowed:
        return None

    if position.side == PositionSide.UNKNOWN:
        return None

    signal_direction = str(signal.direction).upper()
    if signal_direction not in {"LONG", "SHORT"}:
        return None

    signal_order_type = str(signal.order_type).upper()

    # A futures position on a non-active quarterly contract must be closed
    # by the dedicated rollover guard before any signal can open/reverse.
    if (
            position.side in {PositionSide.LONG, PositionSide.SHORT}
            and position.quantity > 0.0
            and position.contract_is_active is False
    ):
        return None

    if position.side == PositionSide.FLAT or position.quantity <= 0.0:
        action = TradeDecisionAction.OPEN_POSITION
        reason = "flat_position_open_by_signal"
        after_side = PositionSide(signal_direction)
        after_qty = 1.0
        order_type = signal_order_type
        limit_price = signal.limit_price if signal_order_type == "LIMIT" else None
        limit_offset_points = signal.limit_offset_points
        ttl_seconds = signal.ttl_seconds

    elif position.side.value == signal_direction:
        return None

    else:
        if signal_order_type == "LIMIT":
            action = TradeDecisionAction.CLOSE_POSITION
            reason = "opposite_limit_signal_close_position_only"
            after_side = PositionSide.FLAT
            after_qty = 0.0
            order_type = "MARKET"
            limit_price = None
            limit_offset_points = None
            ttl_seconds = None

        else:
            action = TradeDecisionAction.REVERSE_POSITION
            reason = "opposite_signal_reverse_position"
            after_side = PositionSide(signal_direction)
            after_qty = max(1.0, float(position.quantity))
            order_type = "MARKET"
            limit_price = None
            limit_offset_points = None
            ttl_seconds = None

    return TradeIntentDraft(
        source_signal_id=signal.source_signal_id,
        instrument_code=signal.instrument_code,
        signal_bar_ts=signal.signal_bar_ts,
        signal_time_ct=signal.signal_time_ct,
        intent_source="SIGNAL",
        action=action,
        reason=reason,
        signal_direction=signal_direction,
        entry_price=float(signal.entry_price),
        potential_end_delta_points=float(signal.potential_end_delta_points),
        regime=signal.regime,
        ma_zone=signal.ma_zone,
        signal_strength=signal.signal_strength,
        order_type=order_type,
        limit_price=limit_price,
        limit_offset_points=limit_offset_points,
        ttl_seconds=ttl_seconds,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=after_side,
        position_after_qty=float(after_qty),
    )

def get_slot_close_context(*, now_ts: int) -> dict | None:
    settings = DEFAULT_SIGNAL_CONFIG

    if settings.signal_window_mode != SignalWindowMode.SLOT:
        return None

    slot_step_seconds = int(settings.slot_step_minutes) * 60
    close_before_seconds = int(settings.slot_close_before_end_seconds)

    if slot_step_seconds <= 0:
        return None

    if close_before_seconds < 0 or close_before_seconds >= slot_step_seconds:
        return None

    slot_start_ts = get_slot_start_ts(
        current_bar_ts=int(now_ts),
        slot_step_minutes=int(settings.slot_step_minutes),
        slot_start_minute_of_day=int(settings.slot_start_minute_of_day),
    )
    slot_end_ts = int(slot_start_ts) + slot_step_seconds
    close_at_ts = int(slot_end_ts) - close_before_seconds

    if close_at_ts <= int(now_ts) < slot_end_ts:
        return {
            "slot_start_ts": int(slot_start_ts),
            "slot_end_ts": int(slot_end_ts),
            "close_at_ts": int(close_at_ts),
            "close_before_seconds": int(close_before_seconds),
        }

    return None

def get_slot_entry_context(*, now_ts: int) -> dict | None:
    settings = DEFAULT_SIGNAL_CONFIG

    if settings.signal_window_mode != SignalWindowMode.SLOT:
        return None

    slot_step_seconds = int(settings.slot_step_minutes) * 60
    slot_back_seconds = int(settings.slot_back_minutes) * 60
    slot_entry_seconds = int(settings.slot_entry_minutes) * 60
    close_before_seconds = int(settings.slot_close_before_end_seconds)

    if slot_step_seconds <= 0:
        return None

    if slot_back_seconds < 0 or slot_entry_seconds < 0:
        return None

    if close_before_seconds < 0 or close_before_seconds >= slot_step_seconds:
        return None

    if slot_back_seconds + slot_entry_seconds > slot_step_seconds:
        return None

    slot_start_ts = get_slot_start_ts(
        current_bar_ts=int(now_ts),
        slot_step_minutes=int(settings.slot_step_minutes),
        slot_start_minute_of_day=int(settings.slot_start_minute_of_day),
    )
    slot_end_ts = int(slot_start_ts) + slot_step_seconds
    entry_start_ts = int(slot_start_ts) + slot_back_seconds
    entry_end_ts = entry_start_ts + slot_entry_seconds
    close_at_ts = int(slot_end_ts) - close_before_seconds
    entry_end_ts = min(entry_end_ts, close_at_ts)

    if entry_start_ts <= int(now_ts) < entry_end_ts:
        return {
            "slot_start_ts": int(slot_start_ts),
            "slot_end_ts": int(slot_end_ts),
            "entry_start_ts": int(entry_start_ts),
            "entry_end_ts": int(entry_end_ts),
            "close_at_ts": int(close_at_ts),
            "slot_back_seconds": int(slot_back_seconds),
            "slot_entry_seconds": int(slot_entry_seconds),
            "close_before_seconds": int(close_before_seconds),
        }

    return None

def is_slot_entry_allowed_now(*, now_ts: int) -> bool:
    settings = DEFAULT_SIGNAL_CONFIG

    if settings.signal_window_mode != SignalWindowMode.SLOT:
        return True

    return get_slot_entry_context(now_ts=now_ts) is not None

def is_risk_increasing_trade_action(action: TradeDecisionAction) -> bool:
    return action in {
        TradeDecisionAction.OPEN_POSITION,
        TradeDecisionAction.REVERSE_POSITION,
    }

def build_slot_close_trade_intent_draft(
        *,
        position: PositionSnapshot,
        close_context: dict,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)

    return TradeIntentDraft(
        source_signal_id=SLOT_CLOSE_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=SLOT_CLOSE_INTENT_SOURCE,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=SLOT_CLOSE_REASON,
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        regime=None,
        ma_zone=None,
        signal_strength="SLOT_CLOSE",
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )

def get_ct_day_ts(*, now_ts: int, hour: int, minute: int = 0, second: int = 0) -> int:
    now_ct = datetime.fromtimestamp(int(now_ts), tz=timezone.utc).astimezone(CT_TIMEZONE)
    target_ct = now_ct.replace(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=0,
    )
    return int(target_ct.astimezone(timezone.utc).timestamp())

def get_futures_daily_flat_context(*, now_ts: int) -> dict | None:
    settings = DEFAULT_SIGNAL_CONFIG
    close_before_seconds = int(settings.slot_close_before_end_seconds)

    no_new_trades_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_NO_NEW_TRADES_HOUR_CT,
    )
    clearing_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_CLEARING_HOUR_CT,
    )
    close_at_ts = no_new_trades_ts - close_before_seconds

    if close_at_ts <= int(now_ts) < clearing_ts:
        return {
            "no_new_trades_ts": int(no_new_trades_ts),
            "clearing_ts": int(clearing_ts),
            "close_at_ts": int(close_at_ts),
            "close_before_seconds": int(close_before_seconds),
        }

    return None

def is_futures_instrument(instrument_code: str) -> bool:
    instrument_row = Instrument.get(str(instrument_code))

    if instrument_row is None:
        return False

    return str(instrument_row.get("secType", "")).upper() == "FUT"

def build_futures_daily_flat_trade_intent_draft(
        *,
        position: PositionSnapshot,
        close_context: dict,
        now_ts: int,
) -> TradeIntentDraft:
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
        regime=None,
        ma_zone=None,
        signal_strength="FUTURES_DAILY_FLAT",
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )

def is_extreme_ma_zone_close_enabled() -> bool:
    return bool(SIGNAL_RULE_SETTINGS["close_position_on_extreme_ma_zone_enabled"])

def get_long_position_extreme_close_ma_zone() -> int:
    return int(SIGNAL_RULES["long_position_extreme_close_ma_zone"])

def get_short_position_extreme_close_ma_zone() -> int:
    return int(SIGNAL_RULES["short_position_extreme_close_ma_zone"])

def should_close_position_on_extreme_ma_zone(
        *,
        position: PositionSnapshot,
        ma_zone: int | None,
) -> bool:
    if ma_zone is None:
        return False

    ma_zone_value = int(ma_zone)

    if position.side == PositionSide.LONG:
        return ma_zone_value >= get_long_position_extreme_close_ma_zone()

    if position.side == PositionSide.SHORT:
        return ma_zone_value <= get_short_position_extreme_close_ma_zone()

    return False

def build_extreme_ma_zone_close_trade_intent_draft(
        *,
        position: PositionSnapshot,
        market: MarketSnapshot,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)

    return TradeIntentDraft(
        source_signal_id=EXTREME_MA_ZONE_CLOSE_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=EXTREME_MA_ZONE_CLOSE_INTENT_SOURCE,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=EXTREME_MA_ZONE_CLOSE_REASON,
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        regime=market.regime,
        ma_zone=market.ma_zone,
        signal_strength="EXTREME_MA_ZONE_CLOSE",
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )

def process_extreme_ma_zone_close_once(conn, *, now_ts: int | None = None) -> list[TradeIntentCreated]:
    if not is_extreme_ma_zone_close_enabled():
        return []

    now_ts = int(time.time() if now_ts is None else now_ts)

    created: list[TradeIntentCreated] = []

    for position in read_open_position_snapshots(conn):
        if has_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=position.instrument_code,
        ):
            continue

        market = read_market_snapshot(
            instrument_code=position.instrument_code,
            signal_bar_ts=now_ts,
        )

        if not should_close_position_on_extreme_ma_zone(
                position=position,
                ma_zone=market.ma_zone,
        ):
            continue

        draft = build_extreme_ma_zone_close_trade_intent_draft(
            position=position,
            market=market,
            now_ts=now_ts,
        )
        created.append(write_trade_intent_and_event(conn, draft))

    return created

def process_slot_close_once(conn, *, now_ts: int | None = None) -> list[TradeIntentCreated]:
    now_ts = int(time.time() if now_ts is None else now_ts)

    # Если включён second-chance механизм, штатным slot-close владеет ib_execution.
    # Это убирает race: trader не создаёт CLOSE_POSITION, пока execution решает
    # закрывать позицию сразу или переводить её в SLOT_LOSS_EXTENSION.
    if bool(DEFAULT_SIGNAL_CONFIG.slot_loss_extension_enabled):
        return []

    close_context = get_slot_close_context(now_ts=now_ts)

    if close_context is None:
        return []

    created: list[TradeIntentCreated] = []

    for position in read_open_position_snapshots(conn):
        if has_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=position.instrument_code,
        ):
            continue

        draft = build_slot_close_trade_intent_draft(
            position=position,
            close_context=close_context,
            now_ts=now_ts,
        )
        created.append(write_trade_intent_and_event(conn, draft))

    return created

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
        regime=None,
        ma_zone=None,
        signal_strength="FUTURES_ROLLOVER_CLOSE",
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
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
    now_ts = int(time.time() if now_ts is None else now_ts)
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

        draft = build_futures_rollover_close_trade_intent_draft(
            position=position,
            now_ts=now_ts,
        )
        created.append(write_trade_intent_and_event(conn, draft))

    return created

def process_futures_daily_flat_once(conn, *, now_ts: int | None = None) -> list[TradeIntentCreated]:
    now_ts = int(time.time() if now_ts is None else now_ts)
    close_context = get_futures_daily_flat_context(now_ts=now_ts)

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

        draft = build_futures_daily_flat_trade_intent_draft(
            position=position,
            close_context=close_context,
            now_ts=now_ts,
        )
        created.append(write_trade_intent_and_event(conn, draft))

    return created

def process_signal_events_once(*, max_signal_age_seconds: int) -> TradeProcessResult:
    signals = read_latest_signal_events(max_signal_age_seconds=max_signal_age_seconds)
    clock_health = read_ib_clock_health(
        max_abs_offset_seconds=app_settings.ib_clock_max_abs_offset_seconds,
        max_sample_age_seconds=app_settings.ib_clock_health_max_age_seconds,
    )
    guard_warnings: list[str] = []

    conn = get_trade_db_connection()

    try:
        initialize_trade_db(conn)

        # ВАЖНО: ib_trader — producer trade_intents, а не владелец исполнения.
        # Не переводим NEW intents в FAILED здесь. Иначе, если ib_execution временно
        # не успел забрать intent, trader сам гасит заявку и создаёт новую по следующему
        # сигналу, засоряя trade_intents пачкой FAILED записей без order_id.
        # Просрочку NEW/SENDING/ACCEPTED должен делать ib_execution через execution_store.
        now_ts = (
            clock_health.corrected_now_ts
            if clock_health.sample is not None
            else int(time.time())
        )
        created: list[TradeIntentCreated] = []
        rejected: list[TradeIntentRejected] = []
        created.extend(process_futures_rollover_close_once(conn, now_ts=now_ts))
        created.extend(process_futures_daily_flat_once(conn, now_ts=now_ts))
        created.extend(process_slot_close_once(conn, now_ts=now_ts))
        created.extend(process_extreme_ma_zone_close_once(conn, now_ts=now_ts))

        for signal in signals:
            if has_trade_intent_for_signal(
                    conn,
                    instrument_code=signal.instrument_code,
                    source_signal_id=signal.source_signal_id,
                    signal_bar_ts=signal.signal_bar_ts,
            ):
                continue

            position = read_position_snapshot(conn, instrument_code=signal.instrument_code)
            freshness = read_position_snapshot_freshness(
                conn,
                instrument_code=signal.instrument_code,
                now_ts=now_ts,
            )

            if has_active_slot_loss_extension_for_instrument(
                    conn,
                    instrument_code=signal.instrument_code,
            ):
                continue

            # Если позиции ещё нет, но висит LIMIT-вход в противоположную сторону,
            # новый сигнал важнее старого лимитника: просим execution отменить pending order.
            cancelled_pending_limit = request_cancel_pending_entry_limit_for_opposite_signal(
                conn,
                signal=signal,
            )
            if cancelled_pending_limit is not None:
                continue

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

            # Нельзя доверять ни FLAT, ни LONG/SHORT, если positions_latest протухла.
            # Иначе можно не только пропустить вход, но и открыть фантомный reverse по старой позиции.
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
                if not clock_health.is_healthy:
                    warning = (
                        f"{signal.instrument_code}: risk-increasing trade blocked by IB clock guard; "
                        f"reason={clock_health.reason}"
                    )
                    if warning not in guard_warnings:
                        guard_warnings.append(warning)
                    continue

                if not is_slot_entry_allowed_now(now_ts=now_ts):
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

__all__ = ['SLOT_CLOSE_SOURCE_SIGNAL_ID', 'SLOT_CLOSE_INTENT_SOURCE', 'SLOT_CLOSE_REASON', 'FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID', 'FUTURES_DAILY_FLAT_INTENT_SOURCE', 'FUTURES_DAILY_FLAT_REASON', 'EXTREME_MA_ZONE_CLOSE_SOURCE_SIGNAL_ID', 'EXTREME_MA_ZONE_CLOSE_INTENT_SOURCE', 'EXTREME_MA_ZONE_CLOSE_REASON', 'FUTURES_ROLLOVER_CLOSE_SOURCE_SIGNAL_ID', 'FUTURES_ROLLOVER_CLOSE_INTENT_SOURCE', 'FUTURES_ROLLOVER_CLOSE_REASON', 'FUTURES_NO_NEW_TRADES_HOUR_CT', 'FUTURES_CLEARING_HOUR_CT', 'build_signal_trade_intent_draft', 'get_slot_close_context', 'get_slot_entry_context', 'is_slot_entry_allowed_now', 'is_risk_increasing_trade_action', 'build_slot_close_trade_intent_draft', 'get_ct_day_ts', 'get_futures_daily_flat_context', 'is_futures_instrument', 'build_futures_daily_flat_trade_intent_draft', 'is_extreme_ma_zone_close_enabled', 'get_long_position_extreme_close_ma_zone', 'get_short_position_extreme_close_ma_zone', 'should_close_position_on_extreme_ma_zone', 'build_extreme_ma_zone_close_trade_intent_draft', 'process_extreme_ma_zone_close_once', 'process_slot_close_once', 'build_futures_rollover_close_trade_intent_draft', 'process_futures_rollover_close_once', 'process_futures_daily_flat_once', 'process_signal_events_once']
