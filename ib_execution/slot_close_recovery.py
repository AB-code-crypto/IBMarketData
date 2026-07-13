from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from core.logger import get_logger, log_info, log_warning
from ib_execution.execution_logic import execute_trade_intent
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
    read_trade_intent_submission_state,
    write_trade_intent_execution_result,
)
from ib_execution.order_service import OrderService
from ib_execution.slot_loss_extension import (
    cancel_exit_orders_for_instrument as cancel_all_exit_orders_for_instrument,
)
from ib_execution.slot_loss_extension_store import has_active_slot_loss_extension_for_instrument
from ib_position_sync.position_models import BrokerPositionSnapshot
from ib_position_sync.position_store import (
    sync_broker_positions_once,
)
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, SignalWindowMode
from ib_signal.signal_schedule import get_slot_start_ts
from ib_trader.trade_models import PositionSide, TradeDecisionAction
from ib_trader.trade_intent_repository import write_trade_intent
from ib_trader.trade_schema import (
    TRADE_INTENTS_TABLE_NAME,
    TradeIntentDraft,
    build_time_text_fields_from_ts,
)

logger = get_logger(__name__)

SLOT_CLOSE_RECOVERY_INTERVAL_SECONDS = 30
SLOT_CLOSE_RECOVERY_SOURCE_SIGNAL_ID = -4
SLOT_CLOSE_RECOVERY_INTENT_SOURCE = "SLOT_CLOSE_RECOVERY"
SLOT_CLOSE_RECOVERY_REASON = "slot_close_recovery_after_execution_restart"
SLOT_CLOSE_RECOVERY_SIGNAL_STRENGTH = "SLOT_CLOSE_RECOVERY"

# Важно: sync_broker_positions_once() возвращает только trading_enabled instruments из contracts.
# Поэтому открытая broker-position из этого списка считается позицией нашего робота/счёта.
CLOSE_CONFIGURED_BROKER_POSITIONS_WITHOUT_DB_ENTRY = True
CLOSE_CONFIGURED_BROKER_POSITIONS_ON_DB_MISMATCH = True

@dataclass(frozen=True)
class SlotCloseRecoveryEvent:
    instrument_code: str
    event: str
    message: str
    intent: TradeIntent | None = None
    result: ExecutionResult | None = None
    log_level: str = "INFO"


def is_open_snapshot(snapshot: BrokerPositionSnapshot) -> bool:
    return str(snapshot.side).upper() in {"LONG", "SHORT"} and float(snapshot.quantity) > 0.0


def is_ib_connected(ib) -> bool:
    """Best-effort check: можно ли сейчас отправлять запросы в IB API."""

    for owner in (ib, getattr(ib, "client", None)):
        if owner is None:
            continue

        method = getattr(owner, "isConnected", None)

        if callable(method):
            try:
                return bool(method())
            except Exception:
                continue

    return True


def is_ib_not_connected_exception(exc: BaseException) -> bool:
    """Распознаёт ожидаемый transient-disconnect IB без traceback-spam."""

    seen: set[int] = set()
    current: BaseException | None = exc

    while current is not None and id(current) not in seen:
        seen.add(id(current))

        if isinstance(current, ConnectionError):
            return True

        if "Not connected" in str(current):
            return True

        current = getattr(current, "__cause__", None) or getattr(current, "__context__", None)

    return False


def build_ib_disconnected_recovery_event(*, reason: str) -> SlotCloseRecoveryEvent:
    return SlotCloseRecoveryEvent(
        instrument_code="ALL",
        event="RECOVERY_SKIPPED_IB_DISCONNECTED",
        message=f"slot-close recovery skipped: IB is not connected; {reason}",
        log_level="INFO",
    )


def find_snapshot(
        snapshots: list[BrokerPositionSnapshot],
        *,
        instrument_code: str,
) -> BrokerPositionSnapshot | None:
    for snapshot in snapshots:
        if str(snapshot.instrument_code) == str(instrument_code):
            return snapshot

    return None


def get_slot_context_for_ts(*, signal_bar_ts: int) -> dict[str, Any] | None:
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
        current_bar_ts=int(signal_bar_ts),
        slot_step_minutes=int(settings.slot_step_minutes),
        slot_start_minute_of_day=int(settings.slot_start_minute_of_day),
    )
    slot_end_ts = int(slot_start_ts) + slot_step_seconds
    close_at_ts = int(slot_end_ts) - close_before_seconds

    return {
        "slot_start_ts": int(slot_start_ts),
        "slot_end_ts": int(slot_end_ts),
        "close_at_ts": int(close_at_ts),
        "close_before_seconds": int(close_before_seconds),
    }


def read_latest_executed_position_intent(conn, *, instrument_code: str) -> dict[str, Any] | None:
    """Возвращает последнее EXECUTED-событие, которое меняло позицию по инструменту."""

    row = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            source_signal_id,
            instrument_code,
            signal_bar_ts,
            intent_source,
            action,
            target_side,
            target_qty,
            position_before_side,
            position_before_qty,
            order_type,
            limit_price,
            limit_offset_points,
            ttl_seconds,
            status,
            order_ref,
            created_at_ts,
            COALESCE(finished_at_ts, sent_at_ts, updated_at_ts, created_at_ts) AS event_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND status = 'EXECUTED'
          AND action IN ('OPEN_POSITION', 'REVERSE_POSITION', 'CLOSE_POSITION')
        ORDER BY event_ts DESC, trade_intent_id DESC
        LIMIT 1
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None:
        return None

    return {
        "trade_intent_id": int(row[0]),
        "source_signal_id": int(row[1]),
        "instrument_code": str(row[2]),
        "signal_bar_ts": int(row[3]),
        "intent_source": str(row[4]),
        "action": str(row[5]).upper(),
        "target_side": str(row[6]).upper(),
        "target_qty": float(row[7]),
        "position_before_side": str(row[8]).upper(),
        "position_before_qty": float(row[9]),
        "order_type": str(row[10]).upper(),
        "limit_price": None if row[11] is None else float(row[11]),
        "limit_offset_points": None if row[12] is None else float(row[12]),
        "ttl_seconds": None if row[13] is None else int(row[13]),
        "status": str(row[14]).upper(),
        "order_ref": "" if row[15] is None else str(row[15]),
        "created_at_ts": int(row[16]),
        "event_ts": int(row[17]),
    }


def read_unresolved_trade_intent_for_instrument(conn, *, instrument_code: str) -> dict[str, Any] | None:
    row = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            intent_source,
            action,
            status,
            order_id,
            created_at_ts,
            updated_at_ts,
            sent_at_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND status IN ('NEW', 'SENDING', 'ACCEPTED', 'RECONCILING')
        ORDER BY
            COALESCE(sent_at_ts, updated_at_ts, created_at_ts) DESC,
            trade_intent_id DESC
        LIMIT 1
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None:
        return None

    return {
        "trade_intent_id": int(row[0]),
        "intent_source": str(row[1]),
        "action": str(row[2]),
        "status": str(row[3]),
        "order_id": None if row[4] is None else int(row[4]),
        "created_at_ts": int(row[5]),
        "updated_at_ts": int(row[6]),
        "sent_at_ts": None if row[7] is None else int(row[7]),
    }


def read_trade_intent_by_id(conn, *, trade_intent_id: int) -> TradeIntent:
    row = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            source_signal_id,
            instrument_code,
            order_ref,

            action,
            target_side,
            target_qty,

            position_before_side,
            position_before_qty,

            order_type,
            limit_price,
            limit_offset_points,
            ttl_seconds,

            status,
            created_at_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE trade_intent_id = ?
        LIMIT 1
        """,
        (int(trade_intent_id),),
    ).fetchone()

    if row is None:
        raise RuntimeError(f"trade_intent not found: trade_intent_id={trade_intent_id}")

    order_ref = "" if row[3] is None else str(row[3]).strip()

    if not order_ref:
        raise RuntimeError(f"recovery trade_intent without order_ref: trade_intent_id={trade_intent_id}")

    return TradeIntent(
        trade_intent_id=int(row[0]),
        source_signal_id=int(row[1]),
        instrument_code=str(row[2]),
        order_ref=order_ref,
        action=str(row[4]),
        target_side=str(row[5]),
        target_qty=float(row[6]),
        position_before_side=str(row[7]),
        position_before_qty=float(row[8]),
        order_type=str(row[9]).upper(),
        limit_price=None if row[10] is None else float(row[10]),
        limit_offset_points=None if row[11] is None else float(row[11]),
        ttl_seconds=None if row[12] is None else int(row[12]),
        status=str(row[13]).upper(),
        created_at_ts=int(row[14]),
    )


async def cancel_exit_orders_for_recovery(
        *,
        order_service: OrderService,
        instrument_code: str,
        now_ts: int,
) -> tuple[bool, list[SlotCloseRecoveryEvent]]:
    cleanup_ok, cleanup_events = await cancel_all_exit_orders_for_instrument(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
        reason="slot-close recovery before market close",
    )

    events = [
        SlotCloseRecoveryEvent(
            instrument_code=str(event.instrument_code),
            event=f"EXIT_CLEANUP_{str(event.event).upper()}",
            message=str(event.message),
            intent=event.intent,
            result=event.result,
            log_level=str(event.log_level),
        )
        for event in cleanup_events
    ]
    return bool(cleanup_ok), events












def build_recovery_reason(
        *,
        snapshot: BrokerPositionSnapshot,
        latest_position_intent: dict[str, Any] | None,
        slot_context: dict[str, Any] | None,
        recovery_reason_detail: str,
) -> str:
    parts = [
        SLOT_CLOSE_RECOVERY_REASON,
        recovery_reason_detail,
        f"broker_position={str(snapshot.side).upper()}/{float(snapshot.quantity):g}",
    ]

    if latest_position_intent is not None:
        parts.extend([
            f"latest_db_trade_intent_id={latest_position_intent['trade_intent_id']}",
            f"latest_db_action={latest_position_intent['action']}",
            f"latest_db_target={latest_position_intent['target_side']}/{latest_position_intent['target_qty']:g}",
            f"latest_db_signal_bar_ts={latest_position_intent['signal_bar_ts']}",
        ])
    else:
        parts.append("latest_db_position_event=missing")

    if slot_context is not None:
        parts.extend([
            f"slot_start_ts={slot_context['slot_start_ts']}",
            f"slot_end_ts={slot_context['slot_end_ts']}",
            f"close_at_ts={slot_context['close_at_ts']}",
        ])
    else:
        parts.append("slot_context=missing_or_not_applicable")

    return "; ".join(parts)


def build_recovery_close_trade_intent_draft(
        *,
        snapshot: BrokerPositionSnapshot,
        latest_position_intent: dict[str, Any] | None,
        slot_context: dict[str, Any] | None,
        recovery_reason_detail: str,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)

    return TradeIntentDraft(
        source_signal_id=SLOT_CLOSE_RECOVERY_SOURCE_SIGNAL_ID,
        instrument_code=str(snapshot.instrument_code),
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=SLOT_CLOSE_RECOVERY_INTENT_SOURCE,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=build_recovery_reason(
            snapshot=snapshot,
            latest_position_intent=latest_position_intent,
            slot_context=slot_context,
            recovery_reason_detail=recovery_reason_detail,
        ),
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        regime=None,
        ma_zone=None,
        signal_strength=SLOT_CLOSE_RECOVERY_SIGNAL_STRENGTH,
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        position_before_side=PositionSide(str(snapshot.side).upper()),
        position_before_qty=float(snapshot.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


async def create_and_execute_recovery_close(
        *,
        order_service: OrderService,
        snapshot: BrokerPositionSnapshot,
        latest_position_intent: dict[str, Any] | None,
        slot_context: dict[str, Any] | None,
        recovery_reason_detail: str,
        now_ts: int,
) -> SlotCloseRecoveryEvent:
    conn = get_trade_db_connection()
    intent: TradeIntent | None = None

    try:
        initialize_execution_db(conn)

        draft = build_recovery_close_trade_intent_draft(
            snapshot=snapshot,
            latest_position_intent=latest_position_intent,
            slot_context=slot_context,
            recovery_reason_detail=recovery_reason_detail,
            now_ts=now_ts,
        )
        trade_intent_id = write_trade_intent(conn, draft)
        conn.commit()

        intent = read_trade_intent_by_id(
            conn,
            trade_intent_id=trade_intent_id,
        )

        if str(intent.status).upper() != ExecutionStatus.NEW.value:
            return SlotCloseRecoveryEvent(
                instrument_code=str(snapshot.instrument_code),
                event="RECOVERY_CLOSE_SKIPPED_EXISTING_INTENT",
                message=(
                    f"{snapshot.instrument_code}: recovery close skipped because "
                    f"trade_intent_id={trade_intent_id} already has status={intent.status}"
                ),
                intent=intent,
                result=None,
                log_level="WARNING",
            )

        mark_trade_intent_sending(
            conn,
            trade_intent_id=intent.trade_intent_id,
        )
        conn.commit()

        async def on_order_submitted(order_id: int, order_action: str, order_quantity: int) -> None:
            mark_trade_intent_order_submitted(
                conn,
                trade_intent_id=intent.trade_intent_id,
                order_id=order_id,
                order_action=order_action,
                order_quantity=order_quantity,
            )
            conn.commit()

        try:
            result = await execute_trade_intent(
                order_service=order_service,
                intent=intent,
                order_submitted_callback=on_order_submitted,
            )

        except Exception as exc:
            submission_state = read_trade_intent_submission_state(
                conn,
                trade_intent_id=intent.trade_intent_id,
            ) or {}
            order_id = (
                submission_state.get("order_id")
                or getattr(exc, "order_id", None)
            )
            result = ExecutionResult(
                trade_intent_id=intent.trade_intent_id,
                order_id=order_id,
                order_action=(
                    submission_state.get("order_action")
                    or getattr(exc, "order_action", None)
                ),
                order_quantity=(
                    submission_state.get("order_quantity")
                    or getattr(exc, "order_quantity", None)
                ),
                status=(
                    ExecutionStatus.RECONCILING
                    if order_id is not None
                    else ExecutionStatus.FAILED
                ),
                avg_fill_price=None,
                total_commission=None,
                realized_pnl=None,
                error_text=f"{type(exc).__name__}: {exc}",
            )

        write_trade_intent_execution_result(
            conn,
            result=result,
        )
        conn.commit()

        log_level = "INFO" if result.status == ExecutionStatus.EXECUTED else "WARNING"

        return SlotCloseRecoveryEvent(
            instrument_code=str(snapshot.instrument_code),
            event="RECOVERY_CLOSE_SENT",
            message=(
                f"{snapshot.instrument_code}: recovery close result: "
                f"trade_intent_id={intent.trade_intent_id}, "
                f"status={result.status.value}, order_id={result.order_id}, "
                f"order_action={result.order_action}, order_qty={result.order_quantity}, "
                f"avg_fill={result.avg_fill_price}, error={result.error_text}"
            ),
            intent=intent,
            result=result,
            log_level=log_level,
        )

    finally:
        conn.close()


def build_recovery_decision(
        *,
        snapshot: BrokerPositionSnapshot,
        latest_position_intent: dict[str, Any] | None,
        now_ts: int,
) -> tuple[bool, dict[str, Any] | None, str, list[SlotCloseRecoveryEvent]]:
    """Решает, надо ли закрывать live broker-position.

    Возвращает: should_close, slot_context, recovery_reason_detail, diagnostic_events.
    """

    instrument_code = str(snapshot.instrument_code)
    events: list[SlotCloseRecoveryEvent] = []

    if latest_position_intent is None:
        if not CLOSE_CONFIGURED_BROKER_POSITIONS_WITHOUT_DB_ENTRY:
            events.append(SlotCloseRecoveryEvent(
                instrument_code=instrument_code,
                event="UNKNOWN_BROKER_POSITION",
                message=(
                    f"{instrument_code}: broker has open position {snapshot.side}/{snapshot.quantity:g}, "
                    f"but no EXECUTED DB position event was found; recovery policy forbids auto-close"
                ),
                log_level="WARNING",
            ))
            return False, None, "", events

        reason_detail = "configured instrument broker position has no EXECUTED DB position event; broker truth wins"
        events.append(SlotCloseRecoveryEvent(
            instrument_code=instrument_code,
            event="UNKNOWN_BROKER_POSITION_WILL_CLOSE",
            message=(
                f"{instrument_code}: broker has open position {snapshot.side}/{snapshot.quantity:g}, "
                f"but no EXECUTED DB position event was found; configured-instrument policy will close live broker position"
            ),
            log_level="WARNING",
        ))
        return True, None, reason_detail, events

    if latest_position_intent["action"] == "CLOSE_POSITION":
        if not CLOSE_CONFIGURED_BROKER_POSITIONS_ON_DB_MISMATCH:
            events.append(SlotCloseRecoveryEvent(
                instrument_code=instrument_code,
                event="BROKER_DB_POSITION_MISMATCH",
                message=(
                    f"{instrument_code}: broker has open position {snapshot.side}/{snapshot.quantity:g}, "
                    f"but latest EXECUTED DB position event is CLOSE_POSITION "
                    f"trade_intent_id={latest_position_intent['trade_intent_id']}; policy forbids auto-close"
                ),
                log_level="WARNING",
            ))
            return False, None, "", events

        reason_detail = "broker still has open position after latest EXECUTED DB CLOSE_POSITION; broker truth wins"
        events.append(SlotCloseRecoveryEvent(
            instrument_code=instrument_code,
            event="BROKER_DB_POSITION_MISMATCH_WILL_CLOSE",
            message=(
                f"{instrument_code}: broker has open position {snapshot.side}/{snapshot.quantity:g}, "
                f"but latest EXECUTED DB position event is CLOSE_POSITION "
                f"trade_intent_id={latest_position_intent['trade_intent_id']}; recovery will close live broker position"
            ),
            log_level="WARNING",
        ))
        return True, None, reason_detail, events

    slot_context = get_slot_context_for_ts(
        signal_bar_ts=int(latest_position_intent["signal_bar_ts"]),
    )

    if slot_context is None:
        events.append(SlotCloseRecoveryEvent(
            instrument_code=instrument_code,
            event="RECOVERY_SKIPPED_NO_SLOT_CONTEXT",
            message=(
                f"{instrument_code}: broker has open position {snapshot.side}/{snapshot.quantity:g}, "
                f"but recovery could not calculate slot context from latest DB entry "
                f"trade_intent_id={latest_position_intent['trade_intent_id']}; market close skipped"
            ),
            log_level="WARNING",
        ))
        return False, None, "", events

    side_mismatch = latest_position_intent["target_side"] != str(snapshot.side).upper()
    qty_mismatch = abs(float(snapshot.quantity) - float(latest_position_intent["target_qty"])) > 1e-9
    is_past_close_at = now_ts >= int(slot_context["close_at_ts"])

    if side_mismatch:
        events.append(SlotCloseRecoveryEvent(
            instrument_code=instrument_code,
            event="BROKER_DB_POSITION_SIDE_MISMATCH",
            message=(
                f"{instrument_code}: broker side={snapshot.side}, "
                f"latest DB entry side={latest_position_intent['target_side']}; "
                f"entry_trade_intent_id={latest_position_intent['trade_intent_id']}; "
                f"broker side will be used if recovery closes"
            ),
            log_level="WARNING",
        ))

    if qty_mismatch:
        events.append(SlotCloseRecoveryEvent(
            instrument_code=instrument_code,
            event="BROKER_DB_POSITION_QTY_MISMATCH",
            message=(
                f"{instrument_code}: broker qty={float(snapshot.quantity):g}, "
                f"latest DB entry qty={latest_position_intent['target_qty']:g}; "
                f"entry_trade_intent_id={latest_position_intent['trade_intent_id']}; "
                f"broker qty will be used if recovery closes"
            ),
            log_level="WARNING",
        ))

    if not is_past_close_at:
        return False, slot_context, "", events

    reason_bits = [
        "position is past slot close_at",
        f"entry_trade_intent_id={latest_position_intent['trade_intent_id']}",
    ]

    if side_mismatch:
        reason_bits.append("broker/db side mismatch ignored because broker truth wins")

    if qty_mismatch:
        reason_bits.append("broker/db qty mismatch ignored because broker truth wins")

    return True, slot_context, "; ".join(reason_bits), events


async def run_slot_close_recovery_once(*, order_service: OrderService) -> list[SlotCloseRecoveryEvent]:
    now_ts = int(time.time())
    events: list[SlotCloseRecoveryEvent] = []

    if DEFAULT_SIGNAL_CONFIG.signal_window_mode != SignalWindowMode.SLOT:
        return events

    if not is_ib_connected(order_service.ib):
        return [build_ib_disconnected_recovery_event(reason="pre-flight connection check")]

    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)
        conn.commit()

    finally:
        conn.close()

    try:
        snapshots = await sync_broker_positions_once(
                    order_service.ib,
                    expected_account_id=order_service.account_id,
                    force_refresh=True,
                )
    except Exception as exc:
        if is_ib_not_connected_exception(exc):
            return [build_ib_disconnected_recovery_event(reason="broker position sync failed with transient disconnect")]

        raise

    open_snapshots = [snapshot for snapshot in snapshots if is_open_snapshot(snapshot)]

    if not open_snapshots:
        return events

    for snapshot in open_snapshots:
        instrument_code = str(snapshot.instrument_code)

        conn = get_trade_db_connection()

        try:
            initialize_execution_db(conn)
            conn.commit()

            if has_active_slot_loss_extension_for_instrument(
                    conn,
                    instrument_code=instrument_code,
            ):
                events.append(SlotCloseRecoveryEvent(
                    instrument_code=instrument_code,
                    event="RECOVERY_SKIPPED_SLOT_LOSS_EXTENSION_ACTIVE",
                    message=(
                        f"{instrument_code}: slot-close recovery skipped because "
                        "SLOT_LOSS_EXTENSION is active for this instrument"
                    ),
                ))
                continue

            latest_position_intent = read_latest_executed_position_intent(
                conn,
                instrument_code=instrument_code,
            )

            should_close, slot_context, recovery_reason_detail, decision_events = build_recovery_decision(
                snapshot=snapshot,
                latest_position_intent=latest_position_intent,
                now_ts=now_ts,
            )
            events.extend(decision_events)

            if not should_close:
                continue

            unresolved_intent = read_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=instrument_code,
            )

            if unresolved_intent is not None:
                events.append(SlotCloseRecoveryEvent(
                    instrument_code=instrument_code,
                    event="RECOVERY_SKIPPED_UNRESOLVED_INTENT",
                    message=(
                        f"{instrument_code}: stale/configured broker position detected, but unresolved "
                        f"trade_intent exists: trade_intent_id={unresolved_intent['trade_intent_id']}, "
                        f"source={unresolved_intent['intent_source']}, action={unresolved_intent['action']}, "
                        f"status={unresolved_intent['status']}, order_id={unresolved_intent['order_id']}; "
                        f"recovery will wait"
                    ),
                    log_level="WARNING",
                ))
                continue

        finally:
            conn.close()

        slot_text = (
            f"slot_start_ts={slot_context['slot_start_ts']}, "
            f"slot_end_ts={slot_context['slot_end_ts']}, "
            f"close_at_ts={slot_context['close_at_ts']}, "
            if slot_context is not None
            else "slot_context=missing_or_not_applicable, "
        )
        latest_db_text = (
            f"latest_db_trade_intent_id={latest_position_intent['trade_intent_id']}, "
            f"latest_db_action={latest_position_intent['action']}, "
            if latest_position_intent is not None
            else "latest_db_position_event=missing, "
        )

        events.append(SlotCloseRecoveryEvent(
            instrument_code=instrument_code,
            event="RECOVERY_BROKER_POSITION_DETECTED_FOR_CLOSE",
            message=(
                f"{instrument_code}: recovery will close broker position: "
                f"broker_position={snapshot.side}/{snapshot.quantity:g}, "
                f"{latest_db_text}"
                f"{slot_text}"
                f"reason={recovery_reason_detail}, now_ts={now_ts}"
            ),
            log_level="WARNING",
        ))

        cleanup_ok, cleanup_events = await cancel_exit_orders_for_recovery(
            order_service=order_service,
            instrument_code=instrument_code,
            now_ts=now_ts,
        )
        events.extend(cleanup_events)

        if not cleanup_ok:
            continue

        snapshots_after_tp = await sync_broker_positions_once(
            order_service.ib,
            expected_account_id=order_service.account_id,
            force_refresh=True,
        )
        snapshot_after_tp = find_snapshot(
            snapshots_after_tp,
            instrument_code=instrument_code,
        )

        if snapshot_after_tp is None or not is_open_snapshot(snapshot_after_tp):
            events.append(SlotCloseRecoveryEvent(
                instrument_code=instrument_code,
                event="RECOVERY_CLOSE_SKIPPED_ALREADY_FLAT",
                message=(
                    f"{instrument_code}: recovery did not send market close because "
                    "broker position is already FLAT after exit-order cleanup"
                ),
            ))
            continue

        if str(snapshot_after_tp.side).upper() != str(snapshot.side).upper():
            events.append(SlotCloseRecoveryEvent(
                instrument_code=instrument_code,
                event="RECOVERY_CLOSE_POSITION_SIDE_CHANGED_BEFORE_MARKET_CLOSE",
                message=(
                    f"{instrument_code}: broker position side changed after TP cancellation: "
                    f"before={snapshot.side}/{snapshot.quantity:g}, "
                    f"after={snapshot_after_tp.side}/{snapshot_after_tp.quantity:g}; "
                    f"recovery will close current live broker side/qty"
                ),
                log_level="WARNING",
            ))

        if abs(float(snapshot_after_tp.quantity) - float(snapshot.quantity)) > 1e-9:
            events.append(SlotCloseRecoveryEvent(
                instrument_code=instrument_code,
                event="RECOVERY_CLOSE_POSITION_QTY_CHANGED_BEFORE_MARKET_CLOSE",
                message=(
                    f"{instrument_code}: broker position qty changed after TP cancellation: "
                    f"before={snapshot.side}/{snapshot.quantity:g}, "
                    f"after={snapshot_after_tp.side}/{snapshot_after_tp.quantity:g}; "
                    f"recovery will close current live broker side/qty"
                ),
                log_level="WARNING",
            ))

        recovery_close_event = await create_and_execute_recovery_close(
            order_service=order_service,
            snapshot=snapshot_after_tp,
            latest_position_intent=latest_position_intent,
            slot_context=slot_context,
            recovery_reason_detail=recovery_reason_detail,
            now_ts=int(time.time()),
        )
        events.append(recovery_close_event)

        try:
            snapshots_after_close = await sync_broker_positions_once(
                    order_service.ib,
                    expected_account_id=order_service.account_id,
                    force_refresh=True,
                )
            snapshot_after_close = find_snapshot(
                snapshots_after_close,
                instrument_code=instrument_code,
            )

            if snapshot_after_close is not None and is_open_snapshot(snapshot_after_close):
                events.append(SlotCloseRecoveryEvent(
                    instrument_code=instrument_code,
                    event="RECOVERY_POSITION_STILL_OPEN_AFTER_MARKET_CLOSE",
                    message=(
                        f"{instrument_code}: recovery market close was sent, but broker still has "
                        f"position {snapshot_after_close.side}/{snapshot_after_close.quantity:g}; "
                        f"manual check required"
                    ),
                    log_level="WARNING",
                ))
            else:
                events.append(SlotCloseRecoveryEvent(
                    instrument_code=instrument_code,
                    event="RECOVERY_POSITION_FLAT_AFTER_MARKET_CLOSE",
                    message=f"{instrument_code}: broker position is FLAT after recovery market close",
                ))

        except Exception as exc:
            events.append(SlotCloseRecoveryEvent(
                instrument_code=instrument_code,
                event="RECOVERY_POSITION_RESYNC_FAILED",
                message=(
                    f"{instrument_code}: recovery close was sent, but position resync failed: "
                    f"{type(exc).__name__}: {exc}"
                ),
                log_level="WARNING",
            ))

    return events
