from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_execution.contract_resolver import build_execution_contract
from ib_execution.execution_logic import execute_trade_intent
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    mark_take_profit_order_status,
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
    read_active_take_profit_orders,
    write_trade_intent_execution_result,
)
from ib_execution.order_service import OrderService
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDER_ROLE_STOP_LOSS,
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
    PROTECTIVE_ORDER_STATUS_ACTIVE,
    PROTECTIVE_ORDER_STATUS_CANCELLED,
    initialize_protective_order_db,
    mark_protective_order_status,
    read_active_protective_orders,
    record_protective_order,
)
from ib_execution.slot_loss_extension_store import (
    has_active_slot_loss_extension_for_instrument,
    initialize_slot_loss_extension_db,
    mark_slot_loss_extension_finished,
    read_active_slot_loss_extensions,
    record_slot_loss_extension_started,
)
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_position_sync.position_models import BrokerPositionSnapshot
from ib_position_sync.position_store import (
    is_same_contract_for_instrument,
    sync_broker_positions_once,
)
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, SignalWindowMode
from ib_trader.trade_models import PositionSide, TradeDecisionAction
from ib_trader.trade_store import (
    ORDER_REF_PREFIX,
    TRADE_INTENTS_TABLE_NAME,
    TradeIntentDraft,
    build_time_text_fields_from_ts,
    get_futures_daily_flat_context,
    get_slot_close_context,
    is_futures_instrument,
    write_trade_intent,
)



SLOT_LOSS_EXTENSION_SOURCE_SIGNAL_ID = -6
SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID = -7
SLOT_LOSS_EXTENSION_EXPIRED_SOURCE_SIGNAL_ID = -8

SLOT_LOSS_EXTENSION_INTENT_SOURCE = "SLOT_LOSS_EXTENSION"
SLOT_CLOSE_DECISION_INTENT_SOURCE = "SLOT_CLOSE_DECISION"
SLOT_LOSS_EXTENSION_EXPIRED_INTENT_SOURCE = "SLOT_LOSS_EXTENSION_EXPIRED"

SLOT_LOSS_EXTENSION_REASON = "slot_loss_extension_to_breakeven"
SLOT_CLOSE_PROFIT_REASON = "slot_close_profit_or_flat"
SLOT_CLOSE_LOSS_REJECTED_REASON = "slot_loss_extension_rejected_market_close"
SLOT_LOSS_EXTENSION_EXPIRED_REASON = "slot_loss_extension_deadline_market_close"

EXTENSION_TAKE_PROFIT_ORDER_REF_SUFFIX = "_EXT_TP"
EXTENSION_STOP_LOSS_ORDER_REF_SUFFIX = "_EXT_SL"
EXTENSION_OCA_GROUP_PREFIX = "IBMD_EXT_OCA"
PROTECTIVE_ORDER_TIME_IN_FORCE = "DAY"
OPEN_ORDER_REFRESH_SETTLE_SECONDS = 0.25

SLOT_LOSS_EXTENSION_WATCHDOG_SOURCE_SIGNAL_ID = -9
SLOT_LOSS_EXTENSION_WATCHDOG_INTENT_SOURCE = "SLOT_LOSS_EXTENSION_WATCHDOG"
SLOT_LOSS_EXTENSION_WATCHDOG_STOP_REASON = "slot_loss_extension_watchdog_stop_price_breached"
SLOT_LOSS_EXTENSION_WATCHDOG_TP_REASON = "slot_loss_extension_watchdog_take_profit_price_touched"
SLOT_LOSS_EXTENSION_WATCHDOG_STALE_REASON = "slot_loss_extension_watchdog_price_path_stale"

# Для extension TP/SL нельзя считать ApiPending/PendingSubmit защитой.
# Это промежуточные статусы: ждём реальный working-status или fail/timeout.
EXTENSION_EXIT_ORDER_ACCEPTED_STATUSES = {
    "PreSubmitted",
    "Submitted",
}
EXTENSION_EXIT_ORDER_FILLED_STATUSES = {"Filled"}
EXTENSION_EXIT_ORDER_REJECTED_STATUSES = {
    "ApiCancelled",
    "Cancelled",
    "Inactive",
    "Rejected",
}


@dataclass(frozen=True)
class SlotLossExtensionEvent:
    instrument_code: str
    event: str
    message: str
    intent: TradeIntent | None = None
    result: ExecutionResult | None = None
    log_level: str = "INFO"


@dataclass(frozen=True)
class PricePathStats:
    latest_bar_ts: int
    current_exit_price: float
    max_adverse_price: float
    max_drawdown_points: float
    current_drawdown_points: float
    drawdown_ratio: float


@dataclass(frozen=True)
class ExtensionPrices:
    take_profit_price: float
    stop_loss_price: float


def is_slot_loss_extension_enabled() -> bool:
    return bool(getattr(DEFAULT_SIGNAL_CONFIG, "slot_loss_extension_enabled", False))


def is_slot_mode() -> bool:
    return DEFAULT_SIGNAL_CONFIG.signal_window_mode == SignalWindowMode.SLOT


def is_open_snapshot(snapshot: BrokerPositionSnapshot) -> bool:
    return str(snapshot.side).upper() in {"LONG", "SHORT"} and float(snapshot.quantity) > 0.0


def find_snapshot(
        snapshots: list[BrokerPositionSnapshot],
        *,
        instrument_code: str,
) -> BrokerPositionSnapshot | None:
    for snapshot in snapshots:
        if str(snapshot.instrument_code) == str(instrument_code):
            return snapshot
    return None


def format_points(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}"


def get_setting_float(name: str, default: float) -> float:
    return float(getattr(DEFAULT_SIGNAL_CONFIG, name, default))


def get_extension_deadline_ts(close_context: dict[str, Any]) -> int:
    # В SLOT-режиме следующая торговая половина начинается после wait/back части следующего слота.
    # Поэтому extension живёт до next_slot_start + slot_back_minutes, а не залезает в новое торговое окно.
    slot_back_seconds = int(DEFAULT_SIGNAL_CONFIG.slot_back_minutes) * 60
    if slot_back_seconds <= 0:
        slot_back_seconds = int(getattr(DEFAULT_SIGNAL_CONFIG, "slot_loss_extension_deadline_minutes", 30)) * 60
    return int(close_context["slot_end_ts"]) + slot_back_seconds


def normalize_price_to_tick_floor(*, price: Decimal, price_tick: Decimal) -> float:
    if price_tick <= Decimal("0"):
        return float(price)
    steps = (price / price_tick).to_integral_value(rounding=ROUND_FLOOR)
    return float(steps * price_tick)


def normalize_price_to_tick_ceiling(*, price: Decimal, price_tick: Decimal) -> float:
    if price_tick <= Decimal("0"):
        return float(price)
    steps = (price / price_tick).to_integral_value(rounding=ROUND_CEILING)
    return float(steps * price_tick)


def calculate_extension_prices(*, instrument_code: str, position_side: str, entry_price: float, max_adverse_price: float) -> ExtensionPrices | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None

    price_tick = Decimal(str(instrument_row.get("price_tick", 0.0) or 0.0))
    buffer_points = Decimal(str(get_setting_float("slot_loss_extension_profit_buffer_points", 2.0)))
    entry = Decimal(str(entry_price))
    adverse = Decimal(str(max_adverse_price))
    side = str(position_side).upper()

    if buffer_points <= Decimal("0"):
        return None

    if side == PositionSide.LONG.value:
        raw_take_profit = entry + buffer_points
        raw_stop_loss = adverse
        take_profit = normalize_price_to_tick_floor(price=raw_take_profit, price_tick=price_tick)
        stop_loss = normalize_price_to_tick_ceiling(price=raw_stop_loss, price_tick=price_tick)
        if take_profit <= float(entry) or stop_loss >= float(entry):
            return None
        return ExtensionPrices(take_profit_price=take_profit, stop_loss_price=stop_loss)

    if side == PositionSide.SHORT.value:
        raw_take_profit = entry - buffer_points
        raw_stop_loss = adverse
        take_profit = normalize_price_to_tick_ceiling(price=raw_take_profit, price_tick=price_tick)
        stop_loss = normalize_price_to_tick_floor(price=raw_stop_loss, price_tick=price_tick)
        if take_profit >= float(entry) or stop_loss <= float(entry):
            return None
        return ExtensionPrices(take_profit_price=take_profit, stop_loss_price=stop_loss)

    return None


def read_latest_executed_position_event(conn, *, instrument_code: str) -> dict[str, Any] | None:
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
            status,
            avg_fill_price,
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
        "order_ref": "" if row[3] is None else str(row[3]),
        "action": str(row[4]).upper(),
        "target_side": str(row[5]).upper(),
        "target_qty": float(row[6]),
        "position_before_side": str(row[7]).upper(),
        "position_before_qty": float(row[8]),
        "order_type": str(row[9]).upper(),
        "limit_price": None if row[10] is None else float(row[10]),
        "status": str(row[11]).upper(),
        "avg_fill_price": None if row[12] is None else float(row[12]),
        "created_at_ts": int(row[13]),
        "event_ts": int(row[14]),
    }


def has_unresolved_execution_intent_for_instrument(conn, *, instrument_code: str) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND status IN ('NEW', 'SENDING', 'ACCEPTED')
          AND COALESCE(cancel_requested, 0) = 0
        LIMIT 1
        """,
        (str(instrument_code),),
    ).fetchone()
    return row is not None


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
        raise RuntimeError(f"trade_intent without order_ref: trade_intent_id={trade_intent_id}")

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


def build_market_close_trade_intent_draft(
        *,
        snapshot: BrokerPositionSnapshot,
        source_signal_id: int,
        intent_source: str,
        reason: str,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)

    return TradeIntentDraft(
        source_signal_id=int(source_signal_id),
        instrument_code=str(snapshot.instrument_code),
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=str(intent_source),
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=str(reason),
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        regime=None,
        ma_zone=None,
        signal_strength=str(intent_source),
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        position_before_side=PositionSide(str(snapshot.side).upper()),
        position_before_qty=float(snapshot.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


def read_price_path_stats(
        *,
        instrument_code: str,
        position_side: str,
        entry_price: float,
        entry_ts: int,
        now_ts: int,
) -> PricePathStats | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None

    feature_db_path = Path(get_instrument_feature_db_path(str(instrument_code), instrument_row))
    if not feature_db_path.is_file():
        return None

    conn = open_sqlite_connection(str(feature_db_path), use_wal=True)

    try:
        latest = conn.execute(
            f"""
            SELECT bar_time_ts, mid_close, spread_close
            FROM {MID_PRICE_TABLE_NAME}
            WHERE bar_time_ts >= ?
              AND bar_time_ts <= ?
            ORDER BY bar_time_ts DESC
            LIMIT 1
            """,
            (int(entry_ts), int(now_ts)),
        ).fetchone()

        if latest is None:
            return None

        side = str(position_side).upper()
        entry = float(entry_price)

        if side == PositionSide.LONG.value:
            adverse_row = conn.execute(
                f"""
                SELECT MIN(mid_low - spread_high / 2.0)
                FROM {MID_PRICE_TABLE_NAME}
                WHERE bar_time_ts >= ?
                  AND bar_time_ts <= ?
                """,
                (int(entry_ts), int(now_ts)),
            ).fetchone()
            if adverse_row is None or adverse_row[0] is None:
                return None

            current_exit_price = float(latest[1]) - float(latest[2]) / 2.0
            max_adverse_price = float(adverse_row[0])
            max_drawdown_points = max(0.0, entry - max_adverse_price)
            current_drawdown_points = max(0.0, entry - current_exit_price)

        elif side == PositionSide.SHORT.value:
            adverse_row = conn.execute(
                f"""
                SELECT MAX(mid_high + spread_high / 2.0)
                FROM {MID_PRICE_TABLE_NAME}
                WHERE bar_time_ts >= ?
                  AND bar_time_ts <= ?
                """,
                (int(entry_ts), int(now_ts)),
            ).fetchone()
            if adverse_row is None or adverse_row[0] is None:
                return None

            current_exit_price = float(latest[1]) + float(latest[2]) / 2.0
            max_adverse_price = float(adverse_row[0])
            max_drawdown_points = max(0.0, max_adverse_price - entry)
            current_drawdown_points = max(0.0, current_exit_price - entry)

        else:
            return None

        if max_drawdown_points <= 0.0:
            return PricePathStats(
                latest_bar_ts=int(latest[0]),
                current_exit_price=current_exit_price,
                max_adverse_price=max_adverse_price,
                max_drawdown_points=max_drawdown_points,
                current_drawdown_points=current_drawdown_points,
                drawdown_ratio=0.0,
            )

        return PricePathStats(
            latest_bar_ts=int(latest[0]),
            current_exit_price=current_exit_price,
            max_adverse_price=max_adverse_price,
            max_drawdown_points=max_drawdown_points,
            current_drawdown_points=current_drawdown_points,
            drawdown_ratio=current_drawdown_points / max_drawdown_points,
        )

    finally:
        conn.close()


async def refresh_open_orders_for_extension(order_service: OrderService) -> tuple[bool, str | None]:
    ib = order_service.ib
    errors: list[str] = []

    for method_name in ("reqAllOpenOrdersAsync", "reqOpenOrdersAsync"):
        method = getattr(ib, method_name, None)
        if method is None:
            continue
        try:
            await method()
            await asyncio.sleep(float(OPEN_ORDER_REFRESH_SETTLE_SECONDS))
            return True, None
        except Exception as exc:
            errors.append(f"{method_name}: {type(exc).__name__}: {exc}")

    for method_name in ("reqAllOpenOrders", "reqOpenOrders"):
        method = getattr(ib, method_name, None)
        if method is None:
            continue
        try:
            maybe_awaitable = method()
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
            await asyncio.sleep(float(OPEN_ORDER_REFRESH_SETTLE_SECONDS))
            return True, None
        except Exception as exc:
            errors.append(f"{method_name}: {type(exc).__name__}: {exc}")

    if not errors:
        return False, "IB object has no reqAllOpenOrders/reqOpenOrders method"
    return False, "; ".join(errors)


def collect_live_exit_orders(
        *,
        order_service: OrderService,
        instrument_code: str,
        now_ts: int,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen_order_ids: set[int] = set()

    try:
        trades = list(order_service.ib.openTrades() or [])
    except Exception:
        return result

    for trade in trades:
        order = getattr(trade, "order", None)
        contract = getattr(trade, "contract", None)
        if order is None or contract is None:
            continue

        order_ref = str(getattr(order, "orderRef", "") or "")
        if not order_ref.startswith(ORDER_REF_PREFIX):
            continue
        if not order_ref.endswith(("_TP", "_SL")):
            continue

        if not is_same_contract_for_instrument(
                position_contract=contract,
                instrument_code=instrument_code,
                now_ts=now_ts,
        ):
            continue

        order_id = int(getattr(order, "orderId", 0) or 0)
        if order_id <= 0 or order_id in seen_order_ids:
            continue

        seen_order_ids.add(order_id)
        result.append({
            "order_id": order_id,
            "order_ref": order_ref,
        })

    return result


async def cancel_exit_orders_for_instrument(
        *,
        order_service: OrderService,
        instrument_code: str,
        now_ts: int,
        reason: str,
) -> tuple[bool, list[SlotLossExtensionEvent]]:
    events: list[SlotLossExtensionEvent] = []
    cancelled_order_ids: set[int] = set()

    conn = get_trade_db_connection()
    try:
        initialize_execution_db(conn)
        initialize_protective_order_db(conn)

        for protective_order in read_active_protective_orders(conn, instrument_code=instrument_code):
            order_id = int(protective_order["order_id"])
            try:
                await order_service.cancel_order_id(order_id)
                cancelled_order_ids.add(order_id)
                mark_protective_order_status(
                    conn,
                    order_id=order_id,
                    status=PROTECTIVE_ORDER_STATUS_CANCELLED,
                    error_text=reason,
                )
                events.append(SlotLossExtensionEvent(
                    instrument_code=instrument_code,
                    event="EXIT_ORDER_CANCELLED",
                    message=f"{instrument_code}: slot-loss extension cancelled protective order_id={order_id}: {reason}",
                ))
            except Exception as exc:
                mark_protective_order_status(
                    conn,
                    order_id=order_id,
                    status=PROTECTIVE_ORDER_STATUS_ACTIVE,
                    error_text=f"cancel failed: {type(exc).__name__}: {exc}",
                )
                events.append(SlotLossExtensionEvent(
                    instrument_code=instrument_code,
                    event="EXIT_ORDER_CANCEL_FAILED",
                    message=f"{instrument_code}: failed to cancel protective order_id={order_id}: {type(exc).__name__}: {exc}",
                    log_level="WARNING",
                ))

        for take_profit_order in read_active_take_profit_orders(conn, instrument_code=instrument_code):
            order_id = int(take_profit_order["order_id"])
            if order_id in cancelled_order_ids:
                continue
            try:
                await order_service.cancel_order_id(order_id)
                cancelled_order_ids.add(order_id)
                mark_take_profit_order_status(
                    conn,
                    order_id=order_id,
                    status="CANCELLED",
                    error_text=reason,
                )
                events.append(SlotLossExtensionEvent(
                    instrument_code=instrument_code,
                    event="LEGACY_TAKE_PROFIT_CANCELLED",
                    message=f"{instrument_code}: slot-loss extension cancelled legacy take-profit order_id={order_id}: {reason}",
                ))
            except Exception as exc:
                mark_take_profit_order_status(
                    conn,
                    order_id=order_id,
                    status="ACTIVE",
                    error_text=f"cancel failed: {type(exc).__name__}: {exc}",
                )
                events.append(SlotLossExtensionEvent(
                    instrument_code=instrument_code,
                    event="LEGACY_TAKE_PROFIT_CANCEL_FAILED",
                    message=f"{instrument_code}: failed to cancel legacy take-profit order_id={order_id}: {type(exc).__name__}: {exc}",
                    log_level="WARNING",
                ))

        conn.commit()

    finally:
        conn.close()

    refresh_ok, refresh_error = await refresh_open_orders_for_extension(order_service)
    if not refresh_ok:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="OPEN_ORDER_REFRESH_FAILED",
            message=f"{instrument_code}: cannot refresh broker open orders before slot-loss extension action: {refresh_error}",
            log_level="WARNING",
        ))
        return False, events

    live_exit_orders = collect_live_exit_orders(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
    )

    for live_order in live_exit_orders:
        order_id = int(live_order["order_id"])
        if order_id in cancelled_order_ids:
            continue
        try:
            await order_service.cancel_order_id(order_id)
            cancelled_order_ids.add(order_id)
            events.append(SlotLossExtensionEvent(
                instrument_code=instrument_code,
                event="LIVE_EXIT_ORDER_CANCELLED",
                message=f"{instrument_code}: slot-loss extension cancelled live exit order_id={order_id}, order_ref={live_order['order_ref']}",
            ))
        except Exception as exc:
            events.append(SlotLossExtensionEvent(
                instrument_code=instrument_code,
                event="LIVE_EXIT_ORDER_CANCEL_FAILED",
                message=(
                    f"{instrument_code}: failed to cancel live exit order_id={order_id}, "
                    f"order_ref={live_order['order_ref']}: {type(exc).__name__}: {exc}"
                ),
                log_level="WARNING",
            ))

    refresh_ok, refresh_error = await refresh_open_orders_for_extension(order_service)
    if not refresh_ok:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="OPEN_ORDER_REFRESH_AFTER_CANCEL_FAILED",
            message=f"{instrument_code}: cannot refresh broker open orders after exit-order cancel: {refresh_error}",
            log_level="WARNING",
        ))
        return False, events

    remaining_live_orders = collect_live_exit_orders(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
    )
    if remaining_live_orders:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="LIVE_EXIT_ORDERS_STILL_OPEN",
            message=f"{instrument_code}: live exit orders are still open; market close/extension is blocked: {remaining_live_orders}",
            log_level="WARNING",
        ))
        return False, events

    return True, events


async def create_and_execute_market_close(
        *,
        order_service: OrderService,
        snapshot: BrokerPositionSnapshot,
        source_signal_id: int,
        intent_source: str,
        reason: str,
        now_ts: int,
) -> SlotLossExtensionEvent:
    conn = get_trade_db_connection()
    intent: TradeIntent | None = None

    try:
        initialize_execution_db(conn)
        draft = build_market_close_trade_intent_draft(
            snapshot=snapshot,
            source_signal_id=source_signal_id,
            intent_source=intent_source,
            reason=reason,
            now_ts=now_ts,
        )
        trade_intent_id = write_trade_intent(conn, draft)
        conn.commit()

        intent = read_trade_intent_by_id(conn, trade_intent_id=trade_intent_id)
        if str(intent.status).upper() != ExecutionStatus.NEW.value:
            return SlotLossExtensionEvent(
                instrument_code=str(snapshot.instrument_code),
                event="MARKET_CLOSE_SKIPPED_EXISTING_INTENT",
                message=(
                    f"{snapshot.instrument_code}: slot-loss extension market close skipped because "
                    f"trade_intent_id={trade_intent_id} already has status={intent.status}"
                ),
                intent=intent,
                result=None,
                log_level="WARNING",
            )

        mark_trade_intent_sending(conn, trade_intent_id=intent.trade_intent_id)
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
            result = ExecutionResult(
                trade_intent_id=intent.trade_intent_id,
                order_id=getattr(exc, "order_id", None),
                order_action=None,
                order_quantity=None,
                status=ExecutionStatus.FAILED,
                avg_fill_price=None,
                total_commission=None,
                realized_pnl=None,
                error_text=f"{type(exc).__name__}: {exc}",
            )

        write_trade_intent_execution_result(conn, result=result)
        conn.commit()

        log_level = "INFO" if result.status == ExecutionStatus.EXECUTED else "WARNING"
        return SlotLossExtensionEvent(
            instrument_code=str(snapshot.instrument_code),
            event="MARKET_CLOSE_SENT",
            message=(
                f"{snapshot.instrument_code}: slot-loss extension market close result: "
                f"intent_source={intent_source}, trade_intent_id={intent.trade_intent_id}, "
                f"status={result.status.value}, order_id={result.order_id}, "
                f"order_action={result.order_action}, order_qty={result.order_quantity}, "
                f"avg_fill={result.avg_fill_price}, reason={reason}, error={result.error_text}"
            ),
            intent=intent,
            result=result,
            log_level=log_level,
        )

    finally:
        conn.close()


async def close_market_safely(
        *,
        order_service: OrderService,
        snapshot: BrokerPositionSnapshot,
        source_signal_id: int,
        intent_source: str,
        reason: str,
        now_ts: int,
) -> list[SlotLossExtensionEvent]:
    instrument_code = str(snapshot.instrument_code)
    events: list[SlotLossExtensionEvent] = []

    cancel_ok, cancel_events = await cancel_exit_orders_for_instrument(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
        reason=f"before {intent_source}: {reason}",
    )
    events.extend(cancel_events)

    if not cancel_ok:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="MARKET_CLOSE_SKIPPED_EXIT_ORDERS_NOT_CANCELLED",
            message=f"{instrument_code}: market close skipped because existing exit orders could not be safely cancelled; reason={reason}",
            log_level="WARNING",
        ))
        return events

    snapshots = await sync_broker_positions_once(order_service.ib)
    current_snapshot = find_snapshot(snapshots, instrument_code=instrument_code)
    if current_snapshot is None or not is_open_snapshot(current_snapshot):
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="MARKET_CLOSE_SKIPPED_ALREADY_FLAT",
            message=f"{instrument_code}: market close skipped because broker position is already FLAT after exit-order cancellation",
        ))
        return events

    events.append(await create_and_execute_market_close(
        order_service=order_service,
        snapshot=current_snapshot,
        source_signal_id=source_signal_id,
        intent_source=intent_source,
        reason=reason,
        now_ts=int(time.time()),
    ))
    return events


def build_extension_order_refs(entry_order_ref: str) -> tuple[str, str]:
    return (
        f"{entry_order_ref}{EXTENSION_TAKE_PROFIT_ORDER_REF_SUFFIX}",
        f"{entry_order_ref}{EXTENSION_STOP_LOSS_ORDER_REF_SUFFIX}",
    )


def build_extension_oca_group(*, entry_trade_intent_id: int, instrument_code: str) -> str:
    return f"{EXTENSION_OCA_GROUP_PREFIX}_{int(entry_trade_intent_id)}_{str(instrument_code)}"


def apply_extension_exit_order_safety_flags(order) -> None:
    """Что делает: включает безопасные flags для extension TP/SL, если IB Order их поддерживает."""
    if hasattr(order, "outsideRth"):
        # Для Globex/overnight нельзя полагаться на дефолт TWS. Stop должен триггериться вне RTH.
        setattr(order, "outsideRth", True)


def describe_ib_error(error) -> str:
    if error is None:
        return "missing"

    parts = []
    for attr_name in ("id", "code", "message"):
        value = getattr(error, attr_name, None)
        if value is not None:
            parts.append(f"{attr_name}={value}")
    return ", ".join(parts) if parts else str(error)


def find_known_trade_order_status(order_service: OrderService, *, order_id: int) -> str | None:
    order_id = int(order_id)

    for method_name in ("trades", "openTrades"):
        method = getattr(order_service.ib, method_name, None)
        if method is None:
            continue

        try:
            trades = list(method() or [])
        except Exception:
            continue

        for trade in trades:
            order = getattr(trade, "order", None)
            if order is None:
                continue

            trade_order_id = int(getattr(order, "orderId", 0) or 0)
            if trade_order_id != order_id:
                continue

            return str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")

    return None


async def wait_for_extension_exit_order_working_or_done(
        *,
        order_service: OrderService,
        trade,
        role: str,
        order_ref: str,
) -> str:
    """Ждёт, что extension TP/SL реально принят IB, а не просто локально создан placeOrder()."""
    loop_time = asyncio.get_running_loop().time
    timeout_seconds = get_setting_float("slot_loss_extension_order_accept_timeout_seconds", 5.0)
    poll_interval_seconds = 0.10
    deadline = loop_time() + float(timeout_seconds)
    order_id = int(getattr(getattr(trade, "order", None), "orderId", 0) or 0)

    while True:
        status = str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")

        if status in EXTENSION_EXIT_ORDER_ACCEPTED_STATUSES:
            return status

        if status in EXTENSION_EXIT_ORDER_FILLED_STATUSES:
            return status

        if status in EXTENSION_EXIT_ORDER_REJECTED_STATUSES:
            raise RuntimeError(
                f"extension {role} order was rejected/cancelled by broker: "
                f"order_id={order_id}, order_ref={order_ref}, status={status}, "
                f"ib_error={describe_ib_error(order_service.monitor.last_error(order_id))}"
            )

        if loop_time() >= deadline:
            raise RuntimeError(
                f"extension {role} order was not accepted by broker before timeout: "
                f"order_id={order_id}, order_ref={order_ref}, status={status}, "
                f"accepted_statuses={sorted(EXTENSION_EXIT_ORDER_ACCEPTED_STATUSES)}, "
                f"timeout_seconds={timeout_seconds}, "
                f"ib_error={describe_ib_error(order_service.monitor.last_error(order_id))}"
            )

        await asyncio.sleep(float(poll_interval_seconds))


async def place_extension_exit_orders(
        *,
        order_service: OrderService,
        snapshot: BrokerPositionSnapshot,
        latest_event: dict[str, Any],
        prices: ExtensionPrices,
) -> tuple[int, int, str, str, str]:
    instrument_code = str(snapshot.instrument_code)
    position_side = str(snapshot.side).upper()
    quantity_float = float(snapshot.quantity)

    if quantity_float <= 0.0 or quantity_float != int(quantity_float):
        raise ValueError(f"unsupported extension quantity: {instrument_code} qty={quantity_float!r}")

    quantity = int(quantity_float)
    if position_side == PositionSide.LONG.value:
        order_action = "SELL"
    elif position_side == PositionSide.SHORT.value:
        order_action = "BUY"
    else:
        raise ValueError(f"unsupported extension side: {instrument_code} side={position_side!r}")

    entry_order_ref = str(latest_event["order_ref"])
    take_profit_order_ref, stop_loss_order_ref = build_extension_order_refs(entry_order_ref)
    oca_group = build_extension_oca_group(
        entry_trade_intent_id=int(latest_event["trade_intent_id"]),
        instrument_code=instrument_code,
    )

    # Важно: SL создаётся первым. Между первым и вторым order place позиция должна быть защищена.
    stop_loss_order = order_service.api.build_stop(
        action=order_action,
        quantity=quantity,
        stop_price=float(prices.stop_loss_price),
        time_in_force=PROTECTIVE_ORDER_TIME_IN_FORCE,
    )
    take_profit_order = order_service.api.build_limit(
        action=order_action,
        quantity=quantity,
        limit_price=float(prices.take_profit_price),
        time_in_force=PROTECTIVE_ORDER_TIME_IN_FORCE,
    )

    apply_extension_exit_order_safety_flags(stop_loss_order)
    apply_extension_exit_order_safety_flags(take_profit_order)

    order_service.api.apply_oca_group(
        [stop_loss_order, take_profit_order],
        oca_group=oca_group,
        oca_type=1,
    )

    contract = build_execution_contract(instrument_code=instrument_code)
    contract_q = await order_service.qualify(contract)

    conn = get_trade_db_connection()
    placed_order_ids: list[int] = []

    try:
        initialize_protective_order_db(conn)

        stop_loss_receipt = await order_service.api.place_order(
            contract_q,
            stop_loss_order,
            order_ref=stop_loss_order_ref,
        )
        stop_loss_order_id = int(stop_loss_receipt.order_id)
        placed_order_ids.append(stop_loss_order_id)
        stop_loss_status = await wait_for_extension_exit_order_working_or_done(
            order_service=order_service,
            trade=stop_loss_receipt.trade,
            role="stop-loss",
            order_ref=stop_loss_order_ref,
        )
        if stop_loss_status in EXTENSION_EXIT_ORDER_FILLED_STATUSES:
            raise RuntimeError(
                f"extension stop-loss filled during placement before TP was submitted: "
                f"order_id={stop_loss_order_id}, order_ref={stop_loss_order_ref}"
            )

        record_protective_order(
            conn,
            instrument_code=instrument_code,
            parent_trade_intent_id=int(latest_event["trade_intent_id"]),
            role=PROTECTIVE_ORDER_ROLE_STOP_LOSS,
            order_ref=stop_loss_order_ref,
            order_id=stop_loss_order_id,
            order_action=order_action,
            order_quantity=quantity,
            order_type="STP",
            limit_price=None,
            stop_price=float(prices.stop_loss_price),
            oca_group=oca_group,
        )
        conn.commit()

        take_profit_receipt = await order_service.api.place_order(
            contract_q,
            take_profit_order,
            order_ref=take_profit_order_ref,
        )
        take_profit_order_id = int(take_profit_receipt.order_id)
        placed_order_ids.append(take_profit_order_id)
        take_profit_status = await wait_for_extension_exit_order_working_or_done(
            order_service=order_service,
            trade=take_profit_receipt.trade,
            role="take-profit",
            order_ref=take_profit_order_ref,
        )
        if take_profit_status in EXTENSION_EXIT_ORDER_FILLED_STATUSES:
            raise RuntimeError(
                f"extension take-profit filled during placement: "
                f"order_id={take_profit_order_id}, order_ref={take_profit_order_ref}"
            )

        record_protective_order(
            conn,
            instrument_code=instrument_code,
            parent_trade_intent_id=int(latest_event["trade_intent_id"]),
            role=PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
            order_ref=take_profit_order_ref,
            order_id=take_profit_order_id,
            order_action=order_action,
            order_quantity=quantity,
            order_type="LIMIT",
            limit_price=float(prices.take_profit_price),
            stop_price=None,
            oca_group=oca_group,
        )
        conn.commit()

        return (
            take_profit_order_id,
            stop_loss_order_id,
            take_profit_order_ref,
            stop_loss_order_ref,
            oca_group,
        )

    except Exception:
        # Если второй ордер не встал, не оставляем одиночный SL/TP без пары.
        for order_id in placed_order_ids:
            try:
                known_status = find_known_trade_order_status(
                    order_service,
                    order_id=order_id,
                )
                if known_status in EXTENSION_EXIT_ORDER_FILLED_STATUSES:
                    continue

                await order_service.cancel_order_id(order_id)
                mark_protective_order_status(
                    conn,
                    order_id=order_id,
                    status=PROTECTIVE_ORDER_STATUS_CANCELLED,
                    error_text="cancelled after failed slot-loss extension order placement",
                )
            except Exception:
                pass
        conn.commit()
        raise

    finally:
        conn.close()


async def start_slot_loss_extension(
        *,
        order_service: OrderService,
        snapshot: BrokerPositionSnapshot,
        latest_event: dict[str, Any],
        price_stats: PricePathStats,
        prices: ExtensionPrices,
        close_context: dict[str, Any],
        now_ts: int,
) -> list[SlotLossExtensionEvent]:
    instrument_code = str(snapshot.instrument_code)
    events: list[SlotLossExtensionEvent] = []

    cancel_ok, cancel_events = await cancel_exit_orders_for_instrument(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
        reason="before SLOT_LOSS_EXTENSION protective replacement",
    )
    events.extend(cancel_events)

    if not cancel_ok:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="SLOT_LOSS_EXTENSION_SKIPPED_EXIT_ORDERS_NOT_CANCELLED",
            message=f"{instrument_code}: slot-loss extension skipped because existing exit orders could not be safely cancelled",
            log_level="WARNING",
        ))
        return events

    snapshots = await sync_broker_positions_once(order_service.ib)
    current_snapshot = find_snapshot(snapshots, instrument_code=instrument_code)
    if current_snapshot is None or not is_open_snapshot(current_snapshot):
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="SLOT_LOSS_EXTENSION_SKIPPED_ALREADY_FLAT",
            message=f"{instrument_code}: slot-loss extension skipped because broker position is already FLAT after exit-order cancellation",
        ))
        return events

    if str(current_snapshot.side).upper() != str(snapshot.side).upper():
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="SLOT_LOSS_EXTENSION_POSITION_SIDE_CHANGED",
            message=(
                f"{instrument_code}: side changed before extension placement: "
                f"before={snapshot.side}/{snapshot.quantity:g}, after={current_snapshot.side}/{current_snapshot.quantity:g}; "
                "market close will be used instead"
            ),
            log_level="WARNING",
        ))
        events.extend(await close_market_safely(
            order_service=order_service,
            snapshot=current_snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_loss_extension_rejected_position_side_changed",
            now_ts=int(time.time()),
        ))
        return events

    try:
        (
            take_profit_order_id,
            stop_loss_order_id,
            take_profit_order_ref,
            stop_loss_order_ref,
            oca_group,
        ) = await place_extension_exit_orders(
            order_service=order_service,
            snapshot=current_snapshot,
            latest_event=latest_event,
            prices=prices,
        )
    except Exception as exc:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="SLOT_LOSS_EXTENSION_ORDER_PLACEMENT_FAILED",
            message=f"{instrument_code}: failed to place slot-loss extension TP/SL: {type(exc).__name__}: {exc}",
            log_level="WARNING",
        ))
        events.extend(await close_market_safely(
            order_service=order_service,
            snapshot=current_snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_loss_extension_order_placement_failed_market_close",
            now_ts=int(time.time()),
        ))
        return events

    deadline_ts = get_extension_deadline_ts(close_context)

    conn = get_trade_db_connection()
    try:
        initialize_slot_loss_extension_db(conn)
        extension_id = record_slot_loss_extension_started(
            conn,
            instrument_code=instrument_code,
            entry_trade_intent_id=int(latest_event["trade_intent_id"]),
            entry_order_ref=str(latest_event["order_ref"]),
            position_side=str(current_snapshot.side).upper(),
            position_qty=float(current_snapshot.quantity),
            entry_price=float(latest_event["avg_fill_price"]),
            slot_start_ts=int(close_context["slot_start_ts"]),
            slot_end_ts=int(close_context["slot_end_ts"]),
            close_at_ts=int(close_context["close_at_ts"]),
            deadline_ts=deadline_ts,
            max_adverse_price=float(price_stats.max_adverse_price),
            max_drawdown_points=float(price_stats.max_drawdown_points),
            current_exit_price=float(price_stats.current_exit_price),
            current_drawdown_points=float(price_stats.current_drawdown_points),
            drawdown_ratio=float(price_stats.drawdown_ratio),
            take_profit_price=float(prices.take_profit_price),
            stop_loss_price=float(prices.stop_loss_price),
            take_profit_order_id=take_profit_order_id,
            stop_loss_order_id=stop_loss_order_id,
            take_profit_order_ref=take_profit_order_ref,
            stop_loss_order_ref=stop_loss_order_ref,
            oca_group=oca_group,
            now_ts=now_ts,
        )
        conn.commit()
    finally:
        conn.close()

    events.append(SlotLossExtensionEvent(
        instrument_code=instrument_code,
        event="SLOT_LOSS_EXTENSION_STARTED",
        message=(
            f"{instrument_code}: slot-loss extension started: "
            f"extension_id={extension_id}, entry_trade_intent_id={latest_event['trade_intent_id']}, "
            f"side={current_snapshot.side}, qty={float(current_snapshot.quantity):g}, "
            f"entry={float(latest_event['avg_fill_price']):.2f}, "
            f"current_exit={price_stats.current_exit_price:.2f}, "
            f"max_drawdown={price_stats.max_drawdown_points:.2f}, "
            f"current_drawdown={price_stats.current_drawdown_points:.2f}, "
            f"ratio={price_stats.drawdown_ratio:.3f}, "
            f"tp={prices.take_profit_price:.2f}, sl={prices.stop_loss_price:.2f}, "
            f"tp_order_id={take_profit_order_id}, sl_order_id={stop_loss_order_id}, "
            f"deadline_ts={deadline_ts}"
        ),
        log_level="WARNING",
    ))
    return events


def is_extension_ratio_allowed(price_stats: PricePathStats) -> bool:
    min_ratio = get_setting_float("slot_loss_extension_min_drawdown_ratio", 0.70)
    max_ratio = get_setting_float("slot_loss_extension_max_drawdown_ratio", 0.95)
    return min_ratio <= float(price_stats.drawdown_ratio) <= max_ratio


def calculate_profit_points(*, side: str, entry_price: float, current_exit_price: float) -> float:
    side_value = str(side).upper()
    if side_value == PositionSide.LONG.value:
        return float(current_exit_price) - float(entry_price)
    if side_value == PositionSide.SHORT.value:
        return float(entry_price) - float(current_exit_price)
    return 0.0


def get_price_path_stale_max_seconds() -> int:
    configured = getattr(DEFAULT_SIGNAL_CONFIG, "slot_loss_extension_price_stale_max_seconds", None)
    if configured is not None:
        return max(1, int(configured))
    return max(1, int(getattr(DEFAULT_SIGNAL_CONFIG, "max_job_bar_lag_seconds", 15)))


def is_price_path_fresh(price_stats: PricePathStats, *, now_ts: int) -> bool:
    return int(now_ts) - int(price_stats.latest_bar_ts) <= get_price_path_stale_max_seconds()


def is_extension_price_watchdog_enabled() -> bool:
    return bool(getattr(DEFAULT_SIGNAL_CONFIG, "slot_loss_extension_price_watchdog_enabled", True))


def is_extension_stale_price_fail_safe_enabled() -> bool:
    return bool(getattr(DEFAULT_SIGNAL_CONFIG, "slot_loss_extension_price_watchdog_stale_close_enabled", True))


def read_latest_feature_bar(
        *,
        instrument_code: str,
        start_ts: int,
        now_ts: int,
) -> dict[str, Any] | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None

    feature_db_path = Path(get_instrument_feature_db_path(str(instrument_code), instrument_row))
    if not feature_db_path.is_file():
        return None

    conn = open_sqlite_connection(str(feature_db_path), use_wal=True)
    try:
        row = conn.execute(
            f"""
            SELECT bar_time_ts, mid_close, spread_close
            FROM {MID_PRICE_TABLE_NAME}
            WHERE bar_time_ts >= ?
              AND bar_time_ts <= ?
            ORDER BY bar_time_ts DESC
            LIMIT 1
            """,
            (int(start_ts), int(now_ts)),
        ).fetchone()

        if row is None:
            return None

        return {
            "bar_time_ts": int(row[0]),
            "mid_close": float(row[1]),
            "spread_close": float(row[2]),
        }

    finally:
        conn.close()


def read_first_level_touch_row(
        *,
        instrument_code: str,
        side: str,
        level_kind: str,
        level_price: float,
        start_ts: int,
        now_ts: int,
) -> dict[str, Any] | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None

    feature_db_path = Path(get_instrument_feature_db_path(str(instrument_code), instrument_row))
    if not feature_db_path.is_file():
        return None

    side_value = str(side).upper()
    level_kind_value = str(level_kind).upper()

    if side_value == PositionSide.LONG.value and level_kind_value == "STOP":
        trigger_expr = "(mid_low - spread_high / 2.0)"
        condition_sql = f"{trigger_expr} <= ?"
    elif side_value == PositionSide.LONG.value and level_kind_value == "TAKE_PROFIT":
        trigger_expr = "(mid_high - spread_low / 2.0)"
        condition_sql = f"{trigger_expr} >= ?"
    elif side_value == PositionSide.SHORT.value and level_kind_value == "STOP":
        trigger_expr = "(mid_high + spread_high / 2.0)"
        condition_sql = f"{trigger_expr} >= ?"
    elif side_value == PositionSide.SHORT.value and level_kind_value == "TAKE_PROFIT":
        trigger_expr = "(mid_low + spread_low / 2.0)"
        condition_sql = f"{trigger_expr} <= ?"
    else:
        return None

    conn = open_sqlite_connection(str(feature_db_path), use_wal=True)
    try:
        row = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                {trigger_expr} AS trigger_price,
                mid_open,
                mid_high,
                mid_low,
                mid_close,
                spread_open,
                spread_high,
                spread_low,
                spread_close
            FROM {MID_PRICE_TABLE_NAME}
            WHERE bar_time_ts >= ?
              AND bar_time_ts <= ?
              AND {condition_sql}
            ORDER BY bar_time_ts ASC
            LIMIT 1
            """,
            (int(start_ts), int(now_ts), float(level_price)),
        ).fetchone()

        if row is None:
            return None

        return {
            "bar_time_ts": int(row[0]),
            "trigger_price": float(row[1]),
            "mid_open": float(row[2]),
            "mid_high": float(row[3]),
            "mid_low": float(row[4]),
            "mid_close": float(row[5]),
            "spread_open": float(row[6]),
            "spread_high": float(row[7]),
            "spread_low": float(row[8]),
            "spread_close": float(row[9]),
        }

    finally:
        conn.close()


def build_watchdog_event_from_level_touch(
        *,
        extension: dict[str, Any],
        level_kind: str,
        row: dict[str, Any],
) -> dict[str, Any]:
    instrument_code = str(extension["instrument_code"])
    level_kind_value = str(level_kind).upper()

    if level_kind_value == "STOP":
        return {
            "event": "SLOT_LOSS_EXTENSION_STOP_PRICE_BREACHED",
            "finish_reason": "watchdog_stop_price_breached_market_close_executed",
            "source_signal_id": SLOT_LOSS_EXTENSION_WATCHDOG_SOURCE_SIGNAL_ID,
            "intent_source": SLOT_LOSS_EXTENSION_WATCHDOG_INTENT_SOURCE,
            "reason": (
                f"{SLOT_LOSS_EXTENSION_WATCHDOG_STOP_REASON}; "
                f"extension_id={extension['slot_loss_extension_id']}; "
                f"stop_loss={float(extension['stop_loss_price']):.2f}; "
                f"trigger_price={float(row['trigger_price']):.2f}; "
                f"trigger_bar_ts={int(row['bar_time_ts'])}"
            ),
            "message": (
                f"{instrument_code}: slot-loss extension watchdog detected STOP breach: "
                f"extension_id={extension['slot_loss_extension_id']}, "
                f"stop_loss={float(extension['stop_loss_price']):.2f}, "
                f"trigger_price={float(row['trigger_price']):.2f}, "
                f"trigger_bar_ts={int(row['bar_time_ts'])}; market close will be sent"
            ),
        }

    return {
        "event": "SLOT_LOSS_EXTENSION_TAKE_PROFIT_PRICE_TOUCHED",
        "finish_reason": "watchdog_take_profit_price_touched_market_close_executed",
        "source_signal_id": SLOT_LOSS_EXTENSION_WATCHDOG_SOURCE_SIGNAL_ID,
        "intent_source": SLOT_LOSS_EXTENSION_WATCHDOG_INTENT_SOURCE,
        "reason": (
            f"{SLOT_LOSS_EXTENSION_WATCHDOG_TP_REASON}; "
            f"extension_id={extension['slot_loss_extension_id']}; "
            f"take_profit={float(extension['take_profit_price']):.2f}; "
            f"trigger_price={float(row['trigger_price']):.2f}; "
            f"trigger_bar_ts={int(row['bar_time_ts'])}"
        ),
        "message": (
            f"{instrument_code}: slot-loss extension watchdog detected TP touch while broker position is still open: "
            f"extension_id={extension['slot_loss_extension_id']}, "
            f"take_profit={float(extension['take_profit_price']):.2f}, "
            f"trigger_price={float(row['trigger_price']):.2f}, "
            f"trigger_bar_ts={int(row['bar_time_ts'])}; market close will be sent"
        ),
    }


def read_active_extension_watchdog_event(
        *,
        extension: dict[str, Any],
        now_ts: int,
) -> dict[str, Any] | None:
    if not is_extension_price_watchdog_enabled():
        return None

    instrument_code = str(extension["instrument_code"])
    start_ts = int(extension["created_at_ts"])
    stale_max_seconds = get_price_path_stale_max_seconds()
    latest_start_ts = max(0, int(now_ts) - stale_max_seconds * 2)

    latest_bar = read_latest_feature_bar(
        instrument_code=instrument_code,
        start_ts=latest_start_ts,
        now_ts=now_ts,
    )

    if latest_bar is None:
        if not is_extension_stale_price_fail_safe_enabled():
            return None
        return {
            "event": "SLOT_LOSS_EXTENSION_PRICE_PATH_MISSING",
            "finish_reason": "watchdog_price_path_missing_market_close_executed",
            "source_signal_id": SLOT_LOSS_EXTENSION_WATCHDOG_SOURCE_SIGNAL_ID,
            "intent_source": SLOT_LOSS_EXTENSION_WATCHDOG_INTENT_SOURCE,
            "reason": (
                f"{SLOT_LOSS_EXTENSION_WATCHDOG_STALE_REASON}; "
                f"extension_id={extension['slot_loss_extension_id']}; latest_bar_ts=missing"
            ),
            "message": (
                f"{instrument_code}: slot-loss extension watchdog cannot read fresh price path: "
                f"extension_id={extension['slot_loss_extension_id']}; market close will be sent"
            ),
        }

    latest_age_seconds = int(now_ts) - int(latest_bar["bar_time_ts"])
    if latest_age_seconds > stale_max_seconds:
        if not is_extension_stale_price_fail_safe_enabled():
            return None
        return {
            "event": "SLOT_LOSS_EXTENSION_PRICE_PATH_STALE",
            "finish_reason": "watchdog_price_path_stale_market_close_executed",
            "source_signal_id": SLOT_LOSS_EXTENSION_WATCHDOG_SOURCE_SIGNAL_ID,
            "intent_source": SLOT_LOSS_EXTENSION_WATCHDOG_INTENT_SOURCE,
            "reason": (
                f"{SLOT_LOSS_EXTENSION_WATCHDOG_STALE_REASON}; "
                f"extension_id={extension['slot_loss_extension_id']}; "
                f"latest_bar_ts={int(latest_bar['bar_time_ts'])}; "
                f"age_seconds={latest_age_seconds}; max_age_seconds={stale_max_seconds}"
            ),
            "message": (
                f"{instrument_code}: slot-loss extension watchdog price path is stale: "
                f"extension_id={extension['slot_loss_extension_id']}, "
                f"latest_bar_ts={int(latest_bar['bar_time_ts'])}, "
                f"age_seconds={latest_age_seconds}, max_age_seconds={stale_max_seconds}; "
                "market close will be sent"
            ),
        }

    stop_row = read_first_level_touch_row(
        instrument_code=instrument_code,
        side=str(extension["position_side"]),
        level_kind="STOP",
        level_price=float(extension["stop_loss_price"]),
        start_ts=start_ts,
        now_ts=now_ts,
    )
    take_profit_row = read_first_level_touch_row(
        instrument_code=instrument_code,
        side=str(extension["position_side"]),
        level_kind="TAKE_PROFIT",
        level_price=float(extension["take_profit_price"]),
        start_ts=start_ts,
        now_ts=now_ts,
    )

    candidates: list[tuple[int, int, dict[str, Any]]] = []
    if stop_row is not None:
        candidates.append((int(stop_row["bar_time_ts"]), 0, build_watchdog_event_from_level_touch(
            extension=extension,
            level_kind="STOP",
            row=stop_row,
        )))
    if take_profit_row is not None:
        candidates.append((int(take_profit_row["bar_time_ts"]), 1, build_watchdog_event_from_level_touch(
            extension=extension,
            level_kind="TAKE_PROFIT",
            row=take_profit_row,
        )))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]



async def handle_slot_close_snapshot(
        *,
        order_service: OrderService,
        snapshot: BrokerPositionSnapshot,
        close_context: dict[str, Any],
        latest_event: dict[str, Any] | None,
        now_ts: int,
) -> list[SlotLossExtensionEvent]:
    instrument_code = str(snapshot.instrument_code)

    if latest_event is None:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_close_no_executed_entry_event_market_close",
            now_ts=now_ts,
        )

    if latest_event["action"] == TradeDecisionAction.CLOSE_POSITION.value:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_close_latest_db_event_is_close_broker_still_open_market_close",
            now_ts=now_ts,
        )

    if latest_event["action"] not in {TradeDecisionAction.OPEN_POSITION.value, TradeDecisionAction.REVERSE_POSITION.value}:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason=f"slot_close_unsupported_latest_action_{latest_event['action']}",
            now_ts=now_ts,
        )

    if latest_event["target_side"] != str(snapshot.side).upper():
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_loss_extension_rejected_broker_db_side_mismatch",
            now_ts=now_ts,
        )

    if abs(float(snapshot.quantity) - float(latest_event["target_qty"])) > 1e-9:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_loss_extension_rejected_broker_db_qty_mismatch",
            now_ts=now_ts,
        )

    if is_futures_instrument(instrument_code) and get_futures_daily_flat_context(now_ts=now_ts) is not None:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="futures_daily_flat_overrides_slot_loss_extension",
            now_ts=now_ts,
        )

    entry_price = latest_event.get("avg_fill_price")
    if entry_price is None:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_loss_extension_rejected_missing_avg_fill_price",
            now_ts=now_ts,
        )

    price_stats = read_price_path_stats(
        instrument_code=instrument_code,
        position_side=str(snapshot.side).upper(),
        entry_price=float(entry_price),
        entry_ts=int(latest_event["event_ts"]),
        now_ts=now_ts,
    )
    if price_stats is None:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_loss_extension_rejected_missing_price_path",
            now_ts=now_ts,
        )

    if not is_price_path_fresh(price_stats, now_ts=now_ts):
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason=(
                "slot_loss_extension_rejected_stale_price_path; "
                f"latest_bar_ts={price_stats.latest_bar_ts}; "
                f"age_seconds={int(now_ts) - int(price_stats.latest_bar_ts)}; "
                f"max_age_seconds={get_price_path_stale_max_seconds()}"
            ),
            now_ts=now_ts,
        )

    profit_points = calculate_profit_points(
        side=str(snapshot.side).upper(),
        entry_price=float(entry_price),
        current_exit_price=float(price_stats.current_exit_price),
    )
    if profit_points >= 0.0:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason=f"{SLOT_CLOSE_PROFIT_REASON}; profit_points={profit_points:.2f}",
            now_ts=now_ts,
        )

    if price_stats.max_drawdown_points <= 0.0:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_loss_extension_rejected_no_adverse_move",
            now_ts=now_ts,
        )

    if not is_extension_ratio_allowed(price_stats):
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason=(
                f"{SLOT_CLOSE_LOSS_REJECTED_REASON}; "
                f"drawdown_ratio={price_stats.drawdown_ratio:.3f}; "
                f"current_drawdown={price_stats.current_drawdown_points:.2f}; "
                f"max_drawdown={price_stats.max_drawdown_points:.2f}"
            ),
            now_ts=now_ts,
        )

    prices = calculate_extension_prices(
        instrument_code=instrument_code,
        position_side=str(snapshot.side).upper(),
        entry_price=float(entry_price),
        max_adverse_price=float(price_stats.max_adverse_price),
    )
    if prices is None:
        return await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_MARKET_CLOSE_SOURCE_SIGNAL_ID,
            intent_source=SLOT_CLOSE_DECISION_INTENT_SOURCE,
            reason="slot_loss_extension_rejected_invalid_extension_prices",
            now_ts=now_ts,
        )

    return await start_slot_loss_extension(
        order_service=order_service,
        snapshot=snapshot,
        latest_event=latest_event,
        price_stats=price_stats,
        prices=prices,
        close_context=close_context,
        now_ts=now_ts,
    )


async def process_active_slot_loss_extensions_once(
        *,
        order_service: OrderService,
        now_ts: int,
) -> list[SlotLossExtensionEvent]:
    events: list[SlotLossExtensionEvent] = []
    conn = get_trade_db_connection()

    try:
        initialize_slot_loss_extension_db(conn)
        active_extensions = read_active_slot_loss_extensions(conn)
    finally:
        conn.close()

    if not active_extensions:
        return events

    snapshots = await sync_broker_positions_once(order_service.ib)

    for extension in active_extensions:
        instrument_code = str(extension["instrument_code"])
        snapshot = find_snapshot(snapshots, instrument_code=instrument_code)

        if snapshot is None or not is_open_snapshot(snapshot):
            conn = get_trade_db_connection()
            try:
                mark_slot_loss_extension_finished(
                    conn,
                    slot_loss_extension_id=int(extension["slot_loss_extension_id"]),
                    finish_reason="broker_position_flat",
                    now_ts=now_ts,
                )
                conn.commit()
            finally:
                conn.close()

            events.append(SlotLossExtensionEvent(
                instrument_code=instrument_code,
                event="SLOT_LOSS_EXTENSION_FINISHED_FLAT",
                message=(
                    f"{instrument_code}: slot-loss extension finished because broker position is FLAT; "
                    f"extension_id={extension['slot_loss_extension_id']}"
                ),
            ))
            continue

        watchdog_event = read_active_extension_watchdog_event(
            extension=extension,
            now_ts=now_ts,
        )
        if watchdog_event is not None:
            events.append(SlotLossExtensionEvent(
                instrument_code=instrument_code,
                event=str(watchdog_event["event"]),
                message=str(watchdog_event["message"]),
                log_level="WARNING",
            ))

            close_events = await close_market_safely(
                order_service=order_service,
                snapshot=snapshot,
                source_signal_id=int(watchdog_event["source_signal_id"]),
                intent_source=str(watchdog_event["intent_source"]),
                reason=str(watchdog_event["reason"]),
                now_ts=now_ts,
            )
            events.extend(close_events)

            close_executed = any(
                event.result is not None and event.result.status == ExecutionStatus.EXECUTED
                for event in close_events
            )
            already_flat = any(
                event.event == "MARKET_CLOSE_SKIPPED_ALREADY_FLAT"
                for event in close_events
            )

            if close_executed or already_flat:
                conn = get_trade_db_connection()
                try:
                    mark_slot_loss_extension_finished(
                        conn,
                        slot_loss_extension_id=int(extension["slot_loss_extension_id"]),
                        finish_reason=str(watchdog_event["finish_reason"]),
                        now_ts=int(time.time()),
                    )
                    conn.commit()
                finally:
                    conn.close()

            continue

        if now_ts < int(extension["deadline_ts"]):
            continue

        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="SLOT_LOSS_EXTENSION_DEADLINE_REACHED",
            message=(
                f"{instrument_code}: slot-loss extension deadline reached; "
                f"extension_id={extension['slot_loss_extension_id']}, deadline_ts={extension['deadline_ts']}; "
                "market close will be sent"
            ),
            log_level="WARNING",
        ))

        close_events = await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=SLOT_LOSS_EXTENSION_EXPIRED_SOURCE_SIGNAL_ID,
            intent_source=SLOT_LOSS_EXTENSION_EXPIRED_INTENT_SOURCE,
            reason=SLOT_LOSS_EXTENSION_EXPIRED_REASON,
            now_ts=now_ts,
        )
        events.extend(close_events)

        close_executed = any(
            event.result is not None and event.result.status == ExecutionStatus.EXECUTED
            for event in close_events
        )
        if not close_executed:
            continue

        conn = get_trade_db_connection()
        try:
            mark_slot_loss_extension_finished(
                conn,
                slot_loss_extension_id=int(extension["slot_loss_extension_id"]),
                finish_reason="deadline_market_close_executed",
                now_ts=int(time.time()),
            )
            conn.commit()
        finally:
            conn.close()

    return events


async def process_slot_close_decisions_once(
        *,
        order_service: OrderService,
        now_ts: int,
) -> list[SlotLossExtensionEvent]:
    events: list[SlotLossExtensionEvent] = []
    close_context = get_slot_close_context(now_ts=now_ts)
    if close_context is None:
        return events

    snapshots = await sync_broker_positions_once(order_service.ib)
    open_snapshots = [snapshot for snapshot in snapshots if is_open_snapshot(snapshot)]
    if not open_snapshots:
        return events

    for snapshot in open_snapshots:
        instrument_code = str(snapshot.instrument_code)
        conn = get_trade_db_connection()

        try:
            initialize_execution_db(conn)
            initialize_slot_loss_extension_db(conn)

            if has_active_slot_loss_extension_for_instrument(conn, instrument_code=instrument_code):
                continue

            if has_unresolved_execution_intent_for_instrument(conn, instrument_code=instrument_code):
                events.append(SlotLossExtensionEvent(
                    instrument_code=instrument_code,
                    event="SLOT_CLOSE_DECISION_SKIPPED_UNRESOLVED_INTENT",
                    message=f"{instrument_code}: slot close decision skipped because unresolved trade_intent exists",
                    log_level="WARNING",
                ))
                continue

            latest_event = read_latest_executed_position_event(
                conn,
                instrument_code=instrument_code,
            )

        finally:
            conn.close()

        events.extend(await handle_slot_close_snapshot(
            order_service=order_service,
            snapshot=snapshot,
            close_context=close_context,
            latest_event=latest_event,
            now_ts=now_ts,
        ))

    return events


async def run_slot_loss_extension_once(*, order_service: OrderService) -> list[SlotLossExtensionEvent]:
    if not is_slot_mode():
        return []

    now_ts = int(time.time())
    events: list[SlotLossExtensionEvent] = []

    # Уже активные extensions надо сопровождать всегда, даже если флаг запуска новых выключили.
    events.extend(await process_active_slot_loss_extensions_once(
        order_service=order_service,
        now_ts=now_ts,
    ))

    if not is_slot_loss_extension_enabled():
        return events

    # active-extension processing can take time and can close positions; refresh timestamp before slot-close decision.
    now_ts = int(time.time())
    events.extend(await process_slot_close_decisions_once(
        order_service=order_service,
        now_ts=now_ts,
    ))

    return events
