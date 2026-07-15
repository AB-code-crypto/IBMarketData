from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Any

from config import settings_live as app_settings
from core.daily_trading_guard import (
    DAILY_GUARD_STATUS_CLOSING,
    DAILY_GUARD_STATUS_HALTED,
    EffectiveDailyTradingHalt,
    format_daily_halt_warning,
    get_moscow_day_context,
    read_effective_daily_trading_halt,
    trigger_daily_trading_halt,
    update_daily_guard_runtime_state,
    upsert_daily_guard_monitoring,
)
from core.ib_connector import refresh_ib_account_updates
from core.instrument_filters import get_trading_enabled_instrument_codes
from core.logger import get_logger, log_warning
from ib_execution.execution_models import ExecutionResult, TradeIntent
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
)
from ib_execution.order_cancellation import (
    OrderFilledDuringCancellationError,
    cancel_order_and_require_terminal,
)
from ib_execution.order_service import OrderService
from ib_execution.slot_loss_extension import (
    SlotLossExtensionEvent,
    cancel_exit_orders_for_instrument,
    close_market_safely,
)
from ib_position_sync.position_models import BrokerPositionSnapshot
from ib_position_sync.position_store import sync_broker_positions_once
from ib_trader.trade_schema import ORDER_REF_PREFIX, TRADE_INTENTS_TABLE_NAME


logger = get_logger(__name__)

DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID = -12
DAILY_TAKE_PROFIT_INTENT_SOURCE = "DAILY_TAKE_PROFIT"
DAILY_TAKE_PROFIT_REASON = "daily_take_profit_reached_moscow_day"
DAILY_TAKE_PROFIT_MONITOR_INTERVAL_SECONDS = 1.0
DAILY_TAKE_PROFIT_ERROR_LOG_INTERVAL_SECONDS = 60

EXIT_ORDER_REF_SUFFIXES = (
    "_TP",
    "_SL",
    "_EXT_TP",
    "_EXT_SL",
)


# In-memory fail-safe for the execution process. A fresh process starts in the
# blocked state until the first complete Moscow-day PnL evaluation succeeds.
_DAILY_PNL_MONITOR_READY_BY_ACCOUNT: dict[str, bool] = {}
_DAILY_PNL_MONITOR_ERROR_BY_ACCOUNT: dict[str, str | None] = {}


@dataclass(frozen=True)
class DailyPnlSnapshot:
    moscow_day: str
    day_start_ts: int
    day_end_ts: int
    realized_pnl_usd: float
    unrealized_pnl_usd: float
    total_pnl_usd: float
    open_positions: tuple[BrokerPositionSnapshot, ...]


@dataclass(frozen=True)
class DailyTakeProfitEvent:
    event: str
    message: str
    log_level: str = "INFO"
    intent: TradeIntent | None = None
    result: ExecutionResult | None = None


@dataclass(frozen=True)
class DailyTakeProfitCleanupResult:
    halt: EffectiveDailyTradingHalt
    events: tuple[DailyTakeProfitEvent, ...]
    cleanup_completed: bool


def is_daily_take_profit_enabled() -> bool:
    return bool(app_settings.daily_take_profit_enabled)


def mark_daily_take_profit_monitor_health(
        *,
        account_id: str,
        ready: bool,
        error_text: str | None = None,
) -> None:
    key = str(account_id)
    _DAILY_PNL_MONITOR_READY_BY_ACCOUNT[key] = bool(ready)
    _DAILY_PNL_MONITOR_ERROR_BY_ACCOUNT[key] = (
        None if error_text is None else str(error_text)
    )


def is_daily_take_profit_monitor_ready(
        order_service: OrderService,
) -> bool:
    if not is_daily_take_profit_enabled():
        return True
    return bool(
        _DAILY_PNL_MONITOR_READY_BY_ACCOUNT.get(
            str(order_service.account_id),
            False,
        )
    )


def get_daily_take_profit_monitor_error(
        order_service: OrderService,
) -> str | None:
    return _DAILY_PNL_MONITOR_ERROR_BY_ACCOUNT.get(
        str(order_service.account_id)
    )


def get_daily_take_profit_target_usd() -> float:
    target = float(app_settings.daily_take_profit_usd)
    if target <= 0.0 or not math.isfinite(target):
        raise ValueError(
            f"daily_take_profit_usd must be a positive finite value: {target!r}"
        )
    return target


def _trade_event_ts_sql() -> str:
    return "COALESCE(finished_at_ts, sent_at_ts, updated_at_ts, created_at_ts)"


def read_realized_pnl_for_moscow_day(
        *,
        day_start_ts: int,
        day_end_ts: int,
) -> tuple[float, int]:
    conn = get_trade_db_connection()
    try:
        initialize_execution_db(conn)
        event_ts = _trade_event_ts_sql()
        row = conn.execute(
            f"""
            SELECT
                COALESCE(SUM(realized_pnl), 0.0)
                - COALESCE(SUM(
                    CASE
                        -- IB realizedPNL is 0 for a pure opening order, while
                        -- its commission is stored separately. Reverse/close
                        -- realizedPNL already includes that order's commission.
                        WHEN action = 'OPEN_POSITION'
                        THEN total_commission
                        ELSE 0.0
                    END
                ), 0.0) AS net_realized_pnl,
                SUM(
                    CASE
                        WHEN realized_pnl IS NULL THEN 1
                        WHEN action = 'OPEN_POSITION'
                             AND total_commission IS NULL THEN 1
                        ELSE 0
                    END
                ) AS pending_rows
            FROM {TRADE_INTENTS_TABLE_NAME}
            WHERE status = 'EXECUTED'
              AND order_id IS NOT NULL
              AND {event_ts} >= ?
              AND {event_ts} < ?
            """,
            (int(day_start_ts), int(day_end_ts)),
        ).fetchone()
        if row is None:
            return 0.0, 0
        return float(row[0] or 0.0), int(row[1] or 0)
    finally:
        conn.close()


def _portfolio_item_account(item) -> str:
    return str(getattr(item, "account", "") or "").strip()


def _contract_key(contract) -> tuple[int, str]:
    return (
        int(getattr(contract, "conId", 0) or 0),
        str(
            getattr(contract, "localSymbol", "")
            or getattr(contract, "symbol", "")
            or ""
        ),
    )


def _snapshot_key(snapshot: BrokerPositionSnapshot) -> tuple[int, str]:
    return (
        int(snapshot.broker_con_id or 0),
        str(snapshot.broker_contract or ""),
    )


def _find_portfolio_item(
        portfolio_items: list[Any],
        *,
        snapshot: BrokerPositionSnapshot,
        account_id: str,
):
    expected_con_id, expected_symbol = _snapshot_key(snapshot)

    for item in portfolio_items:
        item_account = _portfolio_item_account(item)
        if item_account and item_account != str(account_id):
            continue

        contract = getattr(item, "contract", None)
        if contract is None:
            continue

        item_con_id, item_symbol = _contract_key(contract)
        if expected_con_id > 0 and item_con_id == expected_con_id:
            return item
        if expected_symbol and item_symbol == expected_symbol:
            return item

    return None


def _read_live_portfolio_items(order_service: OrderService) -> list[Any]:
    try:
        return list(
            order_service.ib.portfolio(order_service.account_id) or []
        )
    except Exception as exc:
        raise RuntimeError(
            "cannot read live IB portfolio for daily take-profit: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


async def read_unrealized_pnl_from_portfolio(
        order_service: OrderService,
        *,
        open_positions: list[BrokerPositionSnapshot],
) -> float:
    if not open_positions:
        return 0.0

    portfolio_items = _read_live_portfolio_items(order_service)
    missing_snapshots = [
        snapshot
        for snapshot in open_positions
        if _find_portfolio_item(
            portfolio_items,
            snapshot=snapshot,
            account_id=order_service.account_id,
        ) is None
    ]

    if missing_snapshots:
        try:
            await refresh_ib_account_updates(
                order_service.ib,
                account_id=order_service.account_id,
                force=True,
            )
        except Exception as exc:
            missing_text = ", ".join(
                f"{snapshot.broker_contract}/{snapshot.broker_con_id}"
                for snapshot in missing_snapshots
            )
            raise RuntimeError(
                "live IB portfolio is missing open broker position and "
                "account refresh failed: "
                f"account={order_service.account_id}, "
                f"missing={missing_text}, "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        portfolio_items = _read_live_portfolio_items(order_service)

    total = 0.0
    for snapshot in open_positions:
        item = _find_portfolio_item(
            portfolio_items,
            snapshot=snapshot,
            account_id=order_service.account_id,
        )
        if item is None:
            raise RuntimeError(
                "open broker position has no matching live portfolio item "
                "after account refresh: "
                f"instrument={snapshot.instrument_code}, "
                f"contract={snapshot.broker_contract}, "
                f"conId={snapshot.broker_con_id}, "
                f"account={order_service.account_id}"
            )

        portfolio_position = float(getattr(item, "position", 0.0) or 0.0)
        expected_signed_qty = (
            float(snapshot.quantity)
            if str(snapshot.side).upper() == "LONG"
            else -float(snapshot.quantity)
        )
        if abs(portfolio_position - expected_signed_qty) > 1e-9:
            raise RuntimeError(
                "broker position and portfolio position disagree: "
                f"instrument={snapshot.instrument_code}, "
                f"position_snapshot={expected_signed_qty:g}, "
                f"portfolio={portfolio_position:g}"
            )

        value = float(getattr(item, "unrealizedPNL", float("nan")))
        if not math.isfinite(value):
            raise RuntimeError(
                "live IB unrealizedPNL is not ready: "
                f"instrument={snapshot.instrument_code}, value={value!r}"
            )
        total += value

    return float(total)


async def read_current_daily_pnl_snapshot(
        order_service: OrderService,
        *,
        now_ts: int | None = None,
) -> DailyPnlSnapshot:
    now_value = int(time.time() if now_ts is None else now_ts)
    day = get_moscow_day_context(now_value)

    realized_pnl, pending_stats_rows = read_realized_pnl_for_moscow_day(
        day_start_ts=day.start_ts,
        day_end_ts=day.end_ts,
    )
    if pending_stats_rows > 0:
        raise RuntimeError(
            "daily PnL is waiting for execution commission/PnL reconciliation: "
            f"pending_executed_rows={pending_stats_rows}, moscow_day={day.day_key}"
        )

    snapshots = await sync_broker_positions_once(
        order_service.ib,
        expected_account_id=order_service.account_id,
        force_refresh=False,
    )
    open_positions = [
        snapshot
        for snapshot in snapshots
        if str(snapshot.side).upper() in {"LONG", "SHORT"}
        and float(snapshot.quantity) > 0.0
    ]
    unrealized_pnl = await read_unrealized_pnl_from_portfolio(
        order_service,
        open_positions=open_positions,
    )
    total_pnl = float(realized_pnl + unrealized_pnl)
    if not math.isfinite(total_pnl):
        raise RuntimeError(
            f"daily total PnL is not finite: realized={realized_pnl}, "
            f"unrealized={unrealized_pnl}"
        )

    return DailyPnlSnapshot(
        moscow_day=day.day_key,
        day_start_ts=day.start_ts,
        day_end_ts=day.end_ts,
        realized_pnl_usd=float(realized_pnl),
        unrealized_pnl_usd=float(unrealized_pnl),
        total_pnl_usd=total_pnl,
        open_positions=tuple(open_positions),
    )


async def evaluate_and_trigger_daily_take_profit_once(
        order_service: OrderService,
        *,
        now_ts: int | None = None,
) -> tuple[EffectiveDailyTradingHalt | None, DailyPnlSnapshot | None, bool]:
    if not is_daily_take_profit_enabled():
        return None, None, False

    now_value = int(time.time() if now_ts is None else now_ts)
    existing_halt = read_effective_daily_trading_halt(
        account_id=order_service.account_id,
        now_ts=now_value,
    )
    if existing_halt is not None:
        return existing_halt, None, False

    target = get_daily_take_profit_target_usd()
    snapshot = await read_current_daily_pnl_snapshot(
        order_service,
        now_ts=now_value,
    )
    upsert_daily_guard_monitoring(
        account_id=order_service.account_id,
        moscow_day=snapshot.moscow_day,
        target_usd=target,
        realized_pnl_usd=snapshot.realized_pnl_usd,
        unrealized_pnl_usd=snapshot.unrealized_pnl_usd,
        total_pnl_usd=snapshot.total_pnl_usd,
        now_ts=now_value,
    )

    if snapshot.total_pnl_usd < target:
        return None, snapshot, False

    state, first_trigger = trigger_daily_trading_halt(
        account_id=order_service.account_id,
        moscow_day=snapshot.moscow_day,
        target_usd=target,
        realized_pnl_usd=snapshot.realized_pnl_usd,
        unrealized_pnl_usd=snapshot.unrealized_pnl_usd,
        total_pnl_usd=snapshot.total_pnl_usd,
        now_ts=now_value,
    )
    halt = read_effective_daily_trading_halt(
        account_id=order_service.account_id,
        now_ts=now_value,
    )
    if halt is None:
        raise RuntimeError(
            "daily take-profit state was triggered but effective halt is missing: "
            f"account={state.account_id}, day={state.moscow_day}"
        )
    return halt, snapshot, first_trigger


async def run_daily_take_profit_monitor_loop(
        order_service: OrderService,
        *,
        ib_health,
) -> None:
    last_error_text = ""
    last_error_logged_at = 0.0

    while True:
        try:
            if (
                    not order_service.ib.isConnected()
                    or not bool(ib_health.ib_backend_ok)
            ):
                raise ConnectionError(
                    "IB backend unavailable; daily take-profit monitor paused"
                )

            halt, snapshot, first_trigger = (
                await evaluate_and_trigger_daily_take_profit_once(order_service)
            )
            mark_daily_take_profit_monitor_health(
                account_id=order_service.account_id,
                ready=True,
                error_text=None,
            )
            if first_trigger and halt is not None and snapshot is not None:
                # The execution loop may currently be blocked inside a live LIMIT
                # wait. Persisting cancel_requested here lets that coroutine see the
                # daily halt on its next 0.5-second poll instead of waiting for TTL.
                cancelled_new, cancel_requested = (
                    quarantine_trade_intents_for_daily_halt(halt=halt)
                )
                log_warning(
                    logger,
                    (
                        "DAILY TAKE PROFIT reached; trading halt persisted before cleanup: "
                        f"account={order_service.account_id}, "
                        f"moscow_day={snapshot.moscow_day}, "
                        f"target={get_daily_take_profit_target_usd():.2f}, "
                        f"realized={snapshot.realized_pnl_usd:+.2f}, "
                        f"unrealized={snapshot.unrealized_pnl_usd:+.2f}, "
                        f"total={snapshot.total_pnl_usd:+.2f}, "
                        f"cancelled_new={cancelled_new}, "
                        f"cancel_requested={cancel_requested}"
                    ),
                    to_telegram=True,
                )
            last_error_text = ""

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            mark_daily_take_profit_monitor_health(
                account_id=order_service.account_id,
                ready=False,
                error_text=error_text,
            )
            now_mono = time.monotonic()
            if (
                    error_text != last_error_text
                    or now_mono - last_error_logged_at
                    >= DAILY_TAKE_PROFIT_ERROR_LOG_INTERVAL_SECONDS
            ):
                log_warning(
                    logger,
                    f"daily take-profit monitor cannot evaluate current PnL: {error_text}",
                    to_telegram=False,
                )
                last_error_text = error_text
                last_error_logged_at = now_mono

        await asyncio.sleep(DAILY_TAKE_PROFIT_MONITOR_INTERVAL_SECONDS)


def quarantine_trade_intents_for_daily_halt(
        *,
        halt: EffectiveDailyTradingHalt,
) -> tuple[int, int]:
    now_ts = int(time.time())
    reason = (
        "daily take-profit trading halt: "
        f"moscow_day={halt.state.moscow_day}, "
        f"target={halt.state.target_usd:.2f}"
    )
    conn = get_trade_db_connection()
    try:
        initialize_execution_db(conn)

        before = conn.total_changes
        conn.execute(
            f"""
            UPDATE {TRADE_INTENTS_TABLE_NAME}
            SET
                status = 'CANCELLED',
                cancel_requested = 1,
                cancel_reason = ?,
                cancel_source_signal_id = ?,
                cancel_requested_at_ts = ?,
                error_text = ?,
                updated_at_ts = ?,
                finished_at_ts = ?
            WHERE status = 'NEW'
              AND intent_source != ?
            """,
            (
                reason,
                DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID,
                now_ts,
                reason,
                now_ts,
                now_ts,
                DAILY_TAKE_PROFIT_INTENT_SOURCE,
            ),
        )
        cancelled_new = int(conn.total_changes - before)

        before = conn.total_changes
        conn.execute(
            f"""
            UPDATE {TRADE_INTENTS_TABLE_NAME}
            SET
                cancel_requested = 1,
                cancel_reason = ?,
                cancel_source_signal_id = ?,
                cancel_requested_at_ts = ?,
                updated_at_ts = ?
            WHERE status IN ('SENDING', 'ACCEPTED', 'RECONCILING')
              AND intent_source != ?
              AND COALESCE(cancel_requested, 0) = 0
            """,
            (
                reason,
                DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID,
                now_ts,
                now_ts,
                DAILY_TAKE_PROFIT_INTENT_SOURCE,
            ),
        )
        cancel_requested = int(conn.total_changes - before)
        conn.commit()
        return cancelled_new, cancel_requested
    finally:
        conn.close()


def read_unresolved_daily_close_intents() -> list[dict[str, Any]]:
    conn = get_trade_db_connection()
    try:
        initialize_execution_db(conn)
        rows = conn.execute(
            f"""
            SELECT
                trade_intent_id,
                instrument_code,
                status,
                order_ref,
                order_id
            FROM {TRADE_INTENTS_TABLE_NAME}
            WHERE intent_source = ?
              AND status IN ('NEW', 'SENDING', 'ACCEPTED', 'RECONCILING')
            ORDER BY trade_intent_id
            """,
            (DAILY_TAKE_PROFIT_INTENT_SOURCE,),
        ).fetchall()
        return [
            {
                "trade_intent_id": int(row[0]),
                "instrument_code": str(row[1]),
                "status": str(row[2]).upper(),
                "order_ref": "" if row[3] is None else str(row[3]),
                "order_id": None if row[4] is None else int(row[4]),
            }
            for row in rows
        ]
    finally:
        conn.close()


def collect_live_robot_orders(
        order_service: OrderService,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[int] = set()

    for trade in list(order_service.ib.openTrades() or []):
        order = getattr(trade, "order", None)
        if order is None:
            continue

        order_ref = str(getattr(order, "orderRef", "") or "")
        if not order_ref.startswith(ORDER_REF_PREFIX):
            continue

        order_account = str(getattr(order, "account", "") or "").strip()
        if order_account and order_account != order_service.account_id:
            continue

        order_id = int(getattr(order, "orderId", 0) or 0)
        if order_id <= 0 or order_id in seen:
            continue
        seen.add(order_id)

        result.append({
            "order_id": order_id,
            "order_ref": order_ref,
            "status": str(
                getattr(getattr(trade, "orderStatus", None), "status", "")
                or ""
            ),
        })

    return result


def _is_exit_order_ref(order_ref: str) -> bool:
    return str(order_ref).endswith(EXIT_ORDER_REF_SUFFIXES)


async def cancel_non_exit_robot_orders(
        order_service: OrderService,
        *,
        preserved_order_refs: set[str],
) -> tuple[bool, list[DailyTakeProfitEvent]]:
    events: list[DailyTakeProfitEvent] = []
    cleanup_ok = True

    refresh_ok, refresh_error = (
        await order_service.broker_state.refresh_open_orders(force=True)
    )
    if not refresh_ok:
        return (
            False,
            [
                DailyTakeProfitEvent(
                    event="OPEN_ORDER_REFRESH_FAILED",
                    message=(
                        "daily take-profit cannot refresh open orders before "
                        f"cleanup: {refresh_error}"
                    ),
                    log_level="WARNING",
                )
            ],
        )

    for order in collect_live_robot_orders(order_service):
        order_ref = str(order["order_ref"])
        if order_ref in preserved_order_refs or _is_exit_order_ref(order_ref):
            continue

        order_id = int(order["order_id"])
        try:
            cancellation = await cancel_order_and_require_terminal(
                order_service,
                order_id=order_id,
                context=(
                    "daily take-profit pending robot order cleanup: "
                    f"order_ref={order_ref}"
                ),
            )
            events.append(
                DailyTakeProfitEvent(
                    event="ROBOT_ORDER_CANCELLED",
                    message=(
                        "daily take-profit confirmed pending robot order "
                        f"cancellation: order_id={order_id}, "
                        f"order_ref={order_ref}, "
                        f"broker_status={cancellation.terminal_status}"
                    ),
                )
            )

        except OrderFilledDuringCancellationError as exc:
            # The next forced position snapshot will see the fill and flatten it.
            # Do not complete the halt in this same pass: one more reconciliation
            # cycle must prove both FLAT and absence of robot orders.
            cleanup_ok = False
            events.append(
                DailyTakeProfitEvent(
                    event="ROBOT_ORDER_FILLED_DURING_CANCEL",
                    message=(
                        "daily take-profit detected a pending robot order fill "
                        f"during cancellation: order_id={order_id}, "
                        f"order_ref={order_ref}, error={exc}"
                    ),
                    log_level="WARNING",
                )
            )

        except Exception as exc:
            cleanup_ok = False
            events.append(
                DailyTakeProfitEvent(
                    event="ROBOT_ORDER_CANCEL_UNCONFIRMED",
                    message=(
                        "daily take-profit could not confirm pending robot order "
                        f"cancellation: order_id={order_id}, "
                        f"order_ref={order_ref}, "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    log_level="WARNING",
                )
            )

    refresh_ok, refresh_error = (
        await order_service.broker_state.refresh_open_orders(force=True)
    )
    if not refresh_ok:
        events.append(
            DailyTakeProfitEvent(
                event="OPEN_ORDER_REFRESH_AFTER_CANCEL_FAILED",
                message=(
                    "daily take-profit cannot refresh open orders after "
                    f"pending-order cleanup: {refresh_error}"
                ),
                log_level="WARNING",
            )
        )
        return False, events

    remaining_non_exit_orders = [
        order
        for order in collect_live_robot_orders(order_service)
        if str(order["order_ref"]) not in preserved_order_refs
        and not _is_exit_order_ref(str(order["order_ref"]))
    ]
    if remaining_non_exit_orders:
        cleanup_ok = False
        events.append(
            DailyTakeProfitEvent(
                event="PENDING_ROBOT_ORDERS_STILL_OPEN",
                message=(
                    "daily take-profit cannot finish because non-exit robot "
                    f"orders are still live: {remaining_non_exit_orders}"
                ),
                log_level="WARNING",
            )
        )

    return cleanup_ok, events



def _convert_close_event(event: SlotLossExtensionEvent) -> DailyTakeProfitEvent:
    return DailyTakeProfitEvent(
        event=str(event.event),
        message=str(event.message),
        log_level=str(event.log_level),
        intent=event.intent,
        result=event.result,
    )


async def run_daily_take_profit_cleanup_once(
        order_service: OrderService,
        *,
        halt: EffectiveDailyTradingHalt,
) -> DailyTakeProfitCleanupResult:
    events: list[DailyTakeProfitEvent] = []
    update_daily_guard_runtime_state(
        account_id=halt.state.account_id,
        moscow_day=halt.state.moscow_day,
        status=DAILY_GUARD_STATUS_CLOSING,
        error_text=None,
    )

    cancelled_new, cancel_requested = quarantine_trade_intents_for_daily_halt(
        halt=halt
    )
    if cancelled_new or cancel_requested:
        events.append(
            DailyTakeProfitEvent(
                event="INTENTS_QUARANTINED",
                message=(
                    "daily take-profit quarantined trade intents: "
                    f"cancelled_new={cancelled_new}, "
                    f"cancel_requested={cancel_requested}"
                ),
            )
        )

    unresolved_daily = read_unresolved_daily_close_intents()
    unresolved_by_instrument = {
        str(row["instrument_code"]): row
        for row in unresolved_daily
    }
    preserved_order_refs = {
        str(row["order_ref"])
        for row in unresolved_daily
        if str(row["order_ref"])
    }
    non_exit_cleanup_ok, non_exit_events = (
        await cancel_non_exit_robot_orders(
            order_service,
            preserved_order_refs=preserved_order_refs,
        )
    )
    events.extend(non_exit_events)

    snapshots = await sync_broker_positions_once(
        order_service.ib,
        expected_account_id=order_service.account_id,
        force_refresh=True,
    )

    snapshot_by_code = {
        str(snapshot.instrument_code): snapshot
        for snapshot in snapshots
    }
    for instrument_code in get_trading_enabled_instrument_codes():
        snapshot = snapshot_by_code.get(str(instrument_code))
        if snapshot is None:
            continue

        is_open = (
            str(snapshot.side).upper() in {"LONG", "SHORT"}
            and float(snapshot.quantity) > 0.0
        )

        if not is_open:
            cancel_ok, cancel_events = await cancel_exit_orders_for_instrument(
                order_service=order_service,
                instrument_code=instrument_code,
                now_ts=int(time.time()),
                reason=(
                    "daily take-profit cleanup while broker position is FLAT: "
                    f"moscow_day={halt.state.moscow_day}"
                ),
            )
            events.extend(_convert_close_event(event) for event in cancel_events)
            if not cancel_ok:
                events.append(
                    DailyTakeProfitEvent(
                        event="FLAT_EXIT_ORDER_CLEANUP_FAILED",
                        message=(
                            "daily take-profit cannot finish because exit orders remain: "
                            f"instrument={instrument_code}"
                        ),
                        log_level="WARNING",
                    )
                )
            continue

        if instrument_code in unresolved_by_instrument:
            cancel_ok, cancel_events = await cancel_exit_orders_for_instrument(
                order_service=order_service,
                instrument_code=instrument_code,
                now_ts=int(time.time()),
                reason=(
                    "daily take-profit close is already unresolved; "
                    "remove protective exits and wait for reconciliation"
                ),
            )
            events.extend(_convert_close_event(event) for event in cancel_events)
            if not cancel_ok:
                events.append(
                    DailyTakeProfitEvent(
                        event="UNRESOLVED_CLOSE_EXIT_CLEANUP_FAILED",
                        message=(
                            "daily take-profit unresolved close exists and protective "
                            f"orders could not be removed: instrument={instrument_code}"
                        ),
                        log_level="WARNING",
                    )
                )
            continue

        close_events = await close_market_safely(
            order_service=order_service,
            snapshot=snapshot,
            source_signal_id=DAILY_TAKE_PROFIT_SOURCE_SIGNAL_ID,
            intent_source=DAILY_TAKE_PROFIT_INTENT_SOURCE,
            reason=(
                f"{DAILY_TAKE_PROFIT_REASON}; "
                f"moscow_day={halt.state.moscow_day}; "
                f"target={halt.state.target_usd:.2f}; "
                f"trigger_total={halt.state.total_pnl_usd}"
            ),
            now_ts=int(time.time()),
        )
        events.extend(_convert_close_event(event) for event in close_events)

    final_snapshots = await sync_broker_positions_once(
        order_service.ib,
        expected_account_id=order_service.account_id,
        force_refresh=True,
    )
    open_after = [
        snapshot
        for snapshot in final_snapshots
        if str(snapshot.side).upper() in {"LONG", "SHORT"}
        and float(snapshot.quantity) > 0.0
    ]

    refresh_ok, refresh_error = await order_service.broker_state.refresh_open_orders(
        force=True
    )
    if not refresh_ok:
        state = update_daily_guard_runtime_state(
            account_id=halt.state.account_id,
            moscow_day=halt.state.moscow_day,
            status=DAILY_GUARD_STATUS_CLOSING,
            error_text=f"final open-order refresh failed: {refresh_error}",
        )
        current_halt = read_effective_daily_trading_halt(
            account_id=state.account_id
        )
        if current_halt is None:
            raise RuntimeError("daily halt disappeared during cleanup")
        return DailyTakeProfitCleanupResult(
            halt=current_halt,
            events=tuple(events),
            cleanup_completed=False,
        )

    remaining_orders = collect_live_robot_orders(order_service)
    cleanup_completed = (
        non_exit_cleanup_ok
        and not open_after
        and not remaining_orders
    )

    if cleanup_completed:
        state = update_daily_guard_runtime_state(
            account_id=halt.state.account_id,
            moscow_day=halt.state.moscow_day,
            status=DAILY_GUARD_STATUS_HALTED,
            cleanup_completed=True,
            error_text=None,
        )
        events.append(
            DailyTakeProfitEvent(
                event="HALTED",
                message=(
                    "DAILY TAKE PROFIT cleanup completed: broker positions are FLAT, "
                    "robot orders are closed, trading is halted until the next "
                    f"Moscow day; account={state.account_id}, "
                    f"moscow_day={state.moscow_day}, target={state.target_usd:.2f}, "
                    f"trigger_total={state.total_pnl_usd}"
                ),
                log_level="WARNING",
            )
        )
    else:
        error_text = (
            f"cleanup pending: open_positions="
            f"{[(s.instrument_code, s.side, s.quantity) for s in open_after]}, "
            f"open_robot_orders={remaining_orders}"
        )
        update_daily_guard_runtime_state(
            account_id=halt.state.account_id,
            moscow_day=halt.state.moscow_day,
            status=DAILY_GUARD_STATUS_CLOSING,
            error_text=error_text,
        )

    current_halt = read_effective_daily_trading_halt(
        account_id=order_service.account_id
    )
    if current_halt is None:
        if cleanup_completed and not halt.is_current_moscow_day:
            # A prior-day CLOSING row stops being effective immediately after
            # it becomes HALTED. That is the intended unlock for the new MSK day.
            current_halt = EffectiveDailyTradingHalt(
                state=state,
                current_day=halt.current_day,
                is_current_moscow_day=False,
            )
        else:
            raise RuntimeError(
                "daily halt disappeared after cleanup state update"
            )

    return DailyTakeProfitCleanupResult(
        halt=current_halt,
        events=tuple(events),
        cleanup_completed=cleanup_completed,
    )


def read_current_daily_halt_for_order_service(
        order_service: OrderService,
) -> EffectiveDailyTradingHalt | None:
    if not is_daily_take_profit_enabled():
        return None
    return read_effective_daily_trading_halt(
        account_id=order_service.account_id
    )


def format_daily_take_profit_startup_text() -> str:
    if not is_daily_take_profit_enabled():
        return "daily_take_profit=OFF"
    return (
        "daily_take_profit=ON, "
        f"target_usd={get_daily_take_profit_target_usd():.2f}, "
        "day_timezone=Europe/Moscow"
    )


__all__ = [
    "DAILY_TAKE_PROFIT_INTENT_SOURCE",
    "DailyPnlSnapshot",
    "DailyTakeProfitCleanupResult",
    "DailyTakeProfitEvent",
    "evaluate_and_trigger_daily_take_profit_once",
    "format_daily_halt_warning",
    "format_daily_take_profit_startup_text",
    "get_daily_take_profit_monitor_error",
    "get_daily_take_profit_target_usd",
    "is_daily_take_profit_enabled",
    "is_daily_take_profit_monitor_ready",
    "mark_daily_take_profit_monitor_health",
    "read_current_daily_halt_for_order_service",
    "run_daily_take_profit_cleanup_once",
    "run_daily_take_profit_monitor_loop",
]
