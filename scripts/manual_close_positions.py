import asyncio
import time
import traceback
from dataclasses import dataclass

from config import settings_live as app_settings
from core.ib_account import validate_ib_account_access
from core.ib_connector import connect_ib, disconnect_ib
from core.telegram_sender import TelegramSender
from ib_execution.execution_logic import execute_trade_intent
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_store import (
    initialize_execution_db,
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
    read_trade_intent_submission_state,
    write_trade_intent_execution_result,
)
from ib_execution.order_service import OrderService
from ib_position_sync.position_store import (
    find_position_for_instrument,
    get_trading_enabled_instrument_codes,
    request_broker_positions,
    sync_broker_positions_once,
)
from ib_trader.trade_models import PositionSide, TradeDecisionAction
from ib_trader.trade_intent_repository import write_trade_intent
from ib_trader.trade_schema import (
    TradeIntentDraft,
    get_trade_db_connection,
    initialize_trade_db,
)


# Намеренно без CLI-аргументов: скрипт запускается из корня проекта.
# Использует штатные пути и настройки проекта.
REQUIRE_CONFIRMATION = True
CONFIRMATION_TEXT = "CLOSE"

MANUAL_FLAT_INTENT_SOURCE = "MANUAL_FLAT"
MANUAL_FLAT_REASON = "manual_flat_helper_close_broker_open_position"


class ManualFlatIbSettings:
    """Отдельный clientId для ручного закрытия, чтобы не конфликтовать с сервисами."""

    ib_host = app_settings.ib_host
    ib_port = app_settings.ib_port
    ib_client_id = app_settings.ib_client_id + 80


@dataclass(frozen=True)
class ManualFlatPosition:
    instrument_code: str
    side: PositionSide
    quantity: float
    broker_contract: str | None = None
    broker_account: str | None = None


def now_ts() -> int:
    return int(time.time())


def build_manual_source_signal_id(position_index: int) -> int:
    # Отрицательный служебный id, не пересекается с реальными signal_events.
    # Миллисекунды + index защищают от конфликта при повторном запуске в ту же секунду.
    return -9000000000000 - int(time.time() * 1000) - int(position_index)


def normalize_position_side(value) -> PositionSide:
    if isinstance(value, PositionSide):
        return value

    return PositionSide(str(value).upper())


def format_position(position) -> str:
    extra_parts = []

    broker_contract = getattr(position, "broker_contract", None)
    broker_account = getattr(position, "broker_account", None)

    if broker_contract:
        extra_parts.append(f"contract={broker_contract}")

    if broker_account:
        extra_parts.append(f"account={broker_account}")

    extra_text = f" ({', '.join(extra_parts)})" if extra_parts else ""

    return (
        f"{position.instrument_code} "
        f"{position.side.value}/{float(position.quantity):g}"
        f"{extra_text}"
    )


def format_broker_snapshot(snapshot) -> str:
    side = normalize_position_side(getattr(snapshot, "side"))
    quantity = float(getattr(snapshot, "quantity", 0.0) or 0.0)
    broker_contract = getattr(snapshot, "broker_contract", None)
    broker_account = getattr(snapshot, "broker_account", None)

    extra_parts = []
    if broker_contract:
        extra_parts.append(f"contract={broker_contract}")
    if broker_account:
        extra_parts.append(f"account={broker_account}")

    extra_text = f" ({', '.join(extra_parts)})" if extra_parts else ""

    return f"{snapshot.instrument_code} {side.value}/{quantity:g}{extra_text}"


def format_execution_result(intent: TradeIntent, result: ExecutionResult) -> str:
    return (
        f"intent_id={intent.trade_intent_id}, "
        f"order_ref={intent.order_ref}, "
        f"status={result.status.value}, "
        f"order_id={result.order_id}, "
        f"order_action={result.order_action}, "
        f"order_qty={result.order_quantity}, "
        f"avg_fill={result.avg_fill_price}, "
        f"commission={result.total_commission}, "
        f"realized_pnl={result.realized_pnl}, "
        f"error={result.error_text}"
    )


def format_optional_price(value) -> str:
    if value is None:
        return "n/a"

    return f"{float(value):.2f}"


def format_optional_money(value) -> str:
    if value is None:
        return "n/a"

    return f"{float(value):+.2f} USD"


def build_manual_flat_deal_message(intent: TradeIntent, result: ExecutionResult) -> str:
    before_text = f"{intent.position_before_side}/{float(intent.position_before_qty):g}"
    target_text = f"{intent.target_side}/{float(intent.target_qty):g}"

    return (
        "✅ MANUAL FLAT: позиция закрыта\n"
        f"instrument: {intent.instrument_code}\n"
        f"trade_intent_id: {intent.trade_intent_id}\n"
        f"order_ref: {intent.order_ref}\n"
        f"close: {before_text} -> {target_text}\n"
        f"order: {result.order_action} {result.order_quantity}\n"
        f"avg_fill: {format_optional_price(result.avg_fill_price)}\n"
        f"realized_pnl: {format_optional_money(result.realized_pnl)}\n"
        f"commission: {format_optional_price(result.total_commission)}\n"
        f"status: {result.status.value}"
    )


async def send_message(telegram_sender: TelegramSender, text: str, *, to_deal_status: bool = True) -> None:
    print(text)

    message_thread_id = None
    if to_deal_status:
        message_thread_id = getattr(app_settings, "telegram_message_thread_id_deal_status", None)

    try:
        await telegram_sender.send_text(text, message_thread_id=message_thread_id)
    except Exception:
        # Telegram не должен ломать emergency close.
        pass


async def send_deal_message(telegram_sender: TelegramSender, text: str) -> None:
    print(text)

    message_thread_id = getattr(app_settings, "telegram_message_thread_id_deal", None)

    try:
        await telegram_sender.send_text(text, message_thread_id=message_thread_id)
    except Exception:
        # Telegram не должен ломать emergency close.
        pass


async def read_broker_open_positions(ib) -> tuple[list[ManualFlatPosition], list]:
    """Читает позиции напрямую у IB и выбирает только наши trading_enabled инструменты.

    Важно: этот helper намеренно не смотрит в positions_latest.
    Если position_sync умер или positions_latest устарел, manual_flat всё равно должен закрыть
    фактическую брокерскую позицию по инструментам робота.
    """
    read_ts = now_ts()
    broker_positions = await request_broker_positions(ib)
    instrument_codes = get_trading_enabled_instrument_codes()

    snapshots = [
        find_position_for_instrument(
            broker_positions=broker_positions,
            instrument_code=instrument_code,
            now_ts=read_ts,
        )
        for instrument_code in instrument_codes
    ]

    open_positions: list[ManualFlatPosition] = []

    for snapshot in snapshots:
        side = normalize_position_side(snapshot.side)
        quantity = float(snapshot.quantity)

        if side not in {PositionSide.LONG, PositionSide.SHORT}:
            continue

        if quantity <= 0.0:
            continue

        open_positions.append(
            ManualFlatPosition(
                instrument_code=str(snapshot.instrument_code),
                side=side,
                quantity=quantity,
                broker_contract=snapshot.broker_contract,
                broker_account=snapshot.broker_account,
            )
        )

    return open_positions, snapshots


def require_user_confirmation(positions, *, source_text: str) -> None:
    if not REQUIRE_CONFIRMATION:
        return

    print(f"\nБудут закрыты позиции из {source_text}:")
    for position in positions:
        print(f"  {format_position(position)}")

    print(
        "\nЭто отправит реальные MARKET-заявки в IB/TWS и запишет результат в trade_intents."
    )
    value = input(f"Для продолжения введи {CONFIRMATION_TEXT}: ").strip()

    if value != CONFIRMATION_TEXT:
        raise RuntimeError("Операция отменена пользователем")


def create_manual_flat_intent(position, *, position_index: int) -> TradeIntent:
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)

        source_signal_id = build_manual_source_signal_id(position_index)
        created_at_ts = now_ts()

        draft = TradeIntentDraft(
            source_signal_id=source_signal_id,
            instrument_code=position.instrument_code,
            signal_bar_ts=created_at_ts,
            signal_time_ct=None,
            intent_source=MANUAL_FLAT_INTENT_SOURCE,
            action=TradeDecisionAction.CLOSE_POSITION,
            reason=MANUAL_FLAT_REASON,
            signal_direction="CLOSE",
            entry_price=0.0,
            potential_end_delta_points=0.0,
            regime=None,
            ma_zone=None,
            signal_strength=MANUAL_FLAT_INTENT_SOURCE,
            order_type="MARKET",
            limit_price=None,
            limit_offset_points=None,
            ttl_seconds=None,
            position_before_side=position.side,
            position_before_qty=float(position.quantity),
            position_after_side=PositionSide.FLAT,
            position_after_qty=0.0,
        )

        trade_intent_id = write_trade_intent(conn, draft)

        row = conn.execute(
            """
            SELECT order_ref, created_at_ts
            FROM trade_intents
            WHERE trade_intent_id = ?
            """,
            (int(trade_intent_id),),
        ).fetchone()

        if row is None:
            raise RuntimeError(f"trade_intent_id={trade_intent_id} not found after insert")

        order_ref = "" if row[0] is None else str(row[0]).strip()
        if not order_ref:
            raise RuntimeError(f"trade_intent_id={trade_intent_id} created without order_ref")

        created_at_ts = int(row[1])

        # Важно: сразу переводим в SENDING в той же транзакции до commit.
        # Так штатный execution-сервис, если вдруг запущен, не подхватит этот intent как NEW.
        mark_trade_intent_sending(conn, trade_intent_id=trade_intent_id)

        conn.commit()

        return TradeIntent(
            trade_intent_id=int(trade_intent_id),
            source_signal_id=int(source_signal_id),
            instrument_code=str(position.instrument_code),
            order_ref=order_ref,
            action=TradeDecisionAction.CLOSE_POSITION.value,
            target_side=PositionSide.FLAT.value,
            target_qty=0.0,
            position_before_side=position.side.value,
            position_before_qty=float(position.quantity),
            order_type="MARKET",
            limit_price=None,
            limit_offset_points=None,
            ttl_seconds=None,
            status=ExecutionStatus.SENDING.value,
            created_at_ts=created_at_ts,
        )

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


async def execute_manual_flat_intent(order_service: OrderService, intent: TradeIntent) -> ExecutionResult:
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)

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

        write_trade_intent_execution_result(conn, result=result)
        conn.commit()

        return result

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


async def main() -> None:
    telegram_sender = TelegramSender(app_settings, robot_name="manual_flat")
    ib = None

    try:
        ib, _ = await connect_ib(ManualFlatIbSettings)
        await validate_ib_account_access(
            ib,
            expected_account_id=app_settings.ib_account_id,
        )

        positions, snapshots = await read_broker_open_positions(ib)

        if not positions:
            snapshots_text = "\n".join(
                f"- {format_broker_snapshot(snapshot)}"
                for snapshot in snapshots
            ) or "- нет trading_enabled instruments"

            await send_message(
                telegram_sender,
                (
                    "MANUAL_FLAT: у брокера нет открытых позиций по trading_enabled инструментам. "
                    "Нечего закрывать.\n"
                    "Broker snapshots:\n"
                    f"{snapshots_text}"
                ),
            )
            return

        require_user_confirmation(
            positions,
            source_text="IB broker positions / trading_enabled instruments",
        )

        positions_text = "\n".join(f"- {format_position(position)}" for position in positions)
        await send_message(
            telegram_sender,
            (
                "🚨 MANUAL_FLAT started\n"
                "Источник позиций: IB broker positions / trading_enabled instruments\n"
                f"Будут закрыты позиции:\n{positions_text}"
            ),
        )

        order_service = OrderService(
            ib,
            account_id=app_settings.ib_account_id,
        )

        results = []

        for index, position in enumerate(positions):
            await send_message(
                telegram_sender,
                f"MANUAL_FLAT: создаю close intent для {format_position(position)}",
            )

            intent = create_manual_flat_intent(position, position_index=index)

            await send_message(
                telegram_sender,
                (
                    f"MANUAL_FLAT: intent создан\n"
                    f"instrument={intent.instrument_code}\n"
                    f"trade_intent_id={intent.trade_intent_id}\n"
                    f"order_ref={intent.order_ref}\n"
                    f"before={intent.position_before_side}/{intent.position_before_qty:g}\n"
                    f"target={intent.target_side}/{intent.target_qty:g}"
                ),
            )

            result = await execute_manual_flat_intent(order_service, intent)
            results.append((intent, result))

            icon = "✅" if result.status == ExecutionStatus.EXECUTED else "❌"
            await send_message(
                telegram_sender,
                f"{icon} MANUAL_FLAT result\n{format_execution_result(intent, result)}",
            )

            if result.status == ExecutionStatus.EXECUTED:
                await send_deal_message(
                    telegram_sender,
                    build_manual_flat_deal_message(intent, result),
                )

        snapshots = await sync_broker_positions_once(ib)

        sync_text = "\n".join(
            f"- {snapshot.instrument_code} {snapshot.side}/{float(snapshot.quantity):g}"
            for snapshot in snapshots
        ) or "- нет инструментов"

        has_failed = any(result.status != ExecutionStatus.EXECUTED for _, result in results)
        final_icon = "⚠️" if has_failed else "✅"

        await send_message(
            telegram_sender,
            (
                f"{final_icon} MANUAL_FLAT finished\n"
                "positions_latest после broker sync:\n"
                f"{sync_text}"
            ),
        )

        if has_failed:
            raise RuntimeError("MANUAL_FLAT завершился с ошибками. Смотри trade_intents и сообщения выше.")

    except KeyboardInterrupt:
        await send_message(telegram_sender, "MANUAL_FLAT: остановлен пользователем")
        raise

    except Exception as exc:
        await send_message(
            telegram_sender,
            (
                "❌ MANUAL_FLAT failed\n"
                f"{type(exc).__name__}: {exc}\n"
                f"{traceback.format_exc()}"
            ),
        )
        raise

    finally:
        if ib is not None:
            try:
                disconnect_ib(ib)
            except Exception:
                pass

        try:
            await telegram_sender.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
