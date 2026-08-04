import asyncio
import os
import time
import traceback
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Скрипт кладётся в корень IBMarketData рядом с .env.
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env.copy", encoding="utf-8-sig")

from core.ib_account import normalize_account_id, validate_ib_account_access
from core.ib_connector import connect_ib, disconnect_ib, monitor_ib_connection
from core.logger import get_logger, log_info, log_warning, setup_logging
from core.service_instance_lock import service_instance_lock
from ib_execution.contract_resolver import build_execution_contract
from ib_execution.order_service import OrderService
from ib_position_sync.position_store import (
    MultipleBrokerContractPositionsError,
    find_position_for_instrument,
    request_broker_positions,
)


setup_logging()
logger = get_logger(__name__)


# ============================================================
# MVP: только MNQ, только одна итоговая позиция -1 / 0 / +1.
# Каждый ордер копировщика имеет количество ровно 1 контракт.
# ============================================================

INSTRUMENT_CODE = "MNQ"
ORDER_REF = "IBMD_INV_MNQ"
LOOP_SLEEP_SECONDS = 1.0
POSITION_REQUEST_TIMEOUT_SECONDS = 10.0
ORDER_DONE_TIMEOUT_SECONDS = 60.0


# ============================================================
# Два независимых подключения к TWS / IB Gateway.
# Значения читаются из обычного .env проекта.
# ============================================================


def env_text(name: str) -> str:
    value = str(os.getenv(name, "")).strip()
    if not value:
        raise RuntimeError(f"В .env не задана переменная {name}")
    return value


def env_int(name: str) -> int:
    value = env_text(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} должен быть целым числом: {value!r}") from exc


@dataclass(frozen=True)
class IbSettings:
    ib_host: str
    ib_port: int
    ib_client_id: int
    ib_account_id: str


SOURCE_SETTINGS = IbSettings(
    ib_host=env_text("INVERSE_SOURCE_IB_HOST"),
    ib_port=env_int("INVERSE_SOURCE_IB_PORT"),
    ib_client_id=env_int("INVERSE_SOURCE_IB_CLIENT_ID"),
    ib_account_id=env_text("INVERSE_SOURCE_IB_ACCOUNT_ID"),
)

FOLLOWER_SETTINGS = IbSettings(
    ib_host=env_text("INVERSE_FOLLOWER_IB_HOST"),
    ib_port=env_int("INVERSE_FOLLOWER_IB_PORT"),
    ib_client_id=env_int("INVERSE_FOLLOWER_IB_CLIENT_ID"),
    ib_account_id=env_text("INVERSE_FOLLOWER_IB_ACCOUNT_ID"),
)


def validate_settings() -> None:
    source_account = normalize_account_id(SOURCE_SETTINGS.ib_account_id)
    follower_account = normalize_account_id(FOLLOWER_SETTINGS.ib_account_id)

    if source_account == follower_account:
        raise RuntimeError(
            f"Source и follower не могут быть одним счётом: {source_account}"
        )

    same_tws = (
        SOURCE_SETTINGS.ib_host == FOLLOWER_SETTINGS.ib_host
        and SOURCE_SETTINGS.ib_port == FOLLOWER_SETTINGS.ib_port
    )
    if same_tws and SOURCE_SETTINGS.ib_client_id == FOLLOWER_SETTINGS.ib_client_id:
        raise RuntimeError(
            "Для двух подключений к одной TWS нужны разные clientId"
        )


# ============================================================
# Позиции и единственное торговое решение.
# ============================================================


def signed_position(snapshot) -> int:
    side = str(snapshot.side).upper()
    quantity = float(snapshot.quantity)

    if side == "FLAT":
        if quantity != 0.0:
            raise RuntimeError(
                f"Некорректный FLAT snapshot: quantity={quantity:g}"
            )
        return 0

    if quantity != 1.0:
        raise RuntimeError(
            f"Копировщик поддерживает только один контракт MNQ: "
            f"side={side}, quantity={quantity:g}, account={snapshot.broker_account}"
        )

    if side == "LONG":
        return 1
    if side == "SHORT":
        return -1

    raise RuntimeError(f"Неизвестная сторона позиции: {snapshot.side!r}")


def same_contract(source_snapshot, follower_snapshot) -> bool:
    if source_snapshot.broker_con_id and follower_snapshot.broker_con_id:
        return int(source_snapshot.broker_con_id) == int(
            follower_snapshot.broker_con_id
        )

    return (
        bool(source_snapshot.broker_contract)
        and source_snapshot.broker_contract == follower_snapshot.broker_contract
    )


def choose_order(source_snapshot, follower_snapshot):
    """Возвращает (BUY/SELL, snapshot_контракта, reason) либо None.

    Разворот делается двумя рыночными ордерами по одному контракту:
    сначала закрывается follower, затем после нового чтения позиций
    открывается обратная позиция.
    """
    source_position = signed_position(source_snapshot)
    follower_position = signed_position(follower_snapshot)
    target_position = -source_position

    if follower_position == 0:
        if target_position == 0:
            return None

        action = "BUY" if target_position > 0 else "SELL"
        return action, source_snapshot, "OPEN_INVERSE"

    if (
        follower_position == target_position
        and same_contract(source_snapshot, follower_snapshot)
    ):
        return None

    action = "SELL" if follower_position > 0 else "BUY"

    if target_position == 0:
        reason = "CLOSE_TO_FLAT"
    elif follower_position != target_position:
        reason = "REVERSE_STEP_CLOSE"
    else:
        reason = "CHANGE_CONTRACT_STEP_CLOSE"

    return action, follower_snapshot, reason


def format_position(snapshot) -> str:
    value = signed_position(snapshot)
    if value == 0:
        return "FLAT"

    side = "LONG" if value > 0 else "SHORT"
    contract = snapshot.broker_contract or str(snapshot.broker_con_id or "n/a")
    return f"{side} 1 {contract}"


async def read_positions_once(ib, *, account_id: str, force_refresh: bool):
    broker_positions = await asyncio.wait_for(
        request_broker_positions(
            ib,
            expected_account_id=account_id,
            force_refresh=force_refresh,
        ),
        timeout=POSITION_REQUEST_TIMEOUT_SECONDS,
    )

    return find_position_for_instrument(
        broker_positions=broker_positions,
        instrument_code=INSTRUMENT_CODE,
        now_ts=int(time.time()),
        expected_account_id=account_id,
    )


def connection_ready(ib, ib_health) -> bool:
    try:
        return bool(ib.isConnected()) and bool(ib_health.ib_backend_ok)
    except Exception:
        return False


async def execute_order(
    order_service: OrderService,
    *,
    action: str,
    contract_snapshot,
    reason: str,
) -> None:
    contract = build_execution_contract(
        instrument_code=INSTRUMENT_CODE,
        broker_con_id=contract_snapshot.broker_con_id,
        broker_local_symbol=contract_snapshot.broker_contract,
    )
    contract_name = (
        contract_snapshot.broker_contract
        or str(contract_snapshot.broker_con_id or "n/a")
    )

    log_info(
        logger,
        f"inverse order: reason={reason}, action={action}, qty=1, "
        f"contract={contract_name}",
        to_telegram=False,
    )

    if action == "BUY":
        placement = await order_service.buy_market(
            contract=contract,
            quantity=1,
            order_ref=ORDER_REF,
            wait="done",
            done_timeout=ORDER_DONE_TIMEOUT_SECONDS,
        )
    else:
        placement = await order_service.sell_market(
            contract=contract,
            quantity=1,
            order_ref=ORDER_REF,
            wait="done",
            done_timeout=ORDER_DONE_TIMEOUT_SECONDS,
        )

    fill_price = (
        "n/a"
        if placement.avg_fill_price is None
        else f"{placement.avg_fill_price:g}"
    )
    log_info(
        logger,
        f"inverse fill: orderId={placement.receipt.order_id}, "
        f"action={action}, qty=1, contract={contract_name}, "
        f"avg_fill_price={fill_price}",
        to_telegram=False,
    )


async def run_loop(
    *,
    source_ib,
    source_health,
    follower_ib,
    follower_health,
    order_service: OrderService,
) -> None:
    force_refresh = True
    paused = False
    last_state = None
    last_read_error = None

    while True:
        source_ready = connection_ready(source_ib, source_health)
        follower_ready = connection_ready(follower_ib, follower_health)

        if not source_ready or not follower_ready:
            if not paused:
                log_warning(
                    logger,
                    f"inverse copier paused: source_ready={source_ready}, "
                    f"follower_ready={follower_ready}",
                    to_telegram=False,
                )
                paused = True
            force_refresh = True
            await asyncio.sleep(LOOP_SLEEP_SECONDS)
            continue

        if paused:
            log_info(
                logger,
                "inverse copier resumed: оба соединения доступны",
                to_telegram=False,
            )
            paused = False
            force_refresh = True

        try:
            source_snapshot, follower_snapshot = await asyncio.gather(
                read_positions_once(
                    source_ib,
                    account_id=SOURCE_SETTINGS.ib_account_id,
                    force_refresh=force_refresh,
                ),
                read_positions_once(
                    follower_ib,
                    account_id=FOLLOWER_SETTINGS.ib_account_id,
                    force_refresh=force_refresh,
                ),
            )
            force_refresh = False
            last_read_error = None
        except asyncio.CancelledError:
            raise
        except MultipleBrokerContractPositionsError:
            raise
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            if error_text != last_read_error:
                log_warning(
                    logger,
                    f"position read failed: {error_text}",
                    to_telegram=False,
                )
                last_read_error = error_text
            force_refresh = True
            await asyncio.sleep(LOOP_SLEEP_SECONDS)
            continue

        # Нарушение правила одного контракта здесь завершит сервис,
        # а не будет замаскировано под временную ошибку чтения.
        source_position = signed_position(source_snapshot)
        follower_position = signed_position(follower_snapshot)

        state = (
            source_position,
            source_snapshot.broker_contract,
            follower_position,
            follower_snapshot.broker_contract,
        )
        if state != last_state:
            log_info(
                logger,
                f"positions: source={format_position(source_snapshot)} | "
                f"follower={format_position(follower_snapshot)} | "
                f"target={-source_position:+d}",
                to_telegram=False,
            )
            last_state = state

        order = choose_order(source_snapshot, follower_snapshot)
        if order is None:
            await asyncio.sleep(LOOP_SLEEP_SECONDS)
            continue

        action, contract_snapshot, reason = order

        # Ждём завершения единственного market-order. Пока await не закончился,
        # цикл не может отправить второй ордер по старому snapshot.
        await execute_order(
            order_service,
            action=action,
            contract_snapshot=contract_snapshot,
            reason=reason,
        )

        force_refresh = True
        last_state = None


async def cancel_task(task: asyncio.Task | None) -> None:
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


async def main() -> None:
    validate_settings()

    source_ib = None
    follower_ib = None
    source_monitor = None
    follower_monitor = None

    log_info(
        logger,
        "\n===========\n"
        "Старт inverse copier\n"
        f"source={SOURCE_SETTINGS.ib_host}:{SOURCE_SETTINGS.ib_port} "
        f"clientId={SOURCE_SETTINGS.ib_client_id} "
        f"account={SOURCE_SETTINGS.ib_account_id}\n"
        f"follower={FOLLOWER_SETTINGS.ib_host}:{FOLLOWER_SETTINGS.ib_port} "
        f"clientId={FOLLOWER_SETTINGS.ib_client_id} "
        f"account={FOLLOWER_SETTINGS.ib_account_id}\n"
        "instrument=MNQ, order_qty=1\n"
        "===========\n",
        to_telegram=False,
    )

    try:
        source_ib, source_health = await connect_ib(SOURCE_SETTINGS)
        await validate_ib_account_access(
            source_ib,
            expected_account_id=SOURCE_SETTINGS.ib_account_id,
        )

        follower_ib, follower_health = await connect_ib(FOLLOWER_SETTINGS)
        await validate_ib_account_access(
            follower_ib,
            expected_account_id=FOLLOWER_SETTINGS.ib_account_id,
        )

        source_monitor = asyncio.create_task(
            monitor_ib_connection(source_ib, SOURCE_SETTINGS, source_health)
        )
        follower_monitor = asyncio.create_task(
            monitor_ib_connection(follower_ib, FOLLOWER_SETTINGS, follower_health)
        )

        order_service = OrderService(
            follower_ib,
            account_id=FOLLOWER_SETTINGS.ib_account_id,
        )

        await run_loop(
            source_ib=source_ib,
            source_health=source_health,
            follower_ib=follower_ib,
            follower_health=follower_health,
            order_service=order_service,
        )

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log_warning(
            logger,
            f"inverse copier stopped: {type(exc).__name__}: {exc}\n"
            f"{traceback.format_exc()}",
            to_telegram=False,
        )
        raise
    finally:
        await cancel_task(source_monitor)
        await cancel_task(follower_monitor)

        if source_ib is not None:
            disconnect_ib(source_ib)
        if follower_ib is not None:
            disconnect_ib(follower_ib)

        log_info(logger, "Стоп inverse copier", to_telegram=False)


if __name__ == "__main__":
    instance_key = (
        f"{SOURCE_SETTINGS.ib_host}:{SOURCE_SETTINGS.ib_port}:"
        f"{SOURCE_SETTINGS.ib_client_id}:{SOURCE_SETTINGS.ib_account_id}|"
        f"{FOLLOWER_SETTINGS.ib_host}:{FOLLOWER_SETTINGS.ib_port}:"
        f"{FOLLOWER_SETTINGS.ib_client_id}:{FOLLOWER_SETTINGS.ib_account_id}"
    )

    with service_instance_lock(
        "ib_inverse_copier",
        instance_key=instance_key,
    ):
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass
