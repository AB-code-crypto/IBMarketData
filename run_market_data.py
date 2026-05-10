import asyncio
import time
import traceback
from dataclasses import dataclass, field
from typing import Optional

from config import settings_live as settings
from contracts import Instrument
from core.active_instruments import build_active_instruments
from core.db_initializer import initialize_databases_sync
from core.ib_health import build_ib_health_text
from core.ib_connector import (
    connect_ib,
    disconnect_ib,
    get_ib_server_time_text,
    heartbeat_ib_connection,
    monitor_ib_connection,
)
from core.load_history import process_instrument_history
from core.load_realtime import run_realtime_instrument_forever
from core.logger import (
    disable_telegram_logging,
    get_logger,
    log_info,
    log_warning,
    setup_logging,
    setup_telegram_logging,
    wait_telegram_logging,
)
from core.state_db import (
    initialize_state_db,
    mark_history_ready,
    mark_instrument_error,
    mark_realtime_started,
    reset_instrument_state,
)
from core.telegram_sender import TelegramSender

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(settings, robot_name=settings.robot_name)
setup_telegram_logging(telegram_sender)

# Как часто отправлять штатный статус в Telegram.
STATUS_TELEGRAM_INTERVAL_SECONDS = 600


@dataclass
class RuntimeStatus:
    # Текущее состояние сервиса для периодического Telegram-статуса.
    started_monotonic: float = field(default_factory=time.monotonic)
    history_instrument: Optional[str] = None
    realtime_instruments: set[str] = field(default_factory=set)


@dataclass
class BackgroundTasks:
    # Фоновые задачи, которые живут после старта сервиса.
    heartbeat: asyncio.Task
    monitor: asyncio.Task
    status: asyncio.Task
    realtime: list[asyncio.Task] = field(default_factory=list)


def _format_uptime(seconds: float) -> str:
    """Что делает: переводит количество секунд uptime в формат HH:MM:SS. Зачем нужна: делает периодический статус робота читаемым для Telegram и консоли."""
    seconds = max(0, int(seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _format_runtime_status(
        runtime_status: RuntimeStatus,
        ib,
        ib_health,
        server_time_text: str,
) -> str:
    """Что делает: собирает сводный текст состояния market-data сервиса, IB-соединения, history и realtime. Зачем нужна: даёт единый формат для регулярного Telegram-статуса."""
    if runtime_status.history_instrument is None:
        history_text = "нет активной закачки истории"
    else:
        history_text = runtime_status.history_instrument

    realtime_count = len(runtime_status.realtime_instruments)
    if runtime_status.realtime_instruments:
        realtime_text = ", ".join(sorted(runtime_status.realtime_instruments))
    else:
        realtime_text = "нет активных realtime-инструментов"

    uptime_text = _format_uptime(time.monotonic() - runtime_status.started_monotonic)
    api_text = "подключено" if ib.isConnected() else "нет подключения"

    ib_ok = (
            ib.isConnected()
            and ib_health.ib_backend_ok
            and ib_health.market_data_ok
            and ib_health.hmds_ok
    )
    status_title = "Статус: всё нормально работает" if ib_ok else "Статус: есть проблемы"

    return (
        f"{status_title}\n"
        f"Uptime: {uptime_text}\n"
        f"IB API: {api_text}\n"
        f"Время сервера IB: {server_time_text}\n"
        f"{build_ib_health_text(ib_health)}\n"
        f"Закачиваем историю: {history_text}\n"
        f"Получаем рыночные данные: {realtime_text}\n"
        f"Realtime-инструментов: {realtime_count}"
    )


async def _status_reporter(runtime_status: RuntimeStatus, ib, ib_health) -> None:
    """Что делает: периодически отправляет сводный статус market-data сервиса. Зачем нужна: позволяет видеть, что сервис жив и в каком состоянии находятся IB, history и realtime."""
    while True:
        await asyncio.sleep(STATUS_TELEGRAM_INTERVAL_SECONDS)

        server_time_text = "не получено"
        if ib.isConnected():
            try:
                server_time_text = await get_ib_server_time_text(ib)
            except Exception as exc:
                server_time_text = f"не получено ({exc})"

        log_info(
            logger,
            _format_runtime_status(
                runtime_status=runtime_status,
                ib=ib,
                ib_health=ib_health,
                server_time_text=server_time_text,
            ),
            to_telegram=True,
        )


def _log_connection_details(*, server_time_text: str, active_instruments: dict) -> None:
    """Что делает: логирует стартовые параметры подключения и список активных realtime-инструментов. Зачем нужна: фиксирует контекст запуска без засорения Telegram техническими деталями."""
    log_info(logger, "Старт робота", to_telegram=True)
    log_info(logger, f"Host: {settings.ib_host}", to_telegram=False)
    log_info(logger, f"Port: {settings.ib_port}", to_telegram=False)
    log_info(logger, f"Client ID: {settings.ib_client_id}", to_telegram=False)
    log_info(logger, f"Время сервера IB: {server_time_text}", to_telegram=False)
    log_info(logger, f"Активные realtime-инструменты на старте: {active_instruments}", to_telegram=False)
    log_info(logger, f"Price DB dir: {settings.price_db_dir}", to_telegram=False)



def _reset_signal_states_for_enabled_instruments() -> None:
    """Что делает: сбрасывает signal-ready состояние инструментов полного live-контура перед новым запуском market-data. Зачем нужна: защищает job-data и signal сервисы от stale-состояния прошлого запуска."""
    initialize_state_db()

    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["history_enabled"] and instrument_row["realtime_enabled"]:
            reset_instrument_state(instrument_code)


def _start_infrastructure_tasks(*, ib, ib_health, runtime_status: RuntimeStatus) -> BackgroundTasks:
    """Что делает: запускает фоновые задачи мониторинга IB, heartbeat и Telegram-статуса. Зачем нужна: отделяет инфраструктурные задачи от обработки конкретных инструментов."""
    monitor_task = asyncio.create_task(
        monitor_ib_connection(ib, settings, ib_health),
        name="monitor_ib_connection",
    )
    heartbeat_task = asyncio.create_task(
        heartbeat_ib_connection(ib, ib_health),
        name="heartbeat_ib_connection",
    )
    status_task = asyncio.create_task(
        _status_reporter(runtime_status, ib, ib_health),
        name="telegram_status_reporter",
    )

    return BackgroundTasks(
        heartbeat=heartbeat_task,
        monitor=monitor_task,
        status=status_task,
    )


async def _cancel_tasks(*tasks: asyncio.Task) -> None:
    """Что делает: отменяет переданные asyncio-задачи и дожидается их завершения. Зачем нужна: обеспечивает предсказуемый shutdown без висящих фоновых задач."""
    for task in tasks:
        if task is not None:
            task.cancel()

    for task in tasks:
        if task is None:
            continue

        try:
            await task
        except asyncio.CancelledError:
            pass


def _start_realtime_for_instrument(
        *,
        ib,
        ib_health,
        active_instruments: dict,
        background_tasks: BackgroundTasks,
        runtime_status: RuntimeStatus,
        instrument_code: str,
) -> bool:
    """Что делает: создаёт realtime-задачу для одного инструмента и регистрирует её в runtime-состоянии. Зачем нужна: запускает live-поток сразу после готовности истории конкретного инструмента."""
    active_contract_name = active_instruments.get(instrument_code)

    if active_contract_name is None:
        log_warning(
            logger,
            f"Инструмент {instrument_code}: realtime включён, но active-контракт "
            f"на старте не определён. Realtime по инструменту не запускаю.",
            to_telegram=True,
        )
        return False

    log_info(
        logger,
        f"Найден инструмент для получения рыночных данных: {instrument_code}",
        to_telegram=True,
    )

    realtime_task = asyncio.create_task(
        run_realtime_instrument_forever(
            ib=ib,
            ib_health=ib_health,
            settings=settings,
            instrument_code=instrument_code,
            active_contract_name=active_contract_name,
        ),
        name=f"load_realtime_{instrument_code}",
    )
    background_tasks.realtime.append(realtime_task)
    runtime_status.realtime_instruments.add(instrument_code)

    log_info(
        logger,
        f"Инструмент {instrument_code}: realtime-задача запущена, active={active_contract_name}",
        to_telegram=False,
    )
    return True


async def _process_instrument_then_start_realtime(
        *,
        ib,
        ib_health,
        active_instruments: dict,
        background_tasks: BackgroundTasks,
        runtime_status: RuntimeStatus,
        instrument_code: str,
        instrument_row,
) -> None:
    """Что делает: обрабатывает один инструмент: при необходимости качает историю и затем запускает realtime. Зачем нужна: сохраняет порядок history -> realtime и изолирует ошибку одного инструмента от остальных."""
    history_enabled = instrument_row["history_enabled"]
    realtime_enabled = instrument_row["realtime_enabled"]
    signal_state_enabled = history_enabled and realtime_enabled
    history_ok = True

    if history_enabled:
        runtime_status.history_instrument = instrument_code
        log_info(
            logger,
            f"Найден инструмент для закачивания истории: {instrument_code}",
            to_telegram=True,
        )

        try:
            await process_instrument_history(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                instrument_code=instrument_code,
                instrument_row=instrument_row,
            )
            log_info(
                logger,
                f"Получение исторических данных по инструменту {instrument_code} завершено",
                to_telegram=True,
            )

            if signal_state_enabled:
                mark_history_ready(instrument_code)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            history_ok = False

            if signal_state_enabled:
                mark_instrument_error(instrument_code, str(exc))

            log_warning(
                logger,
                f"Инструмент {instrument_code}: history-загрузка завершилась ошибкой. "
                f"Realtime по этому инструменту не запускаю. error={exc}\n{traceback.format_exc()}",
                to_telegram=True,
            )
        finally:
            if runtime_status.history_instrument == instrument_code:
                runtime_status.history_instrument = None

    if not realtime_enabled:
        return

    if history_enabled and not history_ok:
        return

    realtime_started = _start_realtime_for_instrument(
        ib=ib,
        ib_health=ib_health,
        active_instruments=active_instruments,
        background_tasks=background_tasks,
        runtime_status=runtime_status,
        instrument_code=instrument_code,
    )

    if realtime_started and signal_state_enabled:
        mark_realtime_started(instrument_code)


async def _process_all_instruments_then_keep_realtime(
        *,
        ib,
        ib_health,
        active_instruments: dict,
        background_tasks: BackgroundTasks,
        runtime_status: RuntimeStatus,
) -> None:
    """Что делает: последовательно обходит включённые инструменты, запускает их realtime-задачи и удерживает сервис живым. Зачем нужна: это основной orchestration-контур market-data сервиса после подключения к IB."""
    for instrument_code, instrument_row in Instrument.items():
        if not (instrument_row["history_enabled"] or instrument_row["realtime_enabled"]):
            continue

        await _process_instrument_then_start_realtime(
            ib=ib,
            ib_health=ib_health,
            active_instruments=active_instruments,
            background_tasks=background_tasks,
            runtime_status=runtime_status,
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )

    log_info(
        logger,
        "Обработка включённых market-data инструментов завершена.",
        to_telegram=False,
    )

    if not background_tasks.realtime:
        log_warning(
            logger,
            "Нет запущенных realtime-инструментов. Сервис остаётся в режиме ожидания.",
            to_telegram=True,
        )
        while True:
            await asyncio.sleep(60)

    await asyncio.gather(*background_tasks.realtime)


async def _shutdown_app(*, ib, shutdown_message: str, tasks: Optional[BackgroundTasks]) -> None:
    """Что делает: останавливает фоновые задачи, закрывает IB-соединение и отправляет финальное Telegram-сообщение. Зачем нужна: завершает market-data сервис без потерянных логов и висящих задач."""
    if tasks is not None:
        await _cancel_tasks(tasks.heartbeat, tasks.monitor, tasks.status, *tasks.realtime)

    try:
        disconnect_ib(ib)
        log_info(logger, "Соединение с IB закрыто", to_telegram=False)
    except Exception:
        pass

    log_info(logger, shutdown_message, to_telegram=True)
    await wait_telegram_logging()

    disable_telegram_logging()

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main():
    """Что делает: запускает market-data сервис от подключения к IB до обработки инструментов. Зачем нужна: является async entrypoint для run_market_data.py."""
    shutdown_message = "\n===========\nСтоп робота.\n===========\n"
    background_tasks: Optional[BackgroundTasks] = None
    runtime_status = RuntimeStatus()

    _reset_signal_states_for_enabled_instruments()

    ib, ib_health = await connect_ib(settings)

    try:
        server_time_text = await get_ib_server_time_text(ib)
        active_instruments = build_active_instruments(server_time_text)

        _log_connection_details(
            server_time_text=server_time_text,
            active_instruments=active_instruments,
        )

        initialize_databases_sync(settings)

        background_tasks = _start_infrastructure_tasks(
            ib=ib,
            ib_health=ib_health,
            runtime_status=runtime_status,
        )

        await _process_all_instruments_then_keep_realtime(
            ib=ib,
            ib_health=ib_health,
            active_instruments=active_instruments,
            background_tasks=background_tasks,
            runtime_status=runtime_status,
        )

    except asyncio.CancelledError:
        shutdown_message = "===========\nСтоп робота: остановлен пользователем\n==========="
        raise

    finally:
        await _shutdown_app(
            ib=ib,
            shutdown_message=shutdown_message,
            tasks=background_tasks,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
