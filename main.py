import asyncio
import time
import traceback
from dataclasses import dataclass, field
from typing import Optional

from config import settings_live as settings
from contracts import Instrument
from core.active_instruments import build_active_instruments
from core.db_initializer import initialize_databases_sync
from core.ib_connector import (
    connect_ib,
    disconnect_ib,
    get_ib_server_time_text,
    heartbeat_ib_connection,
    monitor_ib_connection,
)
from core.load_history import (
    is_instrument_history_enabled,
    process_instrument_history,
)
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
from core.telegram_sender import TelegramSender

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(settings)
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

    def as_tuple(self) -> tuple[asyncio.Task, ...]:
        # Удобное представление для групповой отмены задач.
        return self.heartbeat, self.monitor, self.status, *self.realtime


def _format_uptime(seconds: float) -> str:
    # Форматируем uptime в HH:MM:SS.
    seconds = max(0, int(seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _format_runtime_status(runtime_status: RuntimeStatus) -> str:
    # Собираем короткий статус сервиса для Telegram.
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

    return (
        "Статус: всё нормально работает\n"
        f"Uptime: {uptime_text}\n"
        f"Закачиваем историю: {history_text}\n"
        f"Получаем рыночные данные: {realtime_text}\n"
        f"Realtime-инструментов: {realtime_count}"
    )


async def _status_reporter(runtime_status: RuntimeStatus) -> None:
    # Раз в 10 минут отправляем в Telegram общий статус сервиса.
    while True:
        await asyncio.sleep(STATUS_TELEGRAM_INTERVAL_SECONDS)
        log_info(
            logger,
            _format_runtime_status(runtime_status),
            to_telegram=True,
        )


def _log_connection_details(*, server_time_text: str, active_instruments: dict) -> None:
    log_info(logger, "Старт робота", to_telegram=True)
    log_info(logger, f"Host: {settings.ib_host}", to_telegram=False)
    log_info(logger, f"Port: {settings.ib_port}", to_telegram=False)
    log_info(logger, f"Client ID: {settings.ib_client_id}", to_telegram=False)
    log_info(logger, f"Время сервера IB: {server_time_text}", to_telegram=False)
    log_info(logger, f"Активные realtime-инструменты на старте: {active_instruments}", to_telegram=False)
    log_info(logger, f"Price DB dir: {settings.price_db_dir}", to_telegram=False)


def _is_instrument_realtime_enabled(instrument_row) -> bool:
    # Проверяем выключатель realtime-загрузки инструмента.
    return instrument_row.get("realtime_enabled", True)


def _start_infrastructure_tasks(*, ib, ib_health, runtime_status: RuntimeStatus) -> BackgroundTasks:
    # Запускаем фоновые задачи, которые не зависят от конкретного инструмента.
    monitor_task = asyncio.create_task(
        monitor_ib_connection(ib, settings, ib_health),
        name="monitor_ib_connection",
    )
    heartbeat_task = asyncio.create_task(
        heartbeat_ib_connection(ib, ib_health),
        name="heartbeat_ib_connection",
    )
    status_task = asyncio.create_task(
        _status_reporter(runtime_status),
        name="telegram_status_reporter",
    )

    return BackgroundTasks(
        heartbeat=heartbeat_task,
        monitor=monitor_task,
        status=status_task,
    )


async def _cancel_tasks(*tasks: asyncio.Task) -> None:
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
    # Запускаем realtime-задачу одного инструмента.
    # Активный контракт берём из словаря, рассчитанного один раз на старте сервиса.
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
) -> int:
    # Последовательность по одному инструменту:
    # 1. если включена history-загрузка — докачиваем историю;
    # 2. если включён realtime — запускаем realtime-задачу;
    # 3. recent-backfill последнего часа запускается внутри realtime после первого
    #    синхронного BID/ASK бара.
    rows_written = 0
    history_enabled = is_instrument_history_enabled(instrument_row)
    realtime_enabled = _is_instrument_realtime_enabled(instrument_row)
    history_ok = True

    if history_enabled:
        runtime_status.history_instrument = instrument_code
        log_info(
            logger,
            f"Найден инструмент для закачивания истории: {instrument_code}",
            to_telegram=True,
        )

        try:
            rows_written = await process_instrument_history(
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
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            history_ok = False
            log_warning(
                logger,
                f"Инструмент {instrument_code}: history-загрузка завершилась ошибкой. "
                f"Realtime по этому инструменту не запускаю. error={exc}\n{traceback.format_exc()}",
                to_telegram=True,
            )
        finally:
            if runtime_status.history_instrument == instrument_code:
                runtime_status.history_instrument = None
    else:
        log_info(
            logger,
            f"Инструмент {instrument_code}: history-загрузка выключена, пропускаю.",
            to_telegram=False,
        )

    if not realtime_enabled:
        log_info(
            logger,
            f"Инструмент {instrument_code}: realtime-загрузка выключена, пропускаю.",
            to_telegram=False,
        )
        return rows_written

    if history_enabled and not history_ok:
        return rows_written

    _start_realtime_for_instrument(
        ib=ib,
        ib_health=ib_health,
        active_instruments=active_instruments,
        background_tasks=background_tasks,
        runtime_status=runtime_status,
        instrument_code=instrument_code,
    )
    return rows_written


async def _process_all_instruments_then_keep_realtime(
        *,
        ib,
        ib_health,
        active_instruments: dict,
        background_tasks: BackgroundTasks,
        runtime_status: RuntimeStatus,
) -> None:
    # Основная оркестрация:
    # - history идёт последовательно по инструментам;
    # - realtime запускается сразу после готовности своего инструмента;
    # - уже запущенный realtime продолжает работать, пока история следующих
    #   инструментов ещё докачивается.
    total_rows_written = 0

    for instrument_code, instrument_row in Instrument.items():
        total_rows_written += await _process_instrument_then_start_realtime(
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
        f"Обработка history по всем инструментам завершена. Всего записано строк: {total_rows_written}",
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
    if tasks is not None:
        await _cancel_tasks(*tasks.as_tuple())

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
    shutdown_message = "Стоп робота"
    background_tasks: Optional[BackgroundTasks] = None
    runtime_status = RuntimeStatus()

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
        shutdown_message = "Стоп робота: остановлен пользователем"
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
