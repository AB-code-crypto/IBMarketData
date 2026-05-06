import asyncio
import time

from ib_async import IB

from core.ib_health import (
    IbConnectionHealth,
    build_ib_health_text,
    register_ib_health_handlers,
    reset_ib_health_for_new_connect,
)
from core.logger import get_logger, log_info, log_warning

logger = get_logger(__name__)

# Пауза между попытками подключения / переподключения.
RECONNECT_DELAY_SECONDS = 5

# Как часто монитор проверяет, живо ли текущее соединение.
CONNECTION_CHECK_INTERVAL_SECONDS = 1

# Раз в сколько секунд слать heartbeat-сообщение.
HEARTBEAT_INTERVAL_SECONDS = 600


async def connect_ib(settings):
    # Создаём объект клиента IB.
    ib = IB()

    # Создаём объект со служебным состоянием здоровья соединения.
    ib_health = IbConnectionHealth()

    # Подписываемся на системные сообщения TWS один раз.
    # Эти подписки останутся на том же объекте ib и после реконнектов.
    register_ib_health_handlers(ib, ib_health)

    # Засекаем момент начала попыток подключения.
    connect_started_at = time.monotonic()

    # Счётчик попыток подключения.
    connect_attempt = 0

    # Последний текст ошибки, чтобы не дублировать одно и то же слишком часто.
    last_error_text = None

    log_info(logger, "Подключаюсь к IB...", to_telegram=False)

    # Бесконечно пытаемся подключиться, пока соединение не будет установлено
    # или пока пользователь сам не остановит робота.
    while True:
        connect_attempt += 1

        try:
            # Перед новой попыткой подключения приводим состояние "здоровья" в базовое.
            reset_ib_health_for_new_connect(ib_health)

            # Пытаемся открыть соединение с TWS / IB Gateway.
            await ib.connectAsync(
                host=settings.ib_host,
                port=settings.ib_port,
                clientId=settings.ib_client_id,
            )

            # Дополнительная жёсткая проверка:
            # если connectAsync завершился без исключения,
            # но соединение реально не активно, считаем это ошибкой.
            if not ib.isConnected():
                raise RuntimeError("IB вернул connectAsync без активного соединения")

            # Считаем, сколько секунд заняло подключение.
            connect_duration = int(time.monotonic() - connect_started_at)

            log_info(
                logger,
                f"Соединение с IB установлено после {connect_attempt} попыток за {connect_duration} сек",
                to_telegram=False,
            )

            # Возвращаем и сам объект IB, и объект состояния здоровья.
            return ib, ib_health

        except asyncio.CancelledError:
            # Если задачу отменили извне, обязательно пробрасываем отмену дальше.
            raise

        except Exception as e:
            error_text = str(e)

            # При первой неудаче печатаем поясняющее сообщение.
            if connect_attempt == 1:
                log_warning(logger, "Не удалось подключиться к IB при старте")
                log_warning(logger, "Ожидаю доступности TWS/IB Gateway...")

            # Печатаем:
            # - первую ошибку;
            # - каждую 5-ю попытку;
            # - либо если текст ошибки изменился.
            if connect_attempt == 1 or connect_attempt % 5 == 0 or error_text != last_error_text:
                log_warning(logger, f"Попытка подключения #{connect_attempt} не удалась: {error_text}")

            last_error_text = error_text

            # Ждём и пробуем снова.
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


def disconnect_ib(ib):
    # Закрываем соединение только если оно активно.
    if ib.isConnected():
        ib.disconnect()


async def get_ib_server_time_text(ib):
    # Запрашиваем текущее серверное время у IB и возвращаем готовую строку.
    current_time = await ib.reqCurrentTimeAsync()
    return str(current_time).split("+")[0]


async def monitor_ib_connection(ib, settings, ib_health):
    # Запоминаем, было ли соединение активным на момент старта монитора.
    was_connected = ib.isConnected()

    # Момент начала переподключения после потери связи.
    reconnect_started_at = None

    # Счётчик попыток переподключения.
    reconnect_attempt = 0

    # Последний текст ошибки переподключения.
    last_error_text = None

    # Монитор работает постоянно, пока жив робот.
    while True:
        # Если соединение активно:
        if ib.isConnected():
            # Если раньше связи не было, а теперь появилась,
            # значит соединение восстановлено.
            if not was_connected:
                reconnect_duration = int(time.monotonic() - reconnect_started_at)

                log_info(
                    logger,
                    f"Соединение с IB восстановлено после {reconnect_attempt} попыток за {reconnect_duration} сек",
                )

                # После восстановления сразу пробуем запросить серверное время.
                try:
                    server_time_text = await get_ib_server_time_text(ib)
                    log_info(logger, f"Время сервера IB: {server_time_text}")
                except Exception as e:
                    log_warning(logger, f"Не удалось получить время сервера IB после восстановления: {e}")

                # Сбрасываем служебные переменные после успешного восстановления.
                reconnect_started_at = None
                reconnect_attempt = 0
                last_error_text = None

            # Обновляем флаг: соединение сейчас активно.
            was_connected = True

            # Даём циклу немного поспать, чтобы не крутиться слишком часто.
            await asyncio.sleep(CONNECTION_CHECK_INTERVAL_SECONDS)
            continue

        # Если мы попали сюда, значит прямо сейчас локального API-соединения нет.
        if was_connected:
            log_warning(logger, "Соединение с IB потеряно")
            log_warning(logger, "Запускаю переподключение к IB...")

            was_connected = False
            reconnect_started_at = time.monotonic()
            reconnect_attempt = 0
            last_error_text = None

        reconnect_attempt += 1

        try:
            # Перед новой попыткой реконнекта приводим состояние "здоровья"
            # в базовое состояние нового локального подключения.
            reset_ib_health_for_new_connect(ib_health)

            # Пытаемся восстановить соединение.
            await ib.connectAsync(
                host=settings.ib_host,
                port=settings.ib_port,
                clientId=settings.ib_client_id,
            )

            # Если подключились успешно, сразу идём на новую итерацию,
            # чтобы без лишней паузы зафиксировать восстановление.
            if ib.isConnected():
                continue

        except asyncio.CancelledError:
            # Корректно пробрасываем отмену задачи наружу.
            raise

        except Exception as e:
            error_text = str(e)

            # Печатаем:
            # - первую ошибку;
            # - каждую 5-ю попытку;
            # - либо если текст ошибки изменился.
            if reconnect_attempt == 1 or reconnect_attempt % 5 == 0 or error_text != last_error_text:
                log_warning(logger, f"Попытка переподключения #{reconnect_attempt} не удалась: {error_text}")

            last_error_text = error_text

        # Если переподключение не удалось, ждём и пробуем снова.
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)


async def heartbeat_ib_connection(ib, ib_health):
    # Бесконечная фоновая задача.
    # Раз в HEARTBEAT_INTERVAL_SECONDS проверяет не только локальный API-сокет,
    # но и реальное состояние TWS / IB по backend, market data farm и HMDS.
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)

        # Если локального API-соединения сейчас нет,
        # heartbeat не пишем — монитор и так уже выводит нужные сообщения.
        if not ib.isConnected():
            continue

        # Пытаемся получить живое серверное время у IB.
        server_time_text = ""
        server_time_ok = False

        try:
            server_time_text = await get_ib_server_time_text(ib)
            server_time_ok = True
        except Exception as e:
            server_time_text = f"не получено ({e})"

        # Полностью "здоровым" считаем состояние только если:
        # - локальный API-сокет жив;
        # - серверное время успешно получено;
        # - backend IB доступен;
        # - нет проблемных market data farm;
        # - нет проблемных HMDS farm.
        if (
                server_time_ok
                and ib_health.ib_backend_ok
                and ib_health.market_data_ok
                and ib_health.hmds_ok
        ):
            log_info(
                logger,
                f"Робот работает в штатном режиме. API-соединение с TWS активно. "
                f"Время сервера IB: {server_time_text}",
                to_telegram=False,
            )
            continue

        # Если хотя бы один признак не в норме — пишем warning,
        # а не "всё хорошо".
        log_warning(
            logger,
            f"Робот работает, но состояние IB нештатное. "
            f"Время сервера IB: {server_time_text}. "
            f"{build_ib_health_text(ib_health)}",
            to_telegram=False,
        )
