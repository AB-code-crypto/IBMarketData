import asyncio

from core.bar_utils import build_duration_str
from core.logger import get_logger, log_info, log_warning
from core.time_utils import format_utc

logger = get_logger(__name__)

# Максимальное время ожидания ответа на reqCurrentTimeAsync.
# Если ответа нет слишком долго, считаем это проблемой соединения / TWS / IB Gateway.
CURRENT_TIME_REQUEST_TIMEOUT_SECONDS = 15

# Максимальное время ожидания одного historical request.
# Это не pacing delay, а именно защитный таймаут от подвисших запросов.
HISTORICAL_REQUEST_TIMEOUT_SECONDS = 90

# Как часто проверяем, поднялось ли соединение после обрыва.
RECONNECT_WAIT_SECONDS = 1

# Как часто проверяем, восстановились ли backend IB и HMDS.
#
# Это отдельная проверка от локального API-соединения.
# Смысл в том, что TWS / Gateway могут быть подключены локально,
# но backend IB или HMDS уже в нештатном состоянии,
# и тогда historical request иногда возвращает битые данные.
IB_HEALTH_WAIT_SECONDS = 1


def format_exception_for_log(exc):
    # У TimeoutError строковое представление часто пустое, и в логе это выглядит неинформативно.
    # Поэтому всегда добавляем имя класса ошибки, а текст — только если он есть.
    """Что делает: форматирует исключение так, чтобы даже пустой TimeoutError был информативен. Зачем нужна: логи reconnect/retry должны объяснять причину ожидания."""
    exc_name = exc.__class__.__name__
    exc_text = str(exc).strip()

    if exc_text:
        return f"{exc_name}: {exc_text}"

    return exc_name


async def wait_for_ib_connection(ib):
    # Ждём восстановления локального соединения с TWS / IB Gateway.
    #
    # Исторический загрузчик не должен падать только потому,
    # что TWS на минуту перезапустился или сеть кратковременно пропала.
    """Что делает: ждёт восстановления локального IB API-соединения. Зачем нужна: historical requests не должны падать при кратком обрыве связи."""
    wait_logged = False

    while not ib.isConnected():
        if not wait_logged:
            log_warning(
                logger,
                "Загрузка истории ждёт восстановления соединения с IB...",
                to_telegram=False,
            )
            wait_logged = True

        await asyncio.sleep(RECONNECT_WAIT_SECONDS)

    if wait_logged:
        log_info(
            logger,
            "Соединение с IB восстановлено, загрузка истории продолжается",
            to_telegram=False,
        )


async def wait_for_ib_history_ready(ib, ib_health):
    # Ждём не только локального API-соединения, но и нормального состояния
    # backend IB / HMDS.
    #
    # Это закрывает ситуацию, когда локальный сокет до TWS ещё жив,
    # но backend IB уже не в порядке и historical request может вернуть
    # частично пустые или битые цены.
    """Что делает: ждёт одновременно локальное соединение, backend IB и HMDS. Зачем нужна: historical-запросы безопасны только когда все эти компоненты доступны."""
    wait_logged = False

    while True:
        await wait_for_ib_connection(ib)

        if ib_health.ib_backend_ok and ib_health.hmds_ok:
            if wait_logged:
                log_info(
                    logger,
                    "Backend IB и HMDS снова доступны, загрузка истории продолжается",
                    to_telegram=False,
                )
            return

        if not wait_logged:
            log_warning(
                logger,
                "Загрузка истории ждёт восстановления backend IB / HMDS...",
                to_telegram=False,
            )
            wait_logged = True

        await asyncio.sleep(IB_HEALTH_WAIT_SECONDS)


def is_connection_problem(exc):
    # Определяем, похожа ли ошибка на проблему соединения.
    #
    # Логику делаем отдельно, чтобы:
    # - не глотать любые ошибки подряд;
    # - но и не падать на временном обрыве, который прилетел не как ConnectionError,
    #   а как RuntimeError / TimeoutError / другая текстовая ошибка библиотеки.
    """Что делает: определяет, похожа ли ошибка на сетевую или connection-related проблему. Зачем нужна: retry нужен только для временных проблем соединения, а не для любых ошибок подряд."""
    if isinstance(exc, ConnectionError):
        return True

    if isinstance(exc, TimeoutError):
        return True

    text = str(exc).lower()

    connection_markers = (
        "not connected",
        "disconnected",
        "connection",
        "socket",
        "timeout",
        "peer closed",
        "transport closed",
    )

    for marker in connection_markers:
        if marker in text:
            return True

    return False


async def request_historical_data_with_reconnect(
        ib,
        ib_health,
        contract,
        end_dt,
        start_dt,
        bar_size_setting,
        what_to_show,
        use_rth,
):
    # Устойчивая обёртка над reqHistoricalDataAsync.
    #
    # Поведение такое:
    # - если локального соединения нет — ждём реконнект;
    # - если backend IB / HMDS не в порядке — тоже ждём;
    # - если запрос оборвался из-за соединения — не падаем, а повторяем;
    # - если ошибка не похожа на сетевую/соединенческую — пробрасываем её наружу.
    """Что делает: выполняет historical request с ожиданием health-state, timeout и retry на connection errors. Зачем нужна: делает history-loader устойчивым к реконнектам и временной недоступности IB."""
    while True:
        await wait_for_ib_history_ready(ib, ib_health)

        try:
            return await asyncio.wait_for(
                ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=format_utc(end_dt, for_ib=True),
                    durationStr=build_duration_str(start_dt, end_dt),
                    barSizeSetting=bar_size_setting,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    formatDate=2,
                    keepUpToDate=False,
                ),
                timeout=HISTORICAL_REQUEST_TIMEOUT_SECONDS,
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            if is_connection_problem(exc) or not ib.isConnected():
                log_warning(
                    logger,
                    f"Проблема соединения во время historical request {what_to_show} по {contract}: "
                    f"{format_exception_for_log(exc)}. Жду реконнект и повторяю запрос",
                    to_telegram=False,
                )
                await asyncio.sleep(RECONNECT_WAIT_SECONDS)
                continue

            raise


async def request_current_time_with_reconnect(ib):
    # Устойчивая обёртка над reqCurrentTimeAsync.
    """Что делает: запрашивает server time IB с ожиданием соединения и retry. Зачем нужна: history-loader регулярно обновляет правую границу истории по времени IB."""
    while True:
        await wait_for_ib_connection(ib)

        try:
            return await asyncio.wait_for(
                ib.reqCurrentTimeAsync(),
                timeout=CURRENT_TIME_REQUEST_TIMEOUT_SECONDS,
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            if is_connection_problem(exc) or not ib.isConnected():
                log_warning(
                    logger,
                    f"Проблема соединения во время запроса server time: "
                    f"{format_exception_for_log(exc)}. Жду реконнект и повторяю запрос",
                    to_telegram=False,
                )
                await asyncio.sleep(RECONNECT_WAIT_SECONDS)
                continue

            raise
