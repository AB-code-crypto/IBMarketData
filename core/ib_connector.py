import asyncio
import time
from datetime import datetime, timezone

from ib_async import IB

from core.ib_clock import sample_and_store_ib_clock
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
HEARTBEAT_INTERVAL_SECONDS = 60

# reqCurrentTimeAsync не должен бессрочно блокировать startup market-data.
IB_SERVER_TIME_REQUEST_TIMEOUT_SECONDS = 5.0

# connect/reconnect/heartbeat уже получают IB time. Повторно используем свежий
# sample вместо немедленного второго reqCurrentTimeAsync после connect.
IB_SERVER_TIME_CACHE_MAX_AGE_SECONDS = 90


def build_ib_connect_kwargs(settings) -> dict:
    account_id = str(settings.ib_account_id or "").strip()
    if not account_id:
        raise RuntimeError("IB account id is empty")

    return {
        "host": settings.ib_host,
        "port": settings.ib_port,
        "clientId": settings.ib_client_id,
        "account": account_id,
    }


IB_ACCOUNT_UPDATES_REQUEST_TIMEOUT_SECONDS = 15.0
IB_ACCOUNT_UPDATES_REFRESH_MIN_INTERVAL_SECONDS = 10.0


async def refresh_ib_account_updates(
        ib,
        *,
        account_id: str,
        force: bool = False,
) -> tuple[int, int]:
    """Ensure the selected account's account/portfolio cache is live."""
    account = str(account_id or "").strip()
    if not account:
        raise RuntimeError("IB account updates require a non-empty account id")

    try:
        connected = bool(ib.isConnected())
    except Exception:
        connected = False
    if not connected:
        raise ConnectionError("IB API is not connected")

    managed_accounts = {
        str(value or "").strip()
        for value in list(ib.managedAccounts() or [])
        if str(value or "").strip()
    }
    if managed_accounts and account not in managed_accounts:
        raise RuntimeError(
            "configured account is absent from IB managed accounts: "
            f"account={account}, managed_accounts={sorted(managed_accounts)}"
        )

    def read_cache() -> tuple[list, list]:
        try:
            account_values = list(ib.accountValues(account) or [])
            portfolio_items = list(ib.portfolio(account) or [])
        except Exception as exc:
            raise RuntimeError(
                "cannot read IB account/portfolio cache: "
                f"account={account}, {type(exc).__name__}: {exc}"
            ) from exc
        return account_values, portfolio_items

    account_values, portfolio_items = read_cache()
    if account_values and not force:
        setattr(ib, "_ibmd_account_updates_ready_account", account)
        setattr(
            ib,
            "_ibmd_account_updates_ready_monotonic",
            time.monotonic(),
        )
        return len(account_values), len(portfolio_items)

    lock = getattr(ib, "_ibmd_account_updates_refresh_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        setattr(ib, "_ibmd_account_updates_refresh_lock", lock)

    async with lock:
        account_values, portfolio_items = read_cache()
        if account_values and not force:
            return len(account_values), len(portfolio_items)

        now_mono = time.monotonic()
        last_attempt = float(
            getattr(
                ib,
                "_ibmd_account_updates_refresh_attempt_monotonic",
                0.0,
            )
            or 0.0
        )
        if (
                force
                and last_attempt > 0.0
                and now_mono - last_attempt
                < IB_ACCOUNT_UPDATES_REFRESH_MIN_INTERVAL_SECONDS
        ):
            return len(account_values), len(portfolio_items)

        setattr(
            ib,
            "_ibmd_account_updates_refresh_attempt_monotonic",
            now_mono,
        )
        try:
            await asyncio.wait_for(
                ib.reqAccountUpdatesAsync(account),
                timeout=IB_ACCOUNT_UPDATES_REQUEST_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            setattr(
                ib,
                "_ibmd_account_updates_refresh_attempt_monotonic",
                0.0,
            )
            raise RuntimeError(
                "cannot refresh IB account/portfolio data: "
                f"account={account}, {type(exc).__name__}: {exc}"
            ) from exc

        account_values, portfolio_items = read_cache()
        if not account_values:
            setattr(
                ib,
                "_ibmd_account_updates_refresh_attempt_monotonic",
                0.0,
            )
            raise RuntimeError(
                "IB account updates completed without account values: "
                f"account={account}"
            )

        setattr(ib, "_ibmd_account_updates_ready_account", account)
        setattr(
            ib,
            "_ibmd_account_updates_ready_monotonic",
            time.monotonic(),
        )
        log_info(
            logger,
            (
                "IB account/portfolio data refreshed: "
                f"account={account}, account_values={len(account_values)}, "
                f"portfolio_items={len(portfolio_items)}"
            ),
            to_telegram=False,
        )
        return len(account_values), len(portfolio_items)


def format_ib_clock_server_time(sample) -> str:
    return datetime.fromtimestamp(
        float(sample.server_time_ts),
        tz=timezone.utc,
    ).strftime("%Y-%m-%d %H:%M:%S UTC")


async def sample_ib_clock_best_effort(ib, settings):
    try:
        sample = await asyncio.wait_for(
            sample_and_store_ib_clock(
                ib,
                source_client_id=getattr(
                    settings,
                    "ib_client_id",
                    None,
                ),
            ),
            timeout=IB_SERVER_TIME_REQUEST_TIMEOUT_SECONDS,
        )
        setattr(ib, "_ibmd_last_clock_sample", sample)
        return sample
    except Exception as exc:
        log_warning(
            logger,
            f"Не удалось обновить IB clock sample: "
            f"{type(exc).__name__}: {exc}",
            to_telegram=False,
        )
        return None


async def connect_ib(settings):
    """Что делает: создаёт IB-клиент, подключается к TWS/Gateway и регистрирует health handlers. Зачем нужна: централизует устойчивый старт IB-соединения."""
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
                **build_ib_connect_kwargs(settings)
            )

            # Дополнительная жёсткая проверка:
            # если connectAsync завершился без исключения,
            # но соединение реально не активно, считаем это ошибкой.
            if not ib.isConnected():
                raise RuntimeError("IB вернул connectAsync без активного соединения")

            await refresh_ib_account_updates(
                ib,
                account_id=settings.ib_account_id,
                force=False,
            )

            # Считаем, сколько секунд заняло подключение.
            connect_duration = int(time.monotonic() - connect_started_at)

            log_info(
                logger,
                f"Соединение с IB установлено после {connect_attempt} попыток за {connect_duration} сек",
                to_telegram=False,
            )

            await sample_ib_clock_best_effort(
                ib,
                settings,
            )

            # Возвращаем и сам объект IB, и объект состояния здоровья.
            return ib, ib_health

        except asyncio.CancelledError:
            # Если задачу отменили извне, обязательно пробрасываем отмену дальше.
            raise

        except Exception as e:
            error_text = f"{type(e).__name__}: {e!r}"

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
    """Что делает: закрывает активное IB-соединение. Зачем нужна: shutdown должен безопасно завершать socket-сессию."""
    if ib.isConnected():
        ib.disconnect()


async def get_ib_server_time_text(ib):
    """Возвращает IB server time без повторного зависающего запроса после connect."""
    cached_sample = getattr(ib, "_ibmd_last_clock_sample", None)

    if cached_sample is not None:
        sample_age_seconds = max(
            0,
            int(time.time()) - int(cached_sample.sampled_at_ts),
        )
        if sample_age_seconds <= IB_SERVER_TIME_CACHE_MAX_AGE_SECONDS:
            return datetime.fromtimestamp(
                float(cached_sample.server_time_ts),
                tz=timezone.utc,
            ).strftime("%Y-%m-%d %H:%M:%S")

    try:
        sample = await asyncio.wait_for(
            sample_and_store_ib_clock(
                ib,
                source_client_id=None,
            ),
            timeout=IB_SERVER_TIME_REQUEST_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError as exc:
        raise TimeoutError(
            "IB reqCurrentTimeAsync timed out while resolving server time: "
            f"timeout_seconds={IB_SERVER_TIME_REQUEST_TIMEOUT_SECONDS}"
        ) from exc

    setattr(ib, "_ibmd_last_clock_sample", sample)
    return datetime.fromtimestamp(
        float(sample.server_time_ts),
        tz=timezone.utc,
    ).strftime("%Y-%m-%d %H:%M:%S")


async def monitor_ib_connection(ib, settings, ib_health):
    """Что делает: постоянно следит за локальным IB API-соединением и переподключается при обрыве. Зачем нужна: market-data сервис должен переживать рестарт TWS/Gateway и краткие сетевые сбои."""
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

                clock_sample = await sample_ib_clock_best_effort(
                    ib,
                    settings,
                )
                if clock_sample is not None:
                    log_info(
                        logger,
                        "Время сервера IB: "
                        f"{format_ib_clock_server_time(clock_sample)}, "
                        f"clock_offset={clock_sample.offset_seconds:+.3f}s, "
                        f"rtt={clock_sample.round_trip_seconds:.3f}s",
                    )

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
                **build_ib_connect_kwargs(settings)
            )

            # Если подключились успешно, сразу идём на новую итерацию,
            # чтобы без лишней паузы зафиксировать восстановление.
            if ib.isConnected():
                await refresh_ib_account_updates(
                    ib,
                    account_id=settings.ib_account_id,
                    force=False,
                )
                continue

        except asyncio.CancelledError:
            # Корректно пробрасываем отмену задачи наружу.
            raise

        except Exception as e:
            error_text = f"{type(e).__name__}: {e!r}"

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
    """Что делает: периодически проверяет server time и health-флаги IB. Зачем нужна: даёт регулярное подтверждение, что соединение и data farms находятся в штатном состоянии."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)

        # Если локального API-соединения сейчас нет,
        # heartbeat не пишем — монитор и так уже выводит нужные сообщения.
        if not ib.isConnected():
            continue

        # Один запрос одновременно подтверждает backend и обновляет
        # канонический clock sample для trader guard.
        server_time_text = ""
        server_time_ok = False
        clock_sample = await sample_ib_clock_best_effort(
            ib,
            type(
                "HeartbeatSettings",
                (),
                {
                    "ib_client_id": None,
                },
            ),
        )

        if clock_sample is not None:
            server_time_text = (
                f"{format_ib_clock_server_time(clock_sample)}, "
                f"clock_offset={clock_sample.offset_seconds:+.3f}s, "
                f"rtt={clock_sample.round_trip_seconds:.3f}s"
            )
            server_time_ok = True
        else:
            server_time_text = "не получено"

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
