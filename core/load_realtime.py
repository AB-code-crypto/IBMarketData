import asyncio
import time
import traceback
from datetime import timezone

from contracts import Instrument
from core.cme_session import is_expected_realtime_flow_now
from core.contract_utils import (
    build_instrument_contract,
    get_contract_row_by_local_symbol,
    get_contract_storage_name,
)
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.logger import get_logger, log_info, log_warning
from core.price_validation import validate_positive_price
from core.realtime_db import open_quotes_db, write_realtime_bar_to_sqlite
from core.realtime_monitor import (
    is_realtime_ready_now,
    note_realtime_bar_received,
    reset_recent_backfill_state,
)
from core.realtime_subscriptions import (
    clear_realtime_subscription_rows,
    subscribe_realtime_bars,
)
from core.recent_gaps_service import (
    backfill_recent_hour,
    get_recent_backfill_sync_ts,
    is_first_synced_bid_ask_bar_ready,
    note_first_realtime_bar_timestamps,
)
from core.runtime_state import RecentBackfillState, RealtimeMonitorState
from core.time_utils import format_utc

logger = get_logger(__name__)

# Какие потоки данных хотим получать в realtime.
# Для нашей схемы нужны отдельные бары по BID и ASK,
# потому что именно так мы уже работаем с историческими данными и БД.
REALTIME_WHAT_TO_SHOW_LIST = ("BID", "ASK")

# Как часто ждём восстановления соединения / market data farm
# перед самой первой подпиской.
REALTIME_READY_WAIT_SECONDS = 1

# Через сколько секунд без новых баров считаем realtime-поток подозрительно зависшим.
REALTIME_STALL_WARNING_SECONDS = 30

# Как часто слать Telegram-сообщение о штатной работе realtime-потока.
REALTIME_OK_TELEGRAM_INTERVAL_SECONDS = 600

# Сколько секунд после восстановления соединения даём подпискам,
# прежде чем считать отсутствие баров проблемой.
REALTIME_RESUBSCRIBE_GRACE_SECONDS = 15


def get_realtime_instrument_row(instrument_code):
    # Берём настройки инструмента из нашего реестра.
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code} не найден в contracts.py")

    instrument_row = Instrument[instrument_code]

    if instrument_row["secType"] not in ("FUT", "CASH", "CRYPTO"):
        raise ValueError(
            f"Realtime loader не поддерживает secType={instrument_row['secType']}"
        )

    if instrument_row["barSizeSetting"] != "5 secs":
        raise ValueError(
            f"Realtime loader ожидает barSizeSetting='5 secs', "
            f"получено: {instrument_row['barSizeSetting']}"
        )

    return instrument_row


def get_realtime_contract_context(instrument_code, active_contract_name):
    # Возвращает всё, что нужно realtime-loader'у для одного инструмента.
    instrument_row = get_realtime_instrument_row(instrument_code)
    sec_type = instrument_row["secType"]

    if sec_type == "FUT":
        contract_row = get_contract_row_by_local_symbol(instrument_row, active_contract_name)
    else:
        contract_row = None

    contract = build_instrument_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )
    contract_name = get_contract_storage_name(instrument_code, instrument_row, contract_row)

    return instrument_row, contract_row, contract, contract_name


async def wait_for_realtime_ready(ib, ib_health):
    # Для первой подписки ждём нормального состояния соединения и market data.
    wait_reason = ""

    while True:
        if not ib.isConnected():
            if wait_reason != "connection":
                log_warning(
                    logger,
                    "Realtime loader ждёт восстановления API-соединения с IB...",
                    to_telegram=False,
                )
                wait_reason = "connection"

            await asyncio.sleep(REALTIME_READY_WAIT_SECONDS)
            continue

        if not ib_health.ib_backend_ok:
            if wait_reason != "backend":
                log_warning(
                    logger,
                    "Realtime loader ждёт восстановления backend IB...",
                    to_telegram=False,
                )
                wait_reason = "backend"

            await asyncio.sleep(REALTIME_READY_WAIT_SECONDS)
            continue

        if not ib_health.market_data_ok:
            if wait_reason != "market_data":
                log_warning(
                    logger,
                    "Realtime loader ждёт восстановления market data farm...",
                    to_telegram=False,
                )
                wait_reason = "market_data"

            await asyncio.sleep(REALTIME_READY_WAIT_SECONDS)
            continue

        if wait_reason:
            log_info(
                logger,
                "Realtime loader продолжает работу: соединение и market data снова в норме",
                to_telegram=False,
            )

        return


def validate_price_value(value, field_name, stream_name, contract_name, bar_time_text):
    context = (
        f"realtime {stream_name} для {contract_name}, "
        f"bar_time={bar_time_text}"
    )
    return validate_positive_price(
        value,
        field_name=field_name,
        context=context,
    )


def validate_realtime_bar(contract_name, what_to_show, bar):
    # Проверяем весь realtime-бар целиком.
    bar_time_text = format_utc(bar.time)

    for field_name, field_value in (
            ("open", bar.open_),
            ("high", bar.high),
            ("low", bar.low),
            ("close", bar.close),
    ):
        validation_error = validate_price_value(
            value=field_value,
            field_name=field_name,
            stream_name=what_to_show,
            contract_name=contract_name,
            bar_time_text=bar_time_text,
        )
        if validation_error is not None:
            return validation_error

    return None


def format_realtime_bar_message(contract_name, what_to_show, bar):
    # Собираем одну строку лога по новому 5-секундному бару.
    # В лог обязательно добавляем тип потока,
    # чтобы сразу было видно, это BID-бар или ASK-бар.
    bar_time_text = format_utc(bar.time)

    return (
        f"RT BAR {contract_name} | {what_to_show} | {bar_time_text} | "
        f"O={bar.open_} H={bar.high} L={bar.low} C={bar.close} "
        f"V={bar.volume} WAP={bar.wap} COUNT={bar.count}"
    )


def maybe_start_recent_backfill_task(
        ib,
        ib_health,
        settings,
        instrument_code,
        contract_name,
        recent_backfill_state,
        what_to_show,
        bar_time_ts,
):
    # Обновляем состояние первого BID / ASK бара и,
    # когда получен первый синхронный BID/ASK bar_time_ts,
    # один раз запускаем дозагрузку последнего часа.
    first_bid_ts, first_ask_ts = note_first_realtime_bar_timestamps(
        first_bid_ts=recent_backfill_state.first_bid_ts,
        first_ask_ts=recent_backfill_state.first_ask_ts,
        what_to_show=what_to_show,
        bar_time_ts=bar_time_ts,
    )

    recent_backfill_state.first_bid_ts = first_bid_ts
    recent_backfill_state.first_ask_ts = first_ask_ts

    if not is_first_synced_bid_ask_bar_ready(first_bid_ts, first_ask_ts):
        return

    sync_ts = get_recent_backfill_sync_ts(first_bid_ts, first_ask_ts)

    if recent_backfill_state.last_backfill_completed_sync_ts == sync_ts:
        return

    backfill_task = recent_backfill_state.backfill_task
    if backfill_task is not None and not backfill_task.done():
        return

    async def run_recent_backfill():
        try:
            was_loaded = await backfill_recent_hour(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                instrument_code=instrument_code,
                contract_local_symbol=contract_name,
                sync_ts=sync_ts,
            )

            if was_loaded:
                recent_backfill_state.last_backfill_completed_sync_ts = sync_ts

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log_warning(
                logger,
                f"Разовая докачка последнего часа завершилась ошибкой "
                f"для {contract_name}: {exc}",
                to_telegram=False,
            )

        finally:
            recent_backfill_state.backfill_task = None

    recent_backfill_state.backfill_task = asyncio.create_task(run_recent_backfill())


def build_realtime_update_handler(
        ib,
        ib_health,
        settings,
        instrument_code,
        contract_name,
        recent_backfill_state,
        what_to_show,
        conn,
        table_name,
        realtime_monitor_state,
):
    # Для каждой отдельной подписки делаем свой обработчик,
    # чтобы BID и ASK обрабатывались независимо и без догадок по контексту.
    def on_bar_update(bars, has_new_bar):
        try:
            # Пишем только когда реально добавился новый бар,
            # а не когда просто обновился последний.
            if not has_new_bar:
                return

            if len(bars) == 0:
                return

            bar = bars[-1]
            validation_error = validate_realtime_bar(contract_name, what_to_show, bar)

            if validation_error is not None:
                log_warning(
                    logger,
                    f"Пропускаю некорректный realtime-бар. {validation_error}",
                    to_telegram=False,
                )
                return

            write_realtime_bar_to_sqlite(
                conn=conn,
                table_name=table_name,
                contract_name=contract_name,
                what_to_show=what_to_show,
                bar=bar,
            )

            log_info(
                logger,
                format_realtime_bar_message(contract_name, what_to_show, bar),
                to_telegram=False,
            )

            bar_time_ts = int(bar.time.astimezone(timezone.utc).timestamp())
            note_realtime_bar_received(realtime_monitor_state)
            maybe_start_recent_backfill_task(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                instrument_code=instrument_code,
                contract_name=contract_name,
                recent_backfill_state=recent_backfill_state,
                what_to_show=what_to_show,
                bar_time_ts=bar_time_ts,
            )

        except Exception as exc:
            log_warning(
                logger,
                f"Ошибка в realtime update handler "
                f"({contract_name}, {what_to_show}): {exc}\n"
                f"{traceback.format_exc()}",
                to_telegram=False,
            )

    return on_bar_update


async def load_realtime_instrument_task(
        ib,
        ib_health,
        settings,
        instrument_code,
        active_contract_name,
):
    # Realtime loader для одного логического инструмента.
    instrument_row, _, contract, contract_name = get_realtime_contract_context(
        instrument_code=instrument_code,
        active_contract_name=active_contract_name,
    )

    use_rth = instrument_row["useRTH"]
    table_name = get_instrument_table_name(instrument_code, instrument_row)
    db_path = get_instrument_db_path(settings, instrument_code, instrument_row)
    session_model = instrument_row.get("session_model", "")
    db_conn = None

    recent_backfill_state = RecentBackfillState()
    realtime_monitor_state = RealtimeMonitorState()
    current_subscriptions = []

    try:
        log_info(
            logger,
            f"Запускаю realtime loader для {instrument_code}: "
            f"contract={contract_name}, secType={instrument_row['secType']}",
            to_telegram=False,
        )

        db_conn = open_quotes_db(db_path)

        log_info(
            logger,
            f"Realtime loader: запись в БД включена. db={db_path}, table={table_name}",
            to_telegram=False,
        )

        await wait_for_realtime_ready(ib, ib_health)

        def subscribe_all_realtime_streams():
            clear_realtime_subscription_rows(ib, current_subscriptions)

            for what_to_show in REALTIME_WHAT_TO_SHOW_LIST:
                log_info(
                    logger,
                    f"Realtime loader: открываю подписку на {contract_name}, "
                    f"whatToShow={what_to_show}, useRTH={use_rth}",
                    to_telegram=False,
                )

                realtime_bars = subscribe_realtime_bars(
                    ib=ib,
                    contract=contract,
                    what_to_show=what_to_show,
                    use_rth=use_rth,
                )

                update_handler = build_realtime_update_handler(
                    ib=ib,
                    ib_health=ib_health,
                    settings=settings,
                    instrument_code=instrument_code,
                    contract_name=contract_name,
                    recent_backfill_state=recent_backfill_state,
                    what_to_show=what_to_show,
                    conn=db_conn,
                    table_name=table_name,
                    realtime_monitor_state=realtime_monitor_state,
                )

                realtime_bars.updateEvent += update_handler

                current_subscriptions.append(
                    {
                        "what_to_show": what_to_show,
                        "realtime_bars": realtime_bars,
                        "update_handler": update_handler,
                    }
                )

                log_info(
                    logger,
                    f"Подписался на real-time 5-second bars: {contract_name}, "
                    f"whatToShow={what_to_show}, useRTH={use_rth}",
                    to_telegram=False,
                )

        subscribe_all_realtime_streams()

        was_realtime_ready = is_realtime_ready_now(ib, ib_health)
        if was_realtime_ready:
            realtime_monitor_state.last_restore_monotonic = time.monotonic()

        while True:
            realtime_ready_now = is_realtime_ready_now(ib, ib_health)
            now_mono = time.monotonic()

            if was_realtime_ready and not realtime_ready_now:
                reset_recent_backfill_state(recent_backfill_state)

                log_warning(
                    logger,
                    f"Realtime loader: поток {instrument_code} временно недоступен. "
                    f"Сбрасываю состояние recent backfill и жду восстановления подписок.",
                    to_telegram=True,
                )

            elif not was_realtime_ready and realtime_ready_now:
                log_info(
                    logger,
                    f"Realtime loader: соединение/market data восстановлены, "
                    f"переподписываюсь на realtime {instrument_code}",
                    to_telegram=True,
                )

                subscribe_all_realtime_streams()

                realtime_monitor_state.last_restore_monotonic = now_mono
                realtime_monitor_state.last_stall_warning_monotonic = None
                realtime_monitor_state.last_ok_telegram_monotonic = None

            if realtime_ready_now and is_expected_realtime_flow_now(session_model):
                last_bar_monotonic = realtime_monitor_state.last_bar_monotonic
                last_restore_monotonic = realtime_monitor_state.last_restore_monotonic

                bar_is_recent = (
                        last_bar_monotonic is not None
                        and (now_mono - last_bar_monotonic) <= REALTIME_STALL_WARNING_SECONDS
                )

                restore_grace_passed = (
                        last_restore_monotonic is None
                        or (now_mono - last_restore_monotonic) >= REALTIME_RESUBSCRIBE_GRACE_SECONDS
                )

                if restore_grace_passed and not bar_is_recent:
                    last_warning = realtime_monitor_state.last_stall_warning_monotonic
                    if last_warning is None or (now_mono - last_warning) >= REALTIME_STALL_WARNING_SECONDS:
                        log_warning(
                            logger,
                            f"Realtime loader: после восстановления/в рабочее время нет новых "
                            f"BID/ASK баров для {instrument_code} уже "
                            f"{REALTIME_STALL_WARNING_SECONDS}+ секунд. "
                            f"Пробую переподписаться.",
                            to_telegram=True,
                        )

                        subscribe_all_realtime_streams()

                        realtime_monitor_state.last_stall_warning_monotonic = now_mono
                        realtime_monitor_state.last_restore_monotonic = now_mono

                if bar_is_recent:
                    last_ok = realtime_monitor_state.last_ok_telegram_monotonic
                    if last_ok is None or (now_mono - last_ok) >= REALTIME_OK_TELEGRAM_INTERVAL_SECONDS:
                        log_info(
                            logger,
                            f"Realtime loader: поток {instrument_code} работает штатно, "
                            f"новые BID/ASK бары продолжают поступать.",
                            to_telegram=True,
                        )
                        realtime_monitor_state.last_ok_telegram_monotonic = now_mono

            was_realtime_ready = realtime_ready_now
            await asyncio.sleep(1)

    finally:
        clear_realtime_subscription_rows(ib, current_subscriptions)

        if db_conn is not None:
            try:
                db_conn.close()
            except Exception as exc:
                log_warning(
                    logger,
                    f"Не удалось закрыть SQLite-соединение realtime loader-а "
                    f"для {instrument_code}: {exc}",
                    to_telegram=False,
                )


async def load_realtime_task(
        ib,
        ib_health,
        settings,
        active_instruments,
):
    # Запускаем realtime loader по всем активным инструментам.
    if not isinstance(active_instruments, dict) or not active_instruments:
        raise ValueError("active_instruments должен быть непустым словарём")

    tasks = []

    for instrument_code, active_contract_name in active_instruments.items():
        task = asyncio.create_task(
            load_realtime_instrument_task(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                instrument_code=instrument_code,
                active_contract_name=active_contract_name,
            ),
            name=f"load_realtime_{instrument_code}",
        )
        tasks.append(task)

    try:
        await asyncio.gather(*tasks)

    finally:
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
