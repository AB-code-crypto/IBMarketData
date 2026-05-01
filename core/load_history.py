# bar_time_ts    — канонический UTC Unix timestamp бара.
# bar_time_ct    — человекочитаемое время бара в America/Chicago (CT).
# bar_time_ts_ct — внутренняя локальная числовая ось CT:
#                  bar_time_ts + utc_offset_for_this_bar.

import asyncio
from datetime import datetime, timezone

# Instrument — реестр инструментов из contracts.py.
from contracts import Instrument
from core.contract_utils import build_futures_contract, build_table_name

# Из db_sql берём SQL-шаблоны создания таблиц и upsert.
# Структуру таблиц держим централизованно в одном месте.
# Из logger берём:
# - get_logger: чтобы создать логгер именно этого модуля;
# - log_info / log_warning: наши обёртки над logging.
from core.logger import get_logger, log_info, log_warning
from core.time_utils import (
    format_utc,
    parse_utc_iso_to_ts,
)
from core.price_validation import validate_positive_price
from core.price_db import write_quote_rows_to_sqlite, get_contract_history_bounds
from core.quote_rows import build_quote_rows
from core.history_coverage import analyze_history_coverage, describe_missing_segments

# Логгер именно этого файла.
logger = get_logger(__name__)


# Пауза после каждого historical request.
#
# Для IB historical data лучше работать неторопливо,
# чтобы не упереться в pacing limits.
HISTORICAL_REQUEST_DELAY_SECONDS = 11

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

# Размер куска для 5-секундной истории фьючерсов.
# Пользователь явно зафиксировал, что для фьючерсов качаем по одному часу.
FUTURES_5_SECS_CHUNK_SECONDS = 3600



def format_exception_for_log(exc):
    # У TimeoutError строковое представление часто пустое, и в логе это выглядит неинформативно.
    # Поэтому всегда добавляем имя класса ошибки, а текст — только если он есть.
    exc_name = exc.__class__.__name__
    exc_text = str(exc).strip()

    if exc_text:
        return f"{exc_name}: {exc_text}"

    return exc_name


def build_duration_str(start_dt, end_dt):
    # IB historical request работает не со связкой start+end,
    # а с парой endDateTime + durationStr.
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        raise ValueError("Конец интервала должен быть строго больше начала")

    return f"{total_seconds} S"


def get_bar_size_seconds(bar_size_setting):
    if bar_size_setting == "5 secs":
        return 5

    raise ValueError(f"Неподдерживаемый barSizeSetting: {bar_size_setting}")


def get_chunk_seconds(sec_type, bar_size_setting):
    if sec_type == "FUT" and bar_size_setting == "5 secs":
        return FUTURES_5_SECS_CHUNK_SECONDS

    raise ValueError(
        f"Не задан размер куска для secType={sec_type}, barSizeSetting={bar_size_setting}"
    )


def align_timestamp_down(ts, step_seconds):
    # Выравниваем timestamp вниз до ближайшей границы бара.
    #
    # Примеры:
    # - ts=10:03:27 и step=5 -> 10:03:25
    # - ts=10:03:27 и step=3600 -> 10:00:00
    return ts - (ts % step_seconds)


def iter_chunks(start_ts, end_ts, chunk_seconds):
    # Разбиваем полуоткрытый интервал [start_ts, end_ts)
    # на последовательность кусков фиксированного размера.
    current_start_ts = start_ts

    while current_start_ts < end_ts:
        current_end_ts = min(current_start_ts + chunk_seconds, end_ts)
        yield current_start_ts, current_end_ts
        current_start_ts = current_end_ts


def should_load_futures_hour_chunk(chunk_start_ts, chunk_end_ts):
    # Проверяем, попадает ли часовой chunk в гарантированное weekend-окно CME
    # для MNQ/NQ в UTC.
    #
    # на летнее/зимнее время:
    # - с пятницы 22:00 UTC
    # - до воскресенья 22:00 UTC
    #
    # Если час целиком попадает в это окно, рынок гарантированно закрыт,
    # и такой chunk можно не запрашивать.
    #
    # Функция возвращает:
    # - True  -> chunk надо качать;
    # - False -> chunk гарантированно попал на выходные, пропускаем.
    chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
    chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

    # Для safety-логики функция рассчитана именно на часовые chunk-и.
    # Если когда-нибудь сюда начнут передавать другой размер,
    # лучше упасть сразу и явно.
    if int((chunk_end_dt - chunk_start_dt).total_seconds()) > 3600:
        raise ValueError(
            f"Функция should_load_futures_hour_chunk рассчитана только на часовые интервалы. Получен интервал: {chunk_start_dt}-{chunk_end_dt}"
        )

    start_weekday = chunk_start_dt.weekday()
    start_hour = chunk_start_dt.hour

    # Пятница после 22:00 UTC и до конца суток — гарантированно выходные.
    if start_weekday == 4 and start_hour >= 22:
        return False

    # Вся суббота целиком гарантированно попадает в weekend-окно.
    if start_weekday == 5:
        return False

    # Воскресенье до 22:00 UTC не торгуется.
    if start_weekday == 6 and start_hour < 22:
        return False

    return True


def validate_price_value(value, field_name, stream_name, contract_name, interval_text, bar_index):
    context = (
        f"{stream_name} для {contract_name}, "
        f"interval={interval_text}, bar_index={bar_index}"
    )
    return validate_positive_price(
        value,
        field_name=field_name,
        context=context,
    )


async def wait_for_ib_connection(ib):
    # Ждём восстановления локального соединения с TWS / IB Gateway.
    #
    # Исторический загрузчик не должен падать только потому,
    # что TWS на минуту перезапустился или сеть кратковременно пропала.
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
    # Та же идея, но для server time.
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


async def load_history_bid_ask_once(
        ib,
        ib_health,
        contract,
        db_path,
        table_name,
        start_dt,
        end_dt,
        bar_size_setting,
        use_rth,
):
    # Атомарная загрузка одного временного куска BID + ASK.
    #
    # Если historical request вернул битые цены, этот же chunk повторяем заново.
    while True:
        bid_bars = await request_historical_data_with_reconnect(
            ib=ib,
            ib_health=ib_health,
            contract=contract,
            end_dt=end_dt,
            start_dt=start_dt,
            bar_size_setting=bar_size_setting,
            what_to_show="BID",
            use_rth=use_rth,
        )

        await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

        ask_bars = await request_historical_data_with_reconnect(
            ib=ib,
            ib_health=ib_health,
            contract=contract,
            end_dt=end_dt,
            start_dt=start_dt,
            bar_size_setting=bar_size_setting,
            what_to_show="ASK",
            use_rth=use_rth,
        )

        await asyncio.sleep(HISTORICAL_REQUEST_DELAY_SECONDS)

        interval_text = f"{format_utc(start_dt)} -> {format_utc(end_dt)}"
        validation_error = None

        for index, bar in enumerate(bid_bars):
            validation_error = validate_price_value(
                value=bar.open,
                field_name="open",
                stream_name="BID",
                contract_name=contract.localSymbol,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                break

            validation_error = validate_price_value(
                value=bar.high,
                field_name="high",
                stream_name="BID",
                contract_name=contract.localSymbol,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                break

            validation_error = validate_price_value(
                value=bar.low,
                field_name="low",
                stream_name="BID",
                contract_name=contract.localSymbol,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                break

            validation_error = validate_price_value(
                value=bar.close,
                field_name="close",
                stream_name="BID",
                contract_name=contract.localSymbol,
                interval_text=interval_text,
                bar_index=index,
            )
            if validation_error is not None:
                break

        if validation_error is None:
            for index, bar in enumerate(ask_bars):
                validation_error = validate_price_value(
                    value=bar.open,
                    field_name="open",
                    stream_name="ASK",
                    contract_name=contract.localSymbol,
                    interval_text=interval_text,
                    bar_index=index,
                )
                if validation_error is not None:
                    break

                validation_error = validate_price_value(
                    value=bar.high,
                    field_name="high",
                    stream_name="ASK",
                    contract_name=contract.localSymbol,
                    interval_text=interval_text,
                    bar_index=index,
                )
                if validation_error is not None:
                    break

                validation_error = validate_price_value(
                    value=bar.low,
                    field_name="low",
                    stream_name="ASK",
                    contract_name=contract.localSymbol,
                    interval_text=interval_text,
                    bar_index=index,
                )
                if validation_error is not None:
                    break

                validation_error = validate_price_value(
                    value=bar.close,
                    field_name="close",
                    stream_name="ASK",
                    contract_name=contract.localSymbol,
                    interval_text=interval_text,
                    bar_index=index,
                )
                if validation_error is not None:
                    break

        if validation_error is not None:
            log_warning(
                logger,
                f"Фьючерс {contract.localSymbol}: historical request вернул некорректные BID/ASK цены. "
                f"{validation_error}. Повторяю этот же chunk",
                to_telegram=False,
            )
            await asyncio.sleep(RECONNECT_WAIT_SECONDS)
            continue

        rows = build_quote_rows(
            bid_bars=bid_bars,
            ask_bars=ask_bars,
            contract_name=contract.localSymbol,
        )

        await asyncio.to_thread(
            write_quote_rows_to_sqlite,
            db_path,
            table_name,
            rows,
        )

        return len(rows)


async def load_quotes_segment(
        ib,
        ib_health,
        db_path,
        table_name,
        contract,
        bar_size_setting,
        use_rth,
        segment_start_ts,
        segment_end_ts,
        segment_kind,
):
    # Качаем один недостающий сегмент фьючерсной истории.
    #
    # Сегмент может быть:
    # - full: если по контракту ещё нет вообще ничего;
    # - head: если не хватает начала;
    # - tail: если не хватает конца.
    if segment_end_ts <= segment_start_ts:
        return 0

    chunk_seconds = get_chunk_seconds("FUT", bar_size_setting)
    total_rows_written = 0

    for chunk_start_ts, chunk_end_ts in iter_chunks(segment_start_ts, segment_end_ts, chunk_seconds):
        chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
        chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

        if not should_load_futures_hour_chunk(chunk_start_ts, chunk_end_ts):
            log_info(
                logger,
                f"Фьючерс {contract.localSymbol}: chunk {format_utc(chunk_start_dt)} -> "
                f"{format_utc(chunk_end_dt)} гарантированно попал на выходные CME по UTC. Пропускаю.",
                to_telegram=False,
            )
            continue

        log_info(
            logger,
            f"Фьючерс {contract.localSymbol}: запрашиваю {segment_kind}-chunk "
            f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)} (BID + ASK)",
            to_telegram=False,
        )

        rows_written = await load_history_bid_ask_once(
            ib=ib,
            ib_health=ib_health,
            contract=contract,
            db_path=db_path,
            table_name=table_name,
            start_dt=chunk_start_dt,
            end_dt=chunk_end_dt,
            bar_size_setting=bar_size_setting,
            use_rth=use_rth,
        )

        total_rows_written += rows_written

        log_info(
            logger,
            f"Фьючерс {contract.localSymbol}: загружен {segment_kind}-chunk "
            f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)}, rows={rows_written}",
            to_telegram=False,
        )

    return total_rows_written


def get_current_aligned_ts(server_dt, bar_size_seconds):
    # Получаем текущее серверное время IB и сразу выравниваем вниз до границы бара.
    #
    # Это нужно, чтобы не пытаться докачивать ещё не закрытый текущий бар.
    raw_ts = int(server_dt.astimezone(timezone.utc).timestamp())
    return align_timestamp_down(raw_ts, bar_size_seconds)


async def process_futures_contract(
        ib,
        ib_health,
        settings,
        instrument_code,
        instrument_row,
        contract_row,
        table_name,
        current_aligned_ts,
):
    # Полная обработка одного фьючерсного контракта:
    # - использовать уже полученное и выровненное server time IB;
    # - проверить, не контракт ли из будущего;
    # - определить рабочий целевой интервал истории;
    # - посмотреть покрытие в БД по contract;
    # - при необходимости докачать начало и/или конец.
    #
    # Важная оптимизация по согласованной логике:
    # server time для фьючерсов не запрашиваем на каждом контракте заново.
    # Мы получаем его один раз на входе в инструмент,
    # а потом обновляем только после реальной закачки по контракту.
    contract = build_futures_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )

    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])

    # Рабочие границы контракта берём из canonical UTC-строк.
    #
    # Это убирает необходимость хранить рядом дублирующие active_from_ts_utc /
    # active_to_ts_utc и исключает расхождение между строковым временем и заранее
    # кем-то когда-то посчитанным timestamp.
    active_from_ts = parse_utc_iso_to_ts(contract_row["active_from_utc"])
    active_to_ts = parse_utc_iso_to_ts(contract_row["active_to_utc"])

    log_info(
        logger,
        f"Взял в работу фьючерс {contract.localSymbol} (conId={contract_row['conId']}). "
        f"Окно активности: {contract_row['active_from_utc']} -> {contract_row['active_to_utc']}. "
        f"Использую текущее server time IB из кеша: {format_utc_ts(current_aligned_ts)}",
        to_telegram=False,
    )

    # Если контракт ещё не начался — это будущий контракт, его не трогаем.
    if current_aligned_ts <= active_from_ts:
        log_info(
            logger,
            f"Фьючерс {contract.localSymbol} ещё не начался. "
            f"current_aligned={format_utc_ts(current_aligned_ts)}, "
            f"active_from={format_utc_ts(active_from_ts)}. Пропускаю.",
            to_telegram=False,
        )
        return 0, False

    # Правая граница целевого интервала:
    # - для уже завершившегося контракта это active_to_ts;
    # - для текущего контракта это текущее время, выровненное вниз до границы бара.
    target_start_ts = active_from_ts
    target_end_ts = min(active_to_ts, current_aligned_ts)

    # Если после выравнивания правой границы выяснилось,
    # что полного бара ещё нет — просто пропускаем.
    if target_end_ts <= target_start_ts:
        log_info(
            logger,
            f"Фьючерс {contract.localSymbol}: пока нет ни одного закрытого бара в рабочем окне. Пропускаю.",
            to_telegram=False,
        )
        return 0, False

    # Смотрим историю только по этому конкретному контракту.
    db_min_ts, db_max_ts = await asyncio.to_thread(
        get_contract_history_bounds,
        settings.price_db_path,
        table_name,
        contract.localSymbol,
    )

    coverage = analyze_history_coverage(
        target_start_ts=target_start_ts,
        target_end_ts=target_end_ts,
        existing_min_ts=db_min_ts,
        existing_max_ts=db_max_ts,
        bar_size_seconds=bar_size_seconds,
    )

    if coverage["is_full"]:
        log_info(
            logger,
            f"Фьючерс {contract.localSymbol}: история уже есть полностью. "
            f"Нужно покрытие {format_utc_ts(target_start_ts)} -> {format_utc_ts(target_end_ts)}. "
            f"В БД есть {format_utc_ts(db_min_ts)} -> {format_utc_ts(coverage['loaded_until_ts'])}. Пропускаю.",
            to_telegram=False,
        )
        return 0, False

    missing_text = describe_missing_segments(coverage["segments"])

    # В Telegram по загрузчику шлём только старт закачки именно по фьючерсу,
    # как было отдельно оговорено пользователем.
    log_info(
        logger,
        f"Начинаю закачку истории по фьючерсу {contract.localSymbol}. Не хватает: {missing_text}",
        to_telegram=True,
    )

    log_info(
        logger,
        f"Фьючерс {contract.localSymbol}: в БД сейчас есть "
        f"{format_utc_ts(db_min_ts) if db_min_ts is not None else '-'} -> "
        f"{format_utc_ts(coverage['loaded_until_ts']) if coverage['loaded_until_ts'] is not None else '-'}; "
        f"целевой интервал {format_utc_ts(target_start_ts)} -> {format_utc_ts(target_end_ts)}. "
        f"Докачиваю: {missing_text}",
        to_telegram=False,
    )

    total_rows_written = 0

    for segment in coverage["segments"]:
        total_rows_written += await load_quotes_segment(
            ib=ib,
            ib_health=ib_health,
            db_path=settings.price_db_path,
            table_name=table_name,
            contract=contract,
            bar_size_setting=instrument_row["barSizeSetting"],
            use_rth=instrument_row["useRTH"],
            segment_start_ts=segment["start_ts"],
            segment_end_ts=segment["end_ts"],
            segment_kind=segment["kind"],
        )

    # После докачки повторно смотрим границы в БД и пишем итоговый лог.
    new_db_min_ts, new_db_max_ts = await asyncio.to_thread(
        get_contract_history_bounds,
        settings.price_db_path,
        table_name,
        contract.localSymbol,
    )

    new_loaded_until_ts = None
    if new_db_max_ts is not None:
        new_loaded_until_ts = new_db_max_ts + bar_size_seconds

    log_info(
        logger,
        f"Фьючерс {contract.localSymbol}: закачка завершена. "
        f"Теперь в БД есть {format_utc_ts(new_db_min_ts) if new_db_min_ts is not None else '-'} -> "
        f"{format_utc_ts(new_loaded_until_ts) if new_loaded_until_ts is not None else '-'}, "
        f"записано строк: {total_rows_written}. Перехожу к следующему контракту.",
        to_telegram=False,
    )

    return total_rows_written, True


async def load_history_task(ib, ib_health, settings):
    # Главная таска загрузки истории.
    #
    # 1. идём по реестру Instrument;
    # 2. для FUT обрабатываем каждый контракт отдельно;
    # 3. для каждого контракта смотрим покрытие истории именно по contract;
    # 4. пропускаем будущие контракты;
    # 5. если история уже полная — пропускаем;
    # 6. если не хватает начала и/или конца — докачиваем только эти участки;
    # 7. на обрывах связи не падаем, а ждём реконнект и продолжаем.
    log_info(logger, "Запускаю задачу загрузки истории", to_telegram=False)

    total_rows_written = 0

    for instrument_code, instrument_row in Instrument.items():
        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )

        log_info(
            logger,
            f"Начинаю обработку инструмента {instrument_code}. secType={instrument_row['secType']}, "
            f"table={table_name}, barSizeSetting={instrument_row['barSizeSetting']}",
            to_telegram=False,
        )

        if instrument_row["secType"] == "FUT":
            log_info(
                logger,
                f"Инструмент {instrument_code}: всего контрактов в списке {len(instrument_row['contracts'])}",
                to_telegram=False,
            )

            # Получаем server time один раз на входе в инструмент.
            # Этого достаточно, чтобы быстро отсеять будущие контракты и понять,
            # какой именно диапазон нужен по текущему контракту.
            instrument_server_dt = await request_current_time_with_reconnect(ib)
            current_aligned_ts = get_current_aligned_ts(
                instrument_server_dt,
                get_bar_size_seconds(instrument_row["barSizeSetting"]),
            )

            log_info(
                logger,
                f"Инструмент {instrument_code}: стартовое server time IB {format_utc(instrument_server_dt)}. "
                f"Выровненное время по размеру бара: {format_utc_ts(current_aligned_ts)}",
                to_telegram=False,
            )

            for contract_row in instrument_row["contracts"]:
                rows_written, was_loaded = await process_futures_contract(
                    ib=ib,
                    ib_health=ib_health,
                    settings=settings,
                    instrument_code=instrument_code,
                    instrument_row=instrument_row,
                    contract_row=contract_row,
                    table_name=table_name,
                    current_aligned_ts=current_aligned_ts,
                )
                total_rows_written += rows_written

                # Время обновляем только после реальной закачки.
                # Если контракт просто пропустили как будущий или уже полный,
                # лишний reqCurrentTimeAsync нам не нужен.
                if was_loaded:
                    instrument_server_dt = await request_current_time_with_reconnect(ib)
                    current_aligned_ts = get_current_aligned_ts(
                        instrument_server_dt,
                        get_bar_size_seconds(instrument_row["barSizeSetting"]),
                    )

                    log_info(
                        logger,
                        f"Инструмент {instrument_code}: после закачки по контракту обновил server time IB до "
                        f"{format_utc(instrument_server_dt)}. Выровненное время: {format_utc_ts(current_aligned_ts)}",
                        to_telegram=False,
                    )

            log_info(
                logger,
                f"Инструмент {instrument_code}: обработка всех контрактов завершена",
                to_telegram=False,
            )
            continue

        # Если в реестр случайно попадёт неподдерживаемый secType,
        # падаем сразу и явно.
        raise ValueError(
            f"Неподдерживаемый secType в Instrument[{instrument_code}]: {instrument_row['secType']}"
        )

    log_info(
        logger,
        f"Задача загрузки истории завершена. Всего записано строк: {total_rows_written}",
        to_telegram=False,
    )
