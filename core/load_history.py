# bar_time_ts — канонический UTC Unix timestamp бара и единственный ключ времени.
# bar_time — человекочитаемое время бара в UTC.
# bar_time_ct — человекочитаемое время бара в America/Chicago.
# bar_time_msk — человекочитаемое время бара в Europe/Moscow.

import asyncio

# Instrument — реестр инструментов из contracts.py.
from contracts import Instrument
from core.bar_utils import get_bar_size_seconds, get_current_aligned_ts
from core.contract_utils import build_futures_contract, build_table_name
from core.history_coverage import analyze_history_coverage, describe_missing_segments
from core.history_segment_loader import load_quotes_segment
from core.ib_request_utils import request_current_time_with_reconnect
from core.logger import get_logger, log_info
from core.price_db import get_contract_history_bounds
from core.time_utils import (
    format_utc,
    format_utc_ts,
    parse_utc_iso_to_ts,
)

# Логгер именно этого файла.
logger = get_logger(__name__)


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
