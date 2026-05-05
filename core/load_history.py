# bar_time_ts — канонический UTC Unix timestamp бара и единственный ключ времени.
# bar_time — человекочитаемое время бара в UTC.
# bar_time_ct — человекочитаемое время бара в America/Chicago.
# bar_time_msk — человекочитаемое время бара в Europe/Moscow.

import asyncio
import traceback

from contracts import Instrument
from core.bar_utils import (
    DEFAULT_HISTORY_LOOKBACK_DAYS,
    get_bar_size_seconds,
    get_current_aligned_ts,
    get_history_lookback_start_ts,
)
from core.contract_utils import (
    build_instrument_contract,
    get_contract_storage_name,
)
from core.history_coverage import analyze_history_coverage, describe_missing_segments
from core.history_segment_loader import load_quotes_segment
from core.ib_request_utils import request_current_time_with_reconnect
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.logger import get_logger, log_info, log_warning
from core.price_db import get_contract_history_bounds
from core.time_utils import (
    format_utc,
    format_utc_ts,
    parse_utc_iso_to_ts,
)

logger = get_logger(__name__)


def is_instrument_history_enabled(instrument_row):
    # Проверяем выключатель history-загрузки инструмента.
    return instrument_row.get("history_enabled", True)


def get_instrument_configured_start_ts(instrument_row, current_aligned_ts):
    # Возвращает левую границу истории, заданную на уровне инструмента.
    #
    # history_start_utc имеет приоритет над history_lookback_days.
    # Для новых инструментов удобнее задавать lookback, чтобы не тянуть историю
    # глубже лимитов IB и не запускать многодневную initial load.
    history_start_utc = instrument_row.get("history_start_utc")
    if history_start_utc:
        return parse_utc_iso_to_ts(history_start_utc)

    history_lookback_days = instrument_row.get("history_lookback_days")
    if history_lookback_days is not None:
        return get_history_lookback_start_ts(current_aligned_ts, history_lookback_days)

    return None


async def process_instrument_history_target(
        ib,
        ib_health,
        *,
        db_path,
        table_name,
        instrument_row,
        contract,
        contract_name,
        target_start_ts,
        target_end_ts,
):
    # Общая обработка одного целевого интервала истории.
    # Для FUT это интервал конкретного фьючерсного контракта.
    # Для CASH/CRYPTO это интервал самого логического инструмента.
    if target_end_ts <= target_start_ts:
        log_info(
            logger,
            f"Инструмент {contract_name}: пока нет ни одного закрытого бара "
            f"в рабочем окне. Пропускаю.",
            to_telegram=False,
        )
        return 0, False

    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])

    db_min_ts, db_max_ts = await asyncio.to_thread(
        get_contract_history_bounds,
        db_path,
        table_name,
        contract_name,
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
            f"Инструмент {contract_name}: история уже есть полностью. "
            f"Нужно покрытие {format_utc_ts(target_start_ts)} -> {format_utc_ts(target_end_ts)}. "
            f"В БД есть {format_utc_ts(db_min_ts)} -> "
            f"{format_utc_ts(coverage['loaded_until_ts'])}. Пропускаю.",
            to_telegram=False,
        )
        return 0, False

    missing_text = describe_missing_segments(coverage["segments"])

    log_info(
        logger,
        f"Начинаю закачку истории по инструменту {contract_name}. Не хватает: {missing_text}",
        to_telegram=True,
    )

    log_info(
        logger,
        f"Инструмент {contract_name}: в БД сейчас есть "
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
            db_path=db_path,
            table_name=table_name,
            contract=contract,
            contract_name=contract_name,
            sec_type=instrument_row["secType"],
            session_model=instrument_row.get("session_model", ""),
            bar_size_setting=instrument_row["barSizeSetting"],
            use_rth=instrument_row["useRTH"],
            segment_start_ts=segment["start_ts"],
            segment_end_ts=segment["end_ts"],
            segment_kind=segment["kind"],
        )

    new_db_min_ts, new_db_max_ts = await asyncio.to_thread(
        get_contract_history_bounds,
        db_path,
        table_name,
        contract_name,
    )

    new_loaded_until_ts = None
    if new_db_max_ts is not None:
        new_loaded_until_ts = new_db_max_ts + bar_size_seconds

    log_info(
        logger,
        f"Инструмент {contract_name}: закачка завершена. "
        f"Теперь в БД есть {format_utc_ts(new_db_min_ts) if new_db_min_ts is not None else '-'} -> "
        f"{format_utc_ts(new_loaded_until_ts) if new_loaded_until_ts is not None else '-'}, "
        f"записано строк: {total_rows_written}.",
        to_telegram=False,
    )

    return total_rows_written, True


async def process_futures_contract(
        ib,
        ib_health,
        *,
        db_path,
        table_name,
        instrument_code,
        instrument_row,
        contract_row,
        current_aligned_ts,
):
    # Обработка одного фьючерсного контракта из сшиваемого ряда.
    contract = build_instrument_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )
    contract_name = get_contract_storage_name(instrument_code, instrument_row, contract_row)

    active_from_ts = parse_utc_iso_to_ts(contract_row["active_from_utc"])
    active_to_ts = parse_utc_iso_to_ts(contract_row["active_to_utc"])

    configured_start_ts = get_instrument_configured_start_ts(instrument_row, current_aligned_ts)
    if configured_start_ts is not None:
        active_from_ts = max(active_from_ts, configured_start_ts)

    log_info(
        logger,
        f"Взял в работу фьючерс {contract_name}. "
        f"Окно активности: {contract_row['active_from_utc']} -> {contract_row['active_to_utc']}. "
        f"Использую текущее server time IB из кеша: {format_utc_ts(current_aligned_ts)}",
        to_telegram=False,
    )

    if current_aligned_ts <= active_from_ts:
        log_info(
            logger,
            f"Фьючерс {contract_name} ещё не начался или левее заданного lookback. "
            f"current_aligned={format_utc_ts(current_aligned_ts)}, "
            f"target_from={format_utc_ts(active_from_ts)}. Пропускаю.",
            to_telegram=False,
        )
        return 0, False

    target_start_ts = active_from_ts
    target_end_ts = min(active_to_ts, current_aligned_ts)

    return await process_instrument_history_target(
        ib=ib,
        ib_health=ib_health,
        db_path=db_path,
        table_name=table_name,
        instrument_row=instrument_row,
        contract=contract,
        contract_name=contract_name,
        target_start_ts=target_start_ts,
        target_end_ts=target_end_ts,
    )


async def process_single_contract_instrument(
        ib,
        ib_health,
        *,
        db_path,
        table_name,
        instrument_code,
        instrument_row,
        current_aligned_ts,
):
    # Обработка инструмента без фьючерсного rollover: CASH или CRYPTO.
    contract = build_instrument_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )
    contract_name = get_contract_storage_name(instrument_code, instrument_row)

    target_start_ts = get_instrument_configured_start_ts(instrument_row, current_aligned_ts)
    if target_start_ts is None:
        target_start_ts = get_history_lookback_start_ts(
            current_aligned_ts,
            DEFAULT_HISTORY_LOOKBACK_DAYS,
        )

    target_end_ts = current_aligned_ts

    log_info(
        logger,
        f"Взял в работу инструмент {contract_name}. secType={instrument_row['secType']}. "
        f"Целевой интервал: {format_utc_ts(target_start_ts)} -> {format_utc_ts(target_end_ts)}",
        to_telegram=False,
    )

    return await process_instrument_history_target(
        ib=ib,
        ib_health=ib_health,
        db_path=db_path,
        table_name=table_name,
        instrument_row=instrument_row,
        contract=contract,
        contract_name=contract_name,
        target_start_ts=target_start_ts,
        target_end_ts=target_end_ts,
    )


async def process_instrument_history(
        ib,
        ib_health,
        settings,
        instrument_code,
        instrument_row,
):
    # Обрабатываем историю одного инструмента.
    db_path = get_instrument_db_path(settings, instrument_code, instrument_row)
    table_name = get_instrument_table_name(instrument_code, instrument_row)
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])

    log_info(
        logger,
        f"Начинаю обработку инструмента {instrument_code}. "
        f"secType={instrument_row['secType']}, db={db_path}, "
        f"table={table_name}, barSizeSetting={instrument_row['barSizeSetting']}",
        to_telegram=False,
    )

    instrument_server_dt = await request_current_time_with_reconnect(ib)
    current_aligned_ts = get_current_aligned_ts(
        instrument_server_dt,
        bar_size_seconds,
    )

    log_info(
        logger,
        f"Инструмент {instrument_code}: server time IB {format_utc(instrument_server_dt)}. "
        f"Выровненное время по размеру бара: {format_utc_ts(current_aligned_ts)}",
        to_telegram=False,
    )

    if instrument_row["secType"] == "FUT":
        total_rows_written = 0

        log_info(
            logger,
            f"Инструмент {instrument_code}: всего контрактов в списке "
            f"{len(instrument_row['contracts'])}",
            to_telegram=False,
        )

        for contract_row in instrument_row["contracts"]:
            rows_written, was_loaded = await process_futures_contract(
                ib=ib,
                ib_health=ib_health,
                db_path=db_path,
                table_name=table_name,
                instrument_code=instrument_code,
                instrument_row=instrument_row,
                contract_row=contract_row,
                current_aligned_ts=current_aligned_ts,
            )
            total_rows_written += rows_written

            # Время обновляем только после реальной закачки.
            if was_loaded:
                instrument_server_dt = await request_current_time_with_reconnect(ib)
                current_aligned_ts = get_current_aligned_ts(
                    instrument_server_dt,
                    bar_size_seconds,
                )

                log_info(
                    logger,
                    f"Инструмент {instrument_code}: после закачки обновил server time IB до "
                    f"{format_utc(instrument_server_dt)}. "
                    f"Выровненное время: {format_utc_ts(current_aligned_ts)}",
                    to_telegram=False,
                )

        log_info(
            logger,
            f"Инструмент {instrument_code}: обработка всех контрактов завершена",
            to_telegram=False,
        )
        return total_rows_written

    if instrument_row["secType"] in ("CASH", "CRYPTO"):
        rows_written, _ = await process_single_contract_instrument(
            ib=ib,
            ib_health=ib_health,
            db_path=db_path,
            table_name=table_name,
            instrument_code=instrument_code,
            instrument_row=instrument_row,
            current_aligned_ts=current_aligned_ts,
        )
        return rows_written

    raise ValueError(
        f"Неподдерживаемый secType в Instrument[{instrument_code}]: "
        f"{instrument_row['secType']}"
    )


async def load_history_task(ib, ib_health, settings):
    # Главная таска загрузки истории.
    #
    # 1. идём по реестру Instrument;
    # 2. для FUT обрабатываем каждый контракт отдельно;
    # 3. для CASH/CRYPTO обрабатываем сам инструмент как single-contract;
    # 4. для каждого инструмента используем его отдельную SQLite-БД;
    # 5. ошибка одного инструмента не останавливает остальные;
    # 6. на обрывах связи не падаем, а ждём реконнект и продолжаем.
    log_info(logger, "Запускаю задачу загрузки истории", to_telegram=False)

    total_rows_written = 0

    for instrument_code, instrument_row in Instrument.items():
        if not is_instrument_history_enabled(instrument_row):
            log_info(
                logger,
                f"Инструмент {instrument_code}: history-загрузка выключена, пропускаю.",
                to_telegram=False,
            )
            continue

        try:
            total_rows_written += await process_instrument_history(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                instrument_code=instrument_code,
                instrument_row=instrument_row,
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log_warning(
                logger,
                f"Загрузка истории по инструменту {instrument_code} завершилась ошибкой: "
                f"{exc}\n{traceback.format_exc()}",
                to_telegram=True,
            )
            continue

    log_info(
        logger,
        f"Задача загрузки истории завершена. Всего записано строк: {total_rows_written}",
        to_telegram=False,
    )
