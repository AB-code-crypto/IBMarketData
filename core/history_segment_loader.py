import asyncio
from datetime import datetime, timezone

from core.bar_utils import get_chunk_seconds, iter_chunks
from core.history_bar_validation import validate_history_bid_ask_bars
from core.ib_request_utils import (
    RECONNECT_WAIT_SECONDS,
    request_historical_data_with_reconnect,
)
from core.logger import get_logger, log_info, log_warning
from core.market_sessions import should_load_history_chunk
from core.price_db import write_quote_rows_to_sqlite
from core.quote_rows import build_quote_rows
from core.time_utils import format_utc

logger = get_logger(__name__)

# Пауза после каждого historical request.
#
# Для IB historical data лучше работать неторопливо,
# чтобы не упереться в pacing limits.
HISTORICAL_REQUEST_DELAY_SECONDS = 11

# Сколько подряд запрошенных historical chunks могут вернуть 0 строк.
# Если лимит достигнут, считаем текущий сегмент/контракт недоступным
# на заданной глубине и прекращаем его обработку.
MAX_CONSECUTIVE_EMPTY_HISTORY_CHUNKS = 3


async def load_history_bid_ask_once(
        ib,
        ib_health,
        contract,
        contract_name,
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
    """Что делает: загружает один historical chunk BID и ASK, валидирует и записывает его в price DB. Зачем нужна: это атомарная операция получения одного куска истории."""
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
        validation_error = validate_history_bid_ask_bars(
            bid_bars=bid_bars,
            ask_bars=ask_bars,
            contract_name=contract_name,
            interval_text=interval_text,
        )

        if validation_error is not None:
            log_warning(
                logger,
                f"Инструмент {contract_name}: historical request вернул некорректные "
                f"BID/ASK цены. {validation_error}. Повторяю этот же chunk",
                to_telegram=False,
            )
            await asyncio.sleep(RECONNECT_WAIT_SECONDS)
            continue

        rows = build_quote_rows(
            bid_bars=bid_bars,
            ask_bars=ask_bars,
            contract_name=contract_name,
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
        contract_name,
        sec_type,
        session_model,
        bar_size_setting,
        use_rth,
        segment_start_ts,
        segment_end_ts,
        segment_kind,
):
    # Качаем один недостающий сегмент истории.
    #
    # Сегмент может быть:
    # - full: если по инструменту/контракту ещё нет вообще ничего;
    # - head: если не хватает начала;
    # - tail: если не хватает конца;
    # - recent-backfill: если добираем свежий час после старта/reconnect.
    """Что делает: разбивает недостающий сегмент на chunks, фильтрует неторговые окна и загружает каждый chunk. Зачем нужна: используется для full/head/tail/recent-backfill загрузки без дублирования логики."""
    if segment_end_ts <= segment_start_ts:
        return 0

    chunk_seconds = get_chunk_seconds(sec_type, bar_size_setting)
    total_rows_written = 0
    consecutive_empty_chunks = 0

    for chunk_start_ts, chunk_end_ts in iter_chunks(segment_start_ts, segment_end_ts, chunk_seconds):
        chunk_start_dt = datetime.fromtimestamp(chunk_start_ts, tz=timezone.utc)
        chunk_end_dt = datetime.fromtimestamp(chunk_end_ts, tz=timezone.utc)

        if not should_load_history_chunk(
                session_model=session_model,
                chunk_start_ts=chunk_start_ts,
                chunk_end_ts=chunk_end_ts,
        ):
            log_info(
                logger,
                f"Инструмент {contract_name}: chunk {format_utc(chunk_start_dt)} -> "
                f"{format_utc(chunk_end_dt)} попал в нерабочее окно session_model={session_model}. "
                f"Пропускаю.",
                to_telegram=False,
            )
            continue

        log_info(
            logger,
            f"Инструмент {contract_name}: запрашиваю {segment_kind}-chunk "
            f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)} (BID + ASK)",
            to_telegram=False,
        )

        rows_written = await load_history_bid_ask_once(
            ib=ib,
            ib_health=ib_health,
            contract=contract,
            contract_name=contract_name,
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
            f"Инструмент {contract_name}: загружен {segment_kind}-chunk "
            f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)}, rows={rows_written}",
            to_telegram=False,
        )

        if rows_written == 0:
            consecutive_empty_chunks += 1

            log_warning(
                logger,
                f"Инструмент {contract_name}: {segment_kind}-chunk "
                f"{format_utc(chunk_start_dt)} -> {format_utc(chunk_end_dt)} "
                f"вернул 0 строк. Пустых chunks подряд: "
                f"{consecutive_empty_chunks}/{MAX_CONSECUTIVE_EMPTY_HISTORY_CHUNKS}",
                to_telegram=False,
            )

            if consecutive_empty_chunks >= MAX_CONSECUTIVE_EMPTY_HISTORY_CHUNKS:
                log_warning(
                    logger,
                    f"Инструмент {contract_name}: "
                    f"{MAX_CONSECUTIVE_EMPTY_HISTORY_CHUNKS} historical chunks подряд "
                    f"вернули 0 строк. Прекращаю обработку текущего сегмента "
                    f"{format_utc(datetime.fromtimestamp(segment_start_ts, tz=timezone.utc))} -> "
                    f"{format_utc(datetime.fromtimestamp(segment_end_ts, tz=timezone.utc))}. "
                    f"Вероятно, история недоступна на этой глубине или нет BID/ASK данных.",
                    to_telegram=True,
                )
                break

        else:
            consecutive_empty_chunks = 0

    return total_rows_written
