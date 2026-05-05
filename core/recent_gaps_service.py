from datetime import datetime, timezone

from contracts import Instrument
from core.bar_utils import DEFAULT_HISTORY_LOOKBACK_DAYS, get_history_lookback_start_ts
from core.contract_utils import (
    build_instrument_contract,
    get_contract_row_by_local_symbol,
    get_contract_storage_name,
)
from core.history_segment_loader import load_quotes_segment
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.logger import get_logger, log_info
from core.time_utils import format_utc, parse_utc_iso_to_ts

logger = get_logger(__name__)

# Разово добираем только последний час.
RECENT_BACKFILL_WINDOW_SECONDS = 3600


def note_first_realtime_bar_timestamps(first_bid_ts, first_ask_ts, what_to_show, bar_time_ts):
    # Запоминаем только первый увиденный timestamp по каждой стороне.
    #
    # Состояние сервис у себя не хранит.
    # Внешний код передаёт текущие значения внутрь и получает обновлённые наружу.
    if what_to_show == "BID":
        if first_bid_ts is None:
            first_bid_ts = bar_time_ts
        return first_bid_ts, first_ask_ts

    if what_to_show == "ASK":
        if first_ask_ts is None:
            first_ask_ts = bar_time_ts
        return first_bid_ts, first_ask_ts

    raise ValueError(f"Неподдерживаемый realtime stream: {what_to_show}")


def is_first_synced_bid_ask_bar_ready(first_bid_ts, first_ask_ts):
    # Первый полноценный realtime-старт считаем подтверждённым,
    # когда обе стороны уже пришли и timestamp у них одинаковый.
    if first_bid_ts is None:
        return False

    if first_ask_ts is None:
        return False

    return first_bid_ts == first_ask_ts


def get_recent_backfill_sync_ts(first_bid_ts, first_ask_ts):
    # Возвращаем timestamp первого синхронного BID/ASK бара.
    if not is_first_synced_bid_ask_bar_ready(first_bid_ts, first_ask_ts):
        raise ValueError("Первый синхронный BID/ASK бар ещё не получен")

    return first_bid_ts


def get_instrument_left_boundary_ts(instrument_row, contract_row, sync_ts):
    # Левая граница, левее которой недавний добор заходить не должен.
    if instrument_row["secType"] == "FUT":
        return parse_utc_iso_to_ts(contract_row["active_from_utc"])

    history_start_utc = instrument_row.get("history_start_utc")
    if history_start_utc:
        return parse_utc_iso_to_ts(history_start_utc)

    history_lookback_days = instrument_row.get("history_lookback_days", DEFAULT_HISTORY_LOOKBACK_DAYS)
    return get_history_lookback_start_ts(sync_ts, history_lookback_days)


def get_recent_backfill_range(instrument_code, contract_local_symbol, sync_ts):
    # Строим полуоткрытый интервал недавнего добора:
    # [sync_ts - 1 час, sync_ts)
    #
    # Левую границу дополнительно ограничиваем началом рабочего окна инструмента.
    instrument_row = Instrument[instrument_code]

    contract_row = None
    if instrument_row["secType"] == "FUT":
        contract_row = get_contract_row_by_local_symbol(
            instrument_row,
            contract_local_symbol,
        )

    left_boundary_ts = get_instrument_left_boundary_ts(
        instrument_row=instrument_row,
        contract_row=contract_row,
        sync_ts=sync_ts,
    )

    from_ts = max(left_boundary_ts, sync_ts - RECENT_BACKFILL_WINDOW_SECONDS)
    to_ts = sync_ts
    return from_ts, to_ts


async def backfill_recent_hour(ib, ib_health, settings, instrument_code, contract_local_symbol, sync_ts):
    # Разово догружаем последний час историей до первого синхронного realtime-бара.
    #
    # Никаких проверок существования БД или таблицы здесь не делаем.
    # Считаем, что вся инфраструктура уже подготовлена и работает.
    instrument_row = Instrument[instrument_code]
    sec_type = instrument_row["secType"]

    if sec_type == "FUT":
        contract_row = get_contract_row_by_local_symbol(instrument_row, contract_local_symbol)
    elif sec_type in ("CASH", "CRYPTO"):
        contract_row = None
    else:
        raise ValueError(
            f"Сервис добора не поддерживает secType={sec_type} "
            f"для instrument={instrument_code}"
        )

    from_ts, to_ts = get_recent_backfill_range(
        instrument_code=instrument_code,
        contract_local_symbol=contract_local_symbol,
        sync_ts=sync_ts,
    )

    if to_ts <= from_ts:
        return False

    db_path = get_instrument_db_path(settings, instrument_code, instrument_row)
    table_name = get_instrument_table_name(instrument_code, instrument_row)
    contract = build_instrument_contract(instrument_code, instrument_row, contract_row)
    contract_name = get_contract_storage_name(instrument_code, instrument_row, contract_row)

    log_info(
        logger,
        f"Сервис добора: получен первый синхронный BID/ASK бар "
        f"{format_utc(datetime.fromtimestamp(to_ts, tz=timezone.utc))} "
        f"для {contract_name}. "
        f"Запускаю разовую докачку последнего часа: "
        f"{format_utc(datetime.fromtimestamp(from_ts, tz=timezone.utc))} -> "
        f"{format_utc(datetime.fromtimestamp(to_ts, tz=timezone.utc))}",
        to_telegram=False,
    )

    await load_quotes_segment(
        ib=ib,
        ib_health=ib_health,
        db_path=db_path,
        table_name=table_name,
        contract=contract,
        contract_name=contract_name,
        sec_type=sec_type,
        session_model=instrument_row.get("session_model", ""),
        bar_size_setting=instrument_row["barSizeSetting"],
        use_rth=instrument_row["useRTH"],
        segment_start_ts=from_ts,
        segment_end_ts=to_ts,
        segment_kind="recent-backfill",
    )

    log_info(
        logger,
        f"Сервис добора: разовая докачка последнего часа завершена для "
        f"{contract_name}",
        to_telegram=False,
    )
    return True
