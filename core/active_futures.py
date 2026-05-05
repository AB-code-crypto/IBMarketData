from datetime import datetime, timezone

from contracts import Instrument
from core.logger import get_logger, log_warning

logger = get_logger(__name__)


def parse_server_time_text(server_time_text):
    # Разбираем время сервера IB в UTC.
    #
    # Ожидаем строку в формате из get_ib_server_time_text:
    # YYYY-MM-DD HH:MM:SS
    dt = datetime.strptime(server_time_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_contract_utc_text(utc_text):
    # Разбираем UTC-время контракта из contracts.py.
    #
    # В реестре контрактов время хранится в ISO-формате:
    # YYYY-MM-DDTHH:MM:SSZ
    dt = datetime.strptime(utc_text, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_active_futures_local_symbol(instrument_code, instrument_row, current_utc, server_time_text):
    # Возвращает localSymbol активного фьючерсного контракта.
    current_local_symbol = None

    for contract_row in instrument_row["contracts"]:
        active_from_utc = parse_contract_utc_text(contract_row["active_from_utc"])
        active_to_utc = parse_contract_utc_text(contract_row["active_to_utc"])

        if active_from_utc <= current_utc < active_to_utc:
            current_local_symbol = contract_row["localSymbol"]
            break

    if current_local_symbol is None:
        error_text = (
            f"Текущий контракт не найден: instrument={instrument_code}, "
            f"server_time_utc={server_time_text}"
        )
        log_warning(logger, error_text, to_telegram=True)
        raise RuntimeError(error_text)

    return current_local_symbol


def build_active_instruments(server_time_text):
    # Возвращает словарь активных инструментов для realtime.
    #
    # Для FUT значение — localSymbol текущего активного фьючерса.
    # Для CASH/CRYPTO значение — сам код инструмента, потому что rollover отсутствует.
    current_utc = parse_server_time_text(server_time_text)
    active_instruments = {}

    for instrument_code, instrument_row in Instrument.items():
        sec_type = instrument_row["secType"]

        if sec_type == "FUT":
            active_instruments[instrument_code] = get_active_futures_local_symbol(
                instrument_code=instrument_code,
                instrument_row=instrument_row,
                current_utc=current_utc,
                server_time_text=server_time_text,
            )
            continue

        if sec_type in ("CASH", "CRYPTO"):
            active_instruments[instrument_code] = instrument_code
            continue

        raise ValueError(
            f"Неподдерживаемый secType для active instruments: "
            f"instrument={instrument_code}, secType={sec_type}"
        )

    return active_instruments


def build_active_futures(server_time_text):
    # Обратная совместимость для старого имени функции.
    # Возвращает только FUT-инструменты.
    active_instruments = build_active_instruments(server_time_text)
    return {
        instrument_code: active_name
        for instrument_code, active_name in active_instruments.items()
        if Instrument[instrument_code]["secType"] == "FUT"
    }
