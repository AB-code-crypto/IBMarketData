from datetime import datetime, timezone

from contracts import Instrument
from core.logger import get_logger, log_warning

logger = get_logger(__name__)


def parse_server_time_text(server_time_text):
    # Разбираем время сервера IB в UTC.
    #
    # Ожидаем строку в формате из get_ib_server_time_text:
    # YYYY-MM-DD HH:MM:SS
    """Что делает: преобразует строку server time от IB в UTC datetime. Зачем нужна: даёт единый формат времени для выбора активного фьючерсного контракта."""
    dt = datetime.strptime(server_time_text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_contract_utc_text(utc_text):
    # Разбираем UTC-время контракта из contracts.py.
    #
    # В реестре контрактов время хранится в ISO-формате:
    # YYYY-MM-DDTHH:MM:SSZ
    """Что делает: преобразует ISO-время из contracts.py в UTC datetime. Зачем нужна: позволяет сравнивать active_from_utc/active_to_utc с текущим временем IB."""
    dt = datetime.strptime(utc_text, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_active_futures_local_symbol(instrument_code, instrument_row, current_utc, server_time_text):
    # Возвращает localSymbol активного фьючерсного контракта.
    """Что делает: ищет localSymbol фьючерсного контракта, активного на текущий момент. Зачем нужна: realtime должен подписываться на конкретный квартальный контракт, а не на логический инструмент."""
    for contract_row in instrument_row["contracts"]:
        active_from_utc = parse_contract_utc_text(contract_row["active_from_utc"])
        active_to_utc = parse_contract_utc_text(contract_row["active_to_utc"])

        if active_from_utc <= current_utc < active_to_utc:
            return contract_row["localSymbol"]

    raise RuntimeError(
        f"Текущий контракт не найден: instrument={instrument_code}, "
        f"server_time_utc={server_time_text}"
    )


def build_active_instruments(server_time_text):
    # Возвращает словарь активных инструментов для realtime.
    #
    # Для FUT значение — localSymbol текущего активного фьючерса.
    # Для CASH/CRYPTO значение — сам код инструмента, потому что rollover отсутствует.
    """Что делает: строит словарь активных realtime-инструментов на момент старта сервиса. Зачем нужна: фиксирует стартовый набор подписок и не подхватывает изменения contracts.py на лету."""
    current_utc = parse_server_time_text(server_time_text)
    active_instruments = {}

    for instrument_code, instrument_row in Instrument.items():
        if not instrument_row["realtime_enabled"]:
            continue

        try:
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

        except Exception as exc:
            log_warning(
                logger,
                f"Не удалось определить active-инструмент для realtime: "
                f"instrument={instrument_code}, error={exc}. Пропускаю realtime по нему.",
                to_telegram=True,
            )

    return active_instruments
