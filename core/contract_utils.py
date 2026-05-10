from ib_async import Contract

from contracts import PLACEHOLDER_CON_ID


def build_table_name(instrument_code, bar_size_setting):
    """Что делает: строит стабильное имя SQLite-таблицы из кода инструмента и размера бара IB. Зачем нужна: все price DB используют единый формат имени таблицы."""
    suffix = (
        bar_size_setting
        .replace(" ", "")
        .replace("secs", "s")
        .replace("sec", "s")
        .replace("hours", "h")
        .replace("hour", "h")
        .replace("mins", "m")
        .replace("min", "m")
    )
    return f"{instrument_code}_{suffix}"


def build_futures_contract(instrument_code, instrument_row, contract_row):
    """Что делает: собирает IB futures Contract из строки логического инструмента и строки конкретного контракта. Зачем нужна: historical/realtime запросы по FUT требуют точный localSymbol и contract metadata."""
    kwargs = {
        "secType": instrument_row["secType"],
        "symbol": instrument_code,
        "exchange": instrument_row["exchange"],
        "currency": instrument_row["currency"],
        "tradingClass": instrument_row["tradingClass"],
        "multiplier": str(instrument_row["multiplier"]),
        "localSymbol": contract_row["localSymbol"],
        "lastTradeDateOrContractMonth": contract_row["lastTradeDateOrContractMonth"],
    }

    # conId=111 лежит в contracts.py только как явный маркер "заполнить позже".
    con_id = contract_row["conId"]
    if con_id != PLACEHOLDER_CON_ID:
        kwargs["conId"] = con_id

    return Contract(**kwargs)


def build_instrument_contract(instrument_code, instrument_row, contract_row=None):
    """Что делает: создаёт IB Contract для поддерживаемых secType FUT, CASH и CRYPTO. Зачем нужна: скрывает различия построения контрактов от loaders."""
    sec_type = instrument_row["secType"]

    if sec_type == "FUT":
        if contract_row is None:
            raise ValueError(f"Для FUT-инструмента {instrument_code} нужен contract_row")

        return build_futures_contract(instrument_code, instrument_row, contract_row)

    if sec_type in ("CASH", "CRYPTO"):
        return Contract(
            secType=sec_type,
            symbol=instrument_row["symbol"],
            exchange=instrument_row["exchange"],
            currency=instrument_row["currency"],
        )

    raise ValueError(
        f"Неподдерживаемый secType для build_instrument_contract: "
        f"instrument={instrument_code}, secType={sec_type}"
    )


def get_contract_row_by_local_symbol(instrument_row, local_symbol):
    """Что делает: ищет строку фьючерсного контракта по localSymbol. Зачем нужна: repair/realtime-backfill получают active contract name и должны найти его параметры в contracts.py."""
    for contract_row in instrument_row["contracts"]:
        if contract_row["localSymbol"] == local_symbol:
            return contract_row

    raise ValueError(
        f"Контракт {local_symbol} не найден в списке contracts для инструмента"
    )


def get_contract_storage_name(instrument_code, instrument_row, contract_row=None):
    # Значение, которое пишется в поле contract в SQLite.
    #
    # Для фьючерсов это конкретный localSymbol, чтобы видеть сшивку.
    # Для CASH/CRYPTO квартальных контрактов нет, поэтому пишем код инструмента.
    """Что делает: выбирает значение поля contract для записи в price DB. Зачем нужна: FUT хранит localSymbol для сшивки, а CASH/CRYPTO хранят логический инструмент."""
    if instrument_row["secType"] == "FUT":
        if contract_row is None:
            raise ValueError(f"Для FUT-инструмента {instrument_code} нужен contract_row")
        return contract_row["localSymbol"]

    return instrument_code
