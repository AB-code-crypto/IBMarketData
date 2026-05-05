from ib_async import Contract

from contracts import PLACEHOLDER_CON_ID


def build_table_name(instrument_code, bar_size_setting):
    """Строит стабильное имя SQLite-таблицы из кода инструмента и размера бара IB."""
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
    """Строит IB futures Contract из локального реестра contracts.py."""
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

    # conId используем, если он задан и не равен временной заглушке.
    # conId=111 лежит в contracts.py только как явный маркер "заполнить позже".
    con_id = contract_row.get("conId")
    if con_id is not None and con_id != PLACEHOLDER_CON_ID:
        kwargs["conId"] = con_id

    return Contract(**kwargs)


def build_cash_contract(instrument_code, instrument_row):
    """Строит IB CASH Contract, например EUR.USD через IDEALPRO."""
    return Contract(
        secType=instrument_row["secType"],
        symbol=instrument_row.get("symbol", instrument_code),
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
    )


def build_crypto_contract(instrument_code, instrument_row):
    """Строит IB CRYPTO Contract, например BTC.USD через PAXOS/ZEROHASH."""
    return Contract(
        secType=instrument_row["secType"],
        symbol=instrument_row.get("symbol", instrument_code),
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
    )


def build_instrument_contract(instrument_code, instrument_row, contract_row=None):
    """Строит IB Contract для поддерживаемого типа инструмента."""
    sec_type = instrument_row["secType"]

    if sec_type == "FUT":
        if contract_row is None:
            raise ValueError(f"Для FUT-инструмента {instrument_code} нужен contract_row")
        return build_futures_contract(instrument_code, instrument_row, contract_row)

    if sec_type == "CASH":
        return build_cash_contract(instrument_code, instrument_row)

    if sec_type == "CRYPTO":
        return build_crypto_contract(instrument_code, instrument_row)

    raise ValueError(
        f"Неподдерживаемый secType для build_instrument_contract: "
        f"instrument={instrument_code}, secType={sec_type}"
    )


def get_contract_row_by_local_symbol(instrument_row, local_symbol):
    """Ищет строку фьючерсного контракта в реестре инструмента по localSymbol."""
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
    if instrument_row["secType"] == "FUT":
        if contract_row is None:
            raise ValueError(f"Для FUT-инструмента {instrument_code} нужен contract_row")
        return contract_row["localSymbol"]

    return instrument_code
