from ib_async import Contract


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
    return Contract(
        secType=instrument_row["secType"],
        symbol=instrument_code,
        exchange=instrument_row["exchange"],
        currency=instrument_row["currency"],
        tradingClass=instrument_row["tradingClass"],
        multiplier=str(instrument_row["multiplier"]),
        conId=contract_row["conId"],
        localSymbol=contract_row["localSymbol"],
        lastTradeDateOrContractMonth=contract_row["lastTradeDateOrContractMonth"],
    )


def get_contract_row_by_local_symbol(instrument_row, local_symbol):
    """Ищет строку фьючерсного контракта в реестре инструмента по localSymbol."""
    for contract_row in instrument_row["contracts"]:
        if contract_row["localSymbol"] == local_symbol:
            return contract_row

    raise ValueError(
        f"Контракт {local_symbol} не найден в списке contracts для инструмента"
    )
