from ib_async import Contract


def build_table_name(instrument_code, bar_size_setting):
    """Build a stable SQLite table name from instrument code and IB bar size."""
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
    """Build an IB futures Contract from the local contracts.py registry."""
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
    """Find a futures contract row in an instrument registry row by localSymbol."""
    for contract_row in instrument_row["contracts"]:
        if contract_row["localSymbol"] == local_symbol:
            return contract_row

    raise ValueError(
        f"Контракт {local_symbol} не найден в списке contracts для инструмента"
    )
