import time

from contracts import Instrument
from core.contract_utils import build_instrument_contract
from core.time_utils import parse_utc_iso_to_ts


def get_active_contract_row(*, instrument_code: str, now_ts: int | None = None):
    """Что делает: выбирает active contract row из contracts.py для текущего времени.
    Зачем нужна: execution должен отправлять ордер в актуальный фьючерсный контракт."""
    instrument_row = Instrument[instrument_code]

    if instrument_row["secType"] != "FUT":
        return None

    now_ts = int(time.time() if now_ts is None else now_ts)

    for contract_row in instrument_row["contracts"]:
        active_from_ts = parse_utc_iso_to_ts(contract_row["active_from_utc"])
        active_to_ts = parse_utc_iso_to_ts(contract_row["active_to_utc"])

        if active_from_ts <= now_ts < active_to_ts:
            return contract_row

    raise RuntimeError(
        f"Не найден active contract для {instrument_code} на ts={now_ts}"
    )


def build_execution_contract(*, instrument_code: str, now_ts: int | None = None):
    """Что делает: собирает IB Contract для исполнения по логическому instrument_code."""
    instrument_row = Instrument[instrument_code]
    contract_row = get_active_contract_row(
        instrument_code=instrument_code,
        now_ts=now_ts,
    )

    return build_instrument_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )
