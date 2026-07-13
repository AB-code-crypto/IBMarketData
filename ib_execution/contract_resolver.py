import time

from contracts import Instrument, PLACEHOLDER_CON_ID
from core.contract_utils import build_instrument_contract
from core.time_utils import parse_utc_iso_to_ts


def get_active_contract_row(*, instrument_code: str, now_ts: int | None = None):
    """Выбирает active contract row из contracts.py для текущего времени."""
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


def find_registered_contract_row(
        *,
        instrument_code: str,
        con_id: int | None = None,
        local_symbol: str | None = None,
) -> dict | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None

    if str(instrument_row.get("secType", "")).upper() != "FUT":
        return None

    expected_con_id = int(con_id or 0)
    expected_local_symbol = str(local_symbol or "")

    for contract_row in instrument_row.get("contracts", []):
        row_con_id = int(contract_row.get("conId", 0) or 0)
        row_local_symbol = str(
            contract_row.get("localSymbol", "") or ""
        )

        if (
                expected_con_id > 0
                and expected_con_id != PLACEHOLDER_CON_ID
                and row_con_id == expected_con_id
        ):
            return contract_row

        if (
                expected_local_symbol
                and row_local_symbol == expected_local_symbol
        ):
            return contract_row

    return None


def build_execution_contract(
        *,
        instrument_code: str,
        now_ts: int | None = None,
        broker_con_id: int | None = None,
        broker_local_symbol: str | None = None,
):
    """Собирает execution Contract.

    Для обычного входа используется active contract. Для закрытия позиции
    после rollover можно передать broker_con_id/localSymbol и закрыть именно
    тот квартальный контракт, который реально находится на счёте.
    """
    instrument_row = Instrument[instrument_code]
    sec_type = str(instrument_row.get("secType", "")).upper()

    if sec_type == "FUT" and (
            broker_con_id is not None
            or broker_local_symbol
    ):
        contract_row = find_registered_contract_row(
            instrument_code=instrument_code,
            con_id=broker_con_id,
            local_symbol=broker_local_symbol,
        )
        if contract_row is None:
            raise RuntimeError(
                f"Broker contract is not registered for {instrument_code}: "
                f"conId={broker_con_id}, localSymbol={broker_local_symbol}"
            )
    else:
        contract_row = get_active_contract_row(
            instrument_code=instrument_code,
            now_ts=now_ts,
        )

    return build_instrument_contract(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
        contract_row=contract_row,
    )
