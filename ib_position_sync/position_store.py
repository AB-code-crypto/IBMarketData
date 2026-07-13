import time
from typing import Any

from config import settings_live as app_settings
from contracts import Instrument, PLACEHOLDER_CON_ID
from core.ib_account import normalize_account_id, position_belongs_to_account
from ib_execution.broker_state_service import get_broker_state_service
from core.time_utils import format_utc_ts
from ib_execution.contract_resolver import get_active_contract_row
from ib_trader.trade_schema import (
    POSITIONS_LATEST_TABLE_NAME,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_position_sync.position_models import BrokerPositionSnapshot


class MultipleBrokerContractPositionsError(RuntimeError):
    pass


def get_trading_enabled_instrument_codes() -> list[str]:
    """Возвращает инструменты, для которых включена торговля."""
    return sorted(
        str(instrument_code)
        for instrument_code, instrument_row in Instrument.items()
        if bool(instrument_row.get("trading_enabled", False))
    )


async def request_broker_positions(
        ib,
        *,
        expected_account_id: str | None = None,
        force_refresh: bool = False,
) -> list[Any]:
    # Reads positions through the shared throttled broker-state service.
    account_id = normalize_account_id(
        expected_account_id or app_settings.ib_account_id
    )
    service = get_broker_state_service(
        ib,
        account_id=account_id,
    )
    return await service.get_positions(
        force=bool(force_refresh),
    )


def get_contract_attr(contract, name: str):
    return getattr(contract, name, None)


def find_registered_futures_contract_row(
        *,
        position_contract,
        instrument_code: str,
) -> dict | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None

    if str(instrument_row.get("secType", "")).upper() != "FUT":
        return None

    broker_local_symbol = str(
        get_contract_attr(position_contract, "localSymbol") or ""
    )
    broker_con_id = int(
        get_contract_attr(position_contract, "conId") or 0
    )

    for contract_row in instrument_row.get("contracts", []):
        local_symbol = str(contract_row.get("localSymbol", "") or "")
        con_id = int(contract_row.get("conId", 0) or 0)

        if local_symbol and broker_local_symbol == local_symbol:
            return contract_row

        if (
                con_id
                and con_id != PLACEHOLDER_CON_ID
                and broker_con_id == con_id
        ):
            return contract_row

    return None


def is_same_contract_for_instrument(
        *,
        position_contract,
        instrument_code: str,
        now_ts: int | None = None,
) -> bool:
    """Проверяет принадлежность broker contract логическому инструменту.

    Для futures учитываются все зарегистрированные квартальные контракты,
    а не только текущий active contract. Это не даёт старой позиции исчезнуть
    из positions_latest сразу после rollover.
    """
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return False

    sec_type = str(instrument_row.get("secType", "")).upper()

    if sec_type == "FUT":
        return (
            find_registered_futures_contract_row(
                position_contract=position_contract,
                instrument_code=instrument_code,
            )
            is not None
        )

    broker_sec_type = str(
        get_contract_attr(position_contract, "secType") or ""
    ).upper()
    broker_symbol = str(
        get_contract_attr(position_contract, "symbol") or ""
    )
    broker_currency = str(
        get_contract_attr(position_contract, "currency") or ""
    )

    return (
        broker_sec_type == sec_type
        and broker_symbol == str(
            instrument_row.get("symbol", instrument_code)
        )
        and broker_currency == str(
            instrument_row.get("currency", "")
        )
    )


def is_active_contract_for_instrument(
        *,
        position_contract,
        instrument_code: str,
        now_ts: int,
) -> bool | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None

    if str(instrument_row.get("secType", "")).upper() != "FUT":
        return True

    matched_row = find_registered_futures_contract_row(
        position_contract=position_contract,
        instrument_code=instrument_code,
    )
    if matched_row is None:
        return None

    active_row = get_active_contract_row(
        instrument_code=instrument_code,
        now_ts=now_ts,
    )

    matched_con_id = int(matched_row.get("conId", 0) or 0)
    active_con_id = int(active_row.get("conId", 0) or 0)
    matched_local_symbol = str(
        matched_row.get("localSymbol", "") or ""
    )
    active_local_symbol = str(
        active_row.get("localSymbol", "") or ""
    )

    if (
            matched_con_id
            and matched_con_id != PLACEHOLDER_CON_ID
            and active_con_id
            and active_con_id != PLACEHOLDER_CON_ID
    ):
        return matched_con_id == active_con_id

    return (
        bool(matched_local_symbol)
        and matched_local_symbol == active_local_symbol
    )


def normalize_side_and_qty(raw_position: float) -> tuple[str, float]:
    quantity = float(raw_position)

    if quantity > 0.0:
        return "LONG", quantity

    if quantity < 0.0:
        return "SHORT", abs(quantity)

    return "FLAT", 0.0


def find_position_for_instrument(
        *,
        broker_positions: list[Any],
        instrument_code: str,
        now_ts: int,
        expected_account_id: str | None = None,
) -> BrokerPositionSnapshot:
    """Находит единственную фактическую позицию нашего account/instrument.

    Если одновременно открыты два квартальных контракта одного futures,
    позицию нельзя сворачивать в одну строку positions_latest: торговля
    блокируется исключением вместо ложного FLAT/aggregate состояния.
    """
    expected_account = normalize_account_id(
        expected_account_id or app_settings.ib_account_id
    )
    matches: list[tuple[Any, Any, float]] = []

    for broker_position in broker_positions:
        if not position_belongs_to_account(
                broker_position,
                expected_account_id=expected_account,
        ):
            continue

        contract = getattr(broker_position, "contract", None)
        if contract is None:
            continue

        if not is_same_contract_for_instrument(
                position_contract=contract,
                instrument_code=instrument_code,
                now_ts=now_ts,
        ):
            continue

        raw_quantity = float(
            getattr(broker_position, "position", 0.0) or 0.0
        )
        if abs(raw_quantity) <= 1e-12:
            continue

        matches.append(
            (broker_position, contract, raw_quantity)
        )

    if len(matches) > 1:
        details = [
            {
                "account": str(
                    getattr(position, "account", "") or ""
                ),
                "contract": str(
                    get_contract_attr(contract, "localSymbol")
                    or get_contract_attr(contract, "symbol")
                    or ""
                ),
                "conId": int(
                    get_contract_attr(contract, "conId") or 0
                ),
                "position": raw_quantity,
            }
            for position, contract, raw_quantity in matches
        ]
        raise MultipleBrokerContractPositionsError(
            f"{instrument_code}: multiple broker contract positions "
            f"exist on account {expected_account}: {details}"
        )

    if not matches:
        return BrokerPositionSnapshot(
            instrument_code=instrument_code,
            side="FLAT",
            quantity=0.0,
            broker_contract=None,
            broker_account=expected_account,
            broker_con_id=None,
            contract_is_active=None,
        )

    broker_position, contract, raw_quantity = matches[0]
    side, quantity = normalize_side_and_qty(raw_quantity)

    broker_contract = (
        str(get_contract_attr(contract, "localSymbol") or "")
        or str(get_contract_attr(contract, "symbol") or "")
        or None
    )
    broker_con_id_raw = int(
        get_contract_attr(contract, "conId") or 0
    )
    broker_con_id = (
        broker_con_id_raw
        if broker_con_id_raw > 0
        else None
    )

    return BrokerPositionSnapshot(
        instrument_code=instrument_code,
        side=side,
        quantity=quantity,
        broker_contract=broker_contract,
        broker_account=str(
            getattr(broker_position, "account", "") or expected_account
        ),
        broker_con_id=broker_con_id,
        contract_is_active=is_active_contract_for_instrument(
            position_contract=contract,
            instrument_code=instrument_code,
            now_ts=now_ts,
        ),
    )


def write_position_snapshot(
        conn,
        *,
        snapshot: BrokerPositionSnapshot,
        updated_at_ts: int,
) -> None:
    initialize_trade_db(conn)

    conn.execute(
        f"""
        INSERT INTO {POSITIONS_LATEST_TABLE_NAME} (
            instrument_code,
            side,
            quantity,
            broker_contract,
            broker_con_id,
            broker_account,
            contract_is_active,
            updated_at_ts,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT(instrument_code) DO UPDATE SET
            side = excluded.side,
            quantity = excluded.quantity,
            broker_contract = excluded.broker_contract,
            broker_con_id = excluded.broker_con_id,
            broker_account = excluded.broker_account,
            contract_is_active = excluded.contract_is_active,
            updated_at_ts = excluded.updated_at_ts,
            updated_at_utc = excluded.updated_at_utc
        """,
        (
            snapshot.instrument_code,
            snapshot.side,
            float(snapshot.quantity),
            snapshot.broker_contract,
            snapshot.broker_con_id,
            snapshot.broker_account,
            (
                None
                if snapshot.contract_is_active is None
                else int(bool(snapshot.contract_is_active))
            ),
            int(updated_at_ts),
            format_utc_ts(int(updated_at_ts)),
        ),
    )


async def sync_broker_positions_once(
        ib,
        *,
        expected_account_id: str | None = None,
        force_refresh: bool = False,
) -> list[BrokerPositionSnapshot]:
    now_ts = int(time.time())
    expected_account = normalize_account_id(
        expected_account_id or app_settings.ib_account_id
    )
    broker_positions = await request_broker_positions(
        ib,
        expected_account_id=expected_account,
        force_refresh=force_refresh,
    )
    instrument_codes = get_trading_enabled_instrument_codes()

    snapshots = [
        find_position_for_instrument(
            broker_positions=broker_positions,
            instrument_code=instrument_code,
            now_ts=now_ts,
            expected_account_id=expected_account,
        )
        for instrument_code in instrument_codes
    ]

    conn = get_trade_db_connection()

    try:
        initialize_trade_db(conn)

        for snapshot in snapshots:
            write_position_snapshot(
                conn,
                snapshot=snapshot,
                updated_at_ts=now_ts,
            )

        conn.commit()
        return snapshots

    finally:
        conn.close()
