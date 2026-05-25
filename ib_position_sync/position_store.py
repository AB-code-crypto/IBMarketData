import time
from typing import Any

from contracts import Instrument, PLACEHOLDER_CON_ID
from ib_execution.contract_resolver import get_active_contract_row
from ib_trader.trade_store import (
    POSITIONS_LATEST_TABLE_NAME,
    get_trade_db_connection,
    initialize_trade_db,
)
from ib_position_sync.position_models import BrokerPositionSnapshot


def get_trading_enabled_instrument_codes() -> list[str]:
    """Что делает: возвращает инструменты, для которых включена торговля.
    Зачем нужна: position_sync должен обновлять positions_latest только для реально торгуемых инструментов."""
    return sorted(
        str(instrument_code)
        for instrument_code, instrument_row in Instrument.items()
        if bool(instrument_row.get("trading_enabled", False))
    )


async def request_broker_positions(ib) -> list[Any]:
    """Что делает: запрашивает актуальные позиции у IB.
    Зачем нужна: ib.positions() может быть stale, поэтому сначала делаем reqPositions."""
    if hasattr(ib, "reqPositionsAsync"):
        result = await ib.reqPositionsAsync()
        if result is not None:
            return list(result)

    if hasattr(ib, "reqPositions"):
        result = ib.reqPositions()
        if result is not None:
            return list(result)

    if hasattr(ib, "positions"):
        return list(ib.positions())

    return []


def get_contract_attr(contract, name: str):
    return getattr(contract, name, None)


def is_same_contract_for_instrument(
        *,
        position_contract,
        instrument_code: str,
        now_ts: int | None = None,
) -> bool:
    """Что делает: проверяет, относится ли broker-position к нашему instrument_code.
    Зачем нужна: IB отдаёт позиции по конкретным контрактам, а робот работает логическими инструментами."""
    if instrument_code not in Instrument:
        return False

    instrument_row = Instrument[instrument_code]
    sec_type = str(instrument_row.get("secType", "")).upper()

    if sec_type == "FUT":
        try:
            active_contract_row = get_active_contract_row(
                instrument_code=instrument_code,
                now_ts=now_ts,
            )
        except Exception:
            return False

        target_local_symbol = str(active_contract_row.get("localSymbol", ""))
        target_con_id = int(active_contract_row.get("conId", 0) or 0)

        broker_local_symbol = str(get_contract_attr(position_contract, "localSymbol") or "")
        broker_con_id = int(get_contract_attr(position_contract, "conId") or 0)

        if target_local_symbol and broker_local_symbol == target_local_symbol:
            return True

        if target_con_id != PLACEHOLDER_CON_ID and broker_con_id == target_con_id:
            return True

        return False

    broker_sec_type = str(get_contract_attr(position_contract, "secType") or "").upper()
    broker_symbol = str(get_contract_attr(position_contract, "symbol") or "")
    broker_currency = str(get_contract_attr(position_contract, "currency") or "")

    return (
        broker_sec_type == sec_type
        and broker_symbol == str(instrument_row.get("symbol", instrument_code))
        and broker_currency == str(instrument_row.get("currency", ""))
    )


def normalize_side_and_qty(raw_position: float) -> tuple[str, float]:
    """Что делает: переводит signed broker quantity в side/quantity.
    Зачем нужна: positions_latest хранит LONG/SHORT/FLAT в явном виде."""
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
) -> BrokerPositionSnapshot:
    """Что делает: ищет broker-position для одного логического инструмента.
    Зачем нужна: отсутствие позиции у брокера должно стать FLAT в positions_latest."""
    for broker_position in broker_positions:
        contract = getattr(broker_position, "contract", None)

        if contract is None:
            continue

        if not is_same_contract_for_instrument(
                position_contract=contract,
                instrument_code=instrument_code,
                now_ts=now_ts,
        ):
            continue

        raw_quantity = float(getattr(broker_position, "position", 0.0) or 0.0)
        side, quantity = normalize_side_and_qty(raw_quantity)

        return BrokerPositionSnapshot(
            instrument_code=instrument_code,
            side=side,
            quantity=quantity,
            broker_contract=(
                    str(get_contract_attr(contract, "localSymbol") or "")
                    or str(get_contract_attr(contract, "symbol") or "")
                    or None
            ),
            broker_account=(
                    str(getattr(broker_position, "account", "") or "")
                    or None
            ),
        )

    return BrokerPositionSnapshot(
        instrument_code=instrument_code,
        side="FLAT",
        quantity=0.0,
        broker_contract=None,
        broker_account=None,
    )


def write_position_snapshot(
        conn,
        *,
        snapshot: BrokerPositionSnapshot,
        updated_at_ts: int,
) -> None:
    """Что делает: пишет broker-position в positions_latest.
    Зачем нужна: ib_trader должен видеть реальную позицию брокера, а не ручной FLAT."""
    initialize_trade_db(conn)

    conn.execute(
        f"""
        INSERT INTO {POSITIONS_LATEST_TABLE_NAME} (
            instrument_code,
            side,
            quantity,
            updated_at_ts
        )
        VALUES (?, ?, ?, ?)

        ON CONFLICT(instrument_code) DO UPDATE SET
            side = excluded.side,
            quantity = excluded.quantity,
            updated_at_ts = excluded.updated_at_ts
        """,
        (
            snapshot.instrument_code,
            snapshot.side,
            float(snapshot.quantity),
            int(updated_at_ts),
        ),
    )


async def sync_broker_positions_once(ib) -> list[BrokerPositionSnapshot]:
    """Что делает: один раз читает позиции IB и обновляет positions_latest.
    Зачем нужна: runner вызывает эту функцию циклом."""
    now_ts = int(time.time())
    broker_positions = await request_broker_positions(ib)
    instrument_codes = get_trading_enabled_instrument_codes()

    snapshots = [
        find_position_for_instrument(
            broker_positions=broker_positions,
            instrument_code=instrument_code,
            now_ts=now_ts,
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
