from dataclasses import dataclass


@dataclass(frozen=True)
class BrokerPositionSnapshot:
    """Что делает: хранит нормализованную позицию инструмента у брокера.
    Зачем нужна: ib_position_sync пишет в positions_latest не сырые IB-объекты, а LONG/SHORT/FLAT."""
    instrument_code: str
    side: str
    quantity: float
    broker_contract: str | None
    broker_account: str | None
    broker_con_id: int | None = None
    contract_is_active: bool | None = None
