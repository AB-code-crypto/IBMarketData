"""Reusable Interactive Brokers gateway contracts and test doubles."""

from .broker_reconciliation import (
    BrokerReconciliationReadError,
    IBBrokerReconciliationReader,
)
from .fake_broker_reconciliation import ScriptedBrokerReconciliationReader
from .fake_market_data import ScriptedMarketDataReader
from .fake_paper_orders import ScriptedPaperOrderGateway
from .fake_positions import ScriptedPositionReader
from .ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)
from .ib_async_paper_orders import (
    IBAsyncPaperOrderGateway,
    IBPaperOrderConnectionSettings,
    build_paper_market_order,
    build_paper_order_contract,
)
from .market_data import (
    BrokerMarketDataReadError,
    IBMarketDataReader,
    RealtimeQuoteSubscription,
)
from .paper_orders import (
    BrokerOrderSubmitError,
    PaperMarketOrderRequest,
    PaperOrderGateway,
    PaperOrderRoute,
    PaperOrderSubmissionReceipt,
)
from .positions import (
    BrokerPositionReadError,
    IBPositionReader,
    RawBrokerPosition,
)

__all__ = [
    "BrokerMarketDataReadError",
    "BrokerOrderSubmitError",
    "BrokerPositionReadError",
    "BrokerReconciliationReadError",
    "IBAsyncBrokerReconciliationReader",
    "IBAsyncPaperOrderGateway",
    "IBBrokerReconciliationConnectionSettings",
    "IBBrokerReconciliationReader",
    "IBMarketDataReader",
    "IBPaperOrderConnectionSettings",
    "IBPositionReader",
    "PaperMarketOrderRequest",
    "PaperOrderGateway",
    "PaperOrderRoute",
    "PaperOrderSubmissionReceipt",
    "RawBrokerPosition",
    "RealtimeQuoteSubscription",
    "ScriptedBrokerReconciliationReader",
    "ScriptedMarketDataReader",
    "ScriptedPaperOrderGateway",
    "ScriptedPositionReader",
    "build_paper_market_order",
    "build_paper_order_contract",
]
