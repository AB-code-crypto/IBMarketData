"""Reusable Interactive Brokers gateway contracts and test doubles."""

from .broker_reconciliation import (
    BrokerReconciliationReadError,
    IBBrokerReconciliationReader,
)
from .fake_broker_reconciliation import ScriptedBrokerReconciliationReader
from .fake_market_data import ScriptedMarketDataReader
from .fake_paper_cancellations import ScriptedPaperOrderCancellationGateway
from .fake_paper_orders import ScriptedPaperOrderGateway
from .fake_positions import ScriptedPositionReader
from .ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)
from .ib_async_paper_cancellations import (
    IBAsyncPaperOrderCancellationGateway,
    IBPaperCancellationConnectionSettings,
)
from .ib_async_paper_orders import (
    IBAsyncPaperOrderGateway,
    IBPaperOrderConnectionSettings,
    build_paper_market_order,
    build_paper_order_contract,
    build_paper_protective_order,
)
from .market_data import (
    BrokerMarketDataReadError,
    IBMarketDataReader,
    RealtimeQuoteSubscription,
)
from .paper_cancellations import (
    BrokerOrderCancelError,
    PaperOrderCancellationGateway,
    PaperOrderCancelReceipt,
    PaperOrderCancelRequest,
)
from .paper_orders import (
    BrokerOrderSubmitError,
    PaperMarketOrderRequest,
    PaperOrderGateway,
    PaperOrderRoute,
    PaperOrderSubmissionReceipt,
    PaperProtectiveOrderRequest,
)
from .positions import (
    BrokerPositionReadError,
    IBPositionReader,
    RawBrokerPosition,
)

__all__ = [
    "BrokerMarketDataReadError",
    "BrokerOrderCancelError",
    "BrokerOrderSubmitError",
    "BrokerPositionReadError",
    "BrokerReconciliationReadError",
    "IBAsyncBrokerReconciliationReader",
    "IBAsyncPaperOrderCancellationGateway",
    "IBAsyncPaperOrderGateway",
    "IBBrokerReconciliationConnectionSettings",
    "IBBrokerReconciliationReader",
    "IBMarketDataReader",
    "IBPaperCancellationConnectionSettings",
    "IBPaperOrderConnectionSettings",
    "IBPositionReader",
    "PaperMarketOrderRequest",
    "PaperOrderCancellationGateway",
    "PaperOrderCancelReceipt",
    "PaperOrderCancelRequest",
    "PaperOrderGateway",
    "PaperOrderRoute",
    "PaperOrderSubmissionReceipt",
    "PaperProtectiveOrderRequest",
    "RawBrokerPosition",
    "RealtimeQuoteSubscription",
    "ScriptedBrokerReconciliationReader",
    "ScriptedMarketDataReader",
    "ScriptedPaperOrderCancellationGateway",
    "ScriptedPaperOrderGateway",
    "ScriptedPositionReader",
    "build_paper_market_order",
    "build_paper_order_contract",
    "build_paper_protective_order",
]
