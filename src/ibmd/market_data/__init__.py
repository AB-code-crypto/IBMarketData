"""Target shadow market-data service.

The package publishes only complete canonical BID/ASK bars into its own store.
It is not connected to the legacy signal or trading runtime during migration.
"""

from .application.backfill import (
    HistoryBackfillResult,
    backfill_contract_history,
    iter_history_chunks,
)
from .application.config import MarketDataConfigError, MarketDataShadowConfig
from .application.service import MarketDataServiceError, MarketDataShadowService

__all__ = [
    "HistoryBackfillResult",
    "MarketDataConfigError",
    "MarketDataServiceError",
    "MarketDataShadowConfig",
    "MarketDataShadowService",
    "backfill_contract_history",
    "iter_history_chunks",
]
