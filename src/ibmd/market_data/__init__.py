"""Target shadow market-data service.

The package publishes only complete canonical BID/ASK bars into its own store.
It is not connected to the legacy signal or trading runtime during migration.
"""

from .application.config import MarketDataConfigError, MarketDataShadowConfig
from .application.service import MarketDataServiceError, MarketDataShadowService

__all__ = [
    "MarketDataConfigError",
    "MarketDataServiceError",
    "MarketDataShadowConfig",
    "MarketDataShadowService",
]
