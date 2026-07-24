"""Target rolling signal shadow service.

The service reads only the target market-data product, writes only its own
signal store and never emits trade commands.
"""

from .application.config import SignalConfigError, SignalShadowConfig
from .application.service import SignalServiceError, SignalShadowService

__all__ = [
    "SignalConfigError",
    "SignalServiceError",
    "SignalShadowConfig",
    "SignalShadowService",
]
