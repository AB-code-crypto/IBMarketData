"""Compatibility aggregation for target catalog schema models."""

from .common import CatalogError, compute_content_hash
from .contracts import (
    DeclaredActivationGapV1,
    FuturesContractCalendarV1,
    FuturesContractSpecV1,
)
from .instruments import InstrumentMasterV1, InstrumentSpecV1
from .sessions import (
    LocalIntervalV1,
    SessionCalendarV1,
    SessionDefinitionV1,
    SessionExceptionStatus,
    SessionExceptionV1,
    WeeklySessionDayV1,
)
from .strategy import (
    DailyFlatPolicyV1,
    DailyPnlPolicyV1,
    ProtectivePolicyV1,
    SignalPolicyV1,
    StrategyInstrumentPolicyV1,
    StrategyPolicyCatalogV1,
)

__all__ = [
    "CatalogError",
    "DailyFlatPolicyV1",
    "DailyPnlPolicyV1",
    "DeclaredActivationGapV1",
    "FuturesContractCalendarV1",
    "FuturesContractSpecV1",
    "InstrumentMasterV1",
    "InstrumentSpecV1",
    "LocalIntervalV1",
    "ProtectivePolicyV1",
    "SessionCalendarV1",
    "SessionDefinitionV1",
    "SessionExceptionStatus",
    "SessionExceptionV1",
    "SignalPolicyV1",
    "StrategyInstrumentPolicyV1",
    "StrategyPolicyCatalogV1",
    "WeeklySessionDayV1",
    "compute_content_hash",
]
