from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from ibmd.foundation.atomic_json import canonical_json_text, read_json_object

from .models import (
    CatalogError,
    FuturesContractCalendarV1,
    InstrumentMasterV1,
    SessionCalendarV1,
    StrategyPolicyCatalogV1,
)
from .resolver import require_production_qualified_session


@dataclass(frozen=True)
class CatalogBundleV1:
    instrument_master: InstrumentMasterV1
    contract_calendar: FuturesContractCalendarV1
    session_calendar: SessionCalendarV1
    strategy_policy: StrategyPolicyCatalogV1

    @property
    def bundle_hash(self) -> str:
        payload = {
            "instrument_master": self.instrument_master.content_hash,
            "contract_calendar": self.contract_calendar.content_hash,
            "session_calendar": self.session_calendar.content_hash,
            "strategy_policy": self.strategy_policy.content_hash,
        }
        return hashlib.sha256(
            canonical_json_text(payload).encode("utf-8")
        ).hexdigest()

    def validate_cross_references(
        self,
        *,
        require_production_sessions: bool = False,
    ) -> None:
        instrument = self.instrument_master.require(
            self.contract_calendar.instrument_id
        )
        if instrument.sec_type != "FUT":
            raise CatalogError(
                "futures contract calendar references a non-futures instrument: "
                f"{instrument.instrument_id}"
            )
        if (
            self.contract_calendar.source_runtime_commit
            != self.instrument_master.source_runtime_commit
        ):
            raise CatalogError(
                "instrument master and contract calendar use different source commits"
            )
        if (
            self.strategy_policy.source_runtime_commit
            != self.instrument_master.source_runtime_commit
        ):
            raise CatalogError(
                "instrument master and strategy policy use different source commits"
            )
        if (
            self.session_calendar.source_runtime_commit
            != self.instrument_master.source_runtime_commit
        ):
            raise CatalogError(
                "instrument master and session calendar use different source commits"
            )

        for policy in self.strategy_policy.instruments:
            registered = self.instrument_master.require(policy.instrument_id)
            if (
                policy.signal.source_bar_size_seconds
                != registered.default_bar_size_seconds
            ):
                raise CatalogError(
                    "strategy bar size does not match instrument master: "
                    f"instrument={policy.instrument_id}"
                )
            session = self.session_calendar.require(
                policy.daily_flat.session_id
            )
            if require_production_sessions and policy.daily_flat.enabled:
                require_production_qualified_session(session)

        policy_ids = {
            item.instrument_id for item in self.strategy_policy.instruments
        }
        if self.contract_calendar.instrument_id not in policy_ids:
            raise CatalogError(
                "contract calendar instrument has no strategy policy: "
                f"{self.contract_calendar.instrument_id}"
            )


def load_instrument_master(path: str | Path) -> InstrumentMasterV1:
    return InstrumentMasterV1.from_dict(read_json_object(path))


def load_futures_contract_calendar(
    path: str | Path,
) -> FuturesContractCalendarV1:
    return FuturesContractCalendarV1.from_dict(read_json_object(path))


def load_session_calendar(path: str | Path) -> SessionCalendarV1:
    return SessionCalendarV1.from_dict(read_json_object(path))


def load_strategy_policy(path: str | Path) -> StrategyPolicyCatalogV1:
    return StrategyPolicyCatalogV1.from_dict(read_json_object(path))


def load_catalog_bundle(
    root: str | Path,
    *,
    require_production_sessions: bool = False,
) -> CatalogBundleV1:
    base = Path(root)
    bundle = CatalogBundleV1(
        instrument_master=load_instrument_master(
            base / "instruments.v1.json"
        ),
        contract_calendar=load_futures_contract_calendar(
            base / "contracts.mnq.v1.json"
        ),
        session_calendar=load_session_calendar(base / "sessions.v1.json"),
        strategy_policy=load_strategy_policy(
            base / "strategy.IBMarketData.rolling.v1.json"
        ),
    )
    bundle.validate_cross_references(
        require_production_sessions=require_production_sessions
    )
    return bundle
