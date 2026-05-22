from dataclasses import dataclass, field
from typing import Any, Protocol

from ib_trader.trade_models import MarketFeatureSnapshot, PositionSnapshot, TraderSignalEvent


@dataclass(frozen=True)
class TraderRuleContext:
    """Что делает: единый контекст для всех правил.
    Зачем нужна: правила не читают БД сами, а получают уже собранные trader-данные."""
    signal: TraderSignalEvent
    market_features: MarketFeatureSnapshot
    position: PositionSnapshot


@dataclass
class TraderRuleState:
    """Что делает: накапливает результат применения правил.
    Зачем нужна: каждое правило добавляет свой вклад: reject/order_type/signal_strength."""
    allowed: bool = True
    reject_reasons: list[str] = field(default_factory=list)

    signal_strength: str = "NEUTRAL"

    order_type: str = "MARKET"
    order_policy_reason: str = "default_market"
    limit_offset_points: float | None = None
    limit_price: float | None = None
    ttl_seconds: int | None = None

    records: list[dict[str, Any]] = field(default_factory=list)

    def reject(self, *, rule_id: str, reason: str, details: dict[str, Any] | None = None) -> None:
        self.allowed = False
        self.reject_reasons.append(reason)
        self.records.append({
            "rule": rule_id,
            "result": "REJECT",
            "reason": reason,
            "details": details or {},
        })

    def record(self, *, rule_id: str, result: str, details: dict[str, Any] | None = None) -> None:
        self.records.append({
            "rule": rule_id,
            "result": result,
            "details": details or {},
        })


class TraderRule(Protocol):
    rule_id: str

    def apply(self, *, context: TraderRuleContext, state: TraderRuleState, params: dict[str, Any]) -> None:
        ...
