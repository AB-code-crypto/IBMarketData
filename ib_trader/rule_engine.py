from dataclasses import dataclass
import json
from typing import Any

from ib_trader.rules.base import TraderRuleContext, TraderRuleState
from ib_trader.rules.registry import RULE_REGISTRY
from ib_trader.trader_rules_config import ACTIVE_RULES, REQUIRE_MARKET_FEATURES


@dataclass(frozen=True)
class TraderRuleEvaluation:
    """Что делает: хранит финальный результат rule engine.
    Зачем нужна: trade_store пишет эти данные в trade_decisions/trade_intents."""
    allowed: bool
    reject_reasons: list[str]

    signal_strength: str

    order_type: str
    order_policy_reason: str
    limit_offset_points: float | None
    ttl_seconds: int | None

    rules_json: str


def evaluate_trader_rules(context: TraderRuleContext) -> TraderRuleEvaluation:
    """Что делает: последовательно применяет активные правила ib_trader.
    Зачем нужна: правила можно добавлять/отключать через trader_rules_config.py без переписывания trade_store."""
    state = TraderRuleState()

    if (
            REQUIRE_MARKET_FEATURES
            and (context.market_features.regime is None or context.market_features.ma_zone is None)
    ):
        state.reject(
            rule_id="market_features_available",
            reason="market_features_unknown",
            details={
                "regime": context.market_features.regime,
                "ma_zone": context.market_features.ma_zone,
                "feature_bar_ts": context.market_features.feature_bar_ts,
            },
        )

    active_rules = [
        rule_config
        for rule_config in ACTIVE_RULES
        if bool(rule_config.get("enabled", True))
    ]
    active_rules.sort(key=lambda item: int(item.get("priority", 100)))

    for rule_config in active_rules:
        rule_id = str(rule_config["id"])
        rule_cls = RULE_REGISTRY.get(rule_id)

        if rule_cls is None:
            state.reject(
                rule_id="rule_registry",
                reason="unknown_rule_id",
                details={"rule_id": rule_id},
            )
            continue

        params: dict[str, Any] = dict(rule_config.get("params", {}))
        rule_cls.apply(context=context, state=state, params=params)

    rules_json = json.dumps(
        state.records,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return TraderRuleEvaluation(
        allowed=state.allowed,
        reject_reasons=list(state.reject_reasons),
        signal_strength=state.signal_strength,
        order_type=state.order_type,
        order_policy_reason=state.order_policy_reason,
        limit_offset_points=state.limit_offset_points,
        ttl_seconds=state.ttl_seconds,
        rules_json=rules_json,
    )
