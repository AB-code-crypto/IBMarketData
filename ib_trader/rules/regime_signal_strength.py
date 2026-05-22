from typing import Any

from ib_trader.rules.base import TraderRuleContext, TraderRuleState


class RegimeSignalStrengthRule:
    rule_id = "regime_signal_strength"

    @classmethod
    def apply(cls, *, context: TraderRuleContext, state: TraderRuleState, params: dict[str, Any]) -> None:
        regime = context.market_features.regime
        direction = str(context.signal.direction).upper()
        weak_signal_policy = str(params.get("weak_signal_policy", "ALLOW")).upper()

        if regime is None:
            state.signal_strength = "UNKNOWN"
            state.reject(
                rule_id=cls.rule_id,
                reason="regime_unknown",
                details={"direction": direction},
            )
            return

        regime = int(regime)

        if (direction == "LONG" and regime == 1) or (direction == "SHORT" and regime == -1):
            state.signal_strength = "STRONG"
        elif regime == 0:
            state.signal_strength = "NEUTRAL"
        else:
            state.signal_strength = "WEAK"

        if state.signal_strength == "WEAK" and weak_signal_policy == "REJECT":
            state.reject(
                rule_id=cls.rule_id,
                reason="weak_signal_rejected",
                details={
                    "direction": direction,
                    "regime": regime,
                    "signal_strength": state.signal_strength,
                },
            )
            return

        state.record(
            rule_id=cls.rule_id,
            result=state.signal_strength,
            details={
                "direction": direction,
                "regime": regime,
                "weak_signal_policy": weak_signal_policy,
            },
        )
