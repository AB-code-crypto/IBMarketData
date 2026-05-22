from typing import Any

from ib_trader.rules.base import TraderRuleContext, TraderRuleState


class ZoneDirectionPolicyRule:
    rule_id = "zone_direction_policy"

    @classmethod
    def apply(cls, *, context: TraderRuleContext, state: TraderRuleState, params: dict[str, Any]) -> None:
        ma_zone = context.market_features.ma_zone
        direction = str(context.signal.direction).upper()

        if ma_zone is None:
            state.reject(
                rule_id=cls.rule_id,
                reason="ma_zone_unknown",
                details={"direction": direction},
            )
            return

        allowed_by_zone_raw = params.get("allowed_directions_by_zone", {})
        allowed_by_zone = {
            int(zone): {str(item).upper() for item in directions}
            for zone, directions in allowed_by_zone_raw.items()
        }

        allowed_directions = allowed_by_zone.get(int(ma_zone), set())

        if direction not in allowed_directions:
            state.reject(
                rule_id=cls.rule_id,
                reason="ma_zone_direction_forbidden",
                details={
                    "ma_zone": int(ma_zone),
                    "direction": direction,
                    "allowed": sorted(allowed_directions),
                },
            )
            return

        state.record(
            rule_id=cls.rule_id,
            result="ALLOW",
            details={
                "ma_zone": int(ma_zone),
                "direction": direction,
                "allowed": sorted(allowed_directions),
            },
        )
