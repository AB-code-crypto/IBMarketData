from ib_trader.rules.order_type_by_session import OrderTypeBySessionRule
from ib_trader.rules.regime_signal_strength import RegimeSignalStrengthRule
from ib_trader.rules.zone_direction_policy import ZoneDirectionPolicyRule

RULE_REGISTRY = {
    OrderTypeBySessionRule.rule_id: OrderTypeBySessionRule,
    ZoneDirectionPolicyRule.rule_id: ZoneDirectionPolicyRule,
    RegimeSignalStrengthRule.rule_id: RegimeSignalStrengthRule,
}
