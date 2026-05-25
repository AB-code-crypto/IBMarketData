from dataclasses import dataclass
from datetime import time
import json
from typing import Any

from ib_trader.trader_rules_config import TRADER_RULES, TRADER_RULE_SETTINGS
from ib_trader.trade_models import MarketFeatureSnapshot, TraderSignalEvent


@dataclass(frozen=True)
class TraderRuleEvaluation:
    """Итог простой оценки торговых правил."""
    allowed: bool
    reject_reasons: list[str]

    signal_strength: str

    order_type: str
    order_policy_reason: str
    limit_offset_points: float | None
    ttl_seconds: int | None

    rules_json: str


def parse_hhmm(value: str) -> time:
    hour_text, minute_text = str(value).split(":", 1)
    return time(hour=int(hour_text), minute=int(minute_text))


def extract_signal_hhmm_ct(signal_time_ct: str | None) -> time | None:
    if signal_time_ct is None:
        return None

    text = str(signal_time_ct)

    # Обычно формат "YYYY-MM-DD HH:MM:SS".
    if len(text) >= 16 and text[10] == " ":
        hhmm = text[11:16]
    else:
        hhmm = text[:5]

    try:
        return parse_hhmm(hhmm)
    except Exception:
        return None


def is_time_inside_window(value: time, start: str, end: str) -> bool:
    start_time = parse_hhmm(start)
    end_time = parse_hhmm(end)

    if start_time <= end_time:
        return start_time <= value < end_time

    # Окно через полночь.
    return value >= start_time or value < end_time


def is_signal_time_inside_any_window(signal_time_ct: str | None, windows: list[tuple[str, str]]) -> bool:
    signal_time = extract_signal_hhmm_ct(signal_time_ct)

    if signal_time is None:
        return False

    return any(
        is_time_inside_window(signal_time, str(start), str(end))
        for start, end in windows
    )


def build_rules_json(records: list[dict[str, Any]]) -> str:
    return json.dumps(
        records,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def reject_result(
        *,
        reason: str,
        records: list[dict[str, Any]],
        signal_strength: str = "NEUTRAL",
        order_type: str | None = None,
        order_policy_reason: str | None = None,
) -> TraderRuleEvaluation:
    settings = TRADER_RULE_SETTINGS

    if not any(item.get("reason") == reason for item in records):
        records.append({
            "rule": "simple_rules",
            "result": "REJECT",
            "reason": reason,
            "details": {},
        })

    return TraderRuleEvaluation(
        allowed=False,
        reject_reasons=[reason],
        signal_strength=signal_strength,
        order_type=str(order_type or settings["default_order_type"]).upper(),
        order_policy_reason=str(order_policy_reason or "rejected"),
        limit_offset_points=None,
        ttl_seconds=None,
        rules_json=build_rules_json(records),
    )


def get_allowed_zones_for_direction(direction: str) -> list[int]:
    direction = str(direction).upper()

    if direction == "LONG":
        return [int(value) for value in TRADER_RULES["long_allowed_ma_zones"]]

    if direction == "SHORT":
        return [int(value) for value in TRADER_RULES["short_allowed_ma_zones"]]

    raise ValueError(f"Unknown signal direction: {direction!r}")


def classify_signal_strength(*, direction: str, regime: int | None) -> str:
    if regime is None:
        return "UNKNOWN"

    direction = str(direction).upper()
    regime = int(regime)

    if direction == "LONG" and regime in {int(value) for value in TRADER_RULES["strong_long_regimes"]}:
        return "STRONG"

    if direction == "SHORT" and regime in {int(value) for value in TRADER_RULES["strong_short_regimes"]}:
        return "STRONG"

    if regime in {int(value) for value in TRADER_RULES["neutral_regimes"]}:
        return "NEUTRAL"

    return "WEAK"


def choose_order_policy(*, signal: TraderSignalEvent, ma_zone: int | None) -> tuple[str, str, float | None, int | None]:
    settings = TRADER_RULE_SETTINGS

    order_type = str(settings["default_order_type"]).upper()
    reasons: list[str] = []

    limit_order_ma_zones = {int(value) for value in TRADER_RULES["limit_order_ma_zones"]}

    if ma_zone is not None and int(ma_zone) in limit_order_ma_zones:
        order_type = str(settings["limit_order_type"]).upper()
        reasons.append("ma_zone_limit")

    if is_signal_time_inside_any_window(
            signal.signal_time_ct,
            list(TRADER_RULES["limit_order_time_windows_ct"]),
    ):
        order_type = str(settings["limit_order_type"]).upper()
        reasons.append("time_window_limit")

    if order_type == str(settings["limit_order_type"]).upper():
        return (
            order_type,
            ",".join(reasons) if reasons else "limit_rule",
            float(settings["limit_offset_points"]),
            int(settings["limit_ttl_seconds"]),
        )

    return order_type, "default_market", None, None


def evaluate_trader_rules(
        *,
        signal: TraderSignalEvent,
        market_features: MarketFeatureSnapshot,
) -> TraderRuleEvaluation:
    """Оценивает простые словарные правила из trader_rules_config.py."""
    settings = TRADER_RULE_SETTINGS
    records: list[dict[str, Any]] = []

    direction = str(signal.direction).upper()
    ma_zone = market_features.ma_zone
    regime = market_features.regime

    require_market_features = bool(settings["require_market_features"])

    if not require_market_features:
        order_type = str(settings["default_order_type"]).upper()
        records.append({
            "rule": "require_market_features",
            "result": "SKIP_MARKET_FEATURES",
            "details": {
                "require_market_features": False,
                "order_type": order_type,
            },
        })

        return TraderRuleEvaluation(
            allowed=True,
            reject_reasons=[],
            signal_strength="NEUTRAL",
            order_type=order_type,
            order_policy_reason="potential_only",
            limit_offset_points=None,
            ttl_seconds=None,
            rules_json=build_rules_json(records),
        )

    if ma_zone is None or regime is None:
        return reject_result(
            reason="market_features_unknown",
            records=records,
        )

    ma_zone = int(ma_zone)
    regime = int(regime)

    allowed_zones = get_allowed_zones_for_direction(direction)

    if ma_zone not in allowed_zones:
        records.append({
            "rule": "zone_direction",
            "result": "REJECT",
            "reason": "ma_zone_direction_forbidden",
            "details": {
                "direction": direction,
                "ma_zone": ma_zone,
                "allowed_zones": allowed_zones,
            },
        })
        return reject_result(
            reason="ma_zone_direction_forbidden",
            records=records,
        )

    records.append({
        "rule": "zone_direction",
        "result": "ALLOW",
        "details": {
            "direction": direction,
            "ma_zone": ma_zone,
            "allowed_zones": allowed_zones,
        },
    })

    order_type, order_policy_reason, limit_offset_points, ttl_seconds = choose_order_policy(
        signal=signal,
        ma_zone=ma_zone,
    )

    records.append({
        "rule": "order_policy",
        "result": order_type,
        "details": {
            "ma_zone": ma_zone,
            "signal_time_ct": signal.signal_time_ct,
            "order_policy_reason": order_policy_reason,
            "limit_offset_points": limit_offset_points,
            "ttl_seconds": ttl_seconds,
        },
    })

    signal_strength = classify_signal_strength(
        direction=direction,
        regime=regime,
    )

    records.append({
        "rule": "regime_signal_strength",
        "result": signal_strength,
        "details": {
            "direction": direction,
            "regime": regime,
        },
    })

    if (
            signal_strength == "WEAK"
            and str(settings["weak_signal_policy"]).upper() == "REJECT"
    ):
        return reject_result(
            reason="weak_signal_rejected",
            records=records,
            signal_strength=signal_strength,
            order_type=order_type,
            order_policy_reason=order_policy_reason,
        )

    return TraderRuleEvaluation(
        allowed=True,
        reject_reasons=[],
        signal_strength=signal_strength,
        order_type=order_type,
        order_policy_reason=order_policy_reason,
        limit_offset_points=limit_offset_points,
        ttl_seconds=ttl_seconds,
        rules_json=build_rules_json(records),
    )
