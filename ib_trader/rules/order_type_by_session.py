from datetime import time
from typing import Any

from ib_trader.rules.base import TraderRuleContext, TraderRuleState


class OrderTypeBySessionRule:
    rule_id = "order_type_by_session"

    @staticmethod
    def _parse_hhmm(value: str) -> time:
        hour_text, minute_text = str(value).split(":", 1)
        return time(hour=int(hour_text), minute=int(minute_text))

    @staticmethod
    def _signal_time_hhmm(signal_time_ct: str | None) -> time | None:
        if signal_time_ct is None:
            return None

        text = str(signal_time_ct)

        # Формат обычно "YYYY-MM-DD HH:MM:SS".
        if len(text) >= 16 and text[10] == " ":
            hhmm = text[11:16]
        else:
            hhmm = text[:5]

        try:
            return OrderTypeBySessionRule._parse_hhmm(hhmm)
        except Exception:
            return None

    @classmethod
    def _is_inside_window(cls, value: time, start: str, end: str) -> bool:
        start_time = cls._parse_hhmm(start)
        end_time = cls._parse_hhmm(end)

        if start_time <= end_time:
            return start_time <= value < end_time

        # На случай окон через полночь.
        return value >= start_time or value < end_time

    @classmethod
    def apply(cls, *, context: TraderRuleContext, state: TraderRuleState, params: dict[str, Any]) -> None:
        default_order_type = str(params.get("default_order_type", "MARKET")).upper()
        limit_order_type = str(params.get("limit_order_type", "LIMIT")).upper()

        state.order_type = default_order_type
        state.order_policy_reason = "default_market"
        state.limit_offset_points = None
        state.ttl_seconds = None

        signal_time = cls._signal_time_hhmm(context.signal.signal_time_ct)
        limit_windows = params.get("limit_time_windows_ct", [])
        limit_if_abs_zone = int(params.get("limit_if_abs_ma_zone_at_least", 0) or 0)

        selected_reason = None

        if signal_time is not None:
            for start, end in limit_windows:
                if cls._is_inside_window(signal_time, str(start), str(end)):
                    selected_reason = "session_limit_window"
                    break

        ma_zone = context.market_features.ma_zone
        if (
                selected_reason is None
                and limit_if_abs_zone > 0
                and ma_zone is not None
                and abs(int(ma_zone)) >= limit_if_abs_zone
        ):
            selected_reason = "ma_zone_outside_normal_range"

        if selected_reason is not None:
            state.order_type = limit_order_type
            state.order_policy_reason = selected_reason
            state.limit_offset_points = float(params.get("limit_offset_points", 0.0) or 0.0)
            state.ttl_seconds = int(params.get("limit_ttl_seconds", 0) or 0) or None

        state.record(
            rule_id=cls.rule_id,
            result="SET_ORDER_TYPE",
            details={
                "order_type": state.order_type,
                "reason": state.order_policy_reason,
                "signal_time_ct": context.signal.signal_time_ct,
                "ma_zone": ma_zone,
                "limit_offset_points": state.limit_offset_points,
                "ttl_seconds": state.ttl_seconds,
            },
        )
