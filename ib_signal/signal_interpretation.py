from dataclasses import dataclass
from datetime import time
import json
import sqlite3
from typing import Any

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.job_features_config import MA_ZONE_COLUMN_NAME, REGIME_COLUMN_NAME
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.sma_features import SMA_TABLE_NAME
from ib_signal.signal_rules_config import SIGNAL_RULES, SIGNAL_RULE_SETTINGS


@dataclass(frozen=True)
class SignalInterpretation:
    feature_bar_ts: int | None
    regime: int | None
    ma_zone: int | None

    signal_allowed: bool
    signal_reject_reason: str | None

    signal_strength: str

    order_type: str
    order_policy_reason: str
    limit_offset_points: float | None
    limit_price: float | None
    ttl_seconds: int | None

    signal_rules_json: str


@dataclass(frozen=True)
class MarketSnapshot:
    feature_bar_ts: int | None
    regime: int | None
    ma_zone: int | None


def read_market_snapshot(
        *,
        instrument_code: str,
        signal_bar_ts: int,
) -> MarketSnapshot:
    """Читает regime/ma_zone из job DB на момент сигнала."""
    if instrument_code not in Instrument:
        return MarketSnapshot(None, None, None)

    instrument_row = Instrument[instrument_code]
    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    if not feature_db_path.is_file():
        return MarketSnapshot(None, None, None)

    conn = open_sqlite_connection(
        str(feature_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        row = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                {quote_identifier(REGIME_COLUMN_NAME)},
                {quote_identifier(MA_ZONE_COLUMN_NAME)}
            FROM {quote_identifier(SMA_TABLE_NAME)}
            WHERE bar_time_ts <= ?
            ORDER BY bar_time_ts DESC
            LIMIT 1
            """,
            (int(signal_bar_ts),),
        ).fetchone()

    except sqlite3.Error:
        return MarketSnapshot(None, None, None)

    finally:
        conn.close()

    if row is None:
        return MarketSnapshot(None, None, None)

    return MarketSnapshot(
        feature_bar_ts=int(row[0]),
        regime=None if row[1] is None else int(row[1]),
        ma_zone=None if row[2] is None else int(row[2]),
    )


def parse_hhmm(value: str) -> time:
    hour_text, minute_text = str(value).split(":", 1)
    return time(hour=int(hour_text), minute=int(minute_text))


def extract_hhmm_ct(signal_time_ct: str | None) -> time | None:
    if signal_time_ct is None:
        return None

    text = str(signal_time_ct)
    hhmm = text[11:16] if len(text) >= 16 and text[10] == " " else text[:5]

    try:
        return parse_hhmm(hhmm)
    except Exception:
        return None


def is_time_inside_window(value: time, start: str, end: str) -> bool:
    start_time = parse_hhmm(start)
    end_time = parse_hhmm(end)

    if start_time <= end_time:
        return start_time <= value < end_time

    return value >= start_time or value < end_time


def is_signal_time_inside_any_window(signal_time_ct: str | None, windows: list[tuple[str, str]]) -> bool:
    signal_time = extract_hhmm_ct(signal_time_ct)

    if signal_time is None:
        return False

    return any(
        is_time_inside_window(signal_time, str(start), str(end))
        for start, end in windows
    )


def dump_rules(records: list[dict[str, Any]]) -> str:
    return json.dumps(
        records,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def allowed_zones_for_direction(direction: str) -> list[int]:
    direction = str(direction).upper()

    if direction == "LONG":
        return [int(value) for value in SIGNAL_RULES["long_allowed_ma_zones"]]

    if direction == "SHORT":
        return [int(value) for value in SIGNAL_RULES["short_allowed_ma_zones"]]

    return []


def classify_signal_strength(*, direction: str, regime: int | None) -> str:
    if regime is None:
        return "UNKNOWN"

    direction = str(direction).upper()
    regime = int(regime)

    if direction == "LONG" and regime in {int(value) for value in SIGNAL_RULES["strong_long_regimes"]}:
        return "STRONG"

    if direction == "SHORT" and regime in {int(value) for value in SIGNAL_RULES["strong_short_regimes"]}:
        return "STRONG"

    if regime in {int(value) for value in SIGNAL_RULES["neutral_regimes"]}:
        return "NEUTRAL"

    return "WEAK"


def allowed_directions_for_regime(regime: int | None) -> list[str]:
    if regime is None:
        return []

    regime = int(regime)

    if regime > 0:
        return ["LONG"]

    if regime < 0:
        return ["SHORT"]

    return ["LONG", "SHORT"]


def is_direction_allowed_by_regime(*, direction: str, regime: int | None) -> bool:
    return str(direction).upper() in allowed_directions_for_regime(regime)


def choose_order_policy(
        *,
        signal_time_ct: str | None,
        ma_zone: int | None,
) -> tuple[str, str, float | None, int | None]:
    settings = SIGNAL_RULE_SETTINGS
    order_type = str(settings["default_order_type"]).upper()
    reasons: list[str] = []

    limit_zones = {int(value) for value in SIGNAL_RULES["limit_order_ma_zones"]}

    if ma_zone is not None and int(ma_zone) in limit_zones:
        order_type = str(settings["limit_order_type"]).upper()
        reasons.append("ma_zone_limit")

    if is_signal_time_inside_any_window(
            signal_time_ct,
            list(SIGNAL_RULES["limit_order_time_windows_ct"]),
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


def calculate_limit_price(
        *,
        direction: str,
        entry_price: float,
        order_type: str,
        limit_offset_points: float | None,
) -> float | None:
    if str(order_type).upper() != "LIMIT":
        return None

    offset = float(limit_offset_points or 0.0)

    if offset <= 0.0:
        return float(entry_price)

    direction = str(direction).upper()

    if direction == "LONG":
        return float(entry_price) - offset

    if direction == "SHORT":
        return float(entry_price) + offset

    return float(entry_price)


def build_interpretation_result(
        *,
        market: MarketSnapshot,
        allowed: bool,
        reject_reason: str | None,
        signal_strength: str,
        order_type: str,
        order_policy_reason: str,
        limit_offset_points: float | None,
        limit_price: float | None,
        ttl_seconds: int | None,
        records: list[dict[str, Any]],
) -> SignalInterpretation:
    return SignalInterpretation(
        feature_bar_ts=market.feature_bar_ts,
        regime=market.regime,
        ma_zone=market.ma_zone,
        signal_allowed=allowed,
        signal_reject_reason=reject_reason,
        signal_strength=signal_strength,
        order_type=order_type,
        order_policy_reason=order_policy_reason,
        limit_offset_points=limit_offset_points,
        limit_price=limit_price,
        ttl_seconds=ttl_seconds,
        signal_rules_json=dump_rules(records),
    )


def interpret_signal_event(
        *,
        instrument_code: str,
        signal_bar_ts: int,
        signal_time_ct: str | None,
        direction: str,
        entry_price: float,
) -> SignalInterpretation:
    """Интерпретирует raw-сигнал через ma_zone/regime/time и SIGNAL_RULES."""
    market = read_market_snapshot(
        instrument_code=instrument_code,
        signal_bar_ts=signal_bar_ts,
    )
    settings = SIGNAL_RULE_SETTINGS
    records: list[dict[str, Any]] = []

    direction = str(direction).upper()
    ma_zone = market.ma_zone
    regime = market.regime

    if bool(settings["require_market_features"]) and (ma_zone is None or regime is None):
        records.append({
            "rule": "market_features",
            "result": "REJECT",
            "reason": "market_features_unknown",
            "details": {
                "feature_bar_ts": market.feature_bar_ts,
                "regime": regime,
                "ma_zone": ma_zone,
            },
        })
        return build_interpretation_result(
            market=market,
            allowed=False,
            reject_reason="market_features_unknown",
            signal_strength="NEUTRAL",
            order_type=str(settings["default_order_type"]).upper(),
            order_policy_reason="rejected",
            limit_offset_points=None,
            limit_price=None,
            ttl_seconds=None,
            records=records,
        )

    if not bool(settings["require_market_features"]):
        order_type = str(settings["default_order_type"]).upper()
        records.append({
            "rule": "market_features",
            "result": "SKIP",
            "details": {"require_market_features": False},
        })
        return build_interpretation_result(
            market=market,
            allowed=True,
            reject_reason=None,
            signal_strength="NEUTRAL",
            order_type=order_type,
            order_policy_reason="potential_only",
            limit_offset_points=None,
            limit_price=None,
            ttl_seconds=None,
            records=records,
        )

    allowed_zones = allowed_zones_for_direction(direction)

    if int(ma_zone) not in allowed_zones:
        records.append({
            "rule": "zone_direction",
            "result": "REJECT",
            "reason": "ma_zone_direction_forbidden",
            "details": {
                "direction": direction,
                "ma_zone": int(ma_zone),
                "allowed_zones": allowed_zones,
            },
        })
        return build_interpretation_result(
            market=market,
            allowed=False,
            reject_reason="ma_zone_direction_forbidden",
            signal_strength="NEUTRAL",
            order_type=str(settings["default_order_type"]).upper(),
            order_policy_reason="rejected",
            limit_offset_points=None,
            limit_price=None,
            ttl_seconds=None,
            records=records,
        )

    records.append({
        "rule": "zone_direction",
        "result": "ALLOW",
        "details": {
            "direction": direction,
            "ma_zone": int(ma_zone),
            "allowed_zones": allowed_zones,
        },
    })

    regime_direction_policy = str(
        settings.get("regime_direction_policy", "ALLOW"),
    ).upper()
    regime_allowed_directions = allowed_directions_for_regime(int(regime))
    regime_direction_allowed = is_direction_allowed_by_regime(
        direction=direction,
        regime=int(regime),
    )

    if regime_direction_policy == "REJECT" and not regime_direction_allowed:
        signal_strength = classify_signal_strength(
            direction=direction,
            regime=int(regime),
        )
        records.append({
            "rule": "regime_direction_policy",
            "result": "REJECT",
            "reason": "regime_direction_forbidden",
            "details": {
                "direction": direction,
                "regime": int(regime),
                "allowed_directions": regime_allowed_directions,
                "policy": regime_direction_policy,
            },
        })
        return build_interpretation_result(
            market=market,
            allowed=False,
            reject_reason="regime_direction_forbidden",
            signal_strength=signal_strength,
            order_type=str(settings["default_order_type"]).upper(),
            order_policy_reason="rejected",
            limit_offset_points=None,
            limit_price=None,
            ttl_seconds=None,
            records=records,
        )

    records.append({
        "rule": "regime_direction_policy",
        "result": "ALLOW" if regime_direction_policy == "REJECT" else "SKIP",
        "details": {
            "direction": direction,
            "regime": int(regime),
            "allowed_directions": regime_allowed_directions,
            "policy": regime_direction_policy,
        },
    })

    order_type, order_policy_reason, limit_offset_points, ttl_seconds = choose_order_policy(
        signal_time_ct=signal_time_ct,
        ma_zone=int(ma_zone),
    )
    limit_price = calculate_limit_price(
        direction=direction,
        entry_price=float(entry_price),
        order_type=order_type,
        limit_offset_points=limit_offset_points,
    )

    records.append({
        "rule": "order_policy",
        "result": order_type,
        "details": {
            "ma_zone": int(ma_zone),
            "signal_time_ct": signal_time_ct,
            "order_policy_reason": order_policy_reason,
            "limit_offset_points": limit_offset_points,
            "limit_price": limit_price,
            "ttl_seconds": ttl_seconds,
        },
    })

    signal_strength = classify_signal_strength(
        direction=direction,
        regime=int(regime),
    )
    records.append({
        "rule": "regime_signal_strength",
        "result": signal_strength,
        "details": {
            "direction": direction,
            "regime": int(regime),
        },
    })

    if signal_strength == "WEAK" and str(settings["weak_signal_policy"]).upper() == "REJECT":
        records.append({
            "rule": "weak_signal_policy",
            "result": "REJECT",
            "reason": "weak_signal_rejected",
            "details": {"signal_strength": signal_strength},
        })
        return build_interpretation_result(
            market=market,
            allowed=False,
            reject_reason="weak_signal_rejected",
            signal_strength=signal_strength,
            order_type=order_type,
            order_policy_reason=order_policy_reason,
            limit_offset_points=limit_offset_points,
            limit_price=limit_price,
            ttl_seconds=ttl_seconds,
            records=records,
        )

    return build_interpretation_result(
        market=market,
        allowed=True,
        reject_reason=None,
        signal_strength=signal_strength,
        order_type=order_type,
        order_policy_reason=order_policy_reason,
        limit_offset_points=limit_offset_points,
        limit_price=limit_price,
        ttl_seconds=ttl_seconds,
        records=records,
    )
