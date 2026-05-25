from dataclasses import dataclass
import sqlite3

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.job_features_config import MA_ZONE_COLUMN_NAME, REGIME_COLUMN_NAME
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.sma_features import SMA_TABLE_NAME
from ib_trader.rule_engine import evaluate_trader_rules
from ib_trader.trade_models import MarketFeatureSnapshot, TraderSignalEvent


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


def read_market_features_for_signal_event(
        *,
        instrument_code: str,
        signal_bar_ts: int,
) -> MarketFeatureSnapshot:
    if instrument_code not in Instrument:
        return MarketFeatureSnapshot(str(instrument_code), int(signal_bar_ts), None, None, None)

    instrument_row = Instrument[instrument_code]
    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    if not feature_db_path.is_file():
        return MarketFeatureSnapshot(str(instrument_code), int(signal_bar_ts), None, None, None)

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
        return MarketFeatureSnapshot(str(instrument_code), int(signal_bar_ts), None, None, None)

    finally:
        conn.close()

    if row is None:
        return MarketFeatureSnapshot(str(instrument_code), int(signal_bar_ts), None, None, None)

    return MarketFeatureSnapshot(
        instrument_code=str(instrument_code),
        signal_bar_ts=int(signal_bar_ts),
        feature_bar_ts=int(row[0]),
        regime=None if row[1] is None else int(row[1]),
        ma_zone=None if row[2] is None else int(row[2]),
    )


def calculate_signal_limit_price(
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


def build_trader_signal_event_for_interpretation(
        *,
        instrument_code: str,
        signal_bar_ts: int,
        signal_time_ct: str | None,
        direction: str,
        entry_price: float,
        best_pearson: float,
        candidate_score_best: float | None,
        potential_end_delta_points: float,
        potential_max_profit_points: float,
        potential_max_drawdown_points: float,
        potential_used: int,
) -> TraderSignalEvent:
    return TraderSignalEvent(
        source_signal_id=0,
        instrument_code=str(instrument_code),
        signal_bar_ts=int(signal_bar_ts),
        signal_time_utc="",
        signal_time_ct=signal_time_ct,
        signal_time_msk="",
        direction=str(direction).upper(),
        entry_price=float(entry_price),
        best_pearson=float(best_pearson),
        candidate_score_best=candidate_score_best,
        potential_end_delta_points=float(potential_end_delta_points),
        potential_max_profit_points=float(potential_max_profit_points),
        potential_max_drawdown_points=float(potential_max_drawdown_points),
        potential_used=int(potential_used),
    )


def interpret_signal_event(
        *,
        instrument_code: str,
        signal_bar_ts: int,
        signal_time_ct: str | None,
        direction: str,
        entry_price: float,
        best_pearson: float,
        candidate_score_best: float | None,
        potential_end_delta_points: float,
        potential_max_profit_points: float,
        potential_max_drawdown_points: float,
        potential_used: int,
) -> SignalInterpretation:
    market_features = read_market_features_for_signal_event(
        instrument_code=instrument_code,
        signal_bar_ts=signal_bar_ts,
    )
    trader_signal = build_trader_signal_event_for_interpretation(
        instrument_code=instrument_code,
        signal_bar_ts=signal_bar_ts,
        signal_time_ct=signal_time_ct,
        direction=direction,
        entry_price=entry_price,
        best_pearson=best_pearson,
        candidate_score_best=candidate_score_best,
        potential_end_delta_points=potential_end_delta_points,
        potential_max_profit_points=potential_max_profit_points,
        potential_max_drawdown_points=potential_max_drawdown_points,
        potential_used=potential_used,
    )

    rule_result = evaluate_trader_rules(
        signal=trader_signal,
        market_features=market_features,
    )

    limit_price = calculate_signal_limit_price(
        direction=direction,
        entry_price=entry_price,
        order_type=rule_result.order_type,
        limit_offset_points=rule_result.limit_offset_points,
    )

    reject_reason = ";".join(rule_result.reject_reasons) if rule_result.reject_reasons else None

    return SignalInterpretation(
        feature_bar_ts=market_features.feature_bar_ts,
        regime=market_features.regime,
        ma_zone=market_features.ma_zone,
        signal_allowed=bool(rule_result.allowed),
        signal_reject_reason=reject_reason,
        signal_strength=rule_result.signal_strength,
        order_type=str(rule_result.order_type).upper(),
        order_policy_reason=str(rule_result.order_policy_reason),
        limit_offset_points=rule_result.limit_offset_points,
        limit_price=limit_price,
        ttl_seconds=rule_result.ttl_seconds,
        signal_rules_json=rule_result.rules_json,
    )
