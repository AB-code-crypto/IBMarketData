from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import time
from typing import Any

from core.time_utils import build_bar_time_fields_from_utc_dt
from ib_signal.signal_config import SignalConfig


@dataclass(frozen=True)
class SignalEvent:
    signal_id: int | None
    instrument_code: str
    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    signal_time_msk: str
    created_at_ts: int
    direction: str
    entry_price: float
    best_pearson: float
    candidate_score_best: float | None
    potential_end_delta_points: float
    potential_max_profit_points: float
    potential_max_drawdown_points: float
    potential_used: int
    settings_json: str


def signal_config_to_dict(settings: SignalConfig) -> dict[str, Any]:
    return asdict(settings)


def signal_config_to_json(settings: SignalConfig) -> str:
    return json.dumps(
        signal_config_to_dict(settings),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def get_signal_time_fields(signal_bar_ts: int) -> dict[str, str | int]:
    dt_utc = datetime.fromtimestamp(int(signal_bar_ts), tz=timezone.utc)
    return build_bar_time_fields_from_utc_dt(dt_utc)


def build_signal_event(
        *,
        instrument_code: str,
        signal_bar_ts: int,
        signal_time_ct: str | None,
        direction: str,
        entry_price: float,
        settings: SignalConfig,
        best_pearson: float,
        candidate_score_best: float | None,
        potential_end_delta_points: float,
        potential_max_profit_points: float,
        potential_max_drawdown_points: float,
        potential_used: int,
        created_at_ts: int | None = None,
) -> SignalEvent:
    direction_value = str(direction).upper()
    if direction_value not in {"LONG", "SHORT"}:
        raise ValueError(
            f"SignalEvent direction должен быть LONG/SHORT, получено: {direction!r}"
        )
    time_fields = get_signal_time_fields(signal_bar_ts)
    return SignalEvent(
        signal_id=None,
        instrument_code=str(instrument_code),
        signal_bar_ts=int(signal_bar_ts),
        signal_time_utc=str(time_fields["bar_time"]),
        signal_time_ct=(
            str(time_fields["bar_time_ct"])
            if signal_time_ct is None
            else str(signal_time_ct)
        ),
        signal_time_msk=str(time_fields["bar_time_msk"]),
        created_at_ts=int(time.time() if created_at_ts is None else created_at_ts),
        direction=direction_value,
        entry_price=float(entry_price),
        best_pearson=float(best_pearson),
        candidate_score_best=(
            None if candidate_score_best is None else float(candidate_score_best)
        ),
        potential_end_delta_points=float(potential_end_delta_points),
        potential_max_profit_points=float(potential_max_profit_points),
        potential_max_drawdown_points=float(potential_max_drawdown_points),
        potential_used=int(potential_used),
        settings_json=signal_config_to_json(settings),
    )
