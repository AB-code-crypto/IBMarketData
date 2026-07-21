from __future__ import annotations

from datetime import datetime, timezone
import math
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

os.environ.setdefault("IB_ACCOUNT_ID", "U0000000")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("TELEGRAM_CHAT_ID", "-1000000000000")

import numpy as np

from config import settings_live
from contracts import Instrument
from core.instrument_db import get_instrument_table_name
from core.price_source import FreshPriceBarStatus, read_latest_complete_price_bar
from core.time_utils import build_bar_time_fields_from_utc_dt
from ib_signal.pearson import calculate_centered_pearson_batch
from ib_signal.signal_candidate_potential import build_candidate_potential_result
from ib_signal.signal_candidate_rank_features import (
    filter_candidates_by_minmax_ratio,
    rank_candidates_by_score,
)
from ib_signal.signal_candidates import (
    build_candidate_signal_rows_query,
    find_candidate_windows,
)
from ib_signal.signal_config import SignalConfig
from ib_signal.signal_pattern_matrix import build_pattern_matrix
from ib_signal.signal_runner import _calculate_signal_once
from ib_signal.signal_window import build_rolling_signal_window


class RollingPricePipelineIntegrationTest(unittest.TestCase):
    def test_pipeline_reads_midpoint_directly_from_price_db(self) -> None:
        original_price_db_dir = settings_live.price_db_dir
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings_live.price_db_dir = tmp_dir
            try:
                instrument_code = "MNQ"
                instrument_row = Instrument[instrument_code]
                db_path = Path(tmp_dir) / instrument_row["db_filename"]
                table_name = get_instrument_table_name(
                    instrument_code,
                    instrument_row,
                )
                current_signal_ts = int(
                    datetime(2026, 7, 20, 15, 0, tzinfo=timezone.utc).timestamp()
                )
                pattern_points = 90 * 60 // 5
                trade_points = 30 * 60 // 5

                conn = sqlite3.connect(str(db_path))
                try:
                    conn.execute(
                        f'''CREATE TABLE "{table_name}" (
                            bar_time_ts INTEGER PRIMARY KEY,
                            bar_time_ct TEXT NOT NULL,
                            bid_close REAL,
                            ask_close REAL
                        )'''
                    )
                    rows: list[tuple[int, str, float, float]] = []

                    # Complete historical pattern+future windows on previous days.
                    for day_offset in range(-12, 0):
                        signal_ts = current_signal_ts + day_offset * 24 * 60 * 60
                        base_price = 18_000.0 + day_offset * 3.0
                        last_pattern_value = 0.0
                        for point in range(-pattern_points, trade_points):
                            bar_ts = signal_ts + point * 5
                            if point < 0:
                                x = point + pattern_points
                                midpoint = (
                                    base_price
                                    + 0.018 * x
                                    + 3.0 * math.sin(x / 45.0)
                                )
                                last_pattern_value = midpoint
                            else:
                                midpoint = (
                                    last_pattern_value
                                    + 60.0 * (point + 1) / trade_points
                                )
                            time_text = str(
                                build_bar_time_fields_from_utc_dt(
                                    datetime.fromtimestamp(bar_ts, tz=timezone.utc)
                                )["bar_time_ct"]
                            )
                            rows.append(
                                (bar_ts, time_text, midpoint - 0.125, midpoint + 0.125)
                            )

                    # Current rolling pattern has no future data yet.
                    for point in range(-pattern_points, 0):
                        bar_ts = current_signal_ts + point * 5
                        x = point + pattern_points
                        midpoint = (
                            19_000.0
                            + 0.018 * x
                            + 3.0 * math.sin(x / 45.0)
                        )
                        time_text = str(
                            build_bar_time_fields_from_utc_dt(
                                datetime.fromtimestamp(bar_ts, tz=timezone.utc)
                            )["bar_time_ct"]
                        )
                        rows.append(
                            (bar_ts, time_text, midpoint - 0.125, midpoint + 0.125)
                        )

                    conn.executemany(
                        f'''INSERT INTO "{table_name}" (
                            bar_time_ts, bar_time_ct, bid_close, ask_close
                        ) VALUES (?, ?, ?, ?)''',
                        rows,
                    )
                    query, params = build_candidate_signal_rows_query(
                        table_name=table_name,
                        min_signal_bar_ts=current_signal_ts - 30 * 24 * 60 * 60,
                        max_signal_bar_ts=current_signal_ts - 30 * 60,
                        current_signal_bar_ts=current_signal_ts,
                        allowed_hours_ct=list(range(24)),
                        bar_size_seconds=5,
                    )
                    query_plan = conn.execute(
                        "EXPLAIN QUERY PLAN " + query,
                        params,
                    ).fetchall()
                    self.assertTrue(
                        any("SEARCH" in str(row[3]).upper() for row in query_plan),
                        query_plan,
                    )
                    conn.commit()
                finally:
                    conn.close()

                latest_start, latest_close, _, latest_mid = (
                    read_latest_complete_price_bar(instrument_code)
                )
                self.assertEqual(latest_start, current_signal_ts - 5)
                self.assertEqual(latest_close, current_signal_ts)
                self.assertGreater(latest_mid, 19_000.0)

                config = SignalConfig(
                    rolling_back_minutes=90,
                    rolling_trade_minutes=30,
                    history_lookback_days=30,
                    pearson_min=0.7,
                    candidate_potential_min_count=7,
                    candidate_potential_max_count=9,
                    candidate_potential_min_abs_end_delta_points=30.0,
                )
                window = build_rolling_signal_window(
                    signal_bar_ts=current_signal_ts,
                    settings=config,
                )
                search = find_candidate_windows(
                    instrument_code=instrument_code,
                    current_window=window,
                    settings=config,
                )
                matrix = build_pattern_matrix(
                    instrument_code=instrument_code,
                    window=window,
                    candidates=search.candidates,
                )
                pearson = calculate_centered_pearson_batch(
                    matrix.current_values,
                    matrix.candidate_matrix,
                )
                passed = np.flatnonzero(pearson >= config.pearson_min)
                candidates = [matrix.valid_candidates[int(index)] for index in passed]
                candidate_matrix = matrix.candidate_matrix[passed, :]
                candidate_pearson = pearson[passed]

                minmax = filter_candidates_by_minmax_ratio(
                    current_values=matrix.current_values,
                    candidates=candidates,
                    candidate_matrix=candidate_matrix,
                    pearson_scores=candidate_pearson,
                    max_ratio=config.candidate_minmax_hard_filter_max_ratio,
                )
                ranked = rank_candidates_by_score(
                    current_values=matrix.current_values,
                    candidates=minmax.valid_candidates,
                    candidate_matrix=minmax.candidate_matrix,
                    pearson_scores=minmax.pearson_scores,
                    pearson_weight=config.candidate_score_pearson_weight,
                    end_delta_weight=config.candidate_score_end_delta_weight,
                    minmax_weight=config.candidate_score_minmax_weight,
                )
                potential = build_candidate_potential_result(
                    instrument_code=instrument_code,
                    signal_window=window,
                    current_values=matrix.current_values,
                    candidates=ranked.valid_candidates,
                    candidate_scores=ranked.candidate_scores,
                    min_count=config.candidate_potential_min_count,
                    max_count=config.candidate_potential_max_count,
                )

                self.assertGreaterEqual(len(search.candidates), 12)
                self.assertGreaterEqual(len(ranked.valid_candidates), 7)
                self.assertTrue(potential.is_available)
                self.assertEqual(potential.direction, "LONG")
                self.assertGreater(potential.end_delta_points, 50.0)

                state_db_path = Path(tmp_dir) / "state.sqlite3"
                status = FreshPriceBarStatus(
                    instrument_code=instrument_code,
                    is_ready=True,
                    reason="ready",
                    db_path=db_path,
                    table_name=table_name,
                    last_bar_time_ts=latest_start,
                    last_bar_close_ts=latest_close,
                    last_bar_time_ct=str(
                        build_bar_time_fields_from_utc_dt(
                            datetime.fromtimestamp(latest_start, tz=timezone.utc)
                        )["bar_time_ct"]
                    ),
                    last_bar_lag_seconds=0,
                    mid_close=latest_mid,
                )
                with (
                    patch("core.state_db.STATE_DB_PATH", state_db_path),
                    patch("ib_signal.signal_event_store.STATE_DB_PATH", state_db_path),
                    patch(
                        "ib_signal.signal_runner.save_signal_candidate_plot",
                        return_value=Path(tmp_dir) / "signal.png",
                    ),
                ):
                    _calculate_signal_once(
                        instrument_code=instrument_code,
                        due_signal_bar_ts=current_signal_ts,
                        status=status,
                        settings=config,
                    )
                state_conn = sqlite3.connect(str(state_db_path))
                try:
                    signal_row = state_conn.execute(
                        "SELECT instrument_code, direction, signal_bar_ts "
                        "FROM signal_events"
                    ).fetchone()
                finally:
                    state_conn.close()
                self.assertEqual(
                    signal_row,
                    (instrument_code, "LONG", current_signal_ts),
                )
            finally:
                settings_live.price_db_dir = original_price_db_dir


if __name__ == "__main__":
    unittest.main(verbosity=2)
