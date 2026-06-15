import json
import time
from typing import Any

from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db

CANDIDATE_FUNNEL_EVENTS_TABLE_NAME = "candidate_funnel_events"


def create_candidate_funnel_events_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {CANDIDATE_FUNNEL_EVENTS_TABLE_NAME} (
        funnel_id INTEGER PRIMARY KEY AUTOINCREMENT,

        instrument_code TEXT NOT NULL,
        signal_bar_ts INTEGER NOT NULL,
        signal_time_ct TEXT,
        signal_window_mode TEXT NOT NULL,
        market_regime_filter_mode TEXT NOT NULL,

        current_hour_ct INTEGER,
        allowed_hours_ct_json TEXT NOT NULL,
        min_candidate_signal_ts INTEGER,
        max_candidate_signal_ts INTEGER,
        signal_phase_seconds INTEGER,
        slot_offset_seconds INTEGER,

        raw_candidate_rows_count INTEGER NOT NULL,
        slot_offset_kept_count INTEGER NOT NULL,
        slot_offset_dropped_count INTEGER NOT NULL,

        matrix_source_candidates_count INTEGER,
        matrix_valid_candidates_count INTEGER,
        matrix_skipped_candidates_count INTEGER,

        pearson_min REAL,
        pearson_passed_count INTEGER,
        best_pearson REAL,

        current_relation TEXT,
        current_price_direction TEXT,
        current_sma600_direction TEXT,

        regime_source_candidates_count INTEGER,
        regime_pearson_passed_count INTEGER,
        regime_soft_kept_count INTEGER,
        regime_hard_kept_count INTEGER,
        regime_final_kept_count INTEGER,
        regime_skipped_sma_count INTEGER,
        regime_relation_mismatch_count INTEGER,
        regime_direction_mismatch_count INTEGER,
        regime_skip_reason TEXT,

        minmax_source_candidates_count INTEGER,
        minmax_kept_candidates_count INTEGER,
        minmax_dropped_candidates_count INTEGER,
        minmax_disabled_reason TEXT,

        score_candidates_count INTEGER,

        potential_available INTEGER,
        potential_used_candidates_count INTEGER,
        potential_source_candidates_count INTEGER,
        potential_direction TEXT,
        potential_unavailable_reason TEXT,
        potential_end_delta_points REAL,

        final_plot_candidates_count INTEGER,
        png_saved INTEGER,
        skip_reason TEXT,

        created_at_ts INTEGER NOT NULL,
        updated_at_ts INTEGER NOT NULL,

        UNIQUE (
            instrument_code,
            signal_bar_ts,
            signal_window_mode,
            market_regime_filter_mode
        )
    );
    """


def initialize_candidate_funnel_events_table(conn) -> None:
    conn.execute(create_candidate_funnel_events_table_sql())
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_candidate_funnel_events_instrument_ts
        ON {CANDIDATE_FUNNEL_EVENTS_TABLE_NAME}(instrument_code, signal_bar_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_candidate_funnel_events_created_at_ts
        ON {CANDIDATE_FUNNEL_EVENTS_TABLE_NAME}(created_at_ts);
        """
    )


def none_if_missing(value: Any) -> Any:
    return None if value is None else value


def int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def record_candidate_funnel_event(**values: Any) -> None:
    """Пишет диагностику candidate-funnel по одному расчёту signal-loop.

    Запись делается даже когда PNG не был сохранён, чтобы можно было разбирать,
    на каком этапе MES/MNQ теряют кандидатов без чтения runtime-логов.
    """
    initialize_state_db()
    now_ts = int(time.time())

    allowed_hours = values.get("allowed_hours_ct") or []
    allowed_hours_json = json.dumps([int(x) for x in allowed_hours], ensure_ascii=False, separators=(",", ":"))

    row = {
        "instrument_code": str(values["instrument_code"]),
        "signal_bar_ts": int(values["signal_bar_ts"]),
        "signal_time_ct": text_or_none(values.get("signal_time_ct")),
        "signal_window_mode": str(values["signal_window_mode"]),
        "market_regime_filter_mode": str(values["market_regime_filter_mode"]),

        "current_hour_ct": int_or_none(values.get("current_hour_ct")),
        "allowed_hours_ct_json": allowed_hours_json,
        "min_candidate_signal_ts": int_or_none(values.get("min_candidate_signal_ts")),
        "max_candidate_signal_ts": int_or_none(values.get("max_candidate_signal_ts")),
        "signal_phase_seconds": int_or_none(values.get("signal_phase_seconds")),
        "slot_offset_seconds": int_or_none(values.get("slot_offset_seconds")),

        "raw_candidate_rows_count": int(values.get("raw_candidate_rows_count") or 0),
        "slot_offset_kept_count": int(values.get("slot_offset_kept_count") or 0),
        "slot_offset_dropped_count": int(values.get("slot_offset_dropped_count") or 0),

        "matrix_source_candidates_count": int_or_none(values.get("matrix_source_candidates_count")),
        "matrix_valid_candidates_count": int_or_none(values.get("matrix_valid_candidates_count")),
        "matrix_skipped_candidates_count": int_or_none(values.get("matrix_skipped_candidates_count")),

        "pearson_min": float_or_none(values.get("pearson_min")),
        "pearson_passed_count": int_or_none(values.get("pearson_passed_count")),
        "best_pearson": float_or_none(values.get("best_pearson")),

        "current_relation": text_or_none(values.get("current_relation")),
        "current_price_direction": text_or_none(values.get("current_price_direction")),
        "current_sma600_direction": text_or_none(values.get("current_sma600_direction")),

        "regime_source_candidates_count": int_or_none(values.get("regime_source_candidates_count")),
        "regime_pearson_passed_count": int_or_none(values.get("regime_pearson_passed_count")),
        "regime_soft_kept_count": int_or_none(values.get("regime_soft_kept_count")),
        "regime_hard_kept_count": int_or_none(values.get("regime_hard_kept_count")),
        "regime_final_kept_count": int_or_none(values.get("regime_final_kept_count")),
        "regime_skipped_sma_count": int_or_none(values.get("regime_skipped_sma_count")),
        "regime_relation_mismatch_count": int_or_none(values.get("regime_relation_mismatch_count")),
        "regime_direction_mismatch_count": int_or_none(values.get("regime_direction_mismatch_count")),
        "regime_skip_reason": text_or_none(values.get("regime_skip_reason")),

        "minmax_source_candidates_count": int_or_none(values.get("minmax_source_candidates_count")),
        "minmax_kept_candidates_count": int_or_none(values.get("minmax_kept_candidates_count")),
        "minmax_dropped_candidates_count": int_or_none(values.get("minmax_dropped_candidates_count")),
        "minmax_disabled_reason": text_or_none(values.get("minmax_disabled_reason")),

        "score_candidates_count": int_or_none(values.get("score_candidates_count")),

        "potential_available": None if values.get("potential_available") is None else int(bool(values.get("potential_available"))),
        "potential_used_candidates_count": int_or_none(values.get("potential_used_candidates_count")),
        "potential_source_candidates_count": int_or_none(values.get("potential_source_candidates_count")),
        "potential_direction": text_or_none(values.get("potential_direction")),
        "potential_unavailable_reason": text_or_none(values.get("potential_unavailable_reason")),
        "potential_end_delta_points": float_or_none(values.get("potential_end_delta_points")),

        "final_plot_candidates_count": int_or_none(values.get("final_plot_candidates_count")),
        "png_saved": None if values.get("png_saved") is None else int(bool(values.get("png_saved"))),
        "skip_reason": text_or_none(values.get("skip_reason")),

        "created_at_ts": now_ts,
        "updated_at_ts": now_ts,
    }

    columns = list(row.keys())
    placeholders = ", ".join("?" for _ in columns)
    update_columns = [column for column in columns if column not in {"instrument_code", "signal_bar_ts", "signal_window_mode", "market_regime_filter_mode", "created_at_ts"}]
    update_sql = ",\n            ".join(f"{column} = excluded.{column}" for column in update_columns)

    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )
    try:
        initialize_candidate_funnel_events_table(conn)
        conn.execute(
            f"""
            INSERT INTO {CANDIDATE_FUNNEL_EVENTS_TABLE_NAME} (
                {", ".join(columns)}
            )
            VALUES ({placeholders})
            ON CONFLICT (
                instrument_code,
                signal_bar_ts,
                signal_window_mode,
                market_regime_filter_mode
            ) DO UPDATE SET
                {update_sql}
            """,
            [row[column] for column in columns],
        )
        conn.commit()
    finally:
        conn.close()
