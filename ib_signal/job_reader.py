from dataclasses import dataclass
from pathlib import Path
from time import time

from contracts import Instrument
from core.market_sessions import is_expected_realtime_flow_now
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_signal import signal_config
from ib_signal.signal_config import LAST_BAR_SAFETY_SECONDS


@dataclass
class JobDbStatus:
    instrument_code: str
    is_ready: bool
    reason: str

    job_db_path: Path
    table_exists: bool = False

    rows_count: int = 0
    min_bar_time_ts: int | None = None
    max_bar_time_ts: int | None = None

    expected_realtime_flow: bool = False
    last_bar_lag_seconds: int | None = None


def get_signal_job_db_path(instrument_code: str) -> Path:
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    return get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=Instrument[instrument_code],
    )


def get_job_db_status(instrument_code: str) -> JobDbStatus:
    instrument_row = Instrument[instrument_code]
    job_db_path = get_signal_job_db_path(instrument_code)

    if not job_db_path.is_file():
        return JobDbStatus(
            instrument_code=instrument_code,
            is_ready=False,
            reason="job DB file not found",
            job_db_path=job_db_path,
        )

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        table_row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name = ?
            """,
            (MID_PRICE_TABLE_NAME,),
        ).fetchone()

        if table_row is None:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason=f"table {MID_PRICE_TABLE_NAME} not found",
                job_db_path=job_db_path,
                table_exists=False,
            )

        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS rows_count,
                MIN(bar_time_ts) AS min_bar_time_ts,
                MAX(bar_time_ts) AS max_bar_time_ts
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            """
        ).fetchone()

        rows_count = int(row[0] or 0)
        min_bar_time_ts = None if row[1] is None else int(row[1])
        max_bar_time_ts = None if row[2] is None else int(row[2])

        if rows_count <= 0 or max_bar_time_ts is None:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason="job DB table is empty",
                job_db_path=job_db_path,
                table_exists=True,
                rows_count=rows_count,
                min_bar_time_ts=min_bar_time_ts,
                max_bar_time_ts=max_bar_time_ts,
            )

        min_required_rows = 1000
        if rows_count < min_required_rows:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason=f"not enough rows: {rows_count} < {min_required_rows}",
                job_db_path=job_db_path,
                table_exists=True,
                rows_count=rows_count,
                min_bar_time_ts=min_bar_time_ts,
                max_bar_time_ts=max_bar_time_ts,
            )

        expected_realtime_flow = is_expected_realtime_flow_now(
            instrument_row.get("session_model", "")
        )

        last_bar_lag_seconds = int(time()) - max_bar_time_ts

        if expected_realtime_flow:
            max_allowed_lag = signal_config.LAST_BAR_SAFETY_SECONDS

            if last_bar_lag_seconds > max_allowed_lag:
                return JobDbStatus(
                    instrument_code=instrument_code,
                    is_ready=False,
                    reason=(
                        f"last job bar is stale: "
                        f"lag={last_bar_lag_seconds}s > {max_allowed_lag}s"
                    ),
                    job_db_path=job_db_path,
                    table_exists=True,
                    rows_count=rows_count,
                    min_bar_time_ts=min_bar_time_ts,
                    max_bar_time_ts=max_bar_time_ts,
                    expected_realtime_flow=expected_realtime_flow,
                    last_bar_lag_seconds=last_bar_lag_seconds,
                )

        return JobDbStatus(
            instrument_code=instrument_code,
            is_ready=True,
            reason="ready",
            job_db_path=job_db_path,
            table_exists=True,
            rows_count=rows_count,
            min_bar_time_ts=min_bar_time_ts,
            max_bar_time_ts=max_bar_time_ts,
            expected_realtime_flow=expected_realtime_flow,
            last_bar_lag_seconds=last_bar_lag_seconds,
        )

    finally:
        conn.close()


def is_job_db_ready(instrument_code: str) -> bool:
    return get_job_db_status(instrument_code).is_ready


def get_last_job_bar_ts(instrument_code: str) -> int | None:
    status = get_job_db_status(instrument_code)

    if status.max_bar_time_ts is None:
        return None

    return status.max_bar_time_ts
