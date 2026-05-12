import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path

MIN_REQUIRED_ROWS = 1000


@dataclass
class JobDbStatus:
    instrument_code: str
    is_ready: bool
    reason: str
    job_db_path: Path
    rows_count: int | None = None
    last_bar_time_ts: int | None = None
    last_bar_lag_seconds: int | None = None


def get_job_db_status(
    instrument_code: str,
    max_job_bar_lag_seconds: int,
) -> JobDbStatus:
    """Что делает: выполняет полную стартовую проверку job DB: файл, таблица, количество строк и свежесть последнего бара.
    Зачем нужна: signal-сервис должен входить в runtime-loop только после готовности рабочей базы."""
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    job_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=Instrument[instrument_code],
    )

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
            )

        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS rows_count,
                MAX(bar_time_ts) AS last_bar_time_ts
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            """
        ).fetchone()

        rows_count = int(row[0] or 0)
        last_bar_time_ts = None if row[1] is None else int(row[1])

        if rows_count < MIN_REQUIRED_ROWS:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason=f"not enough rows: {rows_count} < {MIN_REQUIRED_ROWS}",
                job_db_path=job_db_path,
                rows_count=rows_count,
                last_bar_time_ts=last_bar_time_ts,
            )

        if last_bar_time_ts is None:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason="last bar not found",
                job_db_path=job_db_path,
                rows_count=rows_count,
                last_bar_time_ts=None,
            )

        last_bar_lag_seconds = int(time.time()) - last_bar_time_ts

        if last_bar_lag_seconds > max_job_bar_lag_seconds:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason=(
                    "last bar is stale: "
                    f"{last_bar_lag_seconds}s > {max_job_bar_lag_seconds}s"
                ),
                job_db_path=job_db_path,
                rows_count=rows_count,
                last_bar_time_ts=last_bar_time_ts,
                last_bar_lag_seconds=last_bar_lag_seconds,
            )

        return JobDbStatus(
            instrument_code=instrument_code,
            is_ready=True,
            reason="ready",
            job_db_path=job_db_path,
            rows_count=rows_count,
            last_bar_time_ts=last_bar_time_ts,
            last_bar_lag_seconds=last_bar_lag_seconds,
        )

    finally:
        conn.close()


def get_latest_job_bar_status(
    instrument_code: str,
    max_job_bar_lag_seconds: int,
) -> JobDbStatus:
    """Что делает: выполняет лёгкую runtime-проверку job DB только по последнему bar_time_ts и его lag.
    Зачем нужна: signal-loop должен отсекать stale-данные без тяжёлого COUNT(*) и проверки sqlite_master каждую секунду."""
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    job_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=Instrument[instrument_code],
    )

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
        try:
            row = conn.execute(
                f"""
                SELECT MAX(bar_time_ts) AS last_bar_time_ts
                FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
                """
            ).fetchone()
        except sqlite3.OperationalError as exc:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason=f"latest bar query failed: {exc}",
                job_db_path=job_db_path,
            )

        last_bar_time_ts = None if row is None or row[0] is None else int(row[0])

        if last_bar_time_ts is None:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason="last bar not found",
                job_db_path=job_db_path,
                last_bar_time_ts=None,
            )

        last_bar_lag_seconds = int(time.time()) - last_bar_time_ts

        if last_bar_lag_seconds > max_job_bar_lag_seconds:
            return JobDbStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason=(
                    "last bar is stale: "
                    f"{last_bar_lag_seconds}s > {max_job_bar_lag_seconds}s"
                ),
                job_db_path=job_db_path,
                last_bar_time_ts=last_bar_time_ts,
                last_bar_lag_seconds=last_bar_lag_seconds,
            )

        return JobDbStatus(
            instrument_code=instrument_code,
            is_ready=True,
            reason="ready",
            job_db_path=job_db_path,
            last_bar_time_ts=last_bar_time_ts,
            last_bar_lag_seconds=last_bar_lag_seconds,
        )

    finally:
        conn.close()
