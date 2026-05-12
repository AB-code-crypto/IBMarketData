import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path


@dataclass(frozen=True)
class FreshJobBarStatus:
    instrument_code: str
    is_ready: bool
    reason: str
    job_db_path: Path
    last_bar_time_ts: int | None = None
    last_bar_lag_seconds: int | None = None


def read_latest_job_bar_ts(instrument_code: str) -> int:
    """Что делает: читает последний bar_time_ts из job DB инструмента.
    Зачем нужна: signal-сервису нужна только свежесть последнего рабочего бара, а не аудит структуры job DB."""
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    job_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=Instrument[instrument_code],
    )

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        row = conn.execute(
            f"""
            SELECT MAX(bar_time_ts) AS last_bar_time_ts
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            """
        ).fetchone()

        if row is None or row[0] is None:
            raise RuntimeError(
                f"Job DB не содержит рабочих баров: "
                f"instrument={instrument_code}, db={job_db_path}"
            )

        return int(row[0])

    finally:
        conn.close()


def get_fresh_job_bar_status(
    instrument_code: str,
    max_job_bar_lag_seconds: int,
) -> FreshJobBarStatus:
    """Что делает: выполняет лёгкую проверку свежести последнего job-бара.
    Зачем нужна: signal-сервис ждёт/пропускает расчёт, если job-data ещё не догнал live-поток."""
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    job_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=Instrument[instrument_code],
    )

    try:
        last_bar_time_ts = read_latest_job_bar_ts(instrument_code)

    except sqlite3.OperationalError as exc:
        # SQLite-lock может быть временным, если job-data прямо сейчас пишет в job DB.
        # Это не повод валить signal-сервис.
        if "locked" in str(exc).lower():
            return FreshJobBarStatus(
                instrument_code=instrument_code,
                is_ready=False,
                reason=f"job DB locked: {exc}",
                job_db_path=job_db_path,
            )

        # Нет файла, нет таблицы, битая SQL-схема и прочие ошибки структуры —
        # это проблема upstream-контура job-data, а не нормальное ожидание signal.
        raise

    last_bar_lag_seconds = int(time.time()) - last_bar_time_ts

    if last_bar_lag_seconds > max_job_bar_lag_seconds:
        return FreshJobBarStatus(
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

    return FreshJobBarStatus(
        instrument_code=instrument_code,
        is_ready=True,
        reason="ready",
        job_db_path=job_db_path,
        last_bar_time_ts=last_bar_time_ts,
        last_bar_lag_seconds=last_bar_lag_seconds,
    )
