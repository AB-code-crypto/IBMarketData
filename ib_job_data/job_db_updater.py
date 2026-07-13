from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from config import settings_live as settings
from contracts import Instrument
from core.instrument_db import (
    get_instrument_db_path,
    get_instrument_table_name,
)
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import (
    MID_PRICE_TABLE_NAME,
    create_mid_price_table_sql,
    insert_missing_mid_price_from_attached_price_db_sql,
    quote_identifier,
)
from ib_job_data.ma_zone_features import update_ma_zone_features
from ib_job_data.rebuild_mid_price import (
    get_instrument_feature_db_path,
)
from ib_job_data.regime_features import update_regime_features
from ib_job_data.sma_features import (
    SMA_TABLE_NAME,
    append_new_sma_rows,
    create_sma_table_sql,
)


PRICE_DB_SCHEMA_NAME = "price_src"

# Recent historical backfill currently covers the last hour. Two hours gives
# enough overlap for delayed writes/reconnect while keeping the query cheap.
LATE_BACKFILL_LOOKBACK_SECONDS = 2 * 60 * 60


@dataclass(frozen=True)
class JobDbUpdateResult:
    inserted_rows: int
    late_backfill_rows: int
    new_tail_rows: int
    earliest_repaired_bar_ts: int | None

    @property
    def has_changes(self) -> bool:
        return self.inserted_rows > 0


def get_last_job_bar_ts(conn) -> int:
    row = conn.execute(
        f"""
        SELECT MAX(bar_time_ts)
        FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
        """
    ).fetchone()

    if row is None or row[0] is None:
        return 0

    return int(row[0])


def read_missing_source_bar_summary(
        conn,
        *,
        source_table_name: str,
        lookback_start_ts: int,
        previous_last_job_bar_ts: int,
) -> tuple[int | None, int, int]:
    source_table_ref = (
        f"{quote_identifier(PRICE_DB_SCHEMA_NAME)}."
        f"{quote_identifier(source_table_name)}"
    )
    target_table_ref = quote_identifier(
        MID_PRICE_TABLE_NAME
    )

    row = conn.execute(
        f"""
        SELECT
            MIN(
                CASE
                    WHEN source.bar_time_ts <= ?
                    THEN source.bar_time_ts
                END
            ) AS earliest_late_bar_ts,
            SUM(
                CASE
                    WHEN source.bar_time_ts <= ?
                    THEN 1
                    ELSE 0
                END
            ) AS late_rows,
            COUNT(*) AS all_missing_rows
        FROM {source_table_ref} AS source
        LEFT JOIN {target_table_ref} AS target
          ON target.bar_time_ts = source.bar_time_ts
        WHERE source.bar_time_ts >= ?
          AND target.bar_time_ts IS NULL
          AND source.bid_open IS NOT NULL
          AND source.ask_open IS NOT NULL
          AND source.bid_high IS NOT NULL
          AND source.ask_high IS NOT NULL
          AND source.bid_low IS NOT NULL
          AND source.ask_low IS NOT NULL
          AND source.bid_close IS NOT NULL
          AND source.ask_close IS NOT NULL
        """,
        (
            int(previous_last_job_bar_ts),
            int(previous_last_job_bar_ts),
            int(lookback_start_ts),
        ),
    ).fetchone()

    if row is None:
        return None, 0, 0

    earliest_late_bar_ts = (
        None if row[0] is None else int(row[0])
    )
    late_rows = int(row[1] or 0)
    all_missing_rows = int(row[2] or 0)

    return (
        earliest_late_bar_ts,
        late_rows,
        all_missing_rows,
    )


def rebuild_feature_tail_after_late_backfill(
        conn,
        *,
        earliest_repaired_bar_ts: int,
        instrument_row: dict,
) -> None:
    conn.execute(create_sma_table_sql())
    conn.execute(
        f"""
        DELETE FROM {quote_identifier(SMA_TABLE_NAME)}
        WHERE bar_time_ts >= ?
        """,
        (int(earliest_repaired_bar_ts),),
    )

    append_new_sma_rows(
        conn,
        mid_price_digits=instrument_row[
            "mid_price_digits"
        ],
    )
    update_regime_features(
        conn,
        instrument_row=instrument_row,
    )
    update_ma_zone_features(conn)


def append_new_mid_price_rows(
        instrument_code: str,
) -> JobDbUpdateResult:
    """Adds new bars and repairs recent holes populated by late raw backfill."""
    if instrument_code not in Instrument:
        raise ValueError(
            f"Инструмент {instrument_code!r} не найден "
            "в contracts.py"
        )

    instrument_row = Instrument[instrument_code]

    price_db_path = Path(
        get_instrument_db_path(
            settings=settings,
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )
    )
    if not price_db_path.is_file():
        raise FileNotFoundError(
            f"Price DB не найдена: {price_db_path}"
        )

    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )
    if not feature_db_path.is_file():
        raise FileNotFoundError(
            f"Job DB не найдена: {feature_db_path}"
        )

    price_table_name = get_instrument_table_name(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(
        str(feature_db_path),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        conn.execute(create_mid_price_table_sql())
        previous_last_job_bar_ts = get_last_job_bar_ts(
            conn
        )
        lookback_start_ts = max(
            0,
            previous_last_job_bar_ts
            - LATE_BACKFILL_LOOKBACK_SECONDS,
        )

        conn.execute(
            f"ATTACH DATABASE ? AS "
            f"{quote_identifier(PRICE_DB_SCHEMA_NAME)}",
            (str(price_db_path),),
        )

        (
            earliest_late_bar_ts,
            late_rows,
            all_missing_rows,
        ) = read_missing_source_bar_summary(
            conn,
            source_table_name=price_table_name,
            lookback_start_ts=lookback_start_ts,
            previous_last_job_bar_ts=(
                previous_last_job_bar_ts
            ),
        )

        if all_missing_rows <= 0:
            return JobDbUpdateResult(
                inserted_rows=0,
                late_backfill_rows=0,
                new_tail_rows=0,
                earliest_repaired_bar_ts=None,
            )

        insert_sql = (
            insert_missing_mid_price_from_attached_price_db_sql(
                attached_schema_name=(
                    PRICE_DB_SCHEMA_NAME
                ),
                source_table_name=price_table_name,
                price_digits=instrument_row[
                    "price_digits"
                ],
                mid_price_digits=instrument_row[
                    "mid_price_digits"
                ],
            )
        )

        changes_before = conn.total_changes
        conn.execute(
            insert_sql,
            (int(lookback_start_ts),),
        )
        inserted_rows = (
            conn.total_changes - changes_before
        )

        late_backfill_rows = min(
            int(late_rows),
            int(inserted_rows),
        )
        new_tail_rows = max(
            0,
            int(inserted_rows)
            - int(late_backfill_rows),
        )

        if (
                late_backfill_rows > 0
                and earliest_late_bar_ts is not None
        ):
            rebuild_feature_tail_after_late_backfill(
                conn,
                earliest_repaired_bar_ts=(
                    earliest_late_bar_ts
                ),
                instrument_row=instrument_row,
            )
        else:
            append_new_sma_rows(
                conn,
                mid_price_digits=instrument_row[
                    "mid_price_digits"
                ],
            )
            update_regime_features(
                conn,
                instrument_row=instrument_row,
            )
            update_ma_zone_features(conn)

        conn.commit()

        return JobDbUpdateResult(
            inserted_rows=int(inserted_rows),
            late_backfill_rows=int(
                late_backfill_rows
            ),
            new_tail_rows=int(new_tail_rows),
            earliest_repaired_bar_ts=(
                earliest_late_bar_ts
                if late_backfill_rows > 0
                else None
            ),
        )

    finally:
        try:
            conn.execute(
                f"DETACH DATABASE "
                f"{quote_identifier(PRICE_DB_SCHEMA_NAME)}"
            )
        except Exception:
            pass

        conn.close()
