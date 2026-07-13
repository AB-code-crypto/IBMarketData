from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH


IB_CLOCK_TABLE_NAME = "ib_clock_health"


@dataclass(frozen=True)
class IbClockSample:
    sampled_at_ts: int
    server_time_ts: float
    local_midpoint_ts: float
    offset_seconds: float
    round_trip_seconds: float
    source_client_id: int | None


@dataclass(frozen=True)
class IbClockHealth:
    is_healthy: bool
    reason: str
    sample: IbClockSample | None
    sample_age_seconds: int | None
    max_abs_offset_seconds: float
    max_sample_age_seconds: int

    @property
    def corrected_now_ts(self) -> int:
        if self.sample is None:
            return int(time.time())

        return int(
            time.time()
            + float(self.sample.offset_seconds)
        )


def initialize_ib_clock_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {IB_CLOCK_TABLE_NAME} (
            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
            sampled_at_ts INTEGER NOT NULL,
            server_time_ts REAL NOT NULL,
            local_midpoint_ts REAL NOT NULL,
            offset_seconds REAL NOT NULL,
            round_trip_seconds REAL NOT NULL,
            source_client_id INTEGER
        );
        """
    )


def _datetime_to_utc_ts(value) -> float:
    if isinstance(value, datetime):
        dt = value
    else:
        raise TypeError(
            f"Unexpected IB server time value: {value!r}"
        )

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return float(dt.astimezone(timezone.utc).timestamp())


async def sample_and_store_ib_clock(
        ib,
        *,
        source_client_id: int | None = None,
) -> IbClockSample:
    wall_started = time.time()
    monotonic_started = time.monotonic()

    server_time = await ib.reqCurrentTimeAsync()

    monotonic_finished = time.monotonic()
    wall_finished = time.time()

    server_time_ts = _datetime_to_utc_ts(server_time)
    local_midpoint_ts = (
        float(wall_started)
        + float(wall_finished)
    ) / 2.0
    offset_seconds = (
        server_time_ts
        - local_midpoint_ts
    )
    round_trip_seconds = max(
        0.0,
        monotonic_finished - monotonic_started,
    )
    sampled_at_ts = int(wall_finished)

    sample = IbClockSample(
        sampled_at_ts=sampled_at_ts,
        server_time_ts=server_time_ts,
        local_midpoint_ts=local_midpoint_ts,
        offset_seconds=offset_seconds,
        round_trip_seconds=round_trip_seconds,
        source_client_id=(
            None
            if source_client_id is None
            else int(source_client_id)
        ),
    )

    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        initialize_ib_clock_table(conn)
        conn.execute(
            f"""
            INSERT INTO {IB_CLOCK_TABLE_NAME} (
                singleton_id,
                sampled_at_ts,
                server_time_ts,
                local_midpoint_ts,
                offset_seconds,
                round_trip_seconds,
                source_client_id
            )
            VALUES (1, ?, ?, ?, ?, ?, ?)

            ON CONFLICT(singleton_id) DO UPDATE SET
                sampled_at_ts = excluded.sampled_at_ts,
                server_time_ts = excluded.server_time_ts,
                local_midpoint_ts = excluded.local_midpoint_ts,
                offset_seconds = excluded.offset_seconds,
                round_trip_seconds = excluded.round_trip_seconds,
                source_client_id = excluded.source_client_id
            """,
            (
                sample.sampled_at_ts,
                sample.server_time_ts,
                sample.local_midpoint_ts,
                sample.offset_seconds,
                sample.round_trip_seconds,
                sample.source_client_id,
            ),
        )
        conn.commit()
        return sample

    finally:
        conn.close()


def read_last_ib_clock_sample() -> IbClockSample | None:
    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        initialize_ib_clock_table(conn)
        row = conn.execute(
            f"""
            SELECT
                sampled_at_ts,
                server_time_ts,
                local_midpoint_ts,
                offset_seconds,
                round_trip_seconds,
                source_client_id
            FROM {IB_CLOCK_TABLE_NAME}
            WHERE singleton_id = 1
            """
        ).fetchone()

        if row is None:
            return None

        return IbClockSample(
            sampled_at_ts=int(row[0]),
            server_time_ts=float(row[1]),
            local_midpoint_ts=float(row[2]),
            offset_seconds=float(row[3]),
            round_trip_seconds=float(row[4]),
            source_client_id=(
                None if row[5] is None else int(row[5])
            ),
        )

    finally:
        conn.close()


def read_ib_clock_health(
        *,
        max_abs_offset_seconds: float,
        max_sample_age_seconds: int,
        now_ts: int | None = None,
) -> IbClockHealth:
    sample = read_last_ib_clock_sample()
    max_offset = abs(float(max_abs_offset_seconds))
    max_age = max(1, int(max_sample_age_seconds))
    now_value = int(
        time.time()
        if now_ts is None
        else now_ts
    )

    if sample is None:
        return IbClockHealth(
            is_healthy=False,
            reason="IB clock sample is missing",
            sample=None,
            sample_age_seconds=None,
            max_abs_offset_seconds=max_offset,
            max_sample_age_seconds=max_age,
        )

    sample_age = max(
        0,
        now_value - int(sample.sampled_at_ts),
    )

    if sample_age > max_age:
        return IbClockHealth(
            is_healthy=False,
            reason=(
                "IB clock sample is stale: "
                f"age_seconds={sample_age}, "
                f"max_age_seconds={max_age}"
            ),
            sample=sample,
            sample_age_seconds=sample_age,
            max_abs_offset_seconds=max_offset,
            max_sample_age_seconds=max_age,
        )

    if abs(float(sample.offset_seconds)) > max_offset:
        return IbClockHealth(
            is_healthy=False,
            reason=(
                "local clock drift exceeds limit: "
                f"offset_seconds={sample.offset_seconds:+.3f}, "
                f"max_abs_offset_seconds={max_offset:.3f}, "
                f"round_trip_seconds={sample.round_trip_seconds:.3f}"
            ),
            sample=sample,
            sample_age_seconds=sample_age,
            max_abs_offset_seconds=max_offset,
            max_sample_age_seconds=max_age,
        )

    return IbClockHealth(
        is_healthy=True,
        reason="ok",
        sample=sample,
        sample_age_seconds=sample_age,
        max_abs_offset_seconds=max_offset,
        max_sample_age_seconds=max_age,
    )
