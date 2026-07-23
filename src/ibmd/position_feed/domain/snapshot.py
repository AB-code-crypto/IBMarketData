from __future__ import annotations

from collections.abc import Iterable

from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotV1,
)


def build_complete_position_snapshot(
    *,
    rows: Iterable[BrokerPositionRowV1],
    snapshot_id: str,
    account_id: str,
    captured_at_utc: str,
    published_at_utc: str,
    source_session_id: str,
) -> BrokerPositionSnapshotV1:
    return BrokerPositionSnapshotV1.complete(
        snapshot_id=snapshot_id,
        account_id=account_id,
        captured_at_utc=captured_at_utc,
        published_at_utc=published_at_utc,
        source_session_id=source_session_id,
        rows=tuple(rows),
    )


def build_failed_position_snapshot(
    *,
    snapshot_id: str,
    account_id: str,
    captured_at_utc: str,
    published_at_utc: str,
    source_session_id: str,
    error: BaseException,
    max_error_length: int = 2_000,
) -> BrokerPositionSnapshotV1:
    error_text = f"{type(error).__name__}: {error}".strip()
    if len(error_text) > int(max_error_length):
        error_text = error_text[: max(0, int(max_error_length) - 3)] + "..."
    return BrokerPositionSnapshotV1.failed(
        snapshot_id=snapshot_id,
        account_id=account_id,
        captured_at_utc=captured_at_utc,
        published_at_utc=published_at_utc,
        source_session_id=source_session_id,
        error_text=error_text,
    )
