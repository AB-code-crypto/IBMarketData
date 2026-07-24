from __future__ import annotations

from collections import deque
from typing import Iterable

from ibmd.foundation.identity import new_id, validate_id
from ibmd.public_contracts.broker_reconciliation import (
    BrokerReconciliationSnapshotV1,
)

from .broker_reconciliation import BrokerReconciliationReadError


class ScriptedBrokerReconciliationReader:
    def __init__(
        self,
        values: Iterable[BrokerReconciliationSnapshotV1 | Exception],
    ) -> None:
        self._values = deque(values)
        self._source_session_id = new_id("ib_session")
        self._closed = False

    @property
    def source_session_id(self) -> str:
        return validate_id(self._source_session_id, expected_kind="ib_session")

    @property
    def connected(self) -> bool:
        return not self._closed

    async def read_snapshot(
        self,
        *,
        account_id: str,
    ) -> BrokerReconciliationSnapshotV1:
        if self._closed:
            raise BrokerReconciliationReadError(
                "scripted broker reconciliation reader is closed"
            )
        if not self._values:
            raise BrokerReconciliationReadError(
                "scripted broker reconciliation reader is exhausted"
            )
        value = self._values.popleft()
        if isinstance(value, Exception):
            raise value
        expected = str(account_id or "").strip()
        if value.account_id != expected:
            raise BrokerReconciliationReadError(
                "scripted broker reconciliation account mismatch: "
                f"snapshot={value.account_id}, requested={expected}"
            )
        return value

    async def close(self) -> None:
        self._closed = True
