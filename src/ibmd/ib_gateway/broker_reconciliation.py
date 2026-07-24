from __future__ import annotations

from typing import Protocol

from ibmd.public_contracts.broker_reconciliation import (
    BrokerReconciliationSnapshotV1,
)


class BrokerReconciliationReadError(RuntimeError):
    pass


class IBBrokerReconciliationReader(Protocol):
    @property
    def source_session_id(self) -> str: ...

    @property
    def connected(self) -> bool: ...

    async def read_snapshot(
        self,
        *,
        account_id: str,
    ) -> BrokerReconciliationSnapshotV1: ...

    async def close(self) -> None: ...
