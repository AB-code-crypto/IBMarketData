from __future__ import annotations

from typing import Protocol

from ibmd.public_contracts.health import ServiceHealthV1
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1


class BrokerPositionSnapshotRepository(Protocol):
    def validate_schema(self) -> None:
        ...

    def publish(self, snapshot: BrokerPositionSnapshotV1) -> None:
        ...

    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None:
        ...


class ServiceHealthPublisher(Protocol):
    def publish(self, health: ServiceHealthV1):
        ...
