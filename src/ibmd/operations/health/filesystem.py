from __future__ import annotations

from pathlib import Path

from ibmd.foundation.atomic_json import atomic_write_json, read_json_object
from ibmd.public_contracts.health import ServiceHealthV1


class ServiceHealthFile:
    def __init__(
        self,
        path: str | Path,
        *,
        expected_service: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.expected_service = (
            None if expected_service is None else str(expected_service)
        )

    def publish(self, health: ServiceHealthV1) -> Path:
        if (
            self.expected_service is not None
            and health.service != self.expected_service
        ):
            raise ValueError(
                "health service mismatch: "
                f"expected={self.expected_service!r}, actual={health.service!r}"
            )
        return atomic_write_json(self.path, health.to_dict())

    def read(self) -> ServiceHealthV1:
        health = ServiceHealthV1.from_dict(read_json_object(self.path))
        if (
            self.expected_service is not None
            and health.service != self.expected_service
        ):
            raise ValueError(
                "health service mismatch: "
                f"expected={self.expected_service!r}, actual={health.service!r}"
            )
        return health


def read_service_health(
    path: str | Path,
    *,
    expected_service: str | None = None,
) -> ServiceHealthV1:
    return ServiceHealthFile(
        path,
        expected_service=expected_service,
    ).read()
