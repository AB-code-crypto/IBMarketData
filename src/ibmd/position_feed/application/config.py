from __future__ import annotations

import math
import re
from dataclasses import dataclass

from ibmd.foundation.identity import validate_id

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class BrokerPositionFeedConfig:
    account_id: str
    deployment_id: str
    instance_id: str
    application_version: str
    configuration_hash: str
    poll_interval_seconds: float = 2.0
    poll_timeout_seconds: float = 20.0
    snapshot_max_age_seconds: float = 10.0
    service_name: str = "broker_position_feed"

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "deployment_id",
            "application_version",
            "service_name",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise ValueError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)

        object.__setattr__(
            self,
            "instance_id",
            validate_id(self.instance_id, expected_kind="instance"),
        )
        configuration_hash = str(self.configuration_hash or "").strip()
        if not _HASH_RE.fullmatch(configuration_hash):
            raise ValueError(
                "configuration_hash must be lowercase SHA-256 hex"
            )
        object.__setattr__(self, "configuration_hash", configuration_hash)

        for field_name in (
            "poll_interval_seconds",
            "poll_timeout_seconds",
            "snapshot_max_age_seconds",
        ):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(
                    f"{field_name} must be finite and positive: {value!r}"
                )
            object.__setattr__(self, field_name, value)
