from __future__ import annotations

from collections.abc import Iterable

from ibmd.foundation.identity import new_id

from .positions import (
    BrokerPositionReadError,
    RawBrokerPosition,
    validate_source_session_id,
)


class ScriptedPositionReader:
    def __init__(
        self,
        script: Iterable[
            tuple[RawBrokerPosition, ...]
            | list[RawBrokerPosition]
            | Exception
        ],
        *,
        source_session_id: str | None = None,
        repeat_last: bool = False,
    ) -> None:
        self._script = list(script)
        if not self._script:
            raise ValueError("scripted position reader requires at least one item")
        self._source_session_id = (
            source_session_id or new_id("ib_session")
        )
        validate_source_session_id(self._source_session_id)
        self.repeat_last = bool(repeat_last)
        self.read_count = 0
        self.closed = False

    @property
    def source_session_id(self) -> str:
        return self._source_session_id

    async def read_positions(
        self,
        *,
        account_id: str,
    ) -> tuple[RawBrokerPosition, ...]:
        if self.closed:
            raise BrokerPositionReadError(
                "scripted position reader is closed"
            )
        index = self.read_count
        self.read_count += 1
        if index >= len(self._script):
            if not self.repeat_last:
                raise BrokerPositionReadError(
                    "scripted position reader is exhausted"
                )
            item = self._script[-1]
        else:
            item = self._script[index]

        if isinstance(item, Exception):
            raise item

        expected = str(account_id or "").strip()
        rows = tuple(item)
        mismatched = sorted(
            {row.account_id for row in rows if row.account_id != expected}
        )
        if mismatched:
            raise BrokerPositionReadError(
                "scripted positions contain an unexpected account: "
                f"expected={expected}, actual={mismatched}"
            )
        return rows

    async def close(self) -> None:
        self.closed = True
