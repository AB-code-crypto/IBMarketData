import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from ib_async import IB, Trade

from core.ib_health import normalize_ib_message

log = logging.getLogger(__name__)


@dataclass(slots=True)
class IBError:
    id: int
    code: int
    message: str
    time_utc: datetime


@dataclass(slots=True)
class AcceptanceResult:
    order_id: int
    status: str
    accepted: bool
    timed_out: bool
    checked_at_utc: datetime
    error: Optional[IBError] = None


@dataclass(slots=True)
class DoneResult:
    order_id: int
    status: str
    done: bool
    timed_out: bool
    checked_at_utc: datetime
    error: Optional[IBError] = None


class OrderMonitor:
    """Слушает IB errorEvent и предоставляет ожидание accept/done для ордеров."""

    _ACCEPTED_STATUSES: frozenset[str] = frozenset({
        "PreSubmitted",
        "Submitted",
        "PendingSubmit",
        "ApiPending",
    })

    _TERMINAL_STATUSES: frozenset[str] = frozenset({
        "Filled",
        "Cancelled",
        "ApiCancelled",
        "Inactive",
        "Rejected",
    })

    _REJECT_STATUSES: frozenset[str] = frozenset({
        "Rejected",
        "Inactive",
        "Cancelled",
        "ApiCancelled",
    })

    def __init__(self, ib: IB) -> None:
        self._ib = ib
        self._errors_by_id: dict[int, IBError] = {}
        self._error_history_by_id: dict[int, deque[IBError]] = {}
        self._cancel_requested_at: dict[int, float] = {}
        self._ib.errorEvent += self._on_error  # type: ignore[operator]

    @property
    def ib(self) -> IB:
        return self._ib

    def last_error(self, order_id: int) -> Optional[IBError]:
        return self._errors_by_id.get(int(order_id))

    def find_error(self, order_id: int, *, code: int) -> Optional[IBError]:
        history = self._error_history_by_id.get(int(order_id))
        if not history:
            return None

        expected_code = int(code)
        for error in reversed(history):
            if int(error.code) == expected_code:
                return error
        return None

    def note_cancel_requested(self, order_id: int) -> None:
        self._cancel_requested_at[int(order_id)] = time.monotonic()

    def clear_cancel_requested(self, order_id: int) -> None:
        self._cancel_requested_at.pop(int(order_id), None)

    def is_cancel_expected(
            self,
            order_id: int,
            *,
            max_age_seconds: float = 120.0,
    ) -> bool:
        requested_at = self._cancel_requested_at.get(int(order_id))
        if requested_at is None:
            return False

        if time.monotonic() - requested_at > float(max_age_seconds):
            self._cancel_requested_at.pop(int(order_id), None)
            return False

        return True

    def _on_error(self, *args) -> None:
        try:
            if len(args) == 1:
                err = args[0]
                req_id = int(getattr(err, "id"))
                code = int(getattr(err, "errorCode"))
                msg = normalize_ib_message(str(getattr(err, "errorString")))
            else:
                req_id = int(args[0])
                code = int(args[1])
                msg = normalize_ib_message(str(args[2]))
        except Exception:
            log.exception("Failed to parse IB errorEvent args=%r", args)
            return

        error = IBError(
            id=req_id,
            code=code,
            message=msg,
            time_utc=datetime.now(timezone.utc),
        )
        self._errors_by_id[req_id] = error
        history = self._error_history_by_id.setdefault(req_id, deque(maxlen=20))
        history.append(error)

        if code == 2109:
            return

        # 202 и 10148 штатны только после cancel, отправленного нашим кодом.
        if code == 202:
            if self.is_cancel_expected(req_id):
                self.clear_cancel_requested(req_id)
                return

            log.warning(
                "Unexpected IB order cancellation: id=%s code=%s msg=%s",
                req_id,
                code,
                msg,
            )
            return

        if code == 10148 and self.is_cancel_expected(req_id):
            return

        log.warning("IB error: id=%s code=%s msg=%s", req_id, code, msg)

    async def wait_for_accept(
            self,
            trade: Trade,
            *,
            timeout: float = 5.0,
            poll_interval: float = 0.10,
    ) -> AcceptanceResult:
        loop_time = asyncio.get_running_loop().time
        deadline = loop_time() + float(timeout)

        order_id = int(trade.order.orderId)

        while True:
            status = str(trade.orderStatus.status or "")
            now = datetime.now(timezone.utc)

            if status in self._ACCEPTED_STATUSES:
                return AcceptanceResult(
                    order_id=order_id,
                    status=status,
                    accepted=True,
                    timed_out=False,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            if status in self._REJECT_STATUSES:
                return AcceptanceResult(
                    order_id=order_id,
                    status=status,
                    accepted=False,
                    timed_out=False,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            if loop_time() >= deadline:
                return AcceptanceResult(
                    order_id=order_id,
                    status=status,
                    accepted=False,
                    timed_out=True,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            await asyncio.sleep(float(poll_interval))

    async def wait_for_done(
            self,
            trade: Trade,
            *,
            timeout: float = 60.0,
            poll_interval: float = 0.10,
    ) -> DoneResult:
        loop_time = asyncio.get_running_loop().time
        deadline = loop_time() + float(timeout)

        order_id = int(trade.order.orderId)

        while True:
            status = str(trade.orderStatus.status or "")
            now = datetime.now(timezone.utc)

            if trade.isDone() or status in self._TERMINAL_STATUSES:
                return DoneResult(
                    order_id=order_id,
                    status=status,
                    done=True,
                    timed_out=False,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            if loop_time() >= deadline:
                return DoneResult(
                    order_id=order_id,
                    status=status,
                    done=False,
                    timed_out=True,
                    checked_at_utc=now,
                    error=self.last_error(order_id),
                )

            await asyncio.sleep(float(poll_interval))
