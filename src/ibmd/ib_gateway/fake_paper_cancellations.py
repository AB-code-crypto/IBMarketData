from __future__ import annotations

from datetime import datetime
from typing import Callable

from ibmd.foundation.time import format_utc, utc_now

from .paper_cancellations import (
    PaperOrderCancelReceipt,
    PaperOrderCancelRequest,
)


class ScriptedPaperOrderCancellationGateway:
    def __init__(
        self,
        *,
        cancel_error: Exception | None = None,
        before_cancel: Callable[[PaperOrderCancelRequest], None] | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.cancel_error = cancel_error
        self.before_cancel = before_cancel
        self.clock = clock
        self.requests: list[PaperOrderCancelRequest] = []
        self.closed = False

    async def cancel_order(
        self,
        request: PaperOrderCancelRequest,
    ) -> PaperOrderCancelReceipt:
        self.requests.append(request)
        if self.before_cancel is not None:
            self.before_cancel(request)
        if self.cancel_error is not None:
            raise self.cancel_error
        return PaperOrderCancelReceipt(
            broker_order_id=request.broker_order_id,
            order_ref=request.order_ref,
            cancel_requested_at_utc=format_utc(self.clock()),
        )

    async def close(self) -> None:
        self.closed = True
