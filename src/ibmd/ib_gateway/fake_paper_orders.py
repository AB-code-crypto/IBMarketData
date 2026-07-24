from __future__ import annotations

from datetime import datetime
from typing import Callable

from ibmd.foundation.time import format_utc, utc_now

from .paper_orders import (
    BrokerOrderSubmitError,
    PaperMarketOrderRequest,
    PaperOrderSubmissionReceipt,
)


class ScriptedPaperOrderGateway:
    def __init__(
        self,
        *,
        broker_order_id: int,
        submit_error: Exception | None = None,
        before_submit: Callable[[PaperMarketOrderRequest], None] | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.broker_order_id = int(broker_order_id)
        if self.broker_order_id <= 0:
            raise ValueError("broker_order_id must be positive")
        self.submit_error = submit_error
        self.before_submit = before_submit
        self.clock = clock
        self.allocated_accounts: list[str] = []
        self.requests: list[PaperMarketOrderRequest] = []
        self.closed = False

    async def allocate_order_id(self, *, account_id: str) -> int:
        account = str(account_id or "").strip()
        if not account:
            raise BrokerOrderSubmitError("account_id is required")
        self.allocated_accounts.append(account)
        return self.broker_order_id

    async def submit_market_order(
        self,
        request: PaperMarketOrderRequest,
    ) -> PaperOrderSubmissionReceipt:
        self.requests.append(request)
        if self.before_submit is not None:
            self.before_submit(request)
        if self.submit_error is not None:
            raise self.submit_error
        return PaperOrderSubmissionReceipt(
            broker_order_id=request.broker_order_id,
            order_ref=request.order_ref,
            submitted_at_utc=format_utc(self.clock()),
        )

    async def close(self) -> None:
        self.closed = True
