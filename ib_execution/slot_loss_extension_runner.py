from __future__ import annotations

import time

from ib_execution.order_service import OrderService
from ib_execution.slot_loss_extension import (
    SlotLossExtensionEvent,
    is_slot_loss_extension_enabled,
    is_slot_mode,
    process_active_slot_loss_extensions_once,
    process_slot_close_decisions_once,
)


async def run_slot_loss_extension_once(
        *,
        order_service: OrderService,
) -> list[SlotLossExtensionEvent]:
    if not is_slot_mode():
        return []

    now_ts = int(time.time())
    events: list[SlotLossExtensionEvent] = []
    events.extend(
        await process_active_slot_loss_extensions_once(
            order_service=order_service,
            now_ts=now_ts,
        )
    )

    if not is_slot_loss_extension_enabled():
        return events

    events.extend(
        await process_slot_close_decisions_once(
            order_service=order_service,
            now_ts=int(time.time()),
        )
    )
    return events
