from __future__ import annotations

from ib_execution.slot_loss_extension import *

async def run_slot_loss_extension_once(*, order_service: OrderService) -> list[SlotLossExtensionEvent]:
    if not is_slot_mode():
        return []

    now_ts = int(time.time())
    events: list[SlotLossExtensionEvent] = []

    # Уже активные extensions надо сопровождать всегда, даже если флаг запуска новых выключили.
    events.extend(await process_active_slot_loss_extensions_once(
        order_service=order_service,
        now_ts=now_ts,
    ))

    if not is_slot_loss_extension_enabled():
        return events

    # active-extension processing can take time and can close positions; refresh timestamp before slot-close decision.
    now_ts = int(time.time())
    events.extend(await process_slot_close_decisions_once(
        order_service=order_service,
        now_ts=now_ts,
    ))

    return events
