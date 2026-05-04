import time


def note_realtime_bar_received(realtime_monitor_state):
    # Запоминаем monotonic-время последнего принятого realtime-бара.
    realtime_monitor_state.last_bar_monotonic = time.monotonic()


def reset_recent_backfill_state(recent_backfill_state):
    # Сбрасываем состояние разовой докачки последнего часа.
    backfill_task = recent_backfill_state.backfill_task

    if backfill_task is not None and not backfill_task.done():
        backfill_task.cancel()

    recent_backfill_state.first_bid_ts = None
    recent_backfill_state.first_ask_ts = None
    recent_backfill_state.last_backfill_completed_sync_ts = None
    recent_backfill_state.backfill_task = None


def is_realtime_ready_now(ib, ib_health):
    # Считаем realtime ready только если:
    # - локальное API-соединение живо;
    # - backend IB доступен;
    # - market data farm в норме.
    return (
            ib.isConnected()
            and ib_health.ib_backend_ok
            and ib_health.market_data_ok
    )
