def reset_recent_backfill_state(recent_backfill_state):
    # Сбрасываем состояние разовой докачки последнего часа.
    """Что делает: отменяет активный recent-backfill и сбрасывает timestamps первого BID/ASK. Зачем нужна: после потери realtime-связи нельзя использовать старую точку синхронизации."""
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
    """Что делает: проверяет готовность realtime по локальному IB-соединению, backend и market data farm. Зачем нужна: подписки и stall-monitor должны работать только при доступном realtime."""
    return (
            ib.isConnected()
            and ib_health.ib_backend_ok
            and ib_health.market_data_ok
    )
