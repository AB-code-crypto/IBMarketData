from core.logger import get_logger, log_warning

logger = get_logger(__name__)

# reqRealTimeBars у IB поддерживает только 5-секундные бары.
REALTIME_BAR_SIZE_SECONDS = 5


def subscribe_realtime_bars(ib, contract, what_to_show, use_rth):
    # Открываем подписку на 5-секундные real-time бары.
    """Что делает: открывает IB reqRealTimeBars подписку на 5-секундные бары. Зачем нужна: скрывает фиксированный barSize и единый способ подписки."""
    return ib.reqRealTimeBars(
        contract=contract,
        barSize=REALTIME_BAR_SIZE_SECONDS,
        whatToShow=what_to_show,
        useRTH=use_rth,
    )


def cancel_realtime_bars_safe(ib, realtime_bars):
    # Безопасно отменяем активную подписку, если она была создана.
    """Что делает: безопасно отменяет realtime-подписку и логирует ошибку вместо падения. Зачем нужна: shutdown/reconnect не должен ломаться из-за проблем отмены подписки."""
    if realtime_bars is None:
        return

    try:
        ib.cancelRealTimeBars(realtime_bars)
    except Exception as exc:
        log_warning(
            logger,
            f"Не удалось отменить realtime-подписку: {exc}",
            to_telegram=False,
        )


def clear_realtime_subscription_rows(ib, current_subscriptions):
    # Снимаем обработчики и отменяем все активные realtime-подписки.
    """Что делает: снимает handlers, отменяет все realtime-подписки и очищает список подписок. Зачем нужна: переподписка и shutdown не должны оставлять старые updateEvent handlers."""
    for subscription_row in current_subscriptions:
        realtime_bars = subscription_row["realtime_bars"]
        update_handler = subscription_row["update_handler"]

        if realtime_bars is not None and update_handler is not None:
            try:
                realtime_bars.updateEvent -= update_handler
            except Exception as exc:
                log_warning(
                    logger,
                    f"Не удалось снять обработчик realtime updateEvent "
                    f"для {subscription_row['what_to_show']}: {exc}",
                    to_telegram=False,
                )

        cancel_realtime_bars_safe(ib, realtime_bars)

    current_subscriptions.clear()
