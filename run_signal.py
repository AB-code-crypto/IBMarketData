from core.instrument_filters import get_live_enabled_instrument_codes
from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_signal.signal_runner import run_signal_loop, wait_for_job_dbs
from ib_signal.signal_settings import SignalSettings

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    instrument_codes = get_live_enabled_instrument_codes()

    if not instrument_codes:
        log_warning(
            logger,
            "Нет инструментов для signal-сервиса: history_enabled=True и realtime_enabled=True не найдены.",
            to_telegram=False,
        )
        return

    settings = SignalSettings.from_config()

    log_info(
        logger,
        f"Инструменты signal-сервиса: {instrument_codes}",
        to_telegram=False,
    )

    ready_instrument_codes = wait_for_job_dbs(
        instrument_codes=instrument_codes,
        settings=settings,
    )

    run_signal_loop(
        instrument_codes=ready_instrument_codes,
        settings=settings,
    )


if __name__ == "__main__":
    main()
