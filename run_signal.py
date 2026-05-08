from core.instrument_filters import get_live_enabled_instrument_codes
from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_signal.signal_runner import run_signal_loop, wait_for_job_dbs

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    # run_signal намеренно не подхватывает новые инструменты на лету.
    # Чтобы добавить инструмент в signal-контур:
    # 1. history_enabled=True — закачать историю;
    # 2. realtime_enabled=True — включить live-контур когда история закачалась;
    # 3. перезапустить run_market_data.py, run_job_data.py и run_signal.py.
    instrument_codes = get_live_enabled_instrument_codes()

    if not instrument_codes:
        log_warning(
            logger,
            "Нет инструментов для signal-сервиса: history_enabled=True и realtime_enabled=True не найдены.",
            to_telegram=False,
        )
        return

    log_info(
        logger,
        f"Инструменты signal-сервиса: {instrument_codes}",
        to_telegram=False,
    )

    ready_instrument_codes = wait_for_job_dbs(instrument_codes)
    run_signal_loop(ready_instrument_codes)


if __name__ == "__main__":
    main()
