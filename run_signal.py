from datetime import datetime

from ib_signal.instruments import get_signal_enabled_instrument_codes
from ib_signal.signal_runner import run_signal_loop, wait_for_job_dbs


def log_message(message: str) -> None:
    # Минимальный консольный логер для signal-сервиса.
    time_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{time_text} | {message}", flush=True)


def main() -> None:
    # run_signal намеренно не подхватывает новые инструменты на лету.
    # Чтобы добавить инструмент в signal-контур:
    # 1. history_enabled=True — закачать историю;
    # 2. realtime_enabled=True — включить live-контур когда история закачалась;
    # 3. перезапустить run_market_data.py, run_job_data.py и run_signal.py.
    instrument_codes = get_signal_enabled_instrument_codes()

    if not instrument_codes:
        log_message("Нет инструментов для signal-сервиса: history_enabled=True и realtime_enabled=True не найдены.")
        return

    log_message(f"Инструменты signal-сервиса: {instrument_codes}")

    ready_instrument_codes = wait_for_job_dbs(
        instrument_codes=instrument_codes,
        log_message=log_message,
    )

    run_signal_loop(
        instrument_codes=ready_instrument_codes,
        log_message=log_message,
    )


if __name__ == "__main__":
    main()