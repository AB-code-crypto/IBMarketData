import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

# В .env хранятся чувствительные и зависящие от экземпляра робота значения.
load_dotenv(BASE_DIR / ".env", encoding="utf-8-sig")


def _read_bool_env(name: str) -> bool:
    value = os.environ[name].strip().lower()
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(
        f"{name} должен быть true или false; получено: {value!r}"
    )


telegram_thread_id_connect = os.getenv("TELEGRAM_THREAD_ID_CONNECT")
telegram_thread_id_deal = os.getenv("TELEGRAM_THREAD_ID_DEAL")
telegram_thread_id_deal_status = os.getenv("TELEGRAM_THREAD_ID_DEAL_STATUS")
telegram_thread_id_error = os.getenv("TELEGRAM_THREAD_ID_ERROR")


@dataclass
class Settings:
    # Имя робота для Telegram-сообщений и логической идентификации сервиса.
    robot_name: str = "IBMarketData"

    # Эти значения задаются отдельно для каждого экземпляра робота в .env.
    # host/port выбирают конкретный TWS / IB Gateway, а базовый clientId
    # разделяет API-клиентов робота внутри выбранной сессии.
    ib_host: str = os.environ["IB_HOST"].strip()
    ib_port: int = int(os.environ["IB_PORT"].strip())
    ib_client_id: int = int(os.environ["IB_CLIENT_ID"].strip())

    # Fail-closed защита от подключения к неправильному счёту.
    # Она не выбирает TWS, а проверяет, что выбранный через host/port терминал
    # действительно предоставляет ожидаемый account id.
    ib_account_id: str = os.environ["IB_ACCOUNT_ID"].strip()

    # Clock guard: новые OPEN/REVERSE запрещены, если локальные часы
    # сильно расходятся с IB server time или sample давно не обновлялся.
    ib_clock_max_abs_offset_seconds: float = 3.0
    ib_clock_health_max_age_seconds: int = 180

    # Дневной take-profit всего робота. Граница дня: 00:00 по Москве.
    daily_take_profit_enabled: bool = _read_bool_env("DAILY_TAKE_PROFIT_ENABLED")
    daily_take_profit_usd: float = float(os.environ["DAILY_TAKE_PROFIT_USD"].strip())

    # Каталог с SQLite-БД цен.
    price_db_dir: str = str(BASE_DIR / "data" / "prices")

    # Telegram-бот, группа и темы остаются в .env.
    telegram_bot_token: str = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    telegram_chat_id_tech: int = int(os.environ["TELEGRAM_CHAT_ID"].strip())
    telegram_message_thread_id_tech: int | None = (
        int(telegram_thread_id_connect)
        if telegram_thread_id_connect
        else None
    )
    telegram_message_thread_id_deal: int | None = (
        int(telegram_thread_id_deal)
        if telegram_thread_id_deal
        else None
    )
    telegram_message_thread_id_deal_status: int | None = (
        int(telegram_thread_id_deal_status)
        if telegram_thread_id_deal_status
        else None
    )
    telegram_message_thread_id_error: int | None = (
        int(telegram_thread_id_error)
        if telegram_thread_id_error
        else None
    )


settings_live = Settings()
