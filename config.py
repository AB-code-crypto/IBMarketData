import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

# Загружаем .env рядом с config.py.
# encoding="utf-8-sig" нужен на случай, если файл сохранён в Windows с BOM.
load_dotenv(ENV_PATH, encoding="utf-8-sig")

telegram_thread_id_connect_env = os.getenv("TELEGRAM_THREAD_ID_CONNECT")
telegram_thread_id_deal_env = os.getenv("TELEGRAM_THREAD_ID_DEAL")
telegram_thread_id_deal_status_env = os.getenv("TELEGRAM_THREAD_ID_DEAL_STATUS")
telegram_thread_id_error_env = os.getenv("TELEGRAM_THREAD_ID_ERROR")


def require_env_text(name: str) -> str:
    value = str(os.getenv(name, "") or "").strip()
    if not value:
        raise RuntimeError(
            f"Required environment variable {name} is missing or empty. "
            f"Add it to {ENV_PATH}."
        )
    return value


@dataclass
class Settings:
    # Имя робота для Telegram-сообщений и логической идентификации сервиса.
    robot_name: str = "IBMarketData"

    ib_host: str = "127.0.0.1"
    ib_port: int = 7496  # 7497 - демо счёт, 7496 - реальный счёт
    ib_client_id: int = 200

    # Обязательный account guard. На двух компьютерах здесь должны быть
    # account ids соответствующих TWS-сессий.
    ib_account_id: str = require_env_text("IB_ACCOUNT_ID")

    # Clock guard: новые OPEN/REVERSE запрещены, если локальные часы
    # сильно расходятся с IB server time или sample давно не обновлялся.
    ib_clock_max_abs_offset_seconds: float = float(
        os.getenv("IB_CLOCK_MAX_ABS_OFFSET_SECONDS", "3.0")
    )
    ib_clock_health_max_age_seconds: int = int(
        os.getenv("IB_CLOCK_HEALTH_MAX_AGE_SECONDS", "180")
    )

    # Каталог с SQLite-БД цен.
    price_db_dir: str = str(BASE_DIR / "data" / "prices")

    # Telegram-бот, группа и тема для сообщений о подключении/состоянии.
    telegram_bot_token: str = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    telegram_chat_id_tech: int = int(os.environ["TELEGRAM_CHAT_ID"])
    telegram_message_thread_id_tech: Optional[int] = (
        int(telegram_thread_id_connect_env)
        if telegram_thread_id_connect_env
        else None
    )
    telegram_message_thread_id_deal: Optional[int] = (
        int(telegram_thread_id_deal_env)
        if telegram_thread_id_deal_env
        else None
    )
    telegram_message_thread_id_deal_status: Optional[int] = (
        int(telegram_thread_id_deal_status_env)
        if telegram_thread_id_deal_status_env
        else None
    )
    telegram_message_thread_id_error: Optional[int] = (
        int(telegram_thread_id_error_env)
        if telegram_thread_id_error_env
        else None
    )


settings_live = Settings()
