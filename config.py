import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if value is None or value.strip() == "":
        raise RuntimeError(f"Не задана обязательная переменная окружения: {name}")

    return value.strip()


def get_required_int_env(name: str) -> int:
    value = get_required_env(name)

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Переменная окружения {name} должна быть целым числом, получено: {value!r}"
        ) from exc


@dataclass
class Settings:
    ib_host: str = "127.0.0.1"
    ib_port: int = 7496  # 7497 - демо счёт, 7496 - реальный счёт
    ib_client_id: int = 200

    # Файл SQLite БД
    price_db_path: str = str(BASE_DIR / "data" / "price.sqlite3")

    # Telegram bot / channels
    telegram_bot_token: str = get_required_env("TELEGRAM_BOT_TOKEN")
    telegram_chat_id_tech: int = get_required_int_env("TELEGRAM_CHAT_ID_TECH")


settings_live = Settings()

settings_for_demo = Settings(
    ib_port=7497,
)
