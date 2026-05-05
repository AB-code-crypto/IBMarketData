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


def normalize_env_value(value: str) -> str:
    # На всякий случай чистим пробелы и внешние кавычки.
    # python-dotenv обычно сам убирает кавычки, но эта защита полезна,
    # если значение пришло из системной переменной окружения.
    value = str(value).strip()

    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1].strip()

    return value


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if value is None or value.strip() == "":
        raise RuntimeError(
            f"Не задана обязательная переменная окружения: {name}. "
            f"Проверь файл .env по пути: {ENV_PATH}"
        )

    return normalize_env_value(value)


def get_optional_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)

    if value is None or value.strip() == "":
        return default

    return normalize_env_value(value)


def get_required_int_env(name: str) -> int:
    value = get_required_env(name)

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Переменная окружения {name} должна быть целым числом, получено: {value!r}"
        ) from exc


def get_optional_int_env(name: str, default: Optional[int] = None) -> Optional[int]:
    value = get_optional_env(name)

    if value is None:
        return default

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

    # Каталог с SQLite-БД цен.
    # Внутри него каждый логический инструмент хранится в своём файле:
    # data/prices/MNQ.sqlite3, data/prices/MES.sqlite3 и т.д.
    price_db_dir: str = str(BASE_DIR / "data" / "prices")

    # Старый путь оставляем только для разовой миграции MNQ из прежней схемы.
    legacy_price_db_path: str = str(BASE_DIR / "data" / "price.sqlite3")

    # Telegram-бот, техническая группа и опциональная тема внутри группы.
    telegram_bot_token: str = get_required_env("TELEGRAM_BOT_TOKEN")
    telegram_chat_id_tech: int = get_required_int_env("TELEGRAM_CHAT_ID_TECH")
    telegram_message_thread_id_tech: Optional[int] = get_optional_int_env(
        "TELEGRAM_MESSAGE_THREAD_ID_TECH"
    )


settings_live = Settings()
