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

telegram_message_thread_id_tech_env = os.getenv("TELEGRAM_MESSAGE_THREAD_ID_TECH")


@dataclass
class Settings:
    ib_host: str = "127.0.0.1"
    ib_port: int = 7496  # 7497 - демо счёт, 7496 - реальный счёт
    ib_client_id: int = 200

    # Каталог с SQLite-БД цен.
    # Внутри него каждый логический инструмент хранится в своём файле:
    # data/prices/MNQ.sqlite3, data/prices/MES.sqlite3 и т.д.
    price_db_dir: str = str(BASE_DIR / "data" / "prices")

    # Telegram-бот, техническая группа и опциональная тема внутри группы.
    telegram_bot_token: str = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    telegram_chat_id_tech: int = int(os.environ["TELEGRAM_CHAT_ID_TECH"])
    telegram_message_thread_id_tech: Optional[int] = (
        int(telegram_message_thread_id_tech_env)
        if telegram_message_thread_id_tech_env
        else None
    )


settings_live = Settings()
