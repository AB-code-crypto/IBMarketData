from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


@dataclass
class Settings:
    ib_host: str = "127.0.0.1"
    ib_port: int = 7496  # 7497 - демо счёт 7496 - реальный счёт
    ib_client_id: int = 200

    # Файл SQLite БД
    price_db_path: str = str(BASE_DIR / "data" / "price.sqlite3")
    prepared_db_path: str = str(BASE_DIR / "data" / "prepared.sqlite3")

    # ==============================
    # Telegram bot / channels
    # ==============================

    telegram_bot_token: str = "8121278489:AAFrj5FlOQmT4lctIfHOFmkqOqDL60vq5zg"
    telegram_chat_id_tech: int = -1003721167929  # технический канал    -       IB Tech


# Набор настроек для "боевого" подключения.
settings_live = Settings()

settings_for_demo = Settings(
    ib_port=7497,
)
settings_for_gap = Settings(
    ib_client_id=105,
)
