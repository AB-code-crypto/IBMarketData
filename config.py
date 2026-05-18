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


@dataclass
class Settings:
    # Имя робота для Telegram-сообщений и логической идентификации сервиса.
    robot_name: str = "IBMarketData"

    ib_host: str = "127.0.0.1"
    ib_port: int = 7496  # 7497 - демо счёт, 7496 - реальный счёт
    ib_client_id: int = 200

    # Каталог с SQLite-БД цен.
    # Внутри него каждый логический инструмент хранится в своём файле:
    # data/prices/MNQ.sqlite3, data/prices/MES.sqlite3 и т.д.
    price_db_dir: str = str(BASE_DIR / "data" / "prices")

    # Глубина расчёта profile-нормы отклонения цены от SMA.
    # Считаем в 5-секундных свечах, чтобы не пересчитывать календарное время.
    # Один торговый день = 23 часа, потому что один час рынок закрыт на клиринг.
    sma_distance_ema_lookback_bars: int = 5 * 23 * 60 * 60 // 5

    # Когда обновляем редкие profile-характеристики инструмента.
    # Время задаётся в Chicago time, потому что расписание инструмента привязано к CT.
    profile_update_timezone: str = "America/Chicago"
    profile_daily_update_hour_ct: int = 16
    profile_daily_update_minute_ct: int = 0

    # Telegram-бот, группа и тема для сообщений о подключении/состоянии.
    telegram_bot_token: str = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    telegram_chat_id_tech: int = int(os.environ["TELEGRAM_CHAT_ID"])
    telegram_message_thread_id_tech: Optional[int] = (
        int(telegram_thread_id_connect_env)
        if telegram_thread_id_connect_env
        else None
    )


settings_live = Settings()
