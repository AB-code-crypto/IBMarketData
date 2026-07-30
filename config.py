import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

# В .env хранятся чувствительные и зависящие от экземпляра робота значения.
load_dotenv(BASE_DIR / ".env", encoding="utf-8-sig")


def _read_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"Обязательная переменная окружения не задана: {name}"
        )
    return value.strip()


def _read_required_bool_env(name: str) -> bool:
    value = _read_required_env(name).lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"{name} должен быть true/false, 1/0, yes/no или on/off; "
        f"получено: {value!r}"
    )


def _read_required_int_env(name: str) -> int:
    value = _read_required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} должен быть целым числом; получено: {value!r}"
        ) from exc


def _read_required_float_env(name: str) -> float:
    value = _read_required_env(name)
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} должен быть числом; получено: {value!r}"
        ) from exc


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
    ib_host: str = _read_required_env("IB_HOST")
    ib_port: int = _read_required_int_env("IB_PORT")
    ib_client_id: int = _read_required_int_env("IB_CLIENT_ID")

    # Fail-closed защита от подключения к неправильному счёту.
    # Она не выбирает TWS, а проверяет, что выбранный через host/port терминал
    # действительно предоставляет ожидаемый account id.
    ib_account_id: str = _read_required_env("IB_ACCOUNT_ID")

    # Clock guard: новые OPEN/REVERSE запрещены, если локальные часы
    # сильно расходятся с IB server time или sample давно не обновлялся.
    ib_clock_max_abs_offset_seconds: float = 3.0
    ib_clock_health_max_age_seconds: int = 180

    # Дневной take-profit всего робота. Граница дня: 00:00 по Москве.
    daily_take_profit_enabled: bool = _read_required_bool_env(
        "DAILY_TAKE_PROFIT_ENABLED"
    )
    daily_take_profit_usd: float = _read_required_float_env(
        "DAILY_TAKE_PROFIT_USD"
    )

    # Каталог с SQLite-БД цен.
    price_db_dir: str = str(BASE_DIR / "data" / "prices")

    # Telegram-бот, группа и темы остаются в .env.
    telegram_bot_token: str = _read_required_env("TELEGRAM_BOT_TOKEN")
    telegram_chat_id_tech: int = _read_required_int_env("TELEGRAM_CHAT_ID")
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

    def __post_init__(self) -> None:
        if self.daily_take_profit_usd < 0.0:
            raise ValueError("DAILY_TAKE_PROFIT_USD должен быть >= 0")

        if (
            self.daily_take_profit_enabled
            and self.daily_take_profit_usd <= 0.0
        ):
            raise ValueError(
                "При DAILY_TAKE_PROFIT_ENABLED=true значение "
                "DAILY_TAKE_PROFIT_USD должно быть > 0"
            )


settings_live = Settings()
