from dataclasses import dataclass, field

from core.logger import get_logger, log_info, log_warning

logger = get_logger(__name__)


@dataclass
class IbConnectionHealth:
    # Состояние связи TWS / IB Gateway с backend IB.
    ib_backend_ok: bool = True

    # Набор market data farm, которые сейчас считаются недоступными.
    # Если набор пустой, значит market data farm в норме.
    market_data_down_farms: set[str] = field(default_factory=set)

    # Набор historical data farm (HMDS), которые сейчас считаются недоступными.
    # Если набор пустой, значит HMDS в норме.
    hmds_down_farms: set[str] = field(default_factory=set)

    @property
    def market_data_ok(self):
        # Market data считаем исправными, если нет ни одной "падающей" farm.
        return len(self.market_data_down_farms) == 0

    @property
    def hmds_ok(self):
        # HMDS считаем исправными, если нет ни одной "падающей" farm.
        return len(self.hmds_down_farms) == 0


def normalize_ib_message(text):
    # Приводим системные сообщения IB к читаемому виду.
    # Иногда TWS присылает уже нормальный текст,
    # а иногда строку с escaped unicode-последовательностями.
    text = str(text)

    if "\\u" not in text:
        return text

    try:
        return text.encode("utf-8").decode("unicode_escape")
    except Exception:
        return text


def extract_ib_farm_name(error_string):
    # Для сообщений 2103 / 2104 / 2105 / 2106 название farm обычно идёт после ":".
    # Например:
    # "Соединение с базой рыночных данных исправно:cashfarm"
    # "Нарушено соединение с базой данных HMDS:euhmds"
    text = str(error_string).strip()

    if ":" not in text:
        return "unknown"

    farm_name = text.rsplit(":", 1)[-1].strip()

    if not farm_name:
        return "unknown"

    return farm_name


def build_farms_text(farms):
    # Красиво собираем список farm в одну строку для логов.
    if not farms:
        return "-"

    return ", ".join(sorted(farms))


def build_ib_health_text(ib_health):
    # Строка со сводным состоянием здоровья соединения.
    return (
        f"ib_backend_ok={ib_health.ib_backend_ok}, "
        f"market_data_ok={ib_health.market_data_ok}, "
        f"market_data_down_farms={build_farms_text(ib_health.market_data_down_farms)}, "
        f"hmds_ok={ib_health.hmds_ok}, "
        f"hmds_down_farms={build_farms_text(ib_health.hmds_down_farms)}"
    )


def reset_ib_health_for_new_connect(ib_health):
    # При новом локальном подключении начинаем с "чистого" состояния.
    # Если TWS во время синхронизации пришлёт системные коды проблем,
    # обработчик errorEvent тут же обновит эти флаги и наборы farm.
    ib_health.ib_backend_ok = True
    ib_health.market_data_down_farms.clear()
    ib_health.hmds_down_farms.clear()


def register_ib_health_handlers(ib, ib_health):
    # Подписываемся на системные сообщения TWS / IB Gateway.
    # Через них будем поддерживать состояние backend, market data farm и HMDS.
    def on_ib_error(req_id, error_code, error_string, contract):
        # Сразу декодируем текст сообщения, чтобы в логах был нормальный русский текст.
        error_string = normalize_ib_message(error_string)

        # 1100 = TWS потерял связь с backend IB.
        # 2110 = Connectivity between TWS and server is broken.
        if error_code in (1100, 2110):
            if ib_health.ib_backend_ok:
                log_warning(
                    logger,
                    f"IB backend недоступен: code={error_code}, message={error_string}",
                )

            ib_health.ib_backend_ok = False
            return

        # 1101 / 1102 = связь TWS с backend IB восстановлена.
        if error_code in (1101, 1102):
            was_bad = not ib_health.ib_backend_ok
            ib_health.ib_backend_ok = True

            if was_bad:
                log_info(
                    logger,
                    f"IB backend снова доступен: code={error_code}, message={error_string}",
                )
            return

        # 2103 = market data farm disconnected.
        if error_code == 2103:
            farm_name = extract_ib_farm_name(error_string)

            if farm_name not in ib_health.market_data_down_farms:
                log_warning(
                    logger,
                    f"Market data farm недоступен: code={error_code}, "
                    f"farm={farm_name}, message={error_string}",
                    to_telegram=False,
                )

            ib_health.market_data_down_farms.add(farm_name)
            return

        # 2104 = market data farm connection is OK.
        if error_code == 2104:
            farm_name = extract_ib_farm_name(error_string)

            if farm_name in ib_health.market_data_down_farms:
                ib_health.market_data_down_farms.discard(farm_name)

                log_info(
                    logger,
                    f"Market data farm снова доступен: code={error_code}, "
                    f"farm={farm_name}, message={error_string}",
                )
            return

        # 2105 = historical data farm disconnected.
        if error_code == 2105:
            farm_name = extract_ib_farm_name(error_string)

            if farm_name not in ib_health.hmds_down_farms:
                log_warning(
                    logger,
                    f"HMDS недоступен: code={error_code}, "
                    f"farm={farm_name}, message={error_string}",
                    to_telegram=False,
                )

            ib_health.hmds_down_farms.add(farm_name)
            return

        # 2106 = historical data farm connection is OK.
        if error_code == 2106:
            farm_name = extract_ib_farm_name(error_string)

            if farm_name in ib_health.hmds_down_farms:
                ib_health.hmds_down_farms.discard(farm_name)

                log_info(
                    logger,
                    f"HMDS снова доступен: code={error_code}, "
                    f"farm={farm_name}, message={error_string}",
                )
            return

    ib.errorEvent += on_ib_error
