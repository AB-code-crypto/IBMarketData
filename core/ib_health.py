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
        """Что делает: возвращает True, если нет проблемных market data farm. Зачем нужна: loaders должны отличать локальное API-соединение от реальной доступности market data."""
        return len(self.market_data_down_farms) == 0

    @property
    def hmds_ok(self):
        """Что делает: возвращает True, если нет проблемных HMDS farm. Зачем нужна: historical loader должен ждать восстановления HMDS перед запросами истории."""
        return len(self.hmds_down_farms) == 0


def normalize_ib_message(text):
    """Что делает: декодирует системное сообщение IB, если оно пришло с escaped unicode. Зачем нужна: логи должны показывать читаемый текст, а не escape-последовательности."""
    text = str(text)

    if "\\u" not in text:
        return text

    try:
        return text.encode("utf-8").decode("unicode_escape")
    except Exception:
        return text


def extract_ib_farm_name(error_string):
    """Что делает: достаёт имя farm из системного сообщения IB. Зачем нужна: health-state хранит конкретные проблемные market data/HMDS farm."""
    text = str(error_string).strip()

    if ":" not in text:
        return "unknown"

    farm_name = text.rsplit(":", 1)[-1].strip()

    if not farm_name:
        return "unknown"

    return farm_name


def build_farms_text(farms):
    """Что делает: форматирует набор farm в строку для логов. Зачем нужна: health-сообщения должны быть компактными и читаемыми."""
    if not farms:
        return "-"

    return ", ".join(sorted(farms))


def build_ib_health_text(ib_health):
    """Что делает: собирает сводку health-флагов IB. Зачем нужна: используется в heartbeat и Telegram-status сообщениях."""
    return (
        f"ib_backend_ok={ib_health.ib_backend_ok}, "
        f"market_data_ok={ib_health.market_data_ok}, "
        f"market_data_down_farms={build_farms_text(ib_health.market_data_down_farms)}, "
        f"hmds_ok={ib_health.hmds_ok}, "
        f"hmds_down_farms={build_farms_text(ib_health.hmds_down_farms)}"
    )


def reset_ib_health_for_new_connect(ib_health):
    """Что делает: возвращает health-state в базовое состояние перед новым подключением. Зачем нужна: старые ошибки farm не должны автоматически переноситься на новое локальное соединение."""
    ib_health.ib_backend_ok = True
    ib_health.market_data_down_farms.clear()
    ib_health.hmds_down_farms.clear()


def register_ib_health_handlers(ib, ib_health):
    """Что делает: подписывает обработчик системных errorEvent IB и обновляет health-state. Зачем нужна: backend, market data и HMDS состояние приходят через системные коды TWS/Gateway."""
    def on_ib_error(req_id, error_code, error_string, contract):
        """Что делает: обрабатывает один системный errorEvent IB и обновляет соответствующие health-флаги. Зачем нужна: поддерживает актуальное состояние backend, market data farm и HMDS."""
        error_string = normalize_ib_message(error_string)

        # 1100 = TWS потерял связь с backend IB.
        # 2110 = Connectivity between TWS and server is broken.
        if error_code in (1100, 2110):
            if ib_health.ib_backend_ok:
                log_warning(
                    logger,
                    f"IB backend недоступен: code={error_code}, message={error_string}",
                    to_telegram=False,
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
                    to_telegram=False,
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
                    to_telegram=False,
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
                    to_telegram=False,
                )
            return

    ib.errorEvent += on_ib_error
