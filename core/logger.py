import asyncio
import logging
import sys

# Имя главного логгера проекта.
PROJECT_LOGGER_NAME = "robot"

# Здесь хранится объект TelegramSender.
telegram_sender = None

# Флаг: можно ли сейчас создавать новые задачи отправки в Telegram.
telegram_logging_enabled = True

# Все созданные фоновые задачи отправки в Telegram.
telegram_tasks = set()


def setup_logging():
    """Что делает: настраивает основной logger проекта и глушит шум ib_async. Зачем нужна: все сервисы получают единый формат логов без дублей в PyCharm."""
    logger = logging.getLogger(PROJECT_LOGGER_NAME)

    # Уровень логирования.
    logger.setLevel(logging.INFO)

    # Очищаем старые обработчики, чтобы в PyCharm не было дублей.
    logger.handlers.clear()

    # Очень важно:
    # не передавать сообщения выше в root logger,
    # иначе туда снова начнут примешиваться сторонние логи.
    logger.propagate = False

    # Пишем логи в стандартный вывод.
    handler = logging.StreamHandler(sys.stdout)

    # Формат:
    # дата-время | уровень | модуль.функция:строка | сообщение
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
        datefmt="%H:%M:%S",
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Глушим внутренние логи библиотеки ib_async,
    # чтобы в консоль шли только наши сообщения.
    ib_async_logger = logging.getLogger("ib_async")
    ib_async_logger.handlers.clear()
    ib_async_logger.addHandler(logging.NullHandler())
    ib_async_logger.propagate = False


def setup_telegram_logging(sender):
    """Что делает: подключает TelegramSender к логгеру. Зачем нужна: log_info/log_warning могут отправлять сообщения в Telegram без прямой зависимости от конкретного сервиса."""
    global telegram_sender
    global telegram_logging_enabled

    telegram_sender = sender
    telegram_logging_enabled = True


def disable_telegram_logging():
    """Что делает: запрещает создавать новые Telegram-задачи. Зачем нужна: shutdown не должен порождать новые fire-and-forget отправки после начала остановки."""
    global telegram_logging_enabled
    telegram_logging_enabled = False


async def wait_telegram_logging():
    """Что делает: дожидается завершения уже созданных Telegram-задач. Зачем нужна: важные сообщения не должны теряться при остановке сервиса."""
    if not telegram_tasks:
        return

    tasks = list(telegram_tasks)
    await asyncio.gather(*tasks, return_exceptions=True)


def get_logger(module_name):
    """Что делает: возвращает дочерний logger для модуля. Зачем нужна: в логах сохраняется источник сообщения внутри единого logger namespace."""
    return logging.getLogger(f"{PROJECT_LOGGER_NAME}.{module_name}")


def _on_telegram_task_done(task):
    """Что делает: удаляет завершённую Telegram-задачу из набора активных задач. Зачем нужна: logger не должен копить ссылки на завершённые задачи."""
    telegram_tasks.discard(task)


def _send_to_telegram(message, to_telegram=True, message_thread_id=None):
    """Что делает: асинхронно отправляет сообщение в Telegram, если это разрешено. Зачем нужна: логирование не блокирует основной поток сервиса."""
    if not to_telegram:
        return

    # Если TelegramSender не подключён, просто ничего не делаем.
    if telegram_sender is None:
        return

    # Если telegram-логирование уже выключено, новые задачи не создаём.
    if not telegram_logging_enabled:
        return

    # Отправку в Telegram делаем fire-and-forget.
    # Но задачу сохраняем, чтобы при shutdown можно было дождаться её завершения.
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Если активного event loop нет, просто пропускаем отправку.
        return

    task = loop.create_task(telegram_sender.send_text(message, message_thread_id=message_thread_id))
    telegram_tasks.add(task)
    task.add_done_callback(_on_telegram_task_done)


def log_info(logger, message, to_telegram=True, message_thread_id=None):
    """Что делает: пишет info-сообщение в logger и при необходимости отправляет его в Telegram. Зачем нужна: единая точка для штатных информационных логов."""
    logger.info(message, stacklevel=2)
    _send_to_telegram(message, to_telegram=to_telegram, message_thread_id=message_thread_id)


def log_warning(logger, message, to_telegram=True, message_thread_id=None):
    """Что делает: пишет warning-сообщение в logger и при необходимости отправляет его в Telegram. Зачем нужна: единая точка для предупреждений и проблемных состояний."""
    logger.warning(message, stacklevel=2)
    _send_to_telegram(message, to_telegram=to_telegram, message_thread_id=message_thread_id)

