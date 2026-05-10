import re

import aiohttp

# Лимит обычного текстового сообщения Telegram.
TELEGRAM_TEXT_LIMIT = 4096
# Общий timeout на один HTTP-запрос в Bot API.
TELEGRAM_REQUEST_TIMEOUT_SECONDS = 20


class TelegramSender:
    def __init__(self, settings, robot_name: str):
        """Что делает: создаёт отправитель Telegram-сообщений с настройками канала и хештегом сервиса. Зачем нужна: каждый сервис маркирует свои сообщения и использует общий Bot API-клиент."""
        self.robot_name = robot_name
        self.robot_hashtag = self._build_robot_hashtag(self.robot_name)
        # Токен бота Telegram.
        self.bot_token = settings.telegram_bot_token
        # Группа или канал по умолчанию для технических сообщений и логов.
        self.default_chat_id = settings.telegram_chat_id_tech
        # Тема по умолчанию внутри Telegram-группы.
        # Если группа без тем или писать нужно в общий чат, оставляем None.
        self.default_message_thread_id = settings.telegram_message_thread_id_tech
        # Если токен или chat_id по умолчанию не заданы,
        # модуль Telegram считаем выключенным.
        self.enabled = bool(self.bot_token and self.default_chat_id)
        # Базовый URL Bot API.
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        # HTTP-сессия создаётся лениво при первом запросе.
        self.session = None

    def _build_robot_hashtag(self, robot_name):
        """Что делает: превращает имя сервиса в безопасный Telegram-хештег. Зачем нужна: в общей группе можно фильтровать сообщения по сервису."""
        tag = str(robot_name).strip()
        if not tag:
            raise ValueError("robot_name не должен быть пустым")

        tag = re.sub(r"\W+", "_", tag, flags=re.UNICODE).strip("_")
        if not tag:
            raise ValueError(f"robot_name={robot_name!r} нельзя превратить в Telegram hashtag")

        return f"#{tag}"

    def _add_robot_hashtag(self, text):
        """Что делает: добавляет хештег сервиса к тексту сообщения. Зачем нужна: все Telegram-уведомления получают единый источник."""
        text = str(text).strip()
        if not text:
            return ""

        if text.startswith(self.robot_hashtag):
            return text

        return f"{self.robot_hashtag}\n{text}"

    async def close(self):
        """Что делает: закрывает aiohttp-сессию TelegramSender. Зачем нужна: shutdown не должен оставлять открытые HTTP-ресурсы."""
        if self.session is not None and not self.session.closed:
            await self.session.close()

    async def _ensure_session(self):
        """Что делает: лениво создаёт aiohttp ClientSession или возвращает False при отключённом Telegram. Зачем нужна: sender не создаёт сетевые ресурсы, пока реально не отправляет сообщение."""
        if not self.enabled:
            return False

        # Создаём ClientSession только один раз.
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=TELEGRAM_REQUEST_TIMEOUT_SECONDS)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return True

    def _resolve_chat_id(self, chat_id):
        """Что делает: выбирает явный chat_id или технический chat_id по умолчанию. Зачем нужна: штатные логи идут в общий канал, но вызов может переопределить адресата."""
        if chat_id is not None and str(chat_id).strip() != "":
            return str(chat_id)

        return str(self.default_chat_id)

    def _resolve_message_thread_id(self, message_thread_id):
        """Что делает: выбирает явную тему Telegram или тему по умолчанию. Зачем нужна: сообщения можно направлять в конкретный thread группы."""
        if message_thread_id is None:
            message_thread_id = self.default_message_thread_id

        # None или пустая строка означает: писать без указания темы.
        if message_thread_id is None or str(message_thread_id).strip() == "":
            return None

        return int(message_thread_id)

    def _split_text(self, text, limit):
        """Что делает: режет длинный текст на куски под лимит Telegram. Зачем нужна: большие traceback/status сообщения не должны теряться из-за лимита Bot API."""
        text = str(text).replace("\r\n", "\n").strip()
        if not text:
            return []

        chunks = []
        while text:
            if len(text) <= limit:
                chunks.append(text)
                break

            split_pos = text.rfind("\n", 0, limit + 1)
            if split_pos == -1 or split_pos < limit // 2:
                split_pos = text.rfind(" ", 0, limit + 1)
            if split_pos == -1 or split_pos < limit // 2:
                split_pos = limit

            chunk = text[:split_pos].strip()
            text = text[split_pos:].strip()
            if chunk:
                chunks.append(chunk)

        return chunks

    async def _post_json(self, method, payload):
        """Что делает: отправляет JSON-запрос в Bot API и возвращает успех без исключений наружу. Зачем нужна: проблема Telegram не должна валить торговый сервис."""
        if not await self._ensure_session():
            return False

        url = f"{self.base_url}/{method}"

        try:
            async with self.session.post(url, json=payload) as response:
                try:
                    response_payload = await response.json(content_type=None)
                except Exception:
                    await response.read()
                    return False

                if response.status < 200 or response.status >= 300:
                    return False

                return bool(response_payload.get("ok"))

        except Exception:
            # Не валим робота из-за проблем Telegram.
            # Повторных попыток не делаем.
            return False

    async def send_text(self, text, chat_id=None, message_thread_id=None):
        """Что делает: отправляет текстовое сообщение, добавляет хештег и режет длинные тексты. Зачем нужна: это публичный метод отправки Telegram-уведомлений сервисов."""
        resolved_chat_id = self._resolve_chat_id(chat_id)
        resolved_message_thread_id = self._resolve_message_thread_id(message_thread_id)

        # Если текст длиннее лимита Telegram, режем на части
        # и отправляем последовательно.
        hashtag_prefix_length = len(self.robot_hashtag) + 1
        chunk_limit = max(1, TELEGRAM_TEXT_LIMIT - hashtag_prefix_length)
        chunks = self._split_text(text, chunk_limit)
        if not chunks:
            return False

        result = True
        for chunk in chunks:
            payload = {
                "chat_id": resolved_chat_id,
                "text": self._add_robot_hashtag(chunk),
            }

            if resolved_message_thread_id is not None:
                payload["message_thread_id"] = resolved_message_thread_id

            ok = await self._post_json("sendMessage", payload)
            if not ok:
                result = False

        return result
