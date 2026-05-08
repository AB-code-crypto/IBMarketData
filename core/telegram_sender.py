import re

import aiohttp

# Лимит обычного текстового сообщения Telegram.
TELEGRAM_TEXT_LIMIT = 4096
# Общий timeout на один HTTP-запрос в Bot API.
TELEGRAM_REQUEST_TIMEOUT_SECONDS = 20


class TelegramSender:
    def __init__(self, settings, robot_name=None):
        # Имя робота/сервиса для маркировки Telegram-сообщений.
        # Если явно не передано, берём общее имя из settings.
        self.robot_name = robot_name or getattr(settings, "robot_name", "IBMarketData")
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
        # Превращаем имя робота в безопасный Telegram-хештег.
        # Например: "IBMarketData" -> "#IBMarketData".
        tag = str(robot_name).strip()
        if not tag:
            tag = "IBMarketData"

        tag = re.sub(r"\W+", "_", tag, flags=re.UNICODE).strip("_")
        if not tag:
            tag = "IBMarketData"

        return f"#{tag}"

    def _add_robot_hashtag(self, text):
        # Каждое Telegram-сообщение помечаем хештегом робота,
        # чтобы в общей группе было понятно, от какого сервиса пришло сообщение.
        text = str(text).strip()
        if not text:
            return ""

        if text.startswith(self.robot_hashtag):
            return text

        return f"{self.robot_hashtag}\n{text}"

    async def close(self):
        # Корректно закрываем HTTP-сессию при завершении программы.
        if self.session is not None and not self.session.closed:
            await self.session.close()

    async def _ensure_session(self):
        # Если Telegram отключён в настройках, просто ничего не делаем.
        if not self.enabled:
            return False

        # Создаём ClientSession только один раз.
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=TELEGRAM_REQUEST_TIMEOUT_SECONDS)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return True

    def _resolve_chat_id(self, chat_id):
        # Если chat_id явно передан в вызове, используем его.
        # Иначе отправляем в группу или канал по умолчанию.
        if chat_id is not None and str(chat_id).strip() != "":
            return str(chat_id)

        return str(self.default_chat_id)

    def _resolve_message_thread_id(self, message_thread_id):
        # Если тема явно передана в вызове, используем её.
        # Иначе используем техническую тему по умолчанию из настроек.
        if message_thread_id is None:
            message_thread_id = self.default_message_thread_id

        # None или пустая строка означает: писать без указания темы.
        if message_thread_id is None or str(message_thread_id).strip() == "":
            return None

        return int(message_thread_id)

    def _split_text(self, text, limit):
        # Разрезаем длинный текст на части не длиннее limit.
        # Стараемся резать по переводу строки, потом по пробелу.
        # Если не получилось, режем жёстко по limit.
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
        # Делаем JSON POST в Bot API.
        # Если Telegram вернул ошибку, возвращаем False.
        # Логировать здесь нельзя: иначе при проблеме Telegram можно получить цикл логирования.
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
        # Отправка обычного текста.
        # chat_id и message_thread_id можно передать явно,
        # но для штатных логов используются значения из настроек.
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
