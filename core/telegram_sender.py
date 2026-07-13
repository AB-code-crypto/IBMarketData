from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path

import aiohttp


TELEGRAM_TEXT_LIMIT = 4096
TELEGRAM_REQUEST_TIMEOUT_SECONDS = 20
TELEGRAM_QUEUE_MAX_SIZE = 1000
TELEGRAM_CLOSE_FLUSH_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class TelegramJob:
    kind: str
    text: str = ""
    photo_path: Path | None = None
    caption: str = ""
    chat_id: str | None = None
    message_thread_id: int | None = None


class TelegramSender:
    """Non-blocking Telegram delivery queue.

    Public send_text/send_photo methods only enqueue work. Network delays,
    DNS failures and Bot API timeouts therefore never hold the trading loop.
    """

    def __init__(self, settings, robot_name: str):
        self.robot_name = robot_name
        self.robot_hashtag = self._build_robot_hashtag(self.robot_name)
        self.bot_token = settings.telegram_bot_token
        self.default_chat_id = settings.telegram_chat_id_tech
        self.default_message_thread_id = (
            settings.telegram_message_thread_id_tech
        )
        self.enabled = bool(self.bot_token and self.default_chat_id)
        self.base_url = (
            f"https://api.telegram.org/bot{self.bot_token}"
        )
        self.session: aiohttp.ClientSession | None = None

        self._queue: asyncio.Queue[TelegramJob] | None = None
        self._worker_task: asyncio.Task | None = None
        self._closing = False
        self._dropped_jobs = 0

    def _build_robot_hashtag(self, robot_name):
        tag = str(robot_name).strip()
        if not tag:
            raise ValueError("robot_name не должен быть пустым")

        tag = re.sub(
            r"\W+",
            "_",
            tag,
            flags=re.UNICODE,
        ).strip("_")
        if not tag:
            raise ValueError(
                f"robot_name={robot_name!r} нельзя превратить "
                "в Telegram hashtag"
            )

        return f"#{tag}"

    def _add_robot_hashtag(self, text):
        text = str(text).strip()
        if not text:
            return ""

        if text.startswith(self.robot_hashtag):
            return text

        return f"{self.robot_hashtag}\n{text}"

    async def _ensure_session(self) -> bool:
        if not self.enabled:
            return False

        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=TELEGRAM_REQUEST_TIMEOUT_SECONDS,
            )
            self.session = aiohttp.ClientSession(
                timeout=timeout,
            )

        return True

    def _ensure_worker(self) -> bool:
        if not self.enabled or self._closing:
            return False

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False

        if self._queue is None:
            self._queue = asyncio.Queue(
                maxsize=TELEGRAM_QUEUE_MAX_SIZE,
            )

        if (
                self._worker_task is None
                or self._worker_task.done()
        ):
            self._worker_task = loop.create_task(
                self._worker_loop(),
                name=f"telegram-worker-{self.robot_name}",
            )

        return True

    def _resolve_chat_id(self, chat_id):
        if chat_id is not None and str(chat_id).strip() != "":
            return str(chat_id)

        return str(self.default_chat_id)

    def _resolve_message_thread_id(self, message_thread_id):
        if message_thread_id is None:
            message_thread_id = self.default_message_thread_id

        if (
                message_thread_id is None
                or str(message_thread_id).strip() == ""
        ):
            return None

        return int(message_thread_id)

    def _split_text(self, text, limit):
        text = str(text).replace("\r\n", "\n").strip()
        if not text:
            return []

        chunks = []

        while text:
            if len(text) <= limit:
                chunks.append(text)
                break

            split_pos = text.rfind(
                "\n",
                0,
                limit + 1,
            )
            if (
                    split_pos == -1
                    or split_pos < limit // 2
            ):
                split_pos = text.rfind(
                    " ",
                    0,
                    limit + 1,
                )
            if (
                    split_pos == -1
                    or split_pos < limit // 2
            ):
                split_pos = limit

            chunk = text[:split_pos].strip()
            text = text[split_pos:].strip()

            if chunk:
                chunks.append(chunk)

        return chunks

    async def _post_json(self, method, payload) -> bool:
        if not await self._ensure_session():
            return False

        url = f"{self.base_url}/{method}"

        try:
            async with self.session.post(
                    url,
                    json=payload,
            ) as response:
                try:
                    response_payload = await response.json(
                        content_type=None,
                    )
                except Exception:
                    await response.read()
                    return False

                if (
                        response.status < 200
                        or response.status >= 300
                ):
                    return False

                return bool(response_payload.get("ok"))

        except Exception:
            return False

    async def _post_form(self, method, form) -> bool:
        if not await self._ensure_session():
            return False

        url = f"{self.base_url}/{method}"

        try:
            async with self.session.post(
                    url,
                    data=form,
            ) as response:
                try:
                    response_payload = await response.json(
                        content_type=None,
                    )
                except Exception:
                    await response.read()
                    return False

                if (
                        response.status < 200
                        or response.status >= 300
                ):
                    return False

                return bool(response_payload.get("ok"))

        except Exception:
            return False

    async def _send_text_now(
            self,
            *,
            text: str,
            chat_id: str,
            message_thread_id: int | None,
    ) -> bool:
        hashtag_prefix_length = len(self.robot_hashtag) + 1
        chunk_limit = max(
            1,
            TELEGRAM_TEXT_LIMIT - hashtag_prefix_length,
        )
        chunks = self._split_text(
            text,
            chunk_limit,
        )

        if not chunks:
            return False

        result = True

        for chunk in chunks:
            payload = {
                "chat_id": chat_id,
                "text": self._add_robot_hashtag(chunk),
            }

            if message_thread_id is not None:
                payload["message_thread_id"] = (
                    message_thread_id
                )

            ok = await self._post_json(
                "sendMessage",
                payload,
            )
            if not ok:
                result = False

        return result

    async def _send_photo_now(
            self,
            *,
            photo_path: Path,
            caption: str,
            chat_id: str,
            message_thread_id: int | None,
    ) -> bool:
        if not photo_path.is_file():
            return False

        caption_text = (
            self._add_robot_hashtag(caption)
            if caption
            else self.robot_hashtag
        )
        if len(caption_text) > 1024:
            caption_text = caption_text[:1021] + "..."

        form = aiohttp.FormData()
        form.add_field("chat_id", chat_id)
        form.add_field("caption", caption_text)

        if message_thread_id is not None:
            form.add_field(
                "message_thread_id",
                str(message_thread_id),
            )

        try:
            with photo_path.open("rb") as photo_file:
                form.add_field(
                    "photo",
                    photo_file,
                    filename=photo_path.name,
                    content_type="image/png",
                )
                return await self._post_form(
                    "sendPhoto",
                    form,
                )

        except Exception:
            return False

    async def _deliver_job(self, job: TelegramJob) -> None:
        if job.kind == "TEXT":
            await self._send_text_now(
                text=job.text,
                chat_id=str(job.chat_id),
                message_thread_id=job.message_thread_id,
            )
            return

        if job.kind != "PHOTO" or job.photo_path is None:
            return

        ok = await self._send_photo_now(
            photo_path=job.photo_path,
            caption=job.caption,
            chat_id=str(job.chat_id),
            message_thread_id=job.message_thread_id,
        )

        if ok:
            return

        fallback_text = job.caption or "Telegram photo delivery failed"
        fallback_text += (
            f"\nPNG delivery failed: {job.photo_path}"
        )
        await self._send_text_now(
            text=fallback_text,
            chat_id=str(job.chat_id),
            message_thread_id=job.message_thread_id,
        )

    async def _worker_loop(self) -> None:
        assert self._queue is not None

        while True:
            job = await self._queue.get()

            try:
                await self._deliver_job(job)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Telegram delivery has no right to fail a trading service.
                pass
            finally:
                self._queue.task_done()

    def _enqueue(self, job: TelegramJob) -> bool:
        if not self._ensure_worker():
            return False

        assert self._queue is not None

        try:
            self._queue.put_nowait(job)
            return True
        except asyncio.QueueFull:
            self._dropped_jobs += 1
            return False

    async def send_photo(
            self,
            photo_path,
            caption="",
            chat_id=None,
            message_thread_id=None,
    ) -> bool:
        photo_path = Path(photo_path)
        if not photo_path.is_file():
            return False

        return self._enqueue(
            TelegramJob(
                kind="PHOTO",
                photo_path=photo_path,
                caption=str(caption),
                chat_id=self._resolve_chat_id(chat_id),
                message_thread_id=(
                    self._resolve_message_thread_id(
                        message_thread_id
                    )
                ),
            )
        )

    async def send_text(
            self,
            text,
            chat_id=None,
            message_thread_id=None,
    ) -> bool:
        if not str(text).strip():
            return False

        return self._enqueue(
            TelegramJob(
                kind="TEXT",
                text=str(text),
                chat_id=self._resolve_chat_id(chat_id),
                message_thread_id=(
                    self._resolve_message_thread_id(
                        message_thread_id
                    )
                ),
            )
        )

    async def close(self) -> None:
        self._closing = True

        if self._queue is not None:
            try:
                await asyncio.wait_for(
                    self._queue.join(),
                    timeout=(
                        TELEGRAM_CLOSE_FLUSH_TIMEOUT_SECONDS
                    ),
                )
            except Exception:
                pass

        if (
                self._worker_task is not None
                and not self._worker_task.done()
        ):
            self._worker_task.cancel()
            await asyncio.gather(
                self._worker_task,
                return_exceptions=True,
            )

        if (
                self.session is not None
                and not self.session.closed
        ):
            await self.session.close()
