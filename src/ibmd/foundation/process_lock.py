from __future__ import annotations

import errno
import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .identity import new_id
from .time import parse_utc, utc_now_text

if os.name == "nt":  # pragma: no cover - exercised on Windows deployments
    import msvcrt
else:  # pragma: no cover - platform branch selected at import
    import fcntl


class ProcessLockError(RuntimeError):
    pass


class ServiceAlreadyRunningError(ProcessLockError):
    def __init__(self, path: Path, metadata: "ProcessLockMetadata | None") -> None:
        self.path = path
        self.metadata = metadata
        detail = "metadata unavailable" if metadata is None else (
            f"pid={metadata.pid}, instance_id={metadata.instance_id}, "
            f"host={metadata.host}"
        )
        super().__init__(f"service process lock is already held: {path}; {detail}")


@dataclass(frozen=True)
class ProcessLockMetadata:
    schema_version: int
    service_name: str
    deployment_id: str
    instance_id: str
    pid: int
    host: str
    acquired_at_utc: str

    def __post_init__(self) -> None:
        if int(self.schema_version) != 1:
            raise ProcessLockError(
                f"unsupported process-lock schema version: {self.schema_version}"
            )
        if not self.service_name or not self.deployment_id or not self.instance_id:
            raise ProcessLockError("process-lock identity fields are required")
        if int(self.pid) <= 0:
            raise ProcessLockError(f"process-lock pid must be positive: {self.pid}")
        parse_utc(self.acquired_at_utc)

    def to_dict(self) -> dict[str, object]:
        return {
            "acquired_at_utc": self.acquired_at_utc,
            "deployment_id": self.deployment_id,
            "host": self.host,
            "instance_id": self.instance_id,
            "pid": self.pid,
            "schema_version": self.schema_version,
            "service_name": self.service_name,
        }

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> "ProcessLockMetadata":
        return cls(
            schema_version=int(value["schema_version"]),
            service_name=str(value["service_name"]),
            deployment_id=str(value["deployment_id"]),
            instance_id=str(value["instance_id"]),
            pid=int(value["pid"]),
            host=str(value["host"]),
            acquired_at_utc=str(value["acquired_at_utc"]),
        )


def read_process_lock_metadata(path: str | Path) -> ProcessLockMetadata | None:
    source = Path(path)
    try:
        text = source.read_text(encoding="utf-8").strip("\x00\r\n ")
    except OSError:
        return None
    if not text:
        return None
    try:
        value = json.loads(text)
        if not isinstance(value, dict):
            return None
        return ProcessLockMetadata.from_dict(value)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, ProcessLockError):
        return None


class ServiceProcessLock:
    def __init__(
        self,
        path: str | Path,
        *,
        service_name: str,
        deployment_id: str,
        instance_id: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.service_name = str(service_name).strip()
        self.deployment_id = str(deployment_id).strip()
        self.instance_id = str(instance_id or new_id("instance"))
        self._handle: BinaryIO | None = None
        self.metadata: ProcessLockMetadata | None = None

    @property
    def acquired(self) -> bool:
        return self._handle is not None

    @staticmethod
    def _try_lock(handle: BinaryIO) -> None:
        if os.name == "nt":  # pragma: no cover - Windows deployment path
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\x00")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock(handle: BinaryIO) -> None:
        if os.name == "nt":  # pragma: no cover - Windows deployment path
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            return
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def acquire(self) -> "ServiceProcessLock":
        if self._handle is not None:
            raise ProcessLockError(f"process lock already acquired by this object: {self.path}")
        if not self.service_name or not self.deployment_id:
            raise ProcessLockError("service_name and deployment_id are required")

        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            self._try_lock(handle)
        except OSError as exc:
            handle.close()
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK, None}:
                raise ServiceAlreadyRunningError(
                    self.path,
                    read_process_lock_metadata(self.path),
                ) from exc
            raise

        metadata = ProcessLockMetadata(
            schema_version=1,
            service_name=self.service_name,
            deployment_id=self.deployment_id,
            instance_id=self.instance_id,
            pid=os.getpid(),
            host=socket.gethostname(),
            acquired_at_utc=utc_now_text(),
        )
        encoded = (json.dumps(metadata.to_dict(), sort_keys=True) + "\n").encode("utf-8")
        handle.seek(0)
        handle.truncate(0)
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
        self._handle = handle
        self.metadata = metadata
        return self

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        try:
            self._unlock(handle)
        finally:
            handle.close()

    def __enter__(self) -> "ServiceProcessLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()
