from __future__ import annotations

import hashlib
import json
import os
import socket
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


class ServiceAlreadyRunningError(RuntimeError):
    pass


def _safe_lock_name(service_name: str, instance_key: str) -> str:
    digest = hashlib.sha256(str(instance_key).encode("utf-8")).hexdigest()[:24]
    safe_service = "".join(
        char if char.isalnum() or char in {"-", "_"} else "_"
        for char in str(service_name)
    ).strip("_")
    return f"{safe_service or 'service'}_{digest}"


def _instance_metadata(service_name: str, instance_key: str) -> dict:
    return {
        "service": str(service_name),
        "instance_key": str(instance_key),
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "cwd": str(Path.cwd().resolve()),
        "started_at_ts": int(time.time()),
    }


@dataclass
class ServiceInstanceLock:
    platform: str
    handle: object
    name: str
    metadata_path: Path | None = None
    released: bool = False

    def release(self) -> None:
        if self.released:
            return

        try:
            if self.platform == "nt":
                import ctypes

                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
                kernel32.CloseHandle.restype = ctypes.c_int
                kernel32.CloseHandle(self.handle)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                self.handle.close()
        finally:
            self.released = True


def _acquire_windows_mutex(
        service_name: str,
        instance_key: str,
) -> ServiceInstanceLock:
    import ctypes
    from ctypes import wintypes

    ERROR_ALREADY_EXISTS = 183
    mutex_name = "Global\\IBMarketData_" + _safe_lock_name(
        service_name,
        instance_key,
    )

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateMutexW.argtypes = [
        ctypes.c_void_p,
        wintypes.BOOL,
        wintypes.LPCWSTR,
    ]
    kernel32.CreateMutexW.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    ctypes.set_last_error(0)
    handle = kernel32.CreateMutexW(None, False, mutex_name)
    if not handle:
        error_code = ctypes.get_last_error()
        raise OSError(
            error_code,
            f"CreateMutexW failed for {mutex_name}",
        )

    error_code = ctypes.get_last_error()
    if error_code == ERROR_ALREADY_EXISTS:
        kernel32.CloseHandle(handle)
        raise ServiceAlreadyRunningError(
            f"{service_name}: another instance is already running; "
            f"instance_key={instance_key}; mutex={mutex_name}"
        )

    # Metadata is diagnostic only. The Global named mutex is the actual lock and
    # works across Windows users/RDP sessions and different project copies.
    metadata_dir = Path(tempfile.gettempdir()) / "IBMarketData_service_locks"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = metadata_dir / (_safe_lock_name(service_name, instance_key) + ".json")
    try:
        metadata_path.write_text(
            json.dumps(
                _instance_metadata(service_name, instance_key),
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception:
        metadata_path = None

    return ServiceInstanceLock(
        platform="nt",
        handle=handle,
        name=mutex_name,
        metadata_path=metadata_path,
    )


def _acquire_posix_file_lock(
        service_name: str,
        instance_key: str,
) -> ServiceInstanceLock:
    import fcntl

    lock_dir = Path(tempfile.gettempdir()) / "IBMarketData_service_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / (_safe_lock_name(service_name, instance_key) + ".lock")
    handle = lock_path.open("a+", encoding="utf-8")

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        try:
            handle.seek(0)
            metadata = handle.read().strip() or "metadata unavailable"
        except Exception:
            metadata = "metadata unavailable"
        handle.close()
        raise ServiceAlreadyRunningError(
            f"{service_name}: another instance is already running; "
            f"instance_key={instance_key}; lock={lock_path}; existing={metadata}"
        ) from exc

    handle.seek(0)
    handle.truncate()
    handle.write(
        json.dumps(
            _instance_metadata(service_name, instance_key),
            ensure_ascii=False,
        )
        + "\n"
    )
    handle.flush()
    os.fsync(handle.fileno())

    return ServiceInstanceLock(
        platform="posix",
        handle=handle,
        name=str(lock_path),
        metadata_path=lock_path,
    )


def acquire_service_instance_lock(
        service_name: str,
        *,
        instance_key: str,
) -> ServiceInstanceLock:
    if os.name == "nt":
        return _acquire_windows_mutex(service_name, instance_key)
    return _acquire_posix_file_lock(service_name, instance_key)


@contextmanager
def service_instance_lock(
        service_name: str,
        *,
        instance_key: str,
) -> Iterator[ServiceInstanceLock]:
    lock = acquire_service_instance_lock(
        service_name,
        instance_key=instance_key,
    )
    try:
        yield lock
    finally:
        lock.release()
