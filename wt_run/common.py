from __future__ import annotations

import ctypes
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
WINDOW_NAME = "IBMarketData"
RUNTIME_DIR = ROOT / "data" / "runtime" / "wt_run"
START_CONFIRM_TIMEOUT_SECONDS = 30.0
ACTIVE_STATES = frozenset({"starting", "running"})


@dataclass(frozen=True)
class ServiceSpec:
    key: str
    script: str
    title: str
    tab_color: str


SERVICES = (
    ServiceSpec("market_data", "run_market_data.py", "MARKET DATA", "#2563EB"),
    ServiceSpec("position_sync", "run_position_sync.py", "POSITION SYNC", "#0891B2"),
    ServiceSpec("execution", "run_execution.py", "EXECUTION", "#DC2626"),
    ServiceSpec("signal", "run_signal.py", "SIGNAL", "#D97706"),
    ServiceSpec("trader", "run_trader.py", "TRADER", "#16A34A"),
)


def normalize_service_name(value: str) -> str:
    result = str(value).strip().lower()
    result = result.replace("-", "_").replace(" ", "_")
    if result.endswith(".py"):
        result = result[:-3]
    if result.startswith("run_"):
        result = result[4:]
    return result


ALIASES: dict[str, ServiceSpec] = {}
for spec in SERVICES:
    names = {
        spec.key,
        spec.script,
        Path(spec.script).stem,
        spec.title,
        spec.title.replace(" ", "_"),
    }

    if spec.key == "market_data":
        names.update({"market", "data"})
    elif spec.key == "position_sync":
        names.update({"position", "positions", "sync"})
    elif spec.key == "execution":
        names.update({"exec"})
    elif spec.key == "signal":
        names.update({"signals"})
    elif spec.key == "trader":
        names.update({"trade"})

    for name in names:
        ALIASES[normalize_service_name(name)] = spec


def get_service(value: str) -> ServiceSpec:
    spec = ALIASES.get(normalize_service_name(value))
    if spec is None:
        available = ", ".join(item.key for item in SERVICES)
        raise ValueError(f"Неизвестный сервис {value!r}. Доступно: {available}")
    return spec


def new_launch_token() -> str:
    return uuid.uuid4().hex


def status_path(service: ServiceSpec | str) -> Path:
    spec = service if isinstance(service, ServiceSpec) else get_service(service)
    return RUNTIME_DIR / f"{spec.key}.json"


def write_status(service: ServiceSpec | str, values: dict[str, Any]) -> None:
    spec = service if isinstance(service, ServiceSpec) else get_service(service)
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    payload = dict(values)
    payload.update(
        {
            "service_key": spec.key,
            "script": spec.script,
            "title": spec.title,
            "tab_color": spec.tab_color,
            "updated_at_ts": int(time.time()),
        }
    )

    target = status_path(spec)
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, target)


def read_status(service: ServiceSpec | str) -> dict[str, Any] | None:
    path = status_path(service)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None


def is_pid_alive(pid: int) -> bool:
    pid = int(pid)
    if pid <= 0:
        return False

    if os.name != "nt":
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    process_query_limited_information = 0x1000
    still_active = 259

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    open_process = kernel32.OpenProcess
    open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    open_process.restype = ctypes.c_void_p

    get_exit_code = kernel32.GetExitCodeProcess
    get_exit_code.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    get_exit_code.restype = ctypes.c_int

    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int

    handle = open_process(process_query_limited_information, 0, pid)
    if not handle:
        return False

    try:
        exit_code = ctypes.c_uint32()
        if not get_exit_code(handle, ctypes.byref(exit_code)):
            return False
        return int(exit_code.value) == still_active
    finally:
        close_handle(handle)


def status_is_active(status: dict[str, Any] | None) -> bool:
    if not status:
        return False

    state = str(status.get("state") or "")
    pid = int(status.get("pid") or 0)
    return state in ACTIVE_STATES and is_pid_alive(pid)


def find_windows_terminal() -> str:
    for executable in ("wt.exe", "wt"):
        path = shutil.which(executable)
        if path:
            return path

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        candidate = (
            Path(local_app_data)
            / "Microsoft"
            / "WindowsApps"
            / "wt.exe"
        )
        if candidate.is_file():
            return str(candidate)

    raise FileNotFoundError(
        "wt.exe не найден. Установите Windows Terminal либо включите "
        "App execution alias для Windows Terminal."
    )


def launch_tab(
    service: ServiceSpec | str,
    *,
    launch_token: str,
    python_executable: str | None = None,
) -> None:
    if os.name != "nt":
        raise RuntimeError("Windows Terminal launcher работает только в Windows")

    spec = service if isinstance(service, ServiceSpec) else get_service(service)
    script_path = ROOT / spec.script
    if not script_path.is_file():
        raise FileNotFoundError(f"Не найден сервис: {script_path}")

    command = [
        find_windows_terminal(),
        "-w",
        WINDOW_NAME,
        "new-tab",
        "--title",
        spec.title,
        "--tabColor",
        spec.tab_color,
        "--suppressApplicationTitle",
        "-d",
        str(ROOT),
        str(python_executable or sys.executable),
        "-m",
        "wt_run.host",
        spec.key,
        str(launch_token),
    ]

    subprocess.Popen(
        command,
        cwd=str(ROOT),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def wait_started(
    service: ServiceSpec | str,
    *,
    launch_token: str,
    timeout_seconds: float = START_CONFIRM_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    spec = service if isinstance(service, ServiceSpec) else get_service(service)
    deadline = time.monotonic() + float(timeout_seconds)

    while time.monotonic() < deadline:
        status = read_status(spec)

        if status and status.get("launch_token") == launch_token:
            state = str(status.get("state") or "")
            pid = int(status.get("pid") or 0)

            if state in {"failed", "stopped"}:
                raise RuntimeError(
                    f"{spec.title} не запущен: state={state}, "
                    f"exit_code={status.get('exit_code')}, "
                    f"error={status.get('error_text')}"
                )

            if state == "running" and is_pid_alive(pid):
                return status

        time.sleep(0.1)

    raise TimeoutError(
        f"{spec.title}: вкладка не подтвердила запуск за "
        f"{timeout_seconds:g} секунд. Status: {status_path(spec)}"
    )
