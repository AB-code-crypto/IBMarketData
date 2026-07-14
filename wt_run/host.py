from __future__ import annotations

import ctypes
import os
import runpy
import sys
import time
import traceback

from wt_run.common import ROOT, get_service, write_status


def set_console_title(title: str) -> None:
    if os.name != "nt":
        return
    try:
        ctypes.windll.kernel32.SetConsoleTitleW(str(title))
    except Exception:
        pass


def system_exit_code(value) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return int(value)
    return 1


def hold_failed_tab() -> None:
    if not sys.stdin.isatty():
        return
    try:
        input("\nНажмите Enter, чтобы закрыть вкладку...")
    except (EOFError, KeyboardInterrupt):
        pass


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 2:
        print(
            "Использование: python -m wt_run.host <service> <launch_token>",
            file=sys.stderr,
        )
        return 2

    spec = get_service(args[0])
    launch_token = str(args[1])
    started_at_ts = int(time.time()) - 1
    pid = os.getpid()

    base_status = {
        "launch_token": launch_token,
        "pid": pid,
        "started_at_ts": started_at_ts,
        "python_executable": sys.executable,
    }

    set_console_title(spec.title)
    write_status(
        spec,
        {
            **base_status,
            "state": "starting",
            "exit_code": None,
            "error_text": None,
        },
    )

    print("=" * 80)
    print(f"Сервис : {spec.title}")
    print(f"Файл   : {spec.script}")
    print(f"PID    : {pid}")
    print(f"Python : {sys.executable}")
    print("=" * 80)
    print()

    exit_code = 0
    error_text: str | None = None

    try:
        os.chdir(ROOT)
        root_text = str(ROOT)
        if root_text not in sys.path:
            sys.path.insert(0, root_text)

        write_status(
            spec,
            {
                **base_status,
                "state": "running",
                "exit_code": None,
                "error_text": None,
            },
        )

        runpy.run_path(str(ROOT / spec.script), run_name="__main__")

    except SystemExit as exc:
        exit_code = system_exit_code(exc.code)
        if exit_code != 0:
            error_text = f"SystemExit: {exc.code!r}"

    except KeyboardInterrupt:
        exit_code = 130
        error_text = "Остановлен пользователем"

    except BaseException as exc:
        exit_code = 1
        error_text = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()

    finally:
        write_status(
            spec,
            {
                **base_status,
                "state": "stopped" if exit_code in {0, 130} else "failed",
                "exit_code": exit_code,
                "error_text": error_text,
                "ended_at_ts": int(time.time()),
            },
        )

    print()
    print("=" * 80)
    print(f"{spec.title} завершён: exit_code={exit_code}")
    if error_text:
        print(f"Причина: {error_text}")
    print("=" * 80)

    hold_failed_tab()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
