from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REEXEC_MARKER = "IBMARKETDATA_WT_REEXEC"


def find_project_python() -> Path | None:
    candidates = [
        ROOT / ".venv" / "Scripts" / "python.exe",
        ROOT / "venv" / "Scripts" / "python.exe",
        ROOT / "env" / "Scripts" / "python.exe",
    ]

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    for child in ROOT.iterdir():
        candidate = child / "Scripts" / "python.exe"
        if child.is_dir() and candidate.is_file():
            return candidate.resolve()

    return None


def reexec_in_project_venv() -> int | None:
    if os.name != "nt":
        return None

    if os.environ.get(REEXEC_MARKER) == "1":
        return None

    project_python = find_project_python()
    if project_python is None:
        return None

    try:
        current_python = Path(sys.executable).resolve()
    except OSError:
        current_python = Path(sys.executable)

    if current_python == project_python:
        return None

    environment = os.environ.copy()
    environment[REEXEC_MARKER] = "1"

    completed = subprocess.run(
        [str(project_python), str(Path(__file__).resolve()), *sys.argv[1:]],
        cwd=str(ROOT),
        env=environment,
        check=False,
    )
    return int(completed.returncode)


def hold_on_error() -> None:
    if not sys.stdin.isatty():
        return
    try:
        input("\nНажмите Enter для выхода...")
    except (EOFError, KeyboardInterrupt):
        pass


def main() -> int:
    reexec_code = reexec_in_project_venv()
    if reexec_code is not None:
        return reexec_code

    from wt_run.launcher import main as launcher_main

    return int(launcher_main())


if __name__ == "__main__":
    try:
        exit_code = main()
    except KeyboardInterrupt:
        exit_code = 130
    except Exception as exc:
        print(f"\nОШИБКА run_wt: {type(exc).__name__}: {exc}", file=sys.stderr)
        hold_on_error()
        exit_code = 1

    raise SystemExit(exit_code)
