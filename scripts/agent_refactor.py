from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEXT_SUFFIXES = {".py", ".md", ".txt", ".yml", ".yaml", ".toml", ".ini", ".json"}
TOKEN_GROUPS = {
    "JOB_DATA": (
        "ib_job_data",
        "job_data",
        "Job DB",
        "job DB",
        "job_db",
        "max_job_bar_lag_seconds",
    ),
    "SLOT_MODE": (
        "SignalWindowMode",
        "SLOT",
        "slot_",
        "SlotLoss",
        "slot-loss",
        "slot loss",
    ),
    "SECOND_CHANCE": (
        "slot_loss_extension",
        "slot_close_recovery",
        "second chance",
        "second-chance",
    ),
    "MARKET_FEATURES": (
        "MarketRegime",
        "market_regime",
        "regime",
        "ma_zone",
        "sma_",
        "moving average",
    ),
    "MID_SPREAD": (
        "mid_open",
        "mid_high",
        "mid_low",
        "mid_close",
        "spread_open",
        "spread_high",
        "spread_low",
        "spread_close",
    ),
}


def git_files() -> list[Path]:
    output = subprocess.check_output(
        ["git", "ls-files"],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
    )
    return [ROOT / line for line in output.splitlines() if line.strip()]


def read_text(path: Path) -> str | None:
    if path.suffix.lower() not in TEXT_SUFFIXES:
        return None
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None


def main() -> None:
    files = git_files()
    print("REPOSITORY FILES")
    print("=" * 100)
    for path in files:
        print(path.relative_to(ROOT).as_posix())

    for group_name, tokens in TOKEN_GROUPS.items():
        print()
        print(group_name)
        print("=" * 100)
        matches = 0
        for path in files:
            rel = path.relative_to(ROOT).as_posix()
            if rel in {
                "scripts/agent_refactor.py",
                "agent_refactor_report.txt",
                ".github/workflows/agent-source-snapshot.yml",
            }:
                continue
            text = read_text(path)
            if text is None:
                continue
            for line_number, line in enumerate(text.splitlines(), start=1):
                if any(token.lower() in line.lower() for token in tokens):
                    print(f"{rel}:{line_number}: {line.rstrip()}")
                    matches += 1
        print(f"-- matches: {matches}")

    print()
    print("PYTHON IMPORTS OF TARGET PACKAGES")
    print("=" * 100)
    for path in files:
        if path.suffix.lower() != ".py":
            continue
        rel = path.relative_to(ROOT).as_posix()
        if rel == "scripts/agent_refactor.py":
            continue
        text = read_text(path) or ""
        for line_number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith(("from ib_job_data", "import ib_job_data")):
                print(f"{rel}:{line_number}: {stripped}")
            if "slot_loss_extension" in stripped or "slot_close_recovery" in stripped:
                print(f"{rel}:{line_number}: {stripped}")


if __name__ == "__main__":
    main()
