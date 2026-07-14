from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from config import settings_live as settings
from contracts import Instrument
from core.instrument_filters import get_trading_enabled_instrument_codes
from core.state_db import STATE_DB_PATH
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_signal.job_reader import get_fresh_job_bar_status
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
from ib_trader.trade_position_repository import POSITION_SNAPSHOT_MAX_AGE_SECONDS
from ib_trader.trade_schema import POSITIONS_LATEST_TABLE_NAME, TRADE_DB_PATH


ROOT = Path(__file__).resolve().parent
POLL_SECONDS = 1
STATUS_EVERY_SECONDS = 10
START_GRACE_SECONDS = 5

SERVICE_FILES = (
    "run_market_data.py",
    "run_position_sync.py",
    "run_execution.py",
    "run_job_data.py",
    "run_signal.py",
    "run_trader.py",
)


@dataclass
class Child:
    script: str
    process: subprocess.Popen
    started_at_ts: int


def start_service(script: str) -> Child:
    path = ROOT / script
    if not path.is_file():
        raise RuntimeError(f"Не найден сервис: {path}")

    child = Child(
        script=script,
        process=subprocess.Popen(
            [sys.executable, str(path)],
            cwd=str(ROOT),
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        ),
        started_at_ts=int(time.time()) - 1,
    )

    time.sleep(1)
    if child.process.poll() is not None:
        raise RuntimeError(
            f"{script} завершился сразу после запуска: "
            f"exit_code={child.process.returncode}"
        )

    print(f"[STARTED] {script}: pid={child.process.pid}")
    return child


def require_alive(children: list[Child]) -> None:
    dead = [
        (child.script, child.process.returncode)
        for child in children
        if child.process.poll() is not None
    ]
    if dead:
        raise RuntimeError(f"Сервис завершился во время запуска: {dead}")


def wait_ready(label: str, children: list[Child], check) -> None:
    next_log = 0.0
    while True:
        require_alive(children)
        try:
            ready, details = check()
        except Exception as exc:
            ready, details = False, f"{type(exc).__name__}: {exc}"

        if ready:
            print(f"[READY] {label}: {details}")
            return

        now = time.monotonic()
        if now >= next_log:
            print(f"[WAIT] {label}: {details}")
            next_log = now + STATUS_EVERY_SECONDS
        time.sleep(POLL_SECONDS)


def market_ready(codes: list[str], since: int) -> tuple[bool, str]:
    if not STATE_DB_PATH.is_file():
        return False, "state.sqlite3 пока не создана"

    marks = ",".join("?" for _ in codes)
    conn = sqlite3.connect(str(STATE_DB_PATH))
    try:
        rows = conn.execute(
            f"""
            SELECT instrument_code, signal_ready, updated_at_ts, error_text
            FROM instrument_state
            WHERE instrument_code IN ({marks})
            """,
            tuple(codes),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        return False, f"instrument_state пока не готова: {exc}"
    finally:
        conn.close()

    state = {str(row[0]): row[1:] for row in rows}
    waiting = []
    for code in codes:
        row = state.get(code)
        if row is None:
            waiting.append(f"{code}=missing")
        elif row[2]:
            waiting.append(f"{code}=error:{row[2]}")
        elif int(row[1]) < since:
            waiting.append(f"{code}=old_state")
        elif int(row[0]) != 1:
            waiting.append(f"{code}=signal_ready=0")

    return (not waiting, ", ".join(waiting) or f"signal_ready={codes}")


def positions_ready(
        codes: list[str],
        since: int | None,
) -> tuple[bool, str]:
    if not TRADE_DB_PATH.is_file():
        return False, "trade.sqlite3 пока не создана"

    marks = ",".join("?" for _ in codes)
    conn = sqlite3.connect(str(TRADE_DB_PATH))
    try:
        rows = conn.execute(
            f"""
            SELECT instrument_code, side, quantity, broker_account, updated_at_ts
            FROM {POSITIONS_LATEST_TABLE_NAME}
            WHERE instrument_code IN ({marks})
            """,
            tuple(codes),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        return False, f"positions_latest пока не готова: {exc}"
    finally:
        conn.close()

    state = {str(row[0]): row[1:] for row in rows}
    now_ts = int(time.time())
    waiting = []
    ready = []

    for code in codes:
        row = state.get(code)
        if row is None:
            waiting.append(f"{code}=missing")
            continue

        side, qty, account, updated_at_ts = row
        updated_at_ts = int(updated_at_ts)
        age = max(0, now_ts - updated_at_ts)

        if str(account or "") != settings.ib_account_id:
            waiting.append(f"{code}=wrong_account:{account}")
        elif since is not None and updated_at_ts < since:
            waiting.append(f"{code}=old_snapshot")
        elif age > POSITION_SNAPSHOT_MAX_AGE_SECONDS:
            waiting.append(f"{code}=stale:{age}s")
        else:
            ready.append(f"{code}={str(side).upper()}/{float(qty):g}")

    return (not waiting, ", ".join(waiting) or ", ".join(ready))


def execution_ready(since: int) -> tuple[bool, str]:
    if not STATE_DB_PATH.is_file():
        return False, "state.sqlite3 пока не создана"

    conn = sqlite3.connect(str(STATE_DB_PATH))
    try:
        try:
            clock = conn.execute(
                """
                SELECT sampled_at_ts, source_client_id
                FROM ib_clock_health
                WHERE singleton_id = 1
                """
            ).fetchone()
        except sqlite3.OperationalError:
            clock = None

        try:
            daily = conn.execute(
                """
                SELECT MAX(updated_at_ts)
                FROM daily_trading_guard
                WHERE account_id = ?
                """,
                (settings.ib_account_id,),
            ).fetchone()
        except sqlite3.OperationalError:
            daily = None
    finally:
        conn.close()

    expected_client_id = int(settings.ib_client_id) + 40
    if clock is not None:
        sample_ts = int(clock[0])
        source_id = None if clock[1] is None else int(clock[1])
        if sample_ts >= since and source_id == expected_client_id:
            return True, f"IB connected, clientId={expected_client_id}"

    if daily and daily[0] is not None and int(daily[0]) >= since:
        return True, "daily take-profit startup evaluation completed"

    return False, f"жду execution clientId={expected_client_id}"


def job_ready(codes: list[str], since: int) -> tuple[bool, str]:
    waiting = []
    ready = []

    for code in codes:
        status = get_fresh_job_bar_status(
            code,
            DEFAULT_SIGNAL_CONFIG.max_job_bar_lag_seconds,
        )
        path = Path(
            get_instrument_feature_db_path(
                instrument_code=code,
                instrument_row=Instrument[code],
            )
        )

        if not path.is_file():
            waiting.append(f"{code}=job_db_missing")
        elif path.stat().st_mtime < since:
            waiting.append(f"{code}=job_db_not_rebuilt")
        elif not status.is_ready:
            waiting.append(f"{code}={status.reason}")
        else:
            ready.append(f"{code}=lag:{status.last_bar_lag_seconds}s")

    return (not waiting, ", ".join(waiting) or ", ".join(ready))


def wait_stable(child: Child, children: list[Child]) -> None:
    deadline = time.monotonic() + START_GRACE_SECONDS
    while time.monotonic() < deadline:
        require_alive(children)
        time.sleep(0.25)
    print(f"[READY] {child.script}: process_alive={START_GRACE_SECONDS}s")


def main() -> int:
    if os.name != "nt":
        raise RuntimeError("run_all.py рассчитан на Windows")
    if any(not (ROOT / name).is_file() for name in SERVICE_FILES):
        raise RuntimeError("run_all.py должен лежать в корне IBMarketData")

    codes = get_trading_enabled_instrument_codes()
    if not codes:
        raise RuntimeError("Нет trading_enabled инструментов")

    print("Запускаю сервисы IBMarketData")
    print(f"account={settings.ib_account_id}")
    print(f"instruments={codes}")
    print(f"python={sys.executable}\n")

    children: list[Child] = []
    try:
        market = start_service("run_market_data.py")
        children.append(market)
        wait_ready(
            "market_data",
            children,
            lambda: market_ready(codes, market.started_at_ts),
        )

        position = start_service("run_position_sync.py")
        children.append(position)
        wait_ready(
            "position_sync",
            children,
            lambda: positions_ready(codes, position.started_at_ts),
        )

        execution = start_service("run_execution.py")
        children.append(execution)
        wait_ready(
            "execution",
            children,
            lambda: execution_ready(execution.started_at_ts),
        )

        job = start_service("run_job_data.py")
        children.append(job)
        wait_ready(
            "job_data",
            children,
            lambda: job_ready(codes, job.started_at_ts),
        )

        signal = start_service("run_signal.py")
        children.append(signal)
        wait_stable(signal, children)

        # Последняя проверка перед сервисом, создающим реальные trade_intents.
        wait_ready(
            "final position snapshot",
            children,
            lambda: positions_ready(codes, None),
        )
        wait_ready(
            "final job bars",
            children,
            lambda: job_ready(codes, job.started_at_ts),
        )

        trader = start_service("run_trader.py")
        children.append(trader)
        wait_stable(trader, children)

        print("\nВсе сервисы успешно запущены:")
        for child in children:
            print(f"  {child.script}: pid={child.process.pid}")
        print(
            "\nЛаунчер завершается. Сервисы продолжают работать "
            "в отдельных окнах."
        )
        return 0

    except KeyboardInterrupt:
        print(
            "\nЗапуск прерван. Уже открытые сервисы продолжают работать."
        )
        return 130
    except Exception as exc:
        print(f"\nОШИБКА run_all: {type(exc).__name__}: {exc}")
        if children:
            print("Уже запущенные сервисы не остановлены автоматически:")
            for child in children:
                status = "alive" if child.process.poll() is None else "stopped"
                print(f"  {child.script}: pid={child.process.pid}, {status}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
