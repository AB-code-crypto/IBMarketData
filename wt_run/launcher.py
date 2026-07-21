from __future__ import annotations

import os
import sqlite3
import sys
import time
from dataclasses import dataclass

from config import settings_live as settings
from core.instrument_filters import get_trading_enabled_instrument_codes
from core.price_source import get_fresh_price_bar_status
from core.state_db import STATE_DB_PATH
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
from ib_trader.trade_position_repository import POSITION_SNAPSHOT_MAX_AGE_SECONDS
from ib_trader.trade_schema import POSITIONS_LATEST_TABLE_NAME, TRADE_DB_PATH
from wt_run.common import (
    ACTIVE_STATES,
    SERVICES,
    ServiceSpec,
    get_service,
    is_pid_alive,
    launch_tab,
    new_launch_token,
    read_status,
    status_is_active,
    wait_started,
)

POLL_SECONDS = 1
STATUS_EVERY_SECONDS = 10
START_GRACE_SECONDS = 5


@dataclass(frozen=True)
class ServiceHandle:
    spec: ServiceSpec
    pid: int
    launch_token: str
    started_at_ts: int


def handle_from_status(spec: ServiceSpec, status: dict) -> ServiceHandle:
    return ServiceHandle(
        spec=spec,
        pid=int(status["pid"]),
        launch_token=str(status.get("launch_token") or ""),
        started_at_ts=int(status.get("started_at_ts") or 0),
    )


def get_failure(handle: ServiceHandle) -> str | None:
    status = read_status(handle.spec)
    if status is None:
        return "status_missing"
    if str(status.get("launch_token") or "") != handle.launch_token:
        return "status_replaced"
    status_pid = int(status.get("pid") or 0)
    if status_pid != handle.pid:
        return f"pid_changed:{status_pid}"
    state = str(status.get("state") or "")
    if state not in ACTIVE_STATES:
        return (
            f"state={state}, exit_code={status.get('exit_code')}, "
            f"error={status.get('error_text')}"
        )
    if not is_pid_alive(handle.pid):
        return "process_not_alive"
    return None


def require_alive(handles: list[ServiceHandle]) -> None:
    failed = []
    for handle in handles:
        failure = get_failure(handle)
        if failure is not None:
            failed.append((handle.spec.key, failure))
    if failed:
        raise RuntimeError(f"Сервис завершился во время запуска: {failed}")


def ensure_started(
        spec: ServiceSpec,
        handles: list[ServiceHandle],
) -> tuple[ServiceHandle, bool]:
    current = read_status(spec)
    if status_is_active(current):
        handle = handle_from_status(spec, current)
        handles.append(handle)
        print(f"[ACTIVE]  {spec.title}: pid={handle.pid}")
        return handle, False

    launch_token = new_launch_token()
    launch_tab(
        spec,
        launch_token=launch_token,
        python_executable=sys.executable,
    )
    status = wait_started(spec, launch_token=launch_token)
    handle = handle_from_status(spec, status)
    handles.append(handle)

    time.sleep(1)
    failure = get_failure(handle)
    if failure is not None:
        raise RuntimeError(f"{spec.title} завершился сразу: {failure}")
    print(f"[STARTED] {spec.title}: pid={handle.pid}")
    return handle, True


def wait_ready(label: str, handles: list[ServiceHandle], check) -> None:
    next_log = 0.0
    while True:
        require_alive(handles)
        try:
            ready, details = check()
        except Exception as exc:
            ready, details = False, f"{type(exc).__name__}: {exc}"
        if ready:
            print(f"[READY]   {label}: {details}")
            return
        now = time.monotonic()
        if now >= next_log:
            print(f"[WAIT]    {label}: {details}")
            next_log = now + STATUS_EVERY_SECONDS
        time.sleep(POLL_SECONDS)


def wait_stable(handle: ServiceHandle, handles: list[ServiceHandle]) -> None:
    deadline = time.monotonic() + START_GRACE_SECONDS
    while time.monotonic() < deadline:
        require_alive(handles)
        time.sleep(0.25)
    print(
        f"[READY]   {handle.spec.title}: "
        f"process_alive={START_GRACE_SECONDS}s"
    )


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
    waiting: list[str] = []
    for code in codes:
        row = state.get(code)
        if row is None:
            waiting.append(f"{code}=missing")
        elif row[2]:
            waiting.append(f"{code}=error:{row[2]}")
        elif int(row[1]) < int(since):
            waiting.append(f"{code}=old_state")
        elif int(row[0]) != 1:
            waiting.append(f"{code}=signal_ready=0")
    return not waiting, ", ".join(waiting) or f"signal_ready={codes}"


def price_bars_ready(codes: list[str]) -> tuple[bool, str]:
    waiting: list[str] = []
    ready: list[str] = []
    max_lag = DEFAULT_SIGNAL_CONFIG.max_price_bar_lag_seconds
    for code in codes:
        status = get_fresh_price_bar_status(code, max_lag)
        if not status.is_ready:
            waiting.append(f"{code}={status.reason}")
        else:
            ready.append(
                f"{code}=lag:{status.last_bar_lag_seconds}s/mid:{status.mid_close}"
            )
    return not waiting, ", ".join(waiting) or ", ".join(ready)


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
    waiting: list[str] = []
    ready: list[str] = []
    for code in codes:
        row = state.get(code)
        if row is None:
            waiting.append(f"{code}=missing")
            continue
        side, qty, account, updated_at_ts = row
        updated_at = int(updated_at_ts)
        age = max(0, now_ts - updated_at)
        if str(account or "") != settings.ib_account_id:
            waiting.append(f"{code}=wrong_account:{account}")
        elif since is not None and updated_at < int(since):
            waiting.append(f"{code}=old_snapshot")
        elif age > POSITION_SNAPSHOT_MAX_AGE_SECONDS:
            waiting.append(f"{code}=stale:{age}s")
        else:
            ready.append(f"{code}={str(side).upper()}/{float(qty):g}")
    return not waiting, ", ".join(waiting) or ", ".join(ready)


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
        if sample_ts >= int(since) and source_id == expected_client_id:
            return True, f"IB connected, clientId={expected_client_id}"
    if daily and daily[0] is not None and int(daily[0]) >= int(since):
        return True, "daily take-profit startup evaluation completed"
    return False, f"жду execution clientId={expected_client_id}"


def print_status() -> int:
    print("Статус сервисов IBMarketData")
    print("=" * 72)
    for spec in SERVICES:
        status = read_status(spec)
        if status_is_active(status):
            print(
                f"{spec.title:<16} RUNNING  "
                f"pid={int(status['pid']):<8} "
                f"started={status.get('started_at_ts')}"
            )
        elif status:
            print(
                f"{spec.title:<16} "
                f"{str(status.get('state') or 'UNKNOWN').upper():<8} "
                f"pid={status.get('pid')} exit={status.get('exit_code')}"
            )
        else:
            print(f"{spec.title:<16} NOT MANAGED")
    return 0


def ensure_all() -> int:
    codes = get_trading_enabled_instrument_codes()
    if not codes:
        raise RuntimeError("Нет trading_enabled инструментов")

    print("Проверяю и запускаю сервисы IBMarketData")
    print(f"account={settings.ib_account_id}")
    print(f"instruments={codes}")
    print(f"python={sys.executable}")
    print("terminal_window=IBMarketData")
    print()

    handles: list[ServiceHandle] = []

    market, _ = ensure_started(get_service("market_data"), handles)
    wait_ready(
        "market_data",
        handles,
        lambda: market_ready(codes, market.started_at_ts),
    )
    wait_ready("fresh price bars", handles, lambda: price_bars_ready(codes))

    position, _ = ensure_started(get_service("position_sync"), handles)
    wait_ready(
        "position_sync",
        handles,
        lambda: positions_ready(codes, position.started_at_ts),
    )

    execution, _ = ensure_started(get_service("execution"), handles)
    wait_ready(
        "execution",
        handles,
        lambda: execution_ready(execution.started_at_ts),
    )

    signal, signal_new = ensure_started(get_service("signal"), handles)
    if signal_new:
        wait_stable(signal, handles)
    else:
        require_alive(handles)

    wait_ready(
        "final position snapshot",
        handles,
        lambda: positions_ready(codes, None),
    )
    wait_ready("final price bars", handles, lambda: price_bars_ready(codes))

    trader, trader_new = ensure_started(get_service("trader"), handles)
    if trader_new:
        wait_stable(trader, handles)
    else:
        require_alive(handles)

    print()
    print("Все сервисы работают:")
    for handle in handles:
        print(f"  {handle.spec.title:<16} pid={handle.pid}")
    print()
    print(
        "Закрытую вкладку можно восстановить повторным запуском run_wt.py: "
        "живые сервисы будут пропущены."
    )
    return 0


def start_one(service_name: str) -> int:
    spec = get_service(service_name)
    current = read_status(spec)
    if status_is_active(current):
        print(f"{spec.title} уже работает: pid={current.get('pid')}")
        return 0

    codes = get_trading_enabled_instrument_codes()
    handles: list[ServiceHandle] = []
    handle, _ = ensure_started(spec, handles)
    if spec.key == "market_data":
        wait_ready(
            spec.key,
            handles,
            lambda: market_ready(codes, handle.started_at_ts),
        )
        wait_ready("fresh price bars", handles, lambda: price_bars_ready(codes))
    elif spec.key == "position_sync":
        wait_ready(
            spec.key,
            handles,
            lambda: positions_ready(codes, handle.started_at_ts),
        )
    elif spec.key == "execution":
        wait_ready(
            spec.key,
            handles,
            lambda: execution_ready(handle.started_at_ts),
        )
    else:
        wait_stable(handle, handles)

    print(f"{spec.title} запущен: pid={handle.pid}")
    return 0


def print_help() -> None:
    print(
        "Использование:\n"
        "  python run_wt.py             Запустить отсутствующие сервисы\n"
        "  python run_wt.py all         То же самое\n"
        "  python run_wt.py status      Показать статус\n"
        "  python run_wt.py execution   Запустить один сервис\n\n"
        "Сервисы:\n"
        "  market_data, position_sync, execution, signal, trader"
    )


def main(argv: list[str] | None = None) -> int:
    if os.name != "nt":
        raise RuntimeError("run_wt.py рассчитан на Windows")

    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0].lower() == "all":
        return ensure_all()
    command = args[0].lower()
    if command in {"status", "list"}:
        return print_status()
    if command in {"help", "-h", "--help", "/?"}:
        print_help()
        return 0
    if len(args) != 1:
        print_help()
        return 2
    return start_one(args[0])
