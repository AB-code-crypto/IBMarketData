import sqlite3
from pathlib import Path

SQLITE_SYNCHRONOUS_MODES = {"OFF", "NORMAL", "FULL", "EXTRA"}


def open_sqlite_connection(
        db_path,
        *,
        create_parent_dir=False,
        require_existing_file=False,
        use_wal=True,
        synchronous="NORMAL",
        foreign_keys=False,
):
    db_path_obj = Path(db_path)
    if create_parent_dir:
        db_path_obj.parent.mkdir(parents=True, exist_ok=True)
    if require_existing_file and not db_path_obj.is_file():
        raise FileNotFoundError(f"Файл SQLite БД не найден: {db_path_obj}")

    mode = str(synchronous).upper()
    if mode not in SQLITE_SYNCHRONOUS_MODES:
        raise ValueError(
            f"Unsupported SQLite synchronous mode: {synchronous!r}; "
            f"allowed={sorted(SQLITE_SYNCHRONOUS_MODES)}"
        )

    conn = sqlite3.connect(str(db_path_obj))
    if use_wal:
        conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(f"PRAGMA synchronous={mode};")
    conn.execute("PRAGMA foreign_keys=ON;" if foreign_keys else "PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn
