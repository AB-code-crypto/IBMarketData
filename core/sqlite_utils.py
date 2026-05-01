import sqlite3
from pathlib import Path


def open_sqlite_connection(
        db_path,
        *,
        create_parent_dir=False,
        require_existing_file=False,
        use_wal=True,
):
    """Открывает SQLite-соединение с едиными настройками проекта."""
    db_path_obj = Path(db_path)

    if create_parent_dir:
        db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    if require_existing_file and not db_path_obj.is_file():
        raise FileNotFoundError(
            f"Файл SQLite БД не найден: {db_path_obj}"
        )

    conn = sqlite3.connect(str(db_path_obj))

    if use_wal:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

    conn.execute("PRAGMA busy_timeout=5000;")
    return conn
