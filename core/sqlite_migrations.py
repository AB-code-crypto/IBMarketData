from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Callable, Iterable


MIGRATIONS_TABLE_NAME = "schema_migrations"


@dataclass(frozen=True)
class SqliteMigration:
    version: int
    name: str
    apply: Callable[[object], None]


def initialize_migrations_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE_NAME} (
            namespace TEXT NOT NULL,
            version INTEGER NOT NULL,
            name TEXT NOT NULL,
            applied_at_ts INTEGER NOT NULL,
            PRIMARY KEY (namespace, version)
        );
        """
    )


def read_applied_migration_versions(
        conn,
        *,
        namespace: str,
) -> set[int]:
    initialize_migrations_table(conn)

    rows = conn.execute(
        f"""
        SELECT version
        FROM {MIGRATIONS_TABLE_NAME}
        WHERE namespace = ?
        """,
        (str(namespace),),
    ).fetchall()

    return {int(row[0]) for row in rows}


def _savepoint_name(namespace: str, version: int) -> str:
    safe_namespace = re.sub(
        r"[^A-Za-z0-9_]+",
        "_",
        str(namespace),
    ).strip("_")
    return f"migration_{safe_namespace or 'default'}_{int(version)}"


def apply_sqlite_migrations(
        conn,
        *,
        namespace: str,
        migrations: Iterable[SqliteMigration],
) -> list[SqliteMigration]:
    namespace_value = str(namespace).strip()
    if not namespace_value:
        raise ValueError("migration namespace must not be empty")

    ordered = sorted(
        list(migrations),
        key=lambda migration: int(migration.version),
    )

    seen_versions: set[int] = set()
    for migration in ordered:
        version = int(migration.version)
        if version <= 0:
            raise ValueError(
                f"migration version must be positive: {migration}"
            )
        if version in seen_versions:
            raise ValueError(
                f"duplicate migration version={version} "
                f"for namespace={namespace_value}"
            )
        seen_versions.add(version)

    applied_versions = read_applied_migration_versions(
        conn,
        namespace=namespace_value,
    )
    applied_now: list[SqliteMigration] = []

    for migration in ordered:
        version = int(migration.version)
        if version in applied_versions:
            continue

        savepoint = _savepoint_name(namespace_value, version)
        conn.execute(f"SAVEPOINT {savepoint}")

        try:
            migration.apply(conn)
            conn.execute(
                f"""
                INSERT INTO {MIGRATIONS_TABLE_NAME} (
                    namespace,
                    version,
                    name,
                    applied_at_ts
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    namespace_value,
                    version,
                    str(migration.name),
                    int(time.time()),
                ),
            )
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")

        except Exception:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise

        applied_versions.add(version)
        applied_now.append(migration)

    return applied_now
