from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import utc_now_text

_STORE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_LEDGER_TABLE = "schema_migrations"
_FORBIDDEN_TRANSACTION_PREFIXES = (
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "END",
)


class MigrationError(RuntimeError):
    pass


class MigrationChecksumMismatch(MigrationError):
    pass


@dataclass(frozen=True)
class SqlMigration:
    version: int
    name: str
    statements: tuple[str, ...]

    def __post_init__(self) -> None:
        if int(self.version) <= 0:
            raise MigrationError(f"migration version must be positive: {self.version}")
        if not str(self.name or "").strip():
            raise MigrationError("migration name is required")
        if not self.statements:
            raise MigrationError(f"migration {self.version} has no SQL statements")
        for statement in self.statements:
            sql = str(statement or "").strip()
            if not sql:
                raise MigrationError(
                    f"migration {self.version} contains an empty SQL statement"
                )
            prefix = sql.split(None, 1)[0].upper()
            if prefix in _FORBIDDEN_TRANSACTION_PREFIXES:
                raise MigrationError(
                    "migration statements must not control transactions: "
                    f"version={self.version}, statement={sql!r}"
                )

    @property
    def checksum(self) -> str:
        payload = canonical_json_text(
            {
                "version": self.version,
                "name": self.name,
                "statements": list(self.statements),
            }
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class AppliedMigration:
    version: int
    name: str
    checksum: str
    applied_at_utc: str
    application_version: str


@dataclass(frozen=True)
class MigrationPlan:
    store_name: str
    database_path: Path
    current_version: int
    applied: tuple[AppliedMigration, ...]
    pending: tuple[SqlMigration, ...]

    @property
    def is_current(self) -> bool:
        return not self.pending

    def to_dict(self) -> dict[str, Any]:
        return {
            "store_name": self.store_name,
            "database_path": str(self.database_path),
            "current_version": self.current_version,
            "is_current": self.is_current,
            "applied_versions": [migration.version for migration in self.applied],
            "pending_versions": [migration.version for migration in self.pending],
        }


def _normalize_migrations(migrations: Iterable[SqlMigration]) -> tuple[SqlMigration, ...]:
    ordered = tuple(sorted(tuple(migrations), key=lambda item: item.version))
    versions = [migration.version for migration in ordered]
    if len(versions) != len(set(versions)):
        raise MigrationError(f"duplicate migration versions: {versions}")
    if versions and versions != list(range(1, versions[-1] + 1)):
        raise MigrationError(
            f"migration versions must be contiguous from 1: {versions}"
        )
    return ordered


def load_migration_manifest(path: str | Path) -> tuple[str, tuple[SqlMigration, ...]]:
    source = Path(path)
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MigrationError(f"cannot read migration manifest {source}: {exc}") from exc
    if not isinstance(value, dict):
        raise MigrationError("migration manifest root must be an object")
    expected_keys = {"store_name", "migrations"}
    if set(value) != expected_keys:
        raise MigrationError(
            "invalid migration manifest keys: "
            f"missing={sorted(expected_keys - set(value))}, "
            f"unknown={sorted(set(value) - expected_keys)}"
        )
    raw_migrations = value["migrations"]
    if not isinstance(raw_migrations, list):
        raise MigrationError("manifest migrations must be a list")
    migrations: list[SqlMigration] = []
    for raw in raw_migrations:
        if not isinstance(raw, Mapping):
            raise MigrationError("migration entry must be an object")
        keys = set(raw)
        required = {"version", "name", "statements"}
        if keys != required:
            raise MigrationError(
                "invalid migration entry keys: "
                f"missing={sorted(required - keys)}, unknown={sorted(keys - required)}"
            )
        statements = raw["statements"]
        if not isinstance(statements, list) or not all(
            isinstance(statement, str) for statement in statements
        ):
            raise MigrationError("migration statements must be a list of strings")
        migrations.append(
            SqlMigration(
                version=int(raw["version"]),
                name=str(raw["name"]),
                statements=tuple(statements),
            )
        )
    return str(value["store_name"]), _normalize_migrations(migrations)


class SQLiteMigrationRunner:
    def __init__(
        self,
        *,
        database_path: str | Path,
        store_name: str,
        migrations: Iterable[SqlMigration],
        application_version: str,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.database_path = Path(database_path)
        self.store_name = str(store_name or "").strip()
        if not _STORE_RE.fullmatch(self.store_name):
            raise MigrationError(f"invalid store_name: {store_name!r}")
        self.migrations = _normalize_migrations(migrations)
        self.application_version = str(application_version or "").strip()
        if not self.application_version:
            raise MigrationError("application_version is required")
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise MigrationError("busy_timeout_ms must be non-negative")

    def _connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        if read_only:
            uri = f"file:{self.database_path.resolve().as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
        else:
            connection = sqlite3.connect(str(self.database_path))
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        return connection

    @staticmethod
    def _ledger_exists(connection: sqlite3.Connection) -> bool:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (_LEDGER_TABLE,),
        ).fetchone()
        return row is not None

    def _ensure_ledger(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_LEDGER_TABLE} (
                store_name TEXT NOT NULL,
                version INTEGER NOT NULL,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at_utc TEXT NOT NULL,
                application_version TEXT NOT NULL,
                PRIMARY KEY (store_name, version)
            )
            """
        )

    def _read_applied(self, connection: sqlite3.Connection) -> tuple[AppliedMigration, ...]:
        if not self._ledger_exists(connection):
            return ()
        foreign_store = connection.execute(
            f"SELECT DISTINCT store_name FROM {_LEDGER_TABLE} WHERE store_name != ? LIMIT 1",
            (self.store_name,),
        ).fetchone()
        if foreign_store is not None:
            raise MigrationError(
                "database migration ledger belongs to another store: "
                f"expected={self.store_name!r}, found={foreign_store[0]!r}"
            )
        rows = connection.execute(
            f"""
            SELECT version, name, checksum, applied_at_utc, application_version
            FROM {_LEDGER_TABLE}
            WHERE store_name = ?
            ORDER BY version
            """,
            (self.store_name,),
        ).fetchall()
        return tuple(
            AppliedMigration(
                version=int(row[0]),
                name=str(row[1]),
                checksum=str(row[2]),
                applied_at_utc=str(row[3]),
                application_version=str(row[4]),
            )
            for row in rows
        )

    def _build_plan(
        self,
        applied: tuple[AppliedMigration, ...],
    ) -> MigrationPlan:
        registered = {migration.version: migration for migration in self.migrations}
        for recorded in applied:
            migration = registered.get(recorded.version)
            if migration is None:
                raise MigrationError(
                    "database has an applied migration missing from this application: "
                    f"version={recorded.version}"
                )
            if recorded.checksum != migration.checksum:
                raise MigrationChecksumMismatch(
                    "applied migration checksum changed: "
                    f"version={recorded.version}, name={recorded.name!r}"
                )
        applied_versions = {migration.version for migration in applied}
        pending = tuple(
            migration
            for migration in self.migrations
            if migration.version not in applied_versions
        )
        current_version = max(applied_versions, default=0)
        if pending and pending[0].version != current_version + 1:
            raise MigrationError(
                "database migration history is not a contiguous prefix: "
                f"current={current_version}, next={pending[0].version}"
            )
        return MigrationPlan(
            store_name=self.store_name,
            database_path=self.database_path,
            current_version=current_version,
            applied=applied,
            pending=pending,
        )

    def inspect(self) -> MigrationPlan:
        if not self.database_path.exists():
            return self._build_plan(())
        connection = self._connect(read_only=True)
        try:
            return self._build_plan(self._read_applied(connection))
        finally:
            connection.close()

    def apply(self) -> MigrationPlan:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect(read_only=False)
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._ensure_ledger(connection)
            connection.commit()

            plan = self._build_plan(self._read_applied(connection))
            for migration in plan.pending:
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    for statement in migration.statements:
                        connection.execute(statement)
                    connection.execute(
                        f"""
                        INSERT INTO {_LEDGER_TABLE} (
                            store_name,
                            version,
                            name,
                            checksum,
                            applied_at_utc,
                            application_version
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            self.store_name,
                            migration.version,
                            migration.name,
                            migration.checksum,
                            utc_now_text(),
                            self.application_version,
                        ),
                    )
                    connection.commit()
                except Exception as exc:
                    connection.rollback()
                    raise MigrationError(
                        "migration failed and was rolled back: "
                        f"store={self.store_name}, version={migration.version}, "
                        f"name={migration.name!r}, error={type(exc).__name__}: {exc}"
                    ) from exc
            return self._build_plan(self._read_applied(connection))
        finally:
            connection.close()
