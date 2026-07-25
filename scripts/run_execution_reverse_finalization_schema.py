from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import utc_now_text

_COMPONENT_LEDGER = "execution_target_schema_components"
_EXPECTED_KEYS = {
    "component_name",
    "component_version",
    "required_execution_schema_version",
    "statements",
}


class ReverseFinalizationSchemaError(RuntimeError):
    pass


def _load_manifest(path: Path) -> tuple[str, int, int, tuple[str, ...], str]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReverseFinalizationSchemaError(
            f"cannot read execution component manifest {path}: {exc}"
        ) from exc
    if not isinstance(value, dict) or set(value) != _EXPECTED_KEYS:
        actual = set(value) if isinstance(value, dict) else set()
        raise ReverseFinalizationSchemaError(
            "execution component manifest fields mismatch: "
            f"missing={sorted(_EXPECTED_KEYS - actual)}, "
            f"unknown={sorted(actual - _EXPECTED_KEYS)}"
        )
    name = str(value["component_name"] or "").strip()
    version = int(value["component_version"])
    required = int(value["required_execution_schema_version"])
    statements = value["statements"]
    if not name or version <= 0 or required <= 0:
        raise ReverseFinalizationSchemaError(
            "component_name and positive component/schema versions are required"
        )
    if not isinstance(statements, list) or not statements or not all(
        isinstance(item, str) and item.strip() for item in statements
    ):
        raise ReverseFinalizationSchemaError(
            "component statements must be non-empty SQL strings"
        )
    checksum = hashlib.sha256(
        canonical_json_text(value).encode("utf-8")
    ).hexdigest()
    return name, version, required, tuple(statements), checksum


def _base_versions(connection: sqlite3.Connection) -> list[int]:
    try:
        rows = connection.execute(
            "SELECT version FROM schema_migrations "
            "WHERE store_name='execution' ORDER BY version"
        ).fetchall()
    except sqlite3.Error as exc:
        raise ReverseFinalizationSchemaError(
            "execution base migration ledger is missing or unreadable"
        ) from exc
    return [int(row[0]) for row in rows]


def _ledger_row(
    connection: sqlite3.Connection,
    component_name: str,
) -> sqlite3.Row | None:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (_COMPONENT_LEDGER,),
    ).fetchone()
    if exists is None:
        return None
    return connection.execute(
        f"SELECT component_version, checksum, applied_at_utc, application_version "
        f"FROM {_COMPONENT_LEDGER} WHERE component_name=? LIMIT 1",
        (component_name,),
    ).fetchone()


def _payload(
    *,
    database: Path,
    component_name: str,
    component_version: int,
    checksum: str,
    applied: bool,
    current: bool,
    application_version: str | None,
    applied_at_utc: str | None,
) -> dict[str, Any]:
    return {
        "database_path": str(database),
        "component_name": component_name,
        "component_version": component_version,
        "checksum": checksum,
        "is_current": current,
        "applied": applied,
        "application_version": application_version,
        "applied_at_utc": applied_at_utc,
        "legacy_database_compatibility_required": False,
        "fresh_target_store_supported": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or explicitly install the execution reverse-finalization "
            "schema component. The default is read-only."
        )
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=ROOT / "migrations" / "execution.reverse_finalization.v1.json",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--application-version", required=True)
    parser.add_argument("--apply", action="store_true")
    arguments = parser.parse_args(argv)

    manifest = arguments.manifest.resolve()
    database = arguments.database.resolve()
    application_version = str(arguments.application_version or "").strip()
    if not application_version:
        raise ReverseFinalizationSchemaError("application_version is required")
    name, version, required, statements, checksum = _load_manifest(manifest)
    if not database.is_file():
        raise ReverseFinalizationSchemaError(
            f"execution database does not exist: {database}; apply base schema first"
        )

    connection = sqlite3.connect(str(database))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    try:
        expected_versions = list(range(1, required + 1))
        actual_versions = _base_versions(connection)
        if actual_versions != expected_versions:
            raise ReverseFinalizationSchemaError(
                "execution base schema mismatch: "
                f"expected={expected_versions}, actual={actual_versions}"
            )
        existing = _ledger_row(connection, name)
        if existing is not None:
            stored_version = int(existing["component_version"])
            stored_checksum = str(existing["checksum"])
            if stored_version != version or stored_checksum != checksum:
                raise ReverseFinalizationSchemaError(
                    "reverse finalization component changed in place; create a fresh "
                    "target execution database instead of migrating development data"
                )
            print(
                json.dumps(
                    _payload(
                        database=database,
                        component_name=name,
                        component_version=version,
                        checksum=checksum,
                        applied=False,
                        current=True,
                        application_version=str(existing["application_version"]),
                        applied_at_utc=str(existing["applied_at_utc"]),
                    ),
                    sort_keys=True,
                    indent=2,
                )
            )
            return 0
        if not arguments.apply:
            print(
                json.dumps(
                    _payload(
                        database=database,
                        component_name=name,
                        component_version=version,
                        checksum=checksum,
                        applied=False,
                        current=False,
                        application_version=None,
                        applied_at_utc=None,
                    ),
                    sort_keys=True,
                    indent=2,
                )
            )
            return 0
        connection.execute("BEGIN IMMEDIATE")
        for statement in statements:
            connection.execute(statement)
        applied_at = utc_now_text()
        connection.execute(
            f"INSERT INTO {_COMPONENT_LEDGER} ("
            "component_name, component_version, checksum, applied_at_utc, "
            "application_version) VALUES (?, ?, ?, ?, ?)",
            (name, version, checksum, applied_at, application_version),
        )
        connection.commit()
        print(
            json.dumps(
                _payload(
                    database=database,
                    component_name=name,
                    component_version=version,
                    checksum=checksum,
                    applied=True,
                    current=True,
                    application_version=application_version,
                    applied_at_utc=applied_at,
                ),
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ReverseFinalizationSchemaError as exc:
        print(
            f"execution reverse finalization schema failed: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
