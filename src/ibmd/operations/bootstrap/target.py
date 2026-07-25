from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ibmd.catalog import CatalogError, load_catalog_bundle
from ibmd.foundation.atomic_json import (
    atomic_write_json,
    canonical_json_text,
    read_json_object,
)
from ibmd.foundation.time import utc_now_text
from ibmd.operations.migrations.runner import (
    SQLiteMigrationRunner,
    load_migration_manifest,
)

_STORE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_COMPONENT_LEDGER = "execution_target_schema_components"
_COMPONENT_KEYS = {
    "component_name",
    "component_version",
    "required_execution_schema_version",
    "statements",
}
_FORBIDDEN_SQL_PREFIXES = {
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "END",
}


class TargetBootstrapError(RuntimeError):
    pass


def _safe_relative_path(value: object, *, field_name: str) -> Path:
    text = str(value or "").strip()
    if not text:
        raise TargetBootstrapError(f"{field_name} is required")
    path = Path(text)
    if path.is_absolute() or ".." in path.parts:
        raise TargetBootstrapError(
            f"{field_name} must be a safe relative path: {text!r}"
        )
    return path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_hash(value: object) -> str:
    return hashlib.sha256(
        canonical_json_text(value).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class BootstrapStoreSpecV1:
    store_name: str
    relative_database_path: Path
    migration_manifest: Path
    component_manifests: tuple[Path, ...]

    def __post_init__(self) -> None:
        name = str(self.store_name or "").strip()
        if not _STORE_RE.fullmatch(name):
            raise TargetBootstrapError(f"invalid bootstrap store_name: {name!r}")
        object.__setattr__(self, "store_name", name)
        database = _safe_relative_path(
            self.relative_database_path,
            field_name="relative_database_path",
        )
        if database.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            raise TargetBootstrapError(
                "bootstrap database path must use a SQLite suffix: "
                f"{database}"
            )
        object.__setattr__(self, "relative_database_path", database)
        migration = _safe_relative_path(
            self.migration_manifest,
            field_name="migration_manifest",
        )
        object.__setattr__(self, "migration_manifest", migration)
        components = tuple(
            _safe_relative_path(item, field_name="component_manifest")
            for item in self.component_manifests
        )
        if len(components) != len(set(components)):
            raise TargetBootstrapError(
                f"duplicate component manifests for store {name}: {components}"
            )
        if components and name != "execution":
            raise TargetBootstrapError(
                "only execution currently supports bootstrap schema components"
            )
        object.__setattr__(self, "component_manifests", components)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BootstrapStoreSpecV1":
        expected = {
            "store_name",
            "relative_database_path",
            "migration_manifest",
            "component_manifests",
        }
        if set(value) != expected:
            raise TargetBootstrapError(
                "bootstrap store fields mismatch: "
                f"missing={sorted(expected - set(value))}, "
                f"unknown={sorted(set(value) - expected)}"
            )
        components = value["component_manifests"]
        if not isinstance(components, list):
            raise TargetBootstrapError("component_manifests must be a list")
        return cls(
            store_name=str(value["store_name"]),
            relative_database_path=Path(str(value["relative_database_path"])),
            migration_manifest=Path(str(value["migration_manifest"])),
            component_manifests=tuple(Path(str(item)) for item in components),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "store_name": self.store_name,
            "relative_database_path": str(self.relative_database_path).replace(
                "\\", "/"
            ),
            "migration_manifest": str(self.migration_manifest).replace(
                "\\", "/"
            ),
            "component_manifests": [
                str(item).replace("\\", "/")
                for item in self.component_manifests
            ],
        }


@dataclass(frozen=True)
class TargetBootstrapManifestV1:
    bootstrap_name: str
    bootstrap_version: int
    catalog_root: Path
    stores: tuple[BootstrapStoreSpecV1, ...]

    def __post_init__(self) -> None:
        name = str(self.bootstrap_name or "").strip()
        if not name:
            raise TargetBootstrapError("bootstrap_name is required")
        object.__setattr__(self, "bootstrap_name", name)
        version = int(self.bootstrap_version)
        if version <= 0:
            raise TargetBootstrapError("bootstrap_version must be positive")
        object.__setattr__(self, "bootstrap_version", version)
        object.__setattr__(
            self,
            "catalog_root",
            _safe_relative_path(self.catalog_root, field_name="catalog_root"),
        )
        stores = tuple(self.stores)
        if not stores:
            raise TargetBootstrapError("target bootstrap must define stores")
        names = [item.store_name for item in stores]
        paths = [item.relative_database_path for item in stores]
        if len(names) != len(set(names)):
            raise TargetBootstrapError(f"duplicate bootstrap store names: {names}")
        if len(paths) != len(set(paths)):
            raise TargetBootstrapError(f"duplicate bootstrap database paths: {paths}")
        expected = {
            "market_data",
            "position_feed",
            "signal",
            "decision",
            "execution",
        }
        if set(names) != expected:
            raise TargetBootstrapError(
                "target bootstrap store set mismatch: "
                f"missing={sorted(expected - set(names))}, "
                f"unknown={sorted(set(names) - expected)}"
            )
        object.__setattr__(self, "stores", stores)

    @property
    def content_hash(self) -> str:
        return _json_hash(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TargetBootstrapManifestV1":
        expected = {
            "bootstrap_name",
            "bootstrap_version",
            "catalog_root",
            "stores",
        }
        if set(value) != expected:
            raise TargetBootstrapError(
                "bootstrap manifest fields mismatch: "
                f"missing={sorted(expected - set(value))}, "
                f"unknown={sorted(set(value) - expected)}"
            )
        raw_stores = value["stores"]
        if not isinstance(raw_stores, list):
            raise TargetBootstrapError("bootstrap stores must be a list")
        return cls(
            bootstrap_name=str(value["bootstrap_name"]),
            bootstrap_version=int(value["bootstrap_version"]),
            catalog_root=Path(str(value["catalog_root"])),
            stores=tuple(
                BootstrapStoreSpecV1.from_dict(item) for item in raw_stores
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bootstrap_name": self.bootstrap_name,
            "bootstrap_version": self.bootstrap_version,
            "catalog_root": str(self.catalog_root).replace("\\", "/"),
            "stores": [item.to_dict() for item in self.stores],
        }


@dataclass(frozen=True)
class ExecutionComponentManifestV1:
    component_name: str
    component_version: int
    required_execution_schema_version: int
    statements: tuple[str, ...]
    checksum: str


@dataclass(frozen=True)
class BootstrapStoreResultV1:
    store_name: str
    database_path: str
    migration_versions: tuple[int, ...]
    component_names: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "store_name": self.store_name,
            "database_path": self.database_path,
            "migration_versions": list(self.migration_versions),
            "component_names": list(self.component_names),
        }


@dataclass(frozen=True)
class TargetBootstrapResultV1:
    bootstrap_name: str
    bootstrap_version: int
    bootstrap_hash: str
    target_root: str
    catalog_bundle_hash: str
    application_version: str
    created_at_utc: str
    stores: tuple[BootstrapStoreResultV1, ...]
    artifact_hashes: Mapping[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "bootstrap_name": self.bootstrap_name,
            "bootstrap_version": self.bootstrap_version,
            "bootstrap_hash": self.bootstrap_hash,
            "target_root": self.target_root,
            "catalog_bundle_hash": self.catalog_bundle_hash,
            "application_version": self.application_version,
            "created_at_utc": self.created_at_utc,
            "stores": [item.to_dict() for item in self.stores],
            "artifact_hashes": dict(sorted(self.artifact_hashes.items())),
            "legacy_code_compatibility_required": False,
            "legacy_trade_state_database_compatibility_required": False,
            "historical_price_import_included": False,
        }


def load_target_bootstrap_manifest(
    path: str | Path,
) -> TargetBootstrapManifestV1:
    source = Path(path)
    try:
        value = read_json_object(source)
    except Exception as exc:
        raise TargetBootstrapError(
            f"cannot read target bootstrap manifest {source}: {exc}"
        ) from exc
    return TargetBootstrapManifestV1.from_dict(value)


def _load_execution_component(
    path: Path,
) -> ExecutionComponentManifestV1:
    try:
        value = read_json_object(path)
    except Exception as exc:
        raise TargetBootstrapError(
            f"cannot read execution component manifest {path}: {exc}"
        ) from exc
    if set(value) != _COMPONENT_KEYS:
        raise TargetBootstrapError(
            "execution component manifest fields mismatch: "
            f"path={path}, missing={sorted(_COMPONENT_KEYS - set(value))}, "
            f"unknown={sorted(set(value) - _COMPONENT_KEYS)}"
        )
    name = str(value["component_name"] or "").strip()
    version = int(value["component_version"])
    required = int(value["required_execution_schema_version"])
    statements = value["statements"]
    if not name or version <= 0 or required <= 0:
        raise TargetBootstrapError(
            f"invalid execution component identity/version: {path}"
        )
    if not isinstance(statements, list) or not statements:
        raise TargetBootstrapError(
            f"execution component statements must be a non-empty list: {path}"
        )
    normalized = []
    for statement in statements:
        sql = str(statement or "").strip()
        if not sql:
            raise TargetBootstrapError(
                f"execution component contains empty SQL: {path}"
            )
        prefix = sql.split(None, 1)[0].upper()
        if prefix in _FORBIDDEN_SQL_PREFIXES:
            raise TargetBootstrapError(
                "execution component cannot control transactions: "
                f"path={path}, statement={sql!r}"
            )
        normalized.append(sql)
    checksum = _json_hash(value)
    return ExecutionComponentManifestV1(
        component_name=name,
        component_version=version,
        required_execution_schema_version=required,
        statements=tuple(normalized),
        checksum=checksum,
    )


class TargetDeploymentBootstrapper:
    def __init__(
        self,
        *,
        source_root: str | Path,
        bootstrap_manifest_path: str | Path,
        target_root: str | Path,
        application_version: str,
        require_production_sessions: bool = False,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.source_root = Path(source_root).resolve()
        manifest_path = Path(bootstrap_manifest_path)
        self.bootstrap_manifest_path = (
            manifest_path.resolve()
            if manifest_path.is_absolute()
            else (self.source_root / manifest_path).resolve()
        )
        self.target_root = Path(target_root).resolve()
        self.application_version = str(application_version or "").strip()
        if not self.application_version:
            raise TargetBootstrapError("application_version is required")
        self.require_production_sessions = bool(require_production_sessions)
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 0:
            raise TargetBootstrapError("busy_timeout_ms must be non-negative")
        if not self.source_root.is_dir():
            raise TargetBootstrapError(
                f"bootstrap source root does not exist: {self.source_root}"
            )
        self.manifest = load_target_bootstrap_manifest(
            self.bootstrap_manifest_path
        )

    def _source_path(self, relative: Path) -> Path:
        path = (self.source_root / relative).resolve()
        try:
            path.relative_to(self.source_root)
        except ValueError as exc:
            raise TargetBootstrapError(
                f"bootstrap source path escapes repository root: {relative}"
            ) from exc
        if not path.is_file() and not path.is_dir():
            raise TargetBootstrapError(
                f"bootstrap source artifact does not exist: {path}"
            )
        return path

    def _artifact_hashes(self) -> dict[str, str]:
        values = {
            str(self.bootstrap_manifest_path.relative_to(self.source_root)).replace(
                "\\", "/"
            ): _sha256_file(self.bootstrap_manifest_path)
        }
        for store in self.manifest.stores:
            for relative in (
                store.migration_manifest,
                *store.component_manifests,
            ):
                path = self._source_path(relative)
                values[str(relative).replace("\\", "/")] = _sha256_file(path)
        catalog_root = self._source_path(self.manifest.catalog_root)
        for path in sorted(catalog_root.glob("*.json")):
            relative = path.relative_to(self.source_root)
            values[str(relative).replace("\\", "/")] = _sha256_file(path)
        return values

    def _load_catalog_bundle(self, root: Path):
        try:
            return load_catalog_bundle(
                root,
                require_production_sessions=self.require_production_sessions,
            )
        except CatalogError as exc:
            raise TargetBootstrapError(
                f"target bootstrap catalog is invalid: {exc}"
            ) from exc

    def plan(self) -> dict[str, Any]:
        catalog_root = self._source_path(self.manifest.catalog_root)
        bundle = self._load_catalog_bundle(catalog_root)
        artifact_hashes = self._artifact_hashes()
        return {
            "bootstrap_name": self.manifest.bootstrap_name,
            "bootstrap_version": self.manifest.bootstrap_version,
            "bootstrap_hash": self.manifest.content_hash,
            "source_root": str(self.source_root),
            "target_root": str(self.target_root),
            "application_version": self.application_version,
            "catalog_bundle_hash": bundle.bundle_hash,
            "require_production_sessions": self.require_production_sessions,
            "stores": [
                {
                    "store_name": item.store_name,
                    "database_path": str(
                        self.target_root / item.relative_database_path
                    ),
                    "migration_manifest": str(item.migration_manifest),
                    "component_manifests": [
                        str(value) for value in item.component_manifests
                    ],
                }
                for item in self.manifest.stores
            ],
            "artifact_hashes": dict(sorted(artifact_hashes.items())),
            "target_must_not_exist": True,
            "legacy_database_compatibility_required": False,
            "historical_price_import_included": False,
        }

    def _apply_component(
        self,
        *,
        database: Path,
        component: ExecutionComponentManifestV1,
        execution_schema_version: int,
    ) -> None:
        if component.required_execution_schema_version != execution_schema_version:
            raise TargetBootstrapError(
                "execution component requires another base schema version: "
                f"component={component.component_name}, "
                f"required={component.required_execution_schema_version}, "
                f"actual={execution_schema_version}"
            )
        connection = sqlite3.connect(str(database))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_COMPONENT_LEDGER} (
                    component_name TEXT PRIMARY KEY,
                    component_version INTEGER NOT NULL CHECK (component_version > 0),
                    checksum TEXT NOT NULL,
                    applied_at_utc TEXT NOT NULL,
                    application_version TEXT NOT NULL
                )
                """
            )
            existing = connection.execute(
                f"SELECT 1 FROM {_COMPONENT_LEDGER} "
                "WHERE component_name=? LIMIT 1",
                (component.component_name,),
            ).fetchone()
            if existing is not None:
                raise TargetBootstrapError(
                    "fresh target execution store already contains component: "
                    f"{component.component_name}"
                )
            for statement in component.statements:
                connection.execute(statement)
            connection.execute(
                f"INSERT INTO {_COMPONENT_LEDGER} ("
                "component_name, component_version, checksum, applied_at_utc, "
                "application_version) VALUES (?, ?, ?, ?, ?)",
                (
                    component.component_name,
                    component.component_version,
                    component.checksum,
                    utc_now_text(),
                    self.application_version,
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _validate_components(
        self,
        *,
        database: Path,
        components: tuple[ExecutionComponentManifestV1, ...],
    ) -> None:
        if not components:
            return
        uri = f"file:{database.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only = ON")
        try:
            rows = connection.execute(
                f"SELECT component_name, component_version, checksum "
                f"FROM {_COMPONENT_LEDGER} ORDER BY component_name"
            ).fetchall()
            actual = {
                str(row["component_name"]): (
                    int(row["component_version"]),
                    str(row["checksum"]),
                )
                for row in rows
            }
            expected = {
                item.component_name: (item.component_version, item.checksum)
                for item in components
            }
            if actual != expected:
                raise TargetBootstrapError(
                    "execution component ledger mismatch after bootstrap: "
                    f"expected={expected}, actual={actual}"
                )
        except sqlite3.Error as exc:
            raise TargetBootstrapError(
                f"cannot validate execution component ledger: {exc}"
            ) from exc
        finally:
            connection.close()

    def _apply_store(
        self,
        *,
        staging_root: Path,
        spec: BootstrapStoreSpecV1,
    ) -> BootstrapStoreResultV1:
        database = staging_root / spec.relative_database_path
        migration_path = self._source_path(spec.migration_manifest)
        store_name, migrations = load_migration_manifest(migration_path)
        if store_name != spec.store_name:
            raise TargetBootstrapError(
                "bootstrap store name differs from migration manifest: "
                f"spec={spec.store_name}, manifest={store_name}, "
                f"path={migration_path}"
            )
        runner = SQLiteMigrationRunner(
            database_path=database,
            store_name=store_name,
            migrations=migrations,
            application_version=self.application_version,
            busy_timeout_ms=self.busy_timeout_ms,
        )
        plan = runner.apply()
        if not plan.is_current:
            raise TargetBootstrapError(
                f"store bootstrap left pending migrations: {store_name}"
            )
        components = tuple(
            _load_execution_component(self._source_path(path))
            for path in spec.component_manifests
        )
        component_names = [item.component_name for item in components]
        if len(component_names) != len(set(component_names)):
            raise TargetBootstrapError(
                f"duplicate execution component names: {component_names}"
            )
        for component in components:
            self._apply_component(
                database=database,
                component=component,
                execution_schema_version=plan.current_version,
            )
        inspected = runner.inspect()
        if not inspected.is_current:
            raise TargetBootstrapError(
                f"store is not current after bootstrap: {store_name}"
            )
        self._validate_components(database=database, components=components)
        return BootstrapStoreResultV1(
            store_name=store_name,
            database_path=str(database),
            migration_versions=tuple(
                item.version for item in inspected.applied
            ),
            component_names=tuple(component_names),
        )

    def _copy_catalog(self, staging_root: Path) -> str:
        source = self._source_path(self.manifest.catalog_root)
        target = staging_root / "catalog"
        shutil.copytree(source, target)
        return self._load_catalog_bundle(target).bundle_hash

    def apply(self) -> TargetBootstrapResultV1:
        if self.target_root.exists():
            raise TargetBootstrapError(
                "target bootstrap root already exists; refusing destructive reuse: "
                f"{self.target_root}"
            )
        plan = self.plan()
        parent = self.target_root.parent
        parent.mkdir(parents=True, exist_ok=True)
        staging = Path(
            tempfile.mkdtemp(
                prefix=f".{self.target_root.name}.bootstrap-",
                dir=str(parent),
            )
        )
        try:
            catalog_hash = self._copy_catalog(staging)
            stores = tuple(
                self._apply_store(staging_root=staging, spec=spec)
                for spec in self.manifest.stores
            )
            created = utc_now_text()
            artifact_hashes = dict(plan["artifact_hashes"])
            staged_result = TargetBootstrapResultV1(
                bootstrap_name=self.manifest.bootstrap_name,
                bootstrap_version=self.manifest.bootstrap_version,
                bootstrap_hash=self.manifest.content_hash,
                target_root=str(self.target_root),
                catalog_bundle_hash=catalog_hash,
                application_version=self.application_version,
                created_at_utc=created,
                stores=tuple(
                    BootstrapStoreResultV1(
                        store_name=item.store_name,
                        database_path=str(
                            self.target_root
                            / Path(item.database_path).relative_to(staging)
                        ),
                        migration_versions=item.migration_versions,
                        component_names=item.component_names,
                    )
                    for item in stores
                ),
                artifact_hashes=artifact_hashes,
            )
            atomic_write_json(
                staging / "runtime" / "bootstrap.json",
                staged_result.to_dict(),
            )
            os.replace(staging, self.target_root)
            return staged_result
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    def validate_target(self) -> TargetBootstrapResultV1:
        if not self.target_root.is_dir():
            raise TargetBootstrapError(
                f"target bootstrap root does not exist: {self.target_root}"
            )
        metadata_path = self.target_root / "runtime" / "bootstrap.json"
        metadata = read_json_object(metadata_path)
        current_plan = self.plan()
        if metadata.get("bootstrap_hash") != self.manifest.content_hash:
            raise TargetBootstrapError(
                "target bootstrap manifest hash differs from current application"
            )
        if metadata.get("artifact_hashes") != current_plan["artifact_hashes"]:
            raise TargetBootstrapError(
                "target bootstrap source artifact hashes differ from current application"
            )
        catalog_hash = self._load_catalog_bundle(
            self.target_root / "catalog"
        ).bundle_hash
        if metadata.get("catalog_bundle_hash") != catalog_hash:
            raise TargetBootstrapError(
                "target bootstrap catalog hash differs from copied catalog"
            )
        store_results = []
        for spec in self.manifest.stores:
            database = self.target_root / spec.relative_database_path
            store_name, migrations = load_migration_manifest(
                self._source_path(spec.migration_manifest)
            )
            runner = SQLiteMigrationRunner(
                database_path=database,
                store_name=store_name,
                migrations=migrations,
                application_version=self.application_version,
                busy_timeout_ms=self.busy_timeout_ms,
            )
            migration_plan = runner.inspect()
            if not migration_plan.is_current:
                raise TargetBootstrapError(
                    f"target store has pending migrations: {store_name}"
                )
            components = tuple(
                _load_execution_component(self._source_path(path))
                for path in spec.component_manifests
            )
            self._validate_components(
                database=database,
                components=components,
            )
            store_results.append(
                BootstrapStoreResultV1(
                    store_name=store_name,
                    database_path=str(database),
                    migration_versions=tuple(
                        item.version for item in migration_plan.applied
                    ),
                    component_names=tuple(
                        item.component_name for item in components
                    ),
                )
            )
        return TargetBootstrapResultV1(
            bootstrap_name=str(metadata["bootstrap_name"]),
            bootstrap_version=int(metadata["bootstrap_version"]),
            bootstrap_hash=str(metadata["bootstrap_hash"]),
            target_root=str(self.target_root),
            catalog_bundle_hash=catalog_hash,
            application_version=str(metadata["application_version"]),
            created_at_utc=str(metadata["created_at_utc"]),
            stores=tuple(store_results),
            artifact_hashes=dict(current_plan["artifact_hashes"]),
        )
