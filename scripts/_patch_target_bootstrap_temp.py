from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"expected one replacement in {path}, got {count}: {old[:120]!r}"
        )
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/operations/bootstrap/target.py",
    '''from ibmd.catalog import load_catalog_bundle
''',
    '''from ibmd.catalog import CatalogError, load_catalog_bundle
''',
)

replace_once(
    "src/ibmd/operations/bootstrap/target.py",
    '''    def plan(self) -> dict[str, Any]:
        catalog_root = self._source_path(self.manifest.catalog_root)
        bundle = load_catalog_bundle(
            catalog_root,
            require_production_sessions=self.require_production_sessions,
        )
''',
    '''    def _load_catalog_bundle(self, root: Path):
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
''',
)

replace_once(
    "src/ibmd/operations/bootstrap/target.py",
    '''        bundle = load_catalog_bundle(
            target,
            require_production_sessions=self.require_production_sessions,
        )
        return bundle.bundle_hash
''',
    '''        return self._load_catalog_bundle(target).bundle_hash
''',
)

replace_once(
    "src/ibmd/operations/bootstrap/target.py",
    '''        catalog_hash = load_catalog_bundle(
            self.target_root / "catalog",
            require_production_sessions=self.require_production_sessions,
        ).bundle_hash
''',
    '''        catalog_hash = self._load_catalog_bundle(
            self.target_root / "catalog"
        ).bundle_hash
''',
)

replace_once(
    "tester/target_deployment_bootstrap_tester.py",
    '''            self.assertEqual(len(plan["artifact_hashes"]), 11)
''',
    '''            self.assertEqual(len(plan["artifact_hashes"]), 14)
''',
)
