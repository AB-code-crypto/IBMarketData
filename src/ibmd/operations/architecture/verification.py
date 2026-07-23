from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

_LEGACY_TOP_LEVEL = {
    "config",
    "contracts",
    "core",
    "ib_execution",
    "ib_position_sync",
    "ib_signal",
    "ib_trader",
    "wt_run",
}
_SERVICE_PACKAGES = {
    "market_data",
    "position_feed",
    "signal",
    "decision",
    "execution",
}
_DOMAIN_FORBIDDEN_IMPORTS = {
    "sqlite3",
    "socket",
    "urllib",
    "http",
    "ftplib",
    "smtplib",
    "requests",
    "aiohttp",
    "ib_async",
    "ib_insync",
}


@dataclass(frozen=True)
class ImportViolation:
    rule_id: str
    path: Path
    line: int
    message: str

    def format(self, *, repo_root: Path | None = None) -> str:
        display_path = self.path
        if repo_root is not None:
            try:
                display_path = self.path.relative_to(repo_root)
            except ValueError:
                pass
        return f"{display_path}:{self.line}: {self.rule_id}: {self.message}"


def _module_name(path: Path, source_root: Path) -> str:
    relative = path.relative_to(source_root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(("ibmd", *parts))


def _source_service(module_name: str) -> str | None:
    parts = module_name.split(".")
    if len(parts) >= 2 and parts[1] in _SERVICE_PACKAGES:
        return parts[1]
    return None


def _imported_modules(node: ast.AST) -> Iterable[tuple[str, int, tuple[str, ...]]]:
    if isinstance(node, ast.Import):
        for alias in node.names:
            yield alias.name, node.lineno, (alias.name,)
    elif isinstance(node, ast.ImportFrom):
        if node.level:
            # Relative imports stay inside the package being parsed. Cross-service
            # rules are enforced on absolute target imports, which are mandatory
            # at service boundaries.
            return
        module = str(node.module or "")
        aliases = tuple(alias.name for alias in node.names)
        yield module, node.lineno, aliases


def _contains_module_scope_environment_lookup(tree: ast.Module) -> list[int]:
    lines: list[int] = []

    class Visitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            return

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            return

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            return

        def visit_Subscript(self, node: ast.Subscript) -> None:
            value = node.value
            if (
                isinstance(value, ast.Attribute)
                and isinstance(value.value, ast.Name)
                and value.value.id == "os"
                and value.attr == "environ"
            ):
                lines.append(node.lineno)
            self.generic_visit(node)

        def visit_Call(self, node: ast.Call) -> None:
            function = node.func
            if (
                isinstance(function, ast.Attribute)
                and isinstance(function.value, ast.Name)
                and function.value.id == "os"
                and function.attr == "getenv"
            ):
                lines.append(node.lineno)
            self.generic_visit(node)

    visitor = Visitor()
    for statement in tree.body:
        visitor.visit(statement)
    return lines


def verify_target_architecture(repo_root: str | Path) -> list[ImportViolation]:
    root = Path(repo_root).resolve()
    source_root = root / "src" / "ibmd"
    if not source_root.is_dir():
        return [
            ImportViolation(
                rule_id="TGT-000",
                path=source_root,
                line=1,
                message="target source root does not exist",
            )
        ]

    violations: list[ImportViolation] = []
    for path in sorted(source_root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError) as exc:
            violations.append(
                ImportViolation(
                    rule_id="TGT-001",
                    path=path,
                    line=getattr(exc, "lineno", 1) or 1,
                    message=f"cannot parse target module: {exc}",
                )
            )
            continue

        module_name = _module_name(path, source_root)
        source_service = _source_service(module_name)
        is_domain = "domain" in path.relative_to(source_root).parts
        is_supervisor = module_name.startswith("ibmd.operations.supervisor")

        for node in ast.walk(tree):
            for imported, line, aliases in _imported_modules(node):
                top_level = imported.split(".", 1)[0]
                if top_level in _LEGACY_TOP_LEVEL:
                    violations.append(
                        ImportViolation(
                            rule_id="TGT-100",
                            path=path,
                            line=line,
                            message=f"target code imports legacy package {imported!r}",
                        )
                    )
                if "settings_live" in aliases:
                    violations.append(
                        ImportViolation(
                            rule_id="TGT-101",
                            path=path,
                            line=line,
                            message="target code imports legacy settings_live singleton",
                        )
                    )

                parts = imported.split(".")
                target_service = (
                    parts[1]
                    if len(parts) >= 2
                    and parts[0] == "ibmd"
                    and parts[1] in _SERVICE_PACKAGES
                    else None
                )
                if (
                    source_service is not None
                    and target_service is not None
                    and source_service != target_service
                ):
                    violations.append(
                        ImportViolation(
                            rule_id="TGT-200",
                            path=path,
                            line=line,
                            message=(
                                "service package imports another service package: "
                                f"{source_service} -> {target_service}"
                            ),
                        )
                    )
                if is_supervisor and target_service is not None:
                    violations.append(
                        ImportViolation(
                            rule_id="TGT-201",
                            path=path,
                            line=line,
                            message=(
                                "supervisor imports a domain service package: "
                                f"{target_service}"
                            ),
                        )
                    )
                if is_domain and top_level in _DOMAIN_FORBIDDEN_IMPORTS:
                    violations.append(
                        ImportViolation(
                            rule_id="TGT-300",
                            path=path,
                            line=line,
                            message=(
                                "domain module imports storage/network/broker library: "
                                f"{imported}"
                            ),
                        )
                    )

        for line in _contains_module_scope_environment_lookup(tree):
            violations.append(
                ImportViolation(
                    rule_id="TGT-400",
                    path=path,
                    line=line,
                    message="module-scope environment lookup is forbidden in target code",
                )
            )

    return sorted(
        violations,
        key=lambda item: (str(item.path), item.line, item.rule_id, item.message),
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Verify target IBMarketData import and layering boundaries."
    )
    parser.add_argument("repo_root", nargs="?", default=".")
    arguments = parser.parse_args(argv)
    root = Path(arguments.repo_root).resolve()
    violations = verify_target_architecture(root)
    if violations:
        for violation in violations:
            print(violation.format(repo_root=root))
        print(f"target architecture verification failed: {len(violations)} violation(s)")
        return 1
    print("target architecture verification passed")
    return 0
