#!/usr/bin/env python3
"""Apply the verified rolling-only payload to an exact clean repository base.

This script is intentionally GitHub-workflow friendly: it performs no branch,
commit or push operations. It only validates and applies a manifest-backed
payload to a repository checkout whose HEAD exactly matches expected_base_sha.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any


def run_output(command: list[str], *, cwd: Path) -> str:
    return subprocess.check_output(
        command,
        cwd=str(cwd),
        text=True,
        encoding="utf-8",
        errors="replace",
    ).strip()


def safe_child(root: Path, relative: str) -> Path:
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise RuntimeError(f"Unsafe payload path: {relative!r}")

    root_resolved = root.resolve()
    target = (root_resolved / relative_path).resolve()
    if target != root_resolved and root_resolved not in target.parents:
        raise RuntimeError(f"Payload path escapes root: {relative!r}")
    return target


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(payload_root: Path) -> dict[str, Any]:
    manifest_path = payload_root / "manifest.json"
    overlay_root = payload_root / "overlay"
    delete_list_path = payload_root / "delete_paths.txt"

    if not manifest_path.is_file():
        raise RuntimeError(f"Payload manifest is missing: {manifest_path}")
    if not overlay_root.is_dir():
        raise RuntimeError(f"Payload overlay is missing: {overlay_root}")
    if not delete_list_path.is_file():
        raise RuntimeError(f"Payload delete list is missing: {delete_list_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    overlay_files = manifest.get("overlay_files")
    delete_paths = manifest.get("delete_paths")
    expected_base_sha = manifest.get("expected_base_sha")

    if not isinstance(overlay_files, list) or not overlay_files:
        raise RuntimeError("Manifest overlay_files must be a non-empty list")
    if not isinstance(delete_paths, list) or not delete_paths:
        raise RuntimeError("Manifest delete_paths must be a non-empty list")
    if not isinstance(expected_base_sha, str) or len(expected_base_sha) != 40:
        raise RuntimeError("Manifest expected_base_sha is invalid")

    delete_file_rows = [
        line.strip()
        for line in delete_list_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if delete_file_rows != [str(value) for value in delete_paths]:
        raise RuntimeError("delete_paths.txt does not match manifest delete_paths")

    seen: set[str] = set()
    for row in overlay_files:
        if not isinstance(row, dict):
            raise RuntimeError(f"Invalid overlay manifest row: {row!r}")
        relative = str(row.get("path", ""))
        expected_hash = str(row.get("sha256", ""))
        expected_size = int(row.get("size", -1))
        if not relative or relative in seen:
            raise RuntimeError(f"Duplicate or empty overlay path: {relative!r}")
        seen.add(relative)

        source = safe_child(overlay_root, relative)
        if not source.is_file():
            raise RuntimeError(f"Overlay file is missing: {relative}")
        if source.stat().st_size != expected_size:
            raise RuntimeError(
                f"Overlay size mismatch: {relative}; "
                f"expected={expected_size}, actual={source.stat().st_size}"
            )
        actual_hash = sha256(source)
        if actual_hash != expected_hash:
            raise RuntimeError(
                f"Overlay checksum mismatch: {relative}; "
                f"expected={expected_hash}, actual={actual_hash}"
            )

    return manifest


def require_exact_clean_base(repo: Path, manifest: dict[str, Any]) -> None:
    if not (repo / ".git").exists():
        raise RuntimeError(f"Not a Git checkout: {repo}")

    expected = str(manifest["expected_base_sha"])
    actual = run_output(["git", "rev-parse", "HEAD"], cwd=repo)
    if actual != expected:
        raise RuntimeError(
            f"Repository HEAD mismatch: expected={expected}, actual={actual}"
        )

    status = run_output(["git", "status", "--porcelain=v1"], cwd=repo)
    if status:
        raise RuntimeError("Repository is not clean before apply:\n" + status)


def parent_directories(repo: Path, relative_paths: list[str]) -> list[Path]:
    root = repo.resolve()
    result: set[Path] = set()
    for relative in relative_paths:
        parent = safe_child(root, relative).parent
        while parent != root and root in parent.parents:
            result.add(parent)
            parent = parent.parent
    return sorted(result, key=lambda path: len(path.parts), reverse=True)


def apply_payload(
    repo: Path,
    payload_root: Path,
    manifest: dict[str, Any],
) -> None:
    overlay_root = payload_root / "overlay"
    delete_paths = [str(value) for value in manifest["delete_paths"]]

    # Exact-base enforcement means every declared obsolete path should exist.
    for relative in delete_paths:
        target = safe_child(repo, relative)
        if not target.exists() and not target.is_symlink():
            raise RuntimeError(f"Declared obsolete path is missing: {relative}")
        if target.is_symlink() or target.is_file():
            target.unlink()
        elif target.is_dir():
            shutil.rmtree(target)
        else:
            raise RuntimeError(f"Unsupported obsolete path type: {relative}")

    # Remove empty package/directories left after deleting their last file.
    for directory in parent_directories(repo, delete_paths):
        try:
            directory.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            # Non-empty directories are expected, for example docs/ and scripts/.
            pass

    for row in manifest["overlay_files"]:
        relative = str(row["path"])
        source = safe_child(overlay_root, relative)
        target = safe_child(repo, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    for relative in delete_paths:
        target = safe_child(repo, relative)
        if target.exists() or target.is_symlink():
            raise RuntimeError(f"Obsolete path still exists after apply: {relative}")

    for row in manifest["overlay_files"]:
        relative = str(row["path"])
        target = safe_child(repo, relative)
        if not target.is_file():
            raise RuntimeError(f"Applied overlay file is missing: {relative}")
        if target.stat().st_size != int(row["size"]):
            raise RuntimeError(f"Applied overlay size mismatch: {relative}")
        if sha256(target) != str(row["sha256"]):
            raise RuntimeError(f"Applied overlay checksum mismatch: {relative}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--payload", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    payload_root = args.payload.resolve()

    manifest = load_manifest(payload_root)
    require_exact_clean_base(repo, manifest)
    apply_payload(repo, payload_root, manifest)

    print(
        "Rolling refactor payload applied: "
        f"overlay={len(manifest['overlay_files'])}, "
        f"deleted={len(manifest['delete_paths'])}, "
        f"base={manifest['expected_base_sha']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
