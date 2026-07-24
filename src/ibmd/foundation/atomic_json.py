from __future__ import annotations

import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping


class JsonDataError(ValueError):
    pass


def _reject_non_finite(value: Any, *, path: str = "$") -> None:
    if isinstance(value, float):
        if not math.isfinite(value):
            raise JsonDataError(f"non-finite number at {path}: {value!r}")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise JsonDataError(f"JSON object key at {path} must be str: {key!r}")
            _reject_non_finite(item, path=f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_non_finite(item, path=f"{path}[{index}]")


def canonical_json_text(value: Any) -> str:
    _reject_non_finite(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise JsonDataError(f"value is not JSON serializable: {exc}") from exc


def pretty_json_text(value: Any) -> str:
    _reject_non_finite(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ) + "\n"
    except (TypeError, ValueError) as exc:
        raise JsonDataError(f"value is not JSON serializable: {exc}") from exc


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        fd = os.open(str(path), flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_json(
    path: str | Path,
    value: Any,
    *,
    file_mode: int = 0o600,
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = pretty_json_text(value).encode("utf-8")

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    temporary = Path(temporary_name)
    try:
        try:
            os.chmod(temporary, int(file_mode))
        except OSError:
            pass
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return target


def read_json_object(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise JsonDataError(f"cannot read JSON object from {source}: {exc}") from exc
    if not isinstance(value, dict):
        raise JsonDataError(f"JSON root must be an object: {source}")
    _reject_non_finite(value)
    return value
