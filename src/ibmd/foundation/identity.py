from __future__ import annotations

import re
import uuid

_KIND_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_VALUE_RE = re.compile(r"^(?P<kind>[a-z][a-z0-9_]{0,31})_(?P<body>[0-9a-f]{32})$")


class IdentityError(ValueError):
    pass


def _validate_kind(kind: str) -> str:
    value = str(kind or "").strip()
    if not _KIND_RE.fullmatch(value):
        raise IdentityError(
            "identity kind must match [a-z][a-z0-9_]{0,31}: "
            f"{kind!r}"
        )
    return value


def new_id(kind: str) -> str:
    return f"{_validate_kind(kind)}_{uuid.uuid4().hex}"


def validate_id(value: str, *, expected_kind: str | None = None) -> str:
    text = str(value or "").strip()
    match = _VALUE_RE.fullmatch(text)
    if match is None:
        raise IdentityError(f"invalid stable identity: {value!r}")
    if expected_kind is not None:
        kind = _validate_kind(expected_kind)
        if match.group("kind") != kind:
            raise IdentityError(
                f"identity kind mismatch: expected={kind!r}, actual={match.group('kind')!r}"
            )
    return text
