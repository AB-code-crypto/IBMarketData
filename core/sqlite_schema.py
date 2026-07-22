from __future__ import annotations


def quote_sqlite_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = ? LIMIT 1""",
        (str(table_name),),
    ).fetchone()
    return row is not None


def _normalize_default(value) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    while result.startswith("(") and result.endswith(")"):
        result = result[1:-1].strip()
    if len(result) >= 2 and result[0] == result[-1] and result[0] in {"'", '"'}:
        result = result[1:-1]
    return result


def read_table_schema(conn, table_name: str) -> tuple[tuple, ...]:
    if not table_exists(conn, table_name):
        raise RuntimeError(f"Required SQLite table is missing: {table_name}")
    return tuple(
        (
            str(row[1]),
            str(row[2]).upper(),
            int(row[3]),
            _normalize_default(row[4]),
            int(row[5]),
        )
        for row in conn.execute(
            f"PRAGMA table_info({quote_sqlite_identifier(table_name)})"
        ).fetchall()
    )


def require_exact_table_schema(
        conn,
        *,
        table_name: str,
        expected_schema: tuple[tuple, ...],
) -> None:
    actual = read_table_schema(conn, table_name)
    expected = tuple(tuple(item) for item in expected_schema)
    if actual == expected:
        return
    actual_names = tuple(row[0] for row in actual)
    expected_names = tuple(row[0] for row in expected)
    missing = [column for column in expected_names if column not in actual_names]
    unexpected = [column for column in actual_names if column not in expected_names]
    raise RuntimeError(
        "Unsupported SQLite schema. Only the current schema is accepted: "
        f"table={table_name}; missing={missing}; unexpected={unexpected}; "
        f"actual={actual}; expected={expected}"
    )


def require_table_absent(conn, table_name: str, *, reason: str) -> None:
    if table_exists(conn, table_name):
        raise RuntimeError(
            f"Obsolete SQLite table is present: table={table_name}; {reason}"
        )

    # execution DB initialization performs one-time legacy cleanup before these
    # final schema guards.  That cleanup may execute DELETE even when no rows
    # match, which opens a SQLite writer transaction.  Async reconciliation must
    # never carry that writer lock into an await, otherwise another task in the
    # same execution process deadlocks on trade.sqlite3.  These guards are the
    # end of the initialization boundary, so commit any pending setup transaction.
    if bool(getattr(conn, "in_transaction", False)):
        conn.commit()
