from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(f"expected one replacement in {path}: {old[:120]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "tester/target_execution_runtime_tester.py",
    'T0 = "2026-07-27T10:00:00Z"',
    'T0 = "2026-07-25T10:00:00Z"',
)

replace_once(
    "src/ibmd/execution/application/runtime.py",
    '''        return ExecutionRuntimeTickV1(
            tick_id=self._tick_id(started),
            started_at_utc=started,
            finished_at_utc=format_utc(utc_now()),
''',
    '''        finished_value = utc_now()
        if finished_value < parse_utc(started):
            finished_value = parse_utc(started)
        return ExecutionRuntimeTickV1(
            tick_id=self._tick_id(started),
            started_at_utc=started,
            finished_at_utc=format_utc(finished_value),
''',
)

replace_once(
    "apps/run_execution_runtime_v2.py",
    '''        execution_state_source=execution_state,
        protection_repository=protection_reader,
    )
    # SQLiteProtectionReader is read-only; use the lifecycle store for plan writes.
    open_finalizer.protection_repository = lifecycle_store
''',
    '''        execution_state_source=execution_state,
        protection_repository=lifecycle_store,
    )
''',
)
