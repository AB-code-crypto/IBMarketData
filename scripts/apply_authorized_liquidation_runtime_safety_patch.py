from pathlib import Path


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


runtime_path = Path("apps/run_execution_runtime_v2.py")
runtime = runtime_path.read_text(encoding="utf-8")
runtime = replace_once(
    runtime,
    '''_TERMINAL_LIQUIDATION_STATES = {
    "SUCCEEDED",
    "FAILED_OPERATOR_REQUIRED",
    "CANCELLED_AS_ALREADY_FLAT",
}
''',
    '''_SUCCESSFUL_LIQUIDATION_STATES = {
    "SUCCEEDED",
    "CANCELLED_AS_ALREADY_FLAT",
}
''',
    label="successful liquidation terminal states",
)
runtime = replace_once(
    runtime,
    '''    if operation.state.value in _TERMINAL_LIQUIDATION_STATES:
        return ExecutionRuntimeStageResultV1.updated(
''',
    '''    if operation.state.value in _SUCCESSFUL_LIQUIDATION_STATES:
        return ExecutionRuntimeStageResultV1.updated(
''',
    label="successful liquidation stage result",
)
runtime = replace_once(
    runtime,
    '''    async def liquidation_advance_step(
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        candidate = runtime_reader.read_active_liquidation(**scope)
''',
    '''    async def liquidation_advance_step(
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        _require_runtime_mutation_authorization(
            authorization_proof=authorization_proof,
            settings=settings,
            catalog_root=arguments.catalog_root.resolve(),
            observed_at_utc=utc_now_text(),
        )
        candidate = runtime_reader.read_active_liquidation(**scope)
''',
    label="liquidation stage authorization refresh",
)
runtime_path.write_text(runtime, encoding="utf-8")


test_runtime_path = Path("tester/target_execution_runtime_tester.py")
test_runtime = test_runtime_path.read_text(encoding="utf-8")
test_runtime = replace_once(
    test_runtime,
    '''        terminal = _liquidation_stage_result(
            run(mutated=False, state="SUCCEEDED"),
            observed_at_utc=T0,
        )
        self.assertEqual(terminal.status, ExecutionRuntimeStageStatus.UPDATED)

    def test_missing_or_reordered_stages_are_rejected(self) -> None:
''',
    '''        terminal = _liquidation_stage_result(
            run(mutated=False, state="SUCCEEDED"),
            observed_at_utc=T0,
        )
        self.assertEqual(terminal.status, ExecutionRuntimeStageStatus.UPDATED)

        operator_required = _liquidation_stage_result(
            run(mutated=False, state="FAILED_OPERATOR_REQUIRED"),
            observed_at_utc=T0,
        )
        self.assertEqual(
            operator_required.status,
            ExecutionRuntimeStageStatus.BLOCKED,
        )
        self.assertTrue(operator_required.blocks_lower_priority)

    def test_missing_or_reordered_stages_are_rejected(self) -> None:
''',
    label="operator-required liquidation stage test",
)
test_runtime_path.write_text(test_runtime, encoding="utf-8")


test_auth_path = Path("tester/target_runtime_authorization_tester.py")
test_auth = test_auth_path.read_text(encoding="utf-8")
test_auth = replace_once(
    test_auth,
    '''from apps import run_execution_authorized_runtime_v2
from apps.run_execution_authorized_runtime_v2 import (
''',
    '''from apps import run_execution_authorized_runtime_v2
from apps.run_execution_runtime_v2 import (
    _require_runtime_mutation_authorization,
)
from apps.run_execution_authorized_runtime_v2 import (
''',
    label="runtime authorization refresh test import",
)
test_auth = replace_once(
    test_auth,
    '''    def test_expired_authorization_blocks_startup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
''',
    '''    def test_expired_authorization_blocks_startup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
''',
    label="expired test anchor",
)
test_auth = replace_once(
    test_auth,
    '''                    observed_at_utc=T2,
                )


class AuthorizedWrapperTest(unittest.TestCase):
''',
    '''                    observed_at_utc=T2,
                )

    def test_in_process_runtime_proof_expires_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "target"
            (
                deployment,
                _bootstrap,
                _manifest,
                manifest_path,
                _authorization,
                authorization_path,
                _summaries,
            ) = build_target(root)
            proof = verify_runtime_start_authorization(
                settings=deployment,
                source_root=ROOT,
                authorization_path=authorization_path,
                acceptance_manifest_path=manifest_path,
                bootstrap_manifest_path=ROOT / "bootstrap" / "target.v1.json",
                catalog_root=root / "catalog",
                observed_at_utc=T1,
            )
            with self.assertRaisesRegex(
                RuntimeAuthorizationError,
                "expired",
            ):
                _require_runtime_mutation_authorization(
                    authorization_proof=proof,
                    settings=deployment,
                    catalog_root=root / "catalog",
                    observed_at_utc=T2,
                )


class AuthorizedWrapperTest(unittest.TestCase):
''',
    label="in-process proof expiry test",
)
test_auth_path.write_text(test_auth, encoding="utf-8")


workflow_path = Path(".github/workflows/runtime-authorization-ci.yml")
workflow = workflow_path.read_text(encoding="utf-8")
workflow = replace_once(
    workflow,
    '''          python -m compileall -q \\
            apps/run_execution_authorized_runtime_v2.py \\
            apps/run_target_supervisor.py \\
''',
    '''          python -m compileall -q \\
            apps/run_execution_runtime_v2.py \\
            apps/run_execution_authorized_runtime_v2.py \\
            apps/run_target_supervisor.py \\
''',
    label="authorization CI runtime compile",
)
workflow = replace_once(
    workflow,
    '''          ruff check \\
            --select E4,E7,E9,F \\
            --ignore E402 \\
            apps/run_execution_authorized_runtime_v2.py \\
''',
    '''          ruff check \\
            --select E4,E7,E9,F \\
            --ignore E402 \\
            apps/run_execution_runtime_v2.py \\
            apps/run_execution_authorized_runtime_v2.py \\
''',
    label="authorization CI runtime lint",
)
workflow_path.write_text(workflow, encoding="utf-8")


print(
    "Applied authorized liquidation safety follow-up: operator-required remains "
    "blocked, authorization is refreshed at stage entry, and CI covers runtime."
)
