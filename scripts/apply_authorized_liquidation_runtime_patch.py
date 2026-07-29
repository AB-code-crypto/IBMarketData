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
    '''from ibmd.execution.application.liquidation_triggers import (
    LiquidationTriggerProducerPolicyV1,
    LiquidationTriggerProducerService,
)
''',
    '''from ibmd.execution.application.liquidation_triggers import (
    LiquidationTriggerProducerPolicyV1,
    LiquidationTriggerProducerService,
)
from ibmd.execution.application.paper_liquidation import (
    PaperLiquidationCoordinator,
    PaperLiquidationPolicy,
)
''',
    label="paper liquidation imports",
)
runtime = replace_once(
    runtime,
    '''from ibmd.foundation.time import parse_utc
''',
    '''from ibmd.foundation.time import parse_utc, utc_now_text
''',
    label="runtime time imports",
)
runtime = replace_once(
    runtime,
    '''from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)
from ibmd.operations.health import ServiceHealthFile
''',
    '''from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)
from ibmd.ib_gateway.ib_async_paper_cancellations import (
    IBAsyncPaperOrderCancellationGateway,
    IBPaperCancellationConnectionSettings,
)
from ibmd.ib_gateway.ib_async_paper_orders import (
    IBAsyncPaperOrderGateway,
    IBPaperOrderConnectionSettings,
)
from ibmd.ib_gateway.paper_orders import PaperOrderRoute
from ibmd.operations.health import ServiceHealthFile
from ibmd.operations.runtime_authorization import (
    RuntimeAuthorizationError,
    RuntimeAuthorizationProofV1,
)
''',
    label="runtime broker and authorization imports",
)
runtime = replace_once(
    runtime,
    '''SERVICE_NAME = "execution"
''',
    '''SERVICE_NAME = "execution"
AUTHORIZED_MUTATION_STAGES = (
    ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
)
_TERMINAL_LIQUIDATION_STATES = {
    "SUCCEEDED",
    "FAILED_OPERATOR_REQUIRED",
    "CANCELLED_AS_ALREADY_FLAT",
}
''',
    label="runtime authorization constants",
)
runtime = replace_once(
    runtime,
    '''            "Run the single-owner execution control loop in broker-safe mode. "
            "The loop performs read-only reconciliation and broker-free state "
            "transitions, while every broker-mutating stage remains disabled "
            "until the controlled paper acceptance gate is completed."
''',
    '''            "Run the single-owner execution control loop in broker-safe mode. "
            "The loop performs read-only reconciliation and broker-free state "
            "transitions. Accepted paper mutation stages are available only when "
            "a verified authorization proof is passed in-process by the authorized "
            "runtime wrapper."
''',
    label="runtime parser description",
)
runtime = replace_once(
    runtime,
    '''    parser.add_argument("--reconciliation-client-id-offset", type=int, default=100)
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
''',
    '''    parser.add_argument("--reconciliation-client-id-offset", type=int, default=100)
    parser.add_argument(
        "--liquidation-cancel-client-id-offset",
        type=int,
        default=140,
    )
    parser.add_argument(
        "--liquidation-submit-client-id-offset",
        type=int,
        default=160,
    )
    parser.add_argument(
        "--liquidation-reconciliation-read-attempts",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--liquidation-reconciliation-poll-seconds",
        type=float,
        default=1.0,
    )
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
''',
    label="liquidation runtime arguments",
)
runtime = replace_once(
    runtime,
    '''def _configuration_hash(
    *,
    settings,
    bundle,
    paths: tuple[Path, ...],
    poll_interval_seconds: float,
) -> str:
    payload = {
        "deployment_hash": settings.configuration_hash,
        "catalog_hash": bundle.bundle_hash,
        "paths": [str(item) for item in paths],
        "poll_interval_seconds": float(poll_interval_seconds),
        "broker_mutations_enabled": False,
        "runtime_stage_order": [item.value for item in EXECUTION_RUNTIME_STAGE_ORDER],
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()
''',
    '''def _configuration_hash(
    *,
    settings,
    bundle,
    paths: tuple[Path, ...],
    poll_interval_seconds: float,
    broker_mutations_enabled: bool,
    authorization_hash: str | None,
    enabled_mutation_stages: tuple[ExecutionRuntimeStage, ...],
) -> str:
    payload = {
        "deployment_hash": settings.configuration_hash,
        "catalog_hash": bundle.bundle_hash,
        "paths": [str(item) for item in paths],
        "poll_interval_seconds": float(poll_interval_seconds),
        "broker_mutations_enabled": bool(broker_mutations_enabled),
        "runtime_authorization_hash": authorization_hash,
        "enabled_mutation_stages": [
            item.value for item in enabled_mutation_stages
        ],
        "runtime_stage_order": [item.value for item in EXECUTION_RUNTIME_STAGE_ORDER],
    }
    return hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()


def _require_runtime_mutation_authorization(
    *,
    authorization_proof: RuntimeAuthorizationProofV1 | None,
    settings,
    catalog_root: Path,
    observed_at_utc: str,
) -> bool:
    if authorization_proof is None:
        return False
    if not isinstance(authorization_proof, RuntimeAuthorizationProofV1):
        raise RuntimeAuthorizationError(
            "execution runtime requires RuntimeAuthorizationProofV1"
        )
    authorization = authorization_proof.authorization
    expected = (
        "PAPER_SOAK",
        settings.environment,
        settings.ib_account_id,
        settings.deployment_id,
        settings.application_version,
        str(settings.data_root.resolve()),
    )
    actual = (
        authorization.mode.value,
        authorization.environment,
        authorization.account_id,
        authorization.deployment_id,
        authorization.application_version,
        str(Path(authorization.data_root).resolve()),
    )
    if actual != expected:
        raise RuntimeAuthorizationError(
            "in-process runtime authorization scope differs from deployment"
        )
    if authorization_proof.acceptance_gate_count != 8:
        raise RuntimeAuthorizationError(
            "authorized liquidation runtime requires all eight acceptance gates"
        )
    if Path(authorization_proof.catalog_root).resolve() != catalog_root.resolve():
        raise RuntimeAuthorizationError(
            "runtime authorization catalog root differs from runtime catalog"
        )
    observed = parse_utc(observed_at_utc)
    if observed < parse_utc(authorization.issued_at_utc):
        raise RuntimeAuthorizationError(
            "runtime authorization cannot be used before issued_at_utc"
        )
    if observed >= parse_utc(authorization.expires_at_utc):
        raise RuntimeAuthorizationError("runtime authorization has expired")
    return True


def _route_for_episode(*, bundle, instrument, episode) -> PaperOrderRoute:
    matches = [
        item
        for item in bundle.contract_calendar.contracts
        if item.con_id == episode.con_id
        and item.local_symbol == episode.local_symbol
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "liquidation episode contract is absent or ambiguous in catalog: "
            f"con_id={episode.con_id}, local_symbol={episode.local_symbol}"
        )
    contract = matches[0]
    return PaperOrderRoute(
        instrument_id=instrument.instrument_id,
        con_id=contract.con_id,
        local_symbol=contract.local_symbol,
        last_trade_date=contract.last_trade_date,
        sec_type=instrument.sec_type,
        exchange=instrument.exchange,
        currency=instrument.currency,
        trading_class=instrument.trading_class,
        multiplier=instrument.multiplier,
    )


def _liquidation_stage_result(
    run,
    *,
    observed_at_utc: str,
) -> ExecutionRuntimeStageResultV1:
    operation = run.after.operation
    detail = (
        f"action={run.action.value}, state={operation.state.value}, "
        f"mutation_error={run.mutation_error}"
    )
    if run.broker_mutation_performed:
        return ExecutionRuntimeStageResultV1.mutated(
            ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
            observed_at_utc=observed_at_utc,
            subject_id=operation.liquidation_operation_id,
            detail=detail,
        )
    if operation.state.value in _TERMINAL_LIQUIDATION_STATES:
        return ExecutionRuntimeStageResultV1.updated(
            ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
            observed_at_utc=observed_at_utc,
            subject_id=operation.liquidation_operation_id,
            detail=detail,
        )
    return ExecutionRuntimeStageResultV1.blocked(
        ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
        observed_at_utc=observed_at_utc,
        subject_id=operation.liquidation_operation_id,
        detail=detail,
    )
''',
    label="runtime authorization and liquidation helpers",
)
runtime = replace_once(
    runtime,
    '''async def run(arguments: argparse.Namespace) -> int:
    settings = load_deployment_settings()
''',
    '''async def run(
    arguments: argparse.Namespace,
    *,
    authorization_proof: RuntimeAuthorizationProofV1 | None = None,
) -> int:
    settings = load_deployment_settings()
''',
    label="runtime run signature",
)
runtime = replace_once(
    runtime,
    '''    if not strategy_policy.trading_enabled:
        raise ValueError(f"strategy trading is disabled for {instrument_id}")

    execution_database = (
''',
    '''    if not strategy_policy.trading_enabled:
        raise ValueError(f"strategy trading is disabled for {instrument_id}")
    broker_mutations_enabled = _require_runtime_mutation_authorization(
        authorization_proof=authorization_proof,
        settings=settings,
        catalog_root=arguments.catalog_root.resolve(),
        observed_at_utc=utc_now_text(),
    )
    enabled_mutation_stages = (
        AUTHORIZED_MUTATION_STAGES if broker_mutations_enabled else ()
    )

    execution_database = (
''',
    label="runtime startup authorization",
)
runtime = replace_once(
    runtime,
    '''                    "broker_mutations_enabled": False,
                    "stage_order": [
''',
    '''                    "broker_mutations_enabled": broker_mutations_enabled,
                    "continuous_broker_mutation_adapters_enabled": (
                        broker_mutations_enabled
                    ),
                    "enabled_mutation_stages": [
                        item.value for item in enabled_mutation_stages
                    ],
                    "runtime_authorization_hash": (
                        None
                        if authorization_proof is None
                        else authorization_proof.authorization.content_hash
                    ),
                    "stage_order": [
''',
    label="runtime validation payload",
)
runtime = replace_once(
    runtime,
    '''    if reconciliation_client_id < 0:
        raise ValueError("resolved reconciliation client id must be non-negative")
    timeout = float(arguments.request_timeout_seconds)
''',
    '''    if reconciliation_client_id < 0:
        raise ValueError("resolved reconciliation client id must be non-negative")
    liquidation_cancel_client_id = (
        settings.ib_client_id
        + int(arguments.liquidation_cancel_client_id_offset)
    )
    liquidation_submit_client_id = (
        settings.ib_client_id
        + int(arguments.liquidation_submit_client_id_offset)
    )
    if broker_mutations_enabled:
        client_ids = {
            reconciliation_client_id,
            liquidation_cancel_client_id,
            liquidation_submit_client_id,
        }
        if min(client_ids) < 0 or len(client_ids) != 3:
            raise ValueError(
                "authorized liquidation client IDs must be distinct and non-negative"
            )
    timeout = float(arguments.request_timeout_seconds)
''',
    label="authorized liquidation client ids",
)
runtime = replace_once(
    runtime,
    '''    def liquidation_pending(_observed_at_utc: str):
        value = runtime_reader.read_active_liquidation(**scope)
        return (
            (None, None)
            if value is None
            else (value.subject_id, value.detail)
        )

    def finalization_step(observed_at_utc: str) -> ExecutionRuntimeStageResultV1:
''',
    '''    def liquidation_pending(_observed_at_utc: str):
        value = runtime_reader.read_active_liquidation(**scope)
        return (
            (None, None)
            if value is None
            else (value.subject_id, value.detail)
        )

    async def liquidation_advance_step(
        observed_at_utc: str,
    ) -> ExecutionRuntimeStageResultV1:
        candidate = runtime_reader.read_active_liquidation(**scope)
        if candidate is None:
            return ExecutionRuntimeStageResultV1.no_action(
                ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
                observed_at_utc=observed_at_utc,
            )
        operation = liquidation_store.read_operation(candidate.subject_id)
        if operation is None:
            raise RuntimeError(
                "active liquidation operation disappeared: "
                f"{candidate.subject_id}"
            )
        episode = protection_reader.read_episode(operation.position_episode_id)
        if episode is None:
            raise RuntimeError(
                "active liquidation episode disappeared: "
                f"{operation.position_episode_id}"
            )
        route = _route_for_episode(
            bundle=bundle,
            instrument=instrument,
            episode=episode,
        )
        order_gateway = IBAsyncPaperOrderGateway(
            IBPaperOrderConnectionSettings(
                host=settings.ib_host,
                port=settings.ib_port,
                client_id=liquidation_submit_client_id,
                account_id=account_id,
                connect_timeout_seconds=float(
                    arguments.connect_timeout_seconds
                ),
            )
        )
        cancellation_gateway = IBAsyncPaperOrderCancellationGateway(
            IBPaperCancellationConnectionSettings(
                host=settings.ib_host,
                port=settings.ib_port,
                client_id=liquidation_cancel_client_id,
                account_id=account_id,
                connect_timeout_seconds=float(
                    arguments.connect_timeout_seconds
                ),
            )
        )
        coordinator = PaperLiquidationCoordinator(
            policy=PaperLiquidationPolicy(
                account_id=account_id,
                environment=settings.environment,
                confirmed_paper_account_id=(
                    authorization_proof.authorization.account_id
                ),
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                deployment_id=deployment_id,
                instrument_id=instrument.instrument_id,
                order_route=route,
                position_max_age_seconds=position_max_age,
                reconciliation_read_attempts=int(
                    arguments.liquidation_reconciliation_read_attempts
                ),
                reconciliation_poll_seconds=float(
                    arguments.liquidation_reconciliation_poll_seconds
                ),
            ),
            protection_source=protection_reader,
            execution_state_source=execution_state,
            position_snapshot_source=position_source,
            repository=liquidation_store,
            order_gateway=order_gateway,
            cancellation_gateway=cancellation_gateway,
            broker_snapshot_source=broker_reader,
        )
        try:
            result = await coordinator.run_once(
                position_episode_id=operation.position_episode_id
            )
        finally:
            await cancellation_gateway.close()
            await order_gateway.close()
        return _liquidation_stage_result(
            result,
            observed_at_utc=observed_at_utc,
        )

    def finalization_step(observed_at_utc: str) -> ExecutionRuntimeStageResultV1:
''',
    label="authorized liquidation stage callback",
)
runtime = replace_once(
    runtime,
    '''    stages = (
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.STRATEGIC_RECONCILIATION,
            strategic_reconciliation_step,
        ),
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.PROTECTIVE_RECONCILIATION,
            protective_reconciliation_step,
        ),
        DisabledMutationExecutionRuntimeStage(
            ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
            pending=liquidation_pending,
            disabled_reason=(
                "continuous liquidation broker mutations are disabled until "
                "the paper acceptance gate passes"
            ),
        ),
''',
    '''    liquidation_stage = (
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
            liquidation_advance_step,
        )
        if broker_mutations_enabled
        else DisabledMutationExecutionRuntimeStage(
            ExecutionRuntimeStage.LIQUIDATION_ADVANCE,
            pending=liquidation_pending,
            disabled_reason=(
                "continuous liquidation broker mutations require a verified "
                "in-process PAPER_SOAK authorization proof"
            ),
        )
    )
    stages = (
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.STRATEGIC_RECONCILIATION,
            strategic_reconciliation_step,
        ),
        CallableExecutionRuntimeStage(
            ExecutionRuntimeStage.PROTECTIVE_RECONCILIATION,
            protective_reconciliation_step,
        ),
        liquidation_stage,
''',
    label="authorized liquidation stage selection",
)
runtime = replace_once(
    runtime,
    '''    coordinator = ExecutionRuntimeCoordinator(
        stages=stages,
        broker_mutations_enabled=False,
    )
''',
    '''    coordinator = ExecutionRuntimeCoordinator(
        stages=stages,
        broker_mutations_enabled=broker_mutations_enabled,
    )
''',
    label="runtime mutation coordinator flag",
)
runtime = replace_once(
    runtime,
    '''        poll_interval_seconds=interval,
    )
''',
    '''        poll_interval_seconds=interval,
        broker_mutations_enabled=broker_mutations_enabled,
        authorization_hash=(
            None
            if authorization_proof is None
            else authorization_proof.authorization.content_hash
        ),
        enabled_mutation_stages=enabled_mutation_stages,
    )
''',
    label="runtime configuration hash authorization",
)
runtime = replace_once(
    runtime,
    '''    async def one_tick():
        nonlocal health
        tick = await coordinator.run_tick()
''',
    '''    async def one_tick():
        nonlocal health
        observed_at_utc = utc_now_text()
        if authorization_proof is not None:
            _require_runtime_mutation_authorization(
                authorization_proof=authorization_proof,
                settings=settings,
                catalog_root=arguments.catalog_root.resolve(),
                observed_at_utc=observed_at_utc,
            )
        tick = await coordinator.run_tick(
            observed_at_utc=observed_at_utc
        )
''',
    label="per-tick authorization expiry check",
)
runtime = replace_once(
    runtime,
    '''def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
''',
    '''def main(
    argv: list[str] | None = None,
    *,
    authorization_proof: RuntimeAuthorizationProofV1 | None = None,
) -> int:
    arguments = build_parser().parse_args(argv)
''',
    label="runtime main authorization signature",
)
runtime = replace_once(
    runtime,
    '''        return asyncio.run(run(arguments))
''',
    '''        return asyncio.run(
            run(
                arguments,
                authorization_proof=authorization_proof,
            )
        )
''',
    label="runtime main proof forwarding",
)
runtime_path.write_text(runtime, encoding="utf-8")


wrapper_path = Path("apps/run_execution_authorized_runtime_v2.py")
wrapper = wrapper_path.read_text(encoding="utf-8")
wrapper = replace_once(
    wrapper,
    '''            "Verify one immutable PAPER_SOAK runtime authorization and then invoke "
            "the canonical execution runtime in the same process. The current "
            "continuous broker-mutation adapters remain disabled."
''',
    '''            "Verify one immutable PAPER_SOAK runtime authorization and then invoke "
            "the canonical execution runtime in the same process. Only explicitly "
            "accepted mutation stages are enabled by the in-process proof."
''',
    label="authorized wrapper description",
)
wrapper = replace_once(
    wrapper,
    '''    return run_execution_runtime_v2.main(forwarded)
''',
    '''    return run_execution_runtime_v2.main(
        forwarded,
        authorization_proof=proof,
    )
''',
    label="authorized wrapper proof forwarding",
)
wrapper_path.write_text(wrapper, encoding="utf-8")


auth_path = Path("src/ibmd/operations/runtime_authorization.py")
auth = auth_path.read_text(encoding="utf-8")
auth = replace_once(
    auth,
    '''class RuntimeAuthorizationError(RuntimeError):
    pass


@dataclass(frozen=True)
''',
    '''class RuntimeAuthorizationError(RuntimeError):
    pass


ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES = (
    "LIQUIDATION_ADVANCE",
)


@dataclass(frozen=True)
''',
    label="authorized mutation stage constant",
)
auth = replace_once(
    auth,
    '''            "continuous_broker_mutations_authorized": True,
            "continuous_broker_mutation_adapters_enabled": False,
            "live_account_enablement": False,
''',
    '''            "continuous_broker_mutations_authorized": True,
            "continuous_broker_mutation_adapters_enabled": True,
            "enabled_broker_mutation_stages": list(
                ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES
            ),
            "live_account_enablement": False,
''',
    label="runtime authorization proof mutation stages",
)
auth = replace_once(
    auth,
    '''__all__ = [
    "RuntimeAuthorizationError",
''',
    '''__all__ = [
    "ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES",
    "RuntimeAuthorizationError",
''',
    label="runtime authorization exports",
)
auth_path.write_text(auth, encoding="utf-8")


supervisor_path = Path("apps/run_target_supervisor.py")
supervisor = supervisor_path.read_text(encoding="utf-8")
supervisor = replace_once(
    supervisor,
    '''from ibmd.operations.runtime_authorization import (
    RuntimeAuthorizationError,
    RuntimeAuthorizationProofV1,
    verify_runtime_start_authorization,
)
''',
    '''from ibmd.operations.runtime_authorization import (
    ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES,
    RuntimeAuthorizationError,
    RuntimeAuthorizationProofV1,
    verify_runtime_start_authorization,
)
''',
    label="supervisor mutation stage import",
)
supervisor = replace_once(
    supervisor,
    ''') -> str:
    payload = {
        "deployment_hash": deployment_hash,
''',
    ''') -> str:
    enabled_stages = (
        ()
        if authorization_proof is None
        else ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES
    )
    payload = {
        "deployment_hash": deployment_hash,
''',
    label="supervisor configuration enabled stages",
)
supervisor = replace_once(
    supervisor,
    '''        "continuous_broker_mutation_adapters_enabled": False,
        "automatic_restart_enabled": False,
''',
    '''        "continuous_broker_mutation_adapters_enabled": bool(enabled_stages),
        "enabled_broker_mutation_stages": list(enabled_stages),
        "automatic_restart_enabled": False,
''',
    label="supervisor configuration mutation flags",
)
supervisor = replace_once(
    supervisor,
    ''') -> dict:
    adapters_enabled = False
    return {
''',
    ''') -> dict:
    enabled_stages = (
        ()
        if authorization_proof is None
        else ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES
    )
    adapters_enabled = bool(enabled_stages)
    return {
''',
    label="supervisor plan mutation flags",
)
supervisor = replace_once(
    supervisor,
    '''        "continuous_broker_mutation_adapters_enabled": adapters_enabled,
        "continuous_broker_mutations_enabled": adapters_enabled,
''',
    '''        "continuous_broker_mutation_adapters_enabled": adapters_enabled,
        "enabled_broker_mutation_stages": list(enabled_stages),
        "continuous_broker_mutations_enabled": adapters_enabled,
''',
    label="supervisor plan enabled stages",
)
supervisor_path.write_text(supervisor, encoding="utf-8")


test_auth_path = Path("tester/target_runtime_authorization_tester.py")
test_auth = test_auth_path.read_text(encoding="utf-8")
test_auth = replace_once(
    test_auth,
    '''import tempfile
import unittest
from pathlib import Path

from apps.run_execution_authorized_runtime_v2 import (
''',
    '''import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from apps import run_execution_authorized_runtime_v2
from apps.run_execution_authorized_runtime_v2 import (
''',
    label="runtime authorization test imports",
)
test_auth = replace_once(
    test_auth,
    '''from apps.run_target_supervisor import _service_specs
''',
    '''from apps.run_target_supervisor import _plan_payload, _service_specs
''',
    label="supervisor plan test import",
)
test_auth = replace_once(
    test_auth,
    '''from ibmd.operations.runtime_authorization import (
    RuntimeAuthorizationError,
    verify_runtime_start_authorization,
)
''',
    '''from ibmd.operations.runtime_authorization import (
    ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES,
    RuntimeAuthorizationError,
    verify_runtime_start_authorization,
)
from ibmd.operations.supervisor import SupervisorPolicyV1
''',
    label="authorization stages test import",
)
test_auth = replace_once(
    test_auth,
    '''            self.assertFalse(
                payload["continuous_broker_mutation_adapters_enabled"]
            )
            self.assertFalse(payload["live_account_enablement"])
''',
    '''            self.assertTrue(
                payload["continuous_broker_mutation_adapters_enabled"]
            )
            self.assertEqual(
                payload["enabled_broker_mutation_stages"],
                list(ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES),
            )
            self.assertFalse(payload["live_account_enablement"])
''',
    label="authorization proof stage assertions",
)
test_auth = replace_once(
    test_auth,
    '''        self.assertEqual(
            forwarded,
            [
                "--continuous",
                "--poll-interval-seconds",
                "2",
                "--catalog-root",
                "/target/catalog",
            ],
        )

    def test_validation_only_rejects_inner_runtime_arguments(self) -> None:
''',
    '''        self.assertEqual(
            forwarded[:4],
            [
                "--continuous",
                "--poll-interval-seconds",
                "2",
                "--catalog-root",
            ],
        )
        self.assertEqual(Path(forwarded[4]), Path("/target/catalog"))

    def test_verified_proof_is_passed_in_process_to_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            arguments = build_parser().parse_args(
                [
                    "--runtime-authorization",
                    str(root / "authorization.json"),
                ]
            )
            proof = SimpleNamespace(to_dict=lambda: {"proof": True})
            deployment = SimpleNamespace(data_root=root)
            with patch.object(
                run_execution_authorized_runtime_v2,
                "load_deployment_settings",
                return_value=deployment,
            ), patch.object(
                run_execution_authorized_runtime_v2,
                "verify_runtime_start_authorization",
                return_value=proof,
            ), patch.object(
                run_execution_authorized_runtime_v2,
                "atomic_write_json",
            ), patch.object(
                run_execution_authorized_runtime_v2.run_execution_runtime_v2,
                "main",
                return_value=0,
            ) as runtime_main:
                result = run_execution_authorized_runtime_v2.run(
                    arguments,
                    ["--once"],
                )
            self.assertEqual(result, 0)
            runtime_main.assert_called_once()
            self.assertIs(
                runtime_main.call_args.kwargs["authorization_proof"],
                proof,
            )

    def test_validation_only_rejects_inner_runtime_arguments(self) -> None:
''',
    label="wrapper in-process proof test",
)
test_auth = replace_once(
    test_auth,
    '''class AuthorizedSupervisorPlanTest(unittest.TestCase):
    def test_authorized_plan_selects_wrapper_but_keeps_adapters_disabled(self) -> None:
''',
    '''class AuthorizedSupervisorPlanTest(unittest.TestCase):
    def test_authorized_plan_enables_only_liquidation_adapter(self) -> None:
''',
    label="authorized supervisor test name",
)
test_auth = replace_once(
    test_auth,
    '''            self.assertTrue(
                execution.argv[1].endswith(
                    "apps/run_execution_authorized_runtime_v2.py"
                )
            )
            self.assertIn("--runtime-authorization", execution.argv)
            self.assertIn("--allow-unqualified-session", execution.argv)
''',
    '''            self.assertEqual(
                Path(execution.argv[1]).name,
                "run_execution_authorized_runtime_v2.py",
            )
            self.assertIn("--runtime-authorization", execution.argv)
            self.assertIn("--allow-unqualified-session", execution.argv)
            plan = _plan_payload(
                settings=deployment,
                specs=specs,
                policy=SupervisorPolicyV1(),
                authorization_proof=proof,
            )
            self.assertTrue(
                plan["continuous_broker_mutation_adapters_enabled"]
            )
            self.assertTrue(plan["continuous_broker_mutations_enabled"])
            self.assertEqual(
                plan["enabled_broker_mutation_stages"],
                list(ENABLED_CONTINUOUS_BROKER_MUTATION_STAGES),
            )
''',
    label="authorized supervisor plan assertions",
)
test_auth = replace_once(
    test_auth,
    '''            self.assertTrue(
                execution.argv[1].endswith("apps/run_execution_runtime_v2.py")
            )
            self.assertNotIn("--runtime-authorization", execution.argv)
''',
    '''            self.assertEqual(
                Path(execution.argv[1]).name,
                "run_execution_runtime_v2.py",
            )
            self.assertNotIn("--runtime-authorization", execution.argv)
            plan = _plan_payload(
                settings=SimpleNamespace(
                    deployment_id="read-only",
                    environment="paper",
                    data_root=root,
                ),
                specs=specs,
                policy=SupervisorPolicyV1(),
                authorization_proof=None,
            )
            self.assertFalse(
                plan["continuous_broker_mutation_adapters_enabled"]
            )
            self.assertFalse(plan["continuous_broker_mutations_enabled"])
            self.assertEqual(plan["enabled_broker_mutation_stages"], [])
''',
    label="read-only supervisor plan assertions",
)
test_auth_path.write_text(test_auth, encoding="utf-8")


test_runtime_path = Path("tester/target_execution_runtime_tester.py")
test_runtime = test_runtime_path.read_text(encoding="utf-8")
test_runtime = replace_once(
    test_runtime,
    '''import asyncio
import unittest
from dataclasses import dataclass

from ibmd.execution.application.runtime import (
''',
    '''import asyncio
import unittest
from dataclasses import dataclass
from types import SimpleNamespace

from apps.run_execution_runtime_v2 import _liquidation_stage_result
from ibmd.execution.application.runtime import (
''',
    label="execution runtime result test imports",
)
test_runtime = replace_once(
    test_runtime,
    '''    def test_missing_or_reordered_stages_are_rejected(self) -> None:
''',
    '''    def test_authorized_liquidation_result_blocks_until_terminal(self) -> None:
        def run(*, mutated: bool, state: str, error: str | None = None):
            return SimpleNamespace(
                after=SimpleNamespace(
                    operation=SimpleNamespace(
                        liquidation_operation_id="liquidation_operation_a",
                        state=SimpleNamespace(value=state),
                    )
                ),
                action=SimpleNamespace(value="CANCEL_TAKE_PROFIT"),
                broker_mutation_performed=mutated,
                mutation_error=error,
            )

        mutated = _liquidation_stage_result(
            run(mutated=True, state="CANCELING_EXITS"),
            observed_at_utc=T0,
        )
        self.assertEqual(mutated.status, ExecutionRuntimeStageStatus.MUTATED)

        blocked = _liquidation_stage_result(
            run(mutated=False, state="CANCELING_EXITS"),
            observed_at_utc=T0,
        )
        self.assertEqual(blocked.status, ExecutionRuntimeStageStatus.BLOCKED)
        self.assertTrue(blocked.blocks_lower_priority)

        terminal = _liquidation_stage_result(
            run(mutated=False, state="SUCCEEDED"),
            observed_at_utc=T0,
        )
        self.assertEqual(terminal.status, ExecutionRuntimeStageStatus.UPDATED)

    def test_missing_or_reordered_stages_are_rejected(self) -> None:
''',
    label="authorized liquidation result tests",
)
test_runtime_path.write_text(test_runtime, encoding="utf-8")


workflow_path = Path(".github/workflows/runtime-authorization-ci.yml")
workflow = workflow_path.read_text(encoding="utf-8")
workflow = replace_once(
    workflow,
    '''          assert value['continuous_broker_mutations_authorized'] is True
          assert value['continuous_broker_mutation_adapters_enabled'] is False
          assert value['execution_runtime_started'] is False
''',
    '''          assert value['continuous_broker_mutations_authorized'] is True
          assert value['continuous_broker_mutation_adapters_enabled'] is True
          assert value['enabled_broker_mutation_stages'] == [
              'LIQUIDATION_ADVANCE'
          ]
          assert value['execution_runtime_started'] is False
''',
    label="authorization CI proof assertions",
)
workflow = replace_once(
    workflow,
    '''          assert value['runtime_authorization_verified'] is True
          assert value['continuous_broker_mutations_authorized'] is True
          assert value['continuous_broker_mutation_adapters_enabled'] is False
          execution = next(
''',
    '''          assert value['runtime_authorization_verified'] is True
          assert value['continuous_broker_mutations_authorized'] is True
          assert value['continuous_broker_mutation_adapters_enabled'] is True
          assert value['continuous_broker_mutations_enabled'] is True
          assert value['enabled_broker_mutation_stages'] == [
              'LIQUIDATION_ADVANCE'
          ]
          execution = next(
''',
    label="authorization CI supervisor assertions",
)
workflow_path.write_text(workflow, encoding="utf-8")


print(
    "Patched the authorized runtime to enable only LIQUIDATION_ADVANCE, "
    "added per-tick proof expiry checks and updated supervisor/CI evidence."
)
