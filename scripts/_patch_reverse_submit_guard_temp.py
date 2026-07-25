from __future__ import annotations

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
    "src/ibmd/execution/application/paper_submit.py",
    "from ibmd.public_contracts.decision import StrategyCommandRequestV1\n",
    '''from ibmd.public_contracts.decision import (\n    StrategyCommandKind,\n    StrategyCommandRequestV1,\n)\n''',
)

replace_once(
    "src/ibmd/execution/application/paper_submit.py",
    '''class BrokerSnapshotSource(Protocol):\n    async def read_snapshot(\n        self,\n        *,\n        account_id: str,\n    ) -> BrokerReconciliationSnapshotV1: ...\n\n\n@dataclass(frozen=True)\nclass PaperSubmitPolicy:\n''',
    '''class BrokerSnapshotSource(Protocol):\n    async def read_snapshot(\n        self,\n        *,\n        account_id: str,\n    ) -> BrokerReconciliationSnapshotV1: ...\n\n\nclass ReverseSubmitGuard(Protocol):\n    def require_ready(\n        self,\n        *,\n        command: ExecutionCommandStateV1,\n        position: StrategyPositionV1,\n    ) -> None: ...\n\n\n@dataclass(frozen=True)\nclass PaperSubmitPolicy:\n''',
)

replace_once(
    "src/ibmd/execution/application/paper_submit.py",
    '''        reconciliation_repository: BrokerReconciliationRepository,\n        order_gateway: PaperOrderGateway,\n        broker_snapshot_source: BrokerSnapshotSource,\n        clock: Callable[[], datetime] = utc_now,\n''',
    '''        reconciliation_repository: BrokerReconciliationRepository,\n        order_gateway: PaperOrderGateway,\n        broker_snapshot_source: BrokerSnapshotSource,\n        reverse_submit_guard: ReverseSubmitGuard | None = None,\n        clock: Callable[[], datetime] = utc_now,\n''',
)

replace_once(
    "src/ibmd/execution/application/paper_submit.py",
    '''        self.order_gateway = order_gateway\n        self.broker_snapshot_source = broker_snapshot_source\n        self.clock = clock\n''',
    '''        self.order_gateway = order_gateway\n        self.broker_snapshot_source = broker_snapshot_source\n        self.reverse_submit_guard = reverse_submit_guard\n        self.clock = clock\n''',
)

replace_once(
    "src/ibmd/execution/application/paper_submit.py",
    '''def _validate_current_state(\n    *,\n    position: StrategyPositionV1,\n    readiness: ExecutionReadinessV1,\n    daily_risk: DailyRiskStateV1,\n    policy: PaperSubmitPolicy,\n    observed_at_utc: str,\n) -> None:\n''',
    '''def _validate_current_state(\n    *,\n    position: StrategyPositionV1,\n    readiness: ExecutionReadinessV1,\n    daily_risk: DailyRiskStateV1,\n    policy: PaperSubmitPolicy,\n    observed_at_utc: str,\n    command_kind: StrategyCommandKind,\n    reverse_handoff_ready: bool,\n) -> None:\n''',
)

replace_once(
    "src/ibmd/execution/application/paper_submit.py",
    '''    if (\n        readiness.status != ExecutionReadinessStatus.READY\n        or not readiness.command_intake_enabled\n        or not readiness.broker_actions_enabled\n        or not readiness.reconciliation_complete\n        or not readiness.clock_healthy\n        or readiness.blocking_reasons\n    ):\n        raise PaperSubmitError(\n            "paper submission requires READY execution with broker actions enabled"\n        )\n''',
    '''    if command_kind == StrategyCommandKind.REVERSE:\n        if not reverse_handoff_ready:\n            raise PaperSubmitError(\n                "REVERSE submission requires completed protective handoff"\n            )\n        unrelated_reasons = tuple(\n            item\n            for item in readiness.blocking_reasons\n            if not item.startswith("protection:")\n            and not item.startswith("reverse_handoff:")\n        )\n        if (\n            not readiness.broker_actions_enabled\n            or not readiness.reconciliation_complete\n            or not readiness.clock_healthy\n            or unrelated_reasons\n        ):\n            raise PaperSubmitError(\n                "REVERSE submission has non-handoff execution blockers: "\n                f"{unrelated_reasons}"\n            )\n    elif (\n        readiness.status != ExecutionReadinessStatus.READY\n        or not readiness.command_intake_enabled\n        or not readiness.broker_actions_enabled\n        or not readiness.reconciliation_complete\n        or not readiness.clock_healthy\n        or readiness.blocking_reasons\n    ):\n        raise PaperSubmitError(\n            "paper submission requires READY execution with broker actions enabled"\n        )\n''',
)

replace_once(
    "src/ibmd/execution/application/paper_submit.py",
    '''        position, readiness, daily_risk = self._load_current_state()\n        _validate_current_state(\n            position=position,\n            readiness=readiness,\n            daily_risk=daily_risk,\n            policy=self.policy,\n            observed_at_utc=observed_at,\n        )\n''',
    '''        position, readiness, daily_risk = self._load_current_state()\n        reverse_handoff_ready = False\n        if command_state.command_kind == StrategyCommandKind.REVERSE:\n            if self.reverse_submit_guard is None:\n                raise PaperSubmitError(\n                    "REVERSE submission has no protective handoff guard"\n                )\n            self.reverse_submit_guard.require_ready(\n                command=command_state,\n                position=position,\n            )\n            reverse_handoff_ready = True\n        _validate_current_state(\n            position=position,\n            readiness=readiness,\n            daily_risk=daily_risk,\n            policy=self.policy,\n            observed_at_utc=observed_at,\n            command_kind=command_state.command_kind,\n            reverse_handoff_ready=reverse_handoff_ready,\n        )\n''',
)

# Append a persisted guard to the reverse-handoff application module.
path = Path("src/ibmd/execution/application/reverse_handoff.py")
text = path.read_text(encoding="utf-8")
marker = "\n\ndef paper_reverse_handoff_payload(\n"
if text.count(marker) != 1:
    raise SystemExit("reverse handoff payload marker did not match once")
addition = '''\n\nclass PersistedReverseSubmitGuard:\n    def __init__(\n        self,\n        *,\n        protection_state_source: ProtectionStateSource,\n        liquidation_state_source: LiquidationStateSource,\n    ) -> None:\n        self.protection_state_source = protection_state_source\n        self.liquidation_state_source = liquidation_state_source\n\n    def require_ready(\n        self,\n        *,\n        command: ExecutionCommandStateV1,\n        position: StrategyPositionV1,\n    ) -> None:\n        if position.position_episode_id is None:\n            raise PaperReverseHandoffError(\n                "REVERSE position has no position_episode_id"\n            )\n        episode = self.protection_state_source.read_episode(\n            position.position_episode_id\n        )\n        protection = self.protection_state_source.read_protection_by_episode(\n            position.position_episode_id\n        )\n        if episode is None or protection is None:\n            raise PaperReverseHandoffError(\n                "REVERSE source episode/protection state is missing"\n            )\n        if self.liquidation_state_source.read_snapshot_by_episode(\n            position.position_episode_id\n        ) is not None:\n            raise PaperReverseHandoffError(\n                "REVERSE is forbidden because liquidation already owns the episode"\n            )\n        try:\n            from ibmd.execution.domain.reverse_handoff import (\n                require_reverse_ready_for_submit,\n            )\n\n            require_reverse_ready_for_submit(\n                command=command,\n                position=position,\n                episode=episode,\n                protection=protection,\n            )\n        except ReverseHandoffError as exc:\n            raise PaperReverseHandoffError(str(exc)) from exc\n'''
path.write_text(text.replace(marker, addition + marker, 1), encoding="utf-8")

# Wire the guard into the submit entrypoint.
replace_once(
    "apps/run_execution_submit_v2.py",
    '''    SQLiteExecutionStateReader,\n    SQLiteExecutionStore,\n)\n''',
    '''    SQLiteExecutionStateReader,\n    SQLiteExecutionStore,\n    SQLiteProtectionReader,\n)\n''',
)
replace_once(
    "apps/run_execution_submit_v2.py",
    '''from ibmd.execution.application.new_risk_window import (\n''',
    '''from ibmd.execution.adapters.sqlite_liquidation import (\n    LiquidationSchemaError,\n    LiquidationStoreError,\n    SQLiteLiquidationStore,\n)\nfrom ibmd.execution.application.new_risk_window import (\n''',
)
replace_once(
    "apps/run_execution_submit_v2.py",
    '''from ibmd.execution.domain import (\n''',
    '''from ibmd.execution.application.reverse_handoff import (\n    PaperReverseHandoffError,\n    PersistedReverseSubmitGuard,\n)\nfrom ibmd.execution.domain import (\n''',
)
replace_once(
    "apps/run_execution_submit_v2.py",
    '''    reconciliation_store = SQLiteBrokerReconciliationStore(execution_database)\n    try:\n''',
    '''    reconciliation_store = SQLiteBrokerReconciliationStore(execution_database)\n    protection_reader = SQLiteProtectionReader(execution_database)\n    liquidation_store = SQLiteLiquidationStore(execution_database)\n    try:\n''',
)
replace_once(
    "apps/run_execution_submit_v2.py",
    '''        attempt_store.validate_schema()\n        reconciliation_store.validate_schema()\n''',
    '''        attempt_store.validate_schema()\n        reconciliation_store.validate_schema()\n        protection_reader.validate_schema()\n        liquidation_store.validate_schema()\n''',
)
replace_once(
    "apps/run_execution_submit_v2.py",
    '''        broker_snapshot_source=reconciliation_source,\n    )\n''',
    '''        broker_snapshot_source=reconciliation_source,\n        reverse_submit_guard=PersistedReverseSubmitGuard(\n            protection_state_source=protection_reader,\n            liquidation_state_source=liquidation_store,\n        ),\n    )\n''',
)
replace_once(
    "apps/run_execution_submit_v2.py",
    '''        NewRiskWindowError,\n        PaperSubmitError,\n''',
    '''        LiquidationSchemaError,\n        LiquidationStoreError,\n        NewRiskWindowError,\n        PaperReverseHandoffError,\n        PaperSubmitError,\n''',
)
