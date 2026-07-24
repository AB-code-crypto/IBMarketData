from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from ib_async.ib import StartupFetchNONE

from ibmd.execution.application.paper_submit import (
    PaperOrderSubmitCoordinator,
    PaperSubmitError,
    PaperSubmitPolicy,
    require_paper_submit_gate,
)
from ibmd.execution.domain import (
    BrokerOperationSnapshot,
    RegisteredFuturesContractV1,
    mark_attempt_submitting,
    plan_broker_operation,
)
from ibmd.foundation.identity import new_id
from ibmd.ib_gateway.fake_paper_orders import ScriptedPaperOrderGateway
from ibmd.ib_gateway.ib_async_paper_orders import (
    IBAsyncPaperOrderGateway,
    IBPaperOrderConnectionSettings,
)
from ibmd.ib_gateway.paper_orders import (
    BrokerOrderSubmitError,
    PaperOrderRoute,
)
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerOperationState,
    BrokerOrderSide,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerOrderFactV1,
    BrokerOrderSource,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
    StrategyCommandRequestV1,
)
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)

ACCOUNT = "DU1234567"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "shadow-mnq-account1"
INSTRUMENT = "MNQ"
POLICY_HASH = "a" * 64
CON_ID = 793_356_225
LOCAL_SYMBOL = "MNQU6"
ORDER_ID = 501
T0 = "2026-07-24T10:00:00Z"
T1 = "2026-07-24T10:00:01Z"
T2 = "2026-07-24T10:00:02Z"
T9 = "2026-07-24T10:00:30Z"
FIXED_CLOCK = lambda: datetime(2026, 7, 24, 10, 0, 1, tzinfo=timezone.utc)


def command_request(*, expires_at: str = T9) -> StrategyCommandRequestV1:
    return StrategyCommandRequestV1(
        command_id=new_id("strategy_command"),
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        source_signal_id=new_id("signal_event"),
        desired_target_side=DesiredTargetSide.LONG,
        desired_target_quantity=1,
        command_kind=StrategyCommandKind.OPEN,
        reason="test paper command",
        created_at_utc=T0,
        expires_at_utc=expires_at,
        policy_hash=POLICY_HASH,
    )


def command_state(request: StrategyCommandRequestV1) -> ExecutionCommandStateV1:
    return ExecutionCommandStateV1(
        command_id=request.command_id,
        strategy_id=request.strategy_id,
        strategy_version=request.strategy_version,
        deployment_id=request.deployment_id,
        instrument_id=request.instrument_id,
        command_kind=request.command_kind,
        desired_target_side=request.desired_target_side,
        desired_target_quantity=request.desired_target_quantity,
        state=ExecutionCommandState.ADMITTED,
        requested_qty=1,
        filled_qty=0,
        remaining_qty=1,
        latest_attempt_id=None,
        blocking_reason=None,
        received_at_utc=T0,
        updated_at_utc=T0,
        terminal_at_utc=None,
    )


def flat_position() -> StrategyPositionV1:
    return StrategyPositionV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_episode_id=None,
        side=StrategyPositionSide.FLAT,
        quantity=0,
        contracts=(),
        projection_status=StrategyPositionStatus.FLAT,
        broker_snapshot_id=new_id("position_snapshot"),
        updated_at_utc=T0,
        source_freshness_seconds=1.0,
    )


def readiness(*, broker_actions: bool = True) -> ExecutionReadinessV1:
    return ExecutionReadinessV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=ExecutionReadinessStatus.READY,
        command_intake_enabled=True,
        broker_actions_enabled=broker_actions,
        reconciliation_complete=True,
        clock_healthy=True,
        blocking_reasons=(),
        updated_at_utc=T0,
    )


def daily_risk() -> DailyRiskStateV1:
    return DailyRiskStateV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        trading_day="2026-07-24",
        status=DailyRiskStatus.MONITORING,
        realized_pnl=10.0,
        unrealized_pnl=2.0,
        total_pnl=12.0,
        target_pnl=500.0,
        pnl_ready=True,
        cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
        updated_at_utc=T0,
    )


def active_contract() -> RegisteredFuturesContractV1:
    return RegisteredFuturesContractV1(
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        contract_is_active=True,
    )


def route() -> PaperOrderRoute:
    return PaperOrderRoute(
        instrument_id=INSTRUMENT,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        last_trade_date="20260918",
        sec_type="FUT",
        exchange="CME",
        currency="USD",
        trading_class="MNQ",
        multiplier=2.0,
    )


def policy(**overrides) -> PaperSubmitPolicy:
    values = {
        "account_id": ACCOUNT,
        "environment": "paper",
        "confirmed_paper_account_id": ACCOUNT,
        "strategy_id": STRATEGY,
        "strategy_version": 1,
        "deployment_id": DEPLOYMENT,
        "instrument_id": INSTRUMENT,
        "policy_hash": POLICY_HASH,
        "daily_risk_timezone": "Europe/Moscow",
        "active_contract": active_contract(),
        "order_route": route(),
        "reconciliation_read_attempts": 1,
        "reconciliation_poll_seconds": 0.0,
    }
    values.update(overrides)
    return PaperSubmitPolicy(**values)


class FixedCommandStateSource:
    def __init__(self, value: ExecutionCommandStateV1) -> None:
        self.value = value

    def read_command_state(self, command_id: str):
        return self.value if command_id == self.value.command_id else None


class FixedCommandRequestSource:
    def __init__(self, value: StrategyCommandRequestV1) -> None:
        self.value = value

    def read_command(self, command_id: str):
        return self.value if command_id == self.value.command_id else None


class FixedExecutionStateSource:
    def __init__(
        self,
        *,
        position: StrategyPositionV1 | None = None,
        readiness_value: ExecutionReadinessV1 | None = None,
        risk: DailyRiskStateV1 | None = None,
    ) -> None:
        self.position = position or flat_position()
        self.readiness_value = readiness_value or readiness()
        self.risk = risk or daily_risk()

    def read_position(self, **kwargs):
        return self.position

    def read_readiness(self, **kwargs):
        return self.readiness_value

    def read_latest_daily_risk(self, **kwargs):
        return self.risk


class MemoryAttemptRepository:
    def __init__(self) -> None:
        self.snapshot: BrokerOperationSnapshot | None = None
        self.history: list[BrokerOperationSnapshot] = []

    def read_operation_by_command(self, command_id: str):
        if self.snapshot is None:
            return None
        return (
            self.snapshot.operation
            if self.snapshot.operation.command_id == command_id
            else None
        )

    def read_snapshot(self, operation_id: str):
        if self.snapshot is None:
            return None
        return (
            self.snapshot
            if self.snapshot.operation.operation_id == operation_id
            else None
        )

    def read_unresolved(self):
        if self.snapshot is None:
            return ()
        if self.snapshot.operation.state in {
            BrokerOperationState.SUCCEEDED,
            BrokerOperationState.FAILED_OPERATOR_REQUIRED,
        }:
            return ()
        return (self.snapshot,)

    def publish_initial(self, snapshot):
        if self.snapshot is not None:
            raise AssertionError("initial operation already exists")
        self.snapshot = snapshot
        self.history.append(snapshot)
        return snapshot

    def publish_state(self, snapshot, *, observation=None):
        self.snapshot = snapshot
        self.history.append(snapshot)
        return snapshot


class MemoryReconciliationRepository:
    def __init__(self, attempts: MemoryAttemptRepository) -> None:
        self.attempts = attempts
        self.results = []

    def publish(self, *, current, updated, result):
        if self.attempts.snapshot != current:
            raise AssertionError("reconciliation current state is not persisted")
        self.results.append(result)
        return self.attempts.publish_state(updated)


class DynamicBrokerSnapshotSource:
    def __init__(self, attempts: MemoryAttemptRepository, *, found: bool) -> None:
        self.attempts = attempts
        self.found = found
        self.reads = 0

    async def read_snapshot(self, *, account_id: str):
        self.reads += 1
        current = self.attempts.snapshot
        if current is None:
            raise AssertionError("broker snapshot read before operation persistence")
        completed = ()
        if self.found:
            completed = (
                BrokerOrderFactV1(
                    account_id=ACCOUNT,
                    order_ref=current.attempt.order_ref,
                    broker_order_id=current.attempt.broker_order_id,
                    broker_perm_id=9001,
                    client_id=320,
                    con_id=CON_ID,
                    local_symbol=LOCAL_SYMBOL,
                    side=BrokerOrderSide.BUY,
                    order_type="MARKET",
                    requested_qty=1,
                    filled_qty=1,
                    remaining_qty=0,
                    status="Filled",
                    source=BrokerOrderSource.COMPLETED,
                    observed_at_utc=T2,
                    completed_status="Filled",
                    warning_text=None,
                ),
            )
        return BrokerReconciliationSnapshotV1(
            source_session_id=new_id("ib_session"),
            account_id=account_id,
            captured_at_utc=T2,
            open_orders=(),
            completed_orders=completed,
            fills=(),
            requests_complete=True,
        )


class FakeClient:
    def getReqId(self):
        return ORDER_ID


class FakeIB:
    def __init__(self) -> None:
        self.client = FakeClient()
        self.connected = False
        self.connect_kwargs = None
        self.place_calls = []

    async def connectAsync(self, **kwargs):
        self.connect_kwargs = kwargs
        self.connected = True
        return self

    def isConnected(self):
        return self.connected

    def disconnect(self):
        self.connected = False

    def managedAccounts(self):
        return [ACCOUNT]

    def placeOrder(self, contract, order):
        self.place_calls.append((contract, order))
        return SimpleNamespace(order=order)


class PaperSubmitGateAndAdapterTest(unittest.TestCase):
    def test_gate_requires_paper_environment_exact_confirmation_and_d_account(self):
        require_paper_submit_gate(policy())
        with self.assertRaisesRegex(PaperSubmitError, "IBMD_ENVIRONMENT=paper"):
            require_paper_submit_gate(policy(environment="live"))
        with self.assertRaisesRegex(PaperSubmitError, "confirmation"):
            require_paper_submit_gate(
                policy(confirmed_paper_account_id="DU9999999")
            )
        with self.assertRaisesRegex(PaperSubmitError, "does not look"):
            require_paper_submit_gate(
                policy(
                    account_id="U1234567",
                    confirmed_paper_account_id="U1234567",
                )
            )

    def test_ib_adapter_allocates_exact_id_and_places_one_market_order(self):
        fake = FakeIB()
        gateway = IBAsyncPaperOrderGateway(
            IBPaperOrderConnectionSettings(
                host="127.0.0.1",
                port=7497,
                client_id=320,
                account_id=ACCOUNT,
            ),
            ib_factory=lambda: fake,
            clock=FIXED_CLOCK,
        )
        order_id = asyncio.run(gateway.allocate_order_id(account_id=ACCOUNT))
        from ibmd.ib_gateway.paper_orders import PaperMarketOrderRequest

        request = PaperMarketOrderRequest(
            account_id=ACCOUNT,
            broker_order_id=order_id,
            order_ref="IBMD:broker_operation_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1",
            side=BrokerOrderSide.BUY,
            quantity=1,
            route=route(),
        )
        receipt = asyncio.run(gateway.submit_market_order(request))
        self.assertFalse(fake.connect_kwargs["readonly"])
        self.assertEqual(fake.connect_kwargs["fetchFields"], StartupFetchNONE)
        self.assertEqual(len(fake.place_calls), 1)
        contract, order = fake.place_calls[0]
        self.assertEqual(contract.conId, CON_ID)
        self.assertEqual(contract.localSymbol, LOCAL_SYMBOL)
        self.assertEqual(order.orderId, ORDER_ID)
        self.assertEqual(order.orderRef, request.order_ref)
        self.assertEqual(order.account, ACCOUNT)
        self.assertEqual(order.orderType, "MKT")
        self.assertEqual(receipt.broker_order_id, ORDER_ID)
        asyncio.run(gateway.close())


class PaperSubmitCoordinatorTest(unittest.TestCase):
    def _coordinator(
        self,
        *,
        request: StrategyCommandRequestV1,
        attempts: MemoryAttemptRepository,
        gateway: ScriptedPaperOrderGateway,
        found: bool,
        readiness_value: ExecutionReadinessV1 | None = None,
        policy_value: PaperSubmitPolicy | None = None,
    ):
        return PaperOrderSubmitCoordinator(
            policy=policy_value or policy(),
            command_state_source=FixedCommandStateSource(
                command_state(request)
            ),
            command_request_source=FixedCommandRequestSource(request),
            execution_state_source=FixedExecutionStateSource(
                readiness_value=readiness_value
            ),
            attempt_repository=attempts,
            reconciliation_repository=MemoryReconciliationRepository(
                attempts
            ),
            order_gateway=gateway,
            broker_snapshot_source=DynamicBrokerSnapshotSource(
                attempts,
                found=found,
            ),
            clock=FIXED_CLOCK,
        )

    def test_submitting_is_persisted_before_one_place_order_and_fill(self):
        request = command_request()
        attempts = MemoryAttemptRepository()

        def assert_boundary(order_request):
            self.assertIsNotNone(attempts.snapshot)
            self.assertEqual(
                attempts.snapshot.operation.state,
                BrokerOperationState.SUBMITTING,
            )
            self.assertEqual(
                attempts.snapshot.attempt.state,
                BrokerAttemptState.SUBMITTING,
            )
            self.assertEqual(
                attempts.snapshot.attempt.broker_order_id,
                ORDER_ID,
            )
            self.assertEqual(order_request.broker_order_id, ORDER_ID)

        gateway = ScriptedPaperOrderGateway(
            broker_order_id=ORDER_ID,
            before_submit=assert_boundary,
            clock=FIXED_CLOCK,
        )
        coordinator = self._coordinator(
            request=request,
            attempts=attempts,
            gateway=gateway,
            found=True,
        )
        first = asyncio.run(
            coordinator.run_once(command_id=request.command_id)
        )
        self.assertTrue(first.submission_performed)
        self.assertEqual(len(gateway.requests), 1)
        self.assertEqual(
            first.after.operation.state,
            BrokerOperationState.SUCCEEDED,
        )
        self.assertEqual(first.after.attempt.state, BrokerAttemptState.FILLED)
        self.assertEqual(first.after.operation.remaining_qty, 0)
        self.assertEqual(
            [item.operation.state for item in attempts.history],
            [
                BrokerOperationState.PREPARING,
                BrokerOperationState.SUBMITTING,
                BrokerOperationState.RECONCILING,
                BrokerOperationState.SUCCEEDED,
            ],
        )

        second = asyncio.run(
            coordinator.run_once(command_id=request.command_id)
        )
        self.assertFalse(second.submission_performed)
        self.assertEqual(len(gateway.requests), 1)
        self.assertEqual(second.after, first.after)

    def test_submit_exception_and_not_found_become_unknown_without_retry(self):
        request = command_request()
        attempts = MemoryAttemptRepository()
        gateway = ScriptedPaperOrderGateway(
            broker_order_id=ORDER_ID,
            submit_error=BrokerOrderSubmitError("connection lost after send"),
            clock=FIXED_CLOCK,
        )
        coordinator = self._coordinator(
            request=request,
            attempts=attempts,
            gateway=gateway,
            found=False,
        )
        first = asyncio.run(
            coordinator.run_once(command_id=request.command_id)
        )
        self.assertTrue(first.submission_performed)
        self.assertEqual(len(gateway.requests), 1)
        self.assertEqual(
            first.after.operation.state,
            BrokerOperationState.UNKNOWN_OUTCOME,
        )
        self.assertEqual(
            first.after.attempt.state,
            BrokerAttemptState.UNKNOWN_OUTCOME,
        )

        second = asyncio.run(
            coordinator.run_once(command_id=request.command_id)
        )
        self.assertFalse(second.submission_performed)
        self.assertEqual(len(gateway.requests), 1)
        self.assertEqual(
            second.after.operation.state,
            BrokerOperationState.UNKNOWN_OUTCOME,
        )

    def test_expiry_and_disabled_broker_actions_block_before_allocation(self):
        expired = command_request(expires_at=T1)
        attempts = MemoryAttemptRepository()
        gateway = ScriptedPaperOrderGateway(
            broker_order_id=ORDER_ID,
            clock=FIXED_CLOCK,
        )
        coordinator = self._coordinator(
            request=expired,
            attempts=attempts,
            gateway=gateway,
            found=True,
        )
        with self.assertRaisesRegex(PaperSubmitError, "expired"):
            asyncio.run(
                coordinator.run_once(command_id=expired.command_id)
            )
        self.assertEqual(gateway.allocated_accounts, [])

        active = command_request()
        coordinator = self._coordinator(
            request=active,
            attempts=MemoryAttemptRepository(),
            gateway=gateway,
            found=True,
            readiness_value=readiness(broker_actions=False),
        )
        with self.assertRaisesRegex(PaperSubmitError, "broker actions enabled"):
            asyncio.run(
                coordinator.run_once(command_id=active.command_id)
            )
        self.assertEqual(gateway.allocated_accounts, [])

    def test_other_unresolved_operation_blocks_a_second_command(self):
        first_request = command_request()
        first_state = command_state(first_request)
        existing = plan_broker_operation(
            command=first_state,
            position=flat_position(),
            active_contract=active_contract(),
            account_id=ACCOUNT,
            observed_at_utc=T0,
        )
        existing = mark_attempt_submitting(
            existing,
            observed_at_utc=T1,
            broker_order_id=ORDER_ID,
        )
        attempts = MemoryAttemptRepository()
        attempts.snapshot = existing

        second_request = command_request()
        gateway = ScriptedPaperOrderGateway(
            broker_order_id=ORDER_ID + 1,
            clock=FIXED_CLOCK,
        )
        coordinator = self._coordinator(
            request=second_request,
            attempts=attempts,
            gateway=gateway,
            found=True,
        )
        with self.assertRaisesRegex(PaperSubmitError, "another unresolved"):
            asyncio.run(
                coordinator.run_once(command_id=second_request.command_id)
            )
        self.assertEqual(gateway.requests, [])


if __name__ == "__main__":
    unittest.main()
