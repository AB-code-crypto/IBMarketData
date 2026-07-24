from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from ib_async.ib import StartupFetchNONE

from ibmd.execution.domain import (
    RegisteredFuturesContractV1,
    mark_attempt_submitting,
    plan_broker_operation,
)
from ibmd.execution.domain.ib_reconciliation import (
    reconcile_broker_attempt_snapshot,
)
from ibmd.foundation.identity import new_id
from ibmd.ib_gateway.ib_async_broker_reconciliation import (
    IBAsyncBrokerReconciliationReader,
    IBBrokerReconciliationConnectionSettings,
)
from ibmd.ib_gateway.broker_reconciliation_mapping import (
    fill_fact_from_ib,
    order_fact_from_ib_trade,
)
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderSide,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerCommissionFactV1,
    BrokerFillFactV1,
    BrokerOrderFactV1,
    BrokerOrderSource,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
)
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)

ACCOUNT = "U0000000"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "shadow-mnq-account1"
INSTRUMENT = "MNQ"
CON_ID = 793_356_225
LOCAL_SYMBOL = "MNQU6"
ORDER_ID = 501
PERM_ID = 9001
ORDER_REF = "IBMD:broker_operation_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1"
T0 = "2026-07-24T10:00:00Z"
T1 = "2026-07-24T10:00:01Z"
T2 = "2026-07-24T10:00:02Z"


def command_state(quantity: int = 1) -> ExecutionCommandStateV1:
    return ExecutionCommandStateV1(
        command_id=new_id("strategy_command"),
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        command_kind=StrategyCommandKind.OPEN,
        desired_target_side=DesiredTargetSide.LONG,
        desired_target_quantity=quantity,
        state=ExecutionCommandState.ADMITTED,
        requested_qty=quantity,
        filled_qty=0,
        remaining_qty=quantity,
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


def active_contract() -> RegisteredFuturesContractV1:
    return RegisteredFuturesContractV1(
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        contract_is_active=True,
    )


def submitting_snapshot(quantity: int = 1):
    planned = plan_broker_operation(
        command=command_state(quantity),
        position=flat_position(),
        active_contract=active_contract(),
        account_id=ACCOUNT,
        observed_at_utc=T0,
    )
    return mark_attempt_submitting(
        planned,
        observed_at_utc=T1,
        broker_order_id=ORDER_ID,
    )


def order_fact(
    *,
    order_ref: str = ORDER_REF,
    order_id: int = ORDER_ID,
    perm_id: int | None = PERM_ID,
    status: str = "Submitted",
    source: BrokerOrderSource = BrokerOrderSource.OPEN,
    requested: int = 1,
    filled: int = 0,
    remaining: int = 1,
    completed_status: str | None = None,
) -> BrokerOrderFactV1:
    return BrokerOrderFactV1(
        account_id=ACCOUNT,
        order_ref=order_ref,
        broker_order_id=order_id,
        broker_perm_id=perm_id,
        client_id=300,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=BrokerOrderSide.BUY,
        order_type="MARKET",
        requested_qty=requested,
        filled_qty=filled,
        remaining_qty=remaining,
        status=status,
        source=source,
        observed_at_utc=T2,
        completed_status=completed_status,
        warning_text=None,
    )


def commission(exec_id: str = "exec-1") -> BrokerCommissionFactV1:
    return BrokerCommissionFactV1(
        exec_id=exec_id,
        commission=0.85,
        currency="USD",
        realized_pnl=0.0,
        reported_at_utc=T2,
    )


def fill_fact(
    *,
    exec_id: str = "exec-1",
    order_ref: str = ORDER_REF,
    order_id: int = ORDER_ID,
    perm_id: int | None = PERM_ID,
    shares: int = 1,
    cumulative: int = 1,
    with_commission: bool = True,
) -> BrokerFillFactV1:
    return BrokerFillFactV1(
        exec_id=exec_id,
        account_id=ACCOUNT,
        order_ref=order_ref,
        broker_order_id=order_id,
        broker_perm_id=perm_id,
        client_id=300,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=BrokerOrderSide.BUY,
        shares=shares,
        price=28_650.25,
        cumulative_qty=cumulative,
        average_price=28_650.25,
        exchange="CME",
        executed_at_utc=T1,
        observed_at_utc=T2,
        commission=(commission(exec_id) if with_commission else None),
    )


def broker_snapshot(
    *,
    open_orders=(),
    completed_orders=(),
    fills=(),
) -> BrokerReconciliationSnapshotV1:
    return BrokerReconciliationSnapshotV1(
        source_session_id=new_id("ib_session"),
        account_id=ACCOUNT,
        captured_at_utc=T2,
        open_orders=tuple(open_orders),
        completed_orders=tuple(completed_orders),
        fills=tuple(fills),
        requests_complete=True,
    )


def ib_trade(
    *,
    status: str = "Submitted",
    filled: int = 0,
    remaining: int = 1,
    source_completed: bool = False,
):
    return SimpleNamespace(
        contract=SimpleNamespace(conId=CON_ID, localSymbol=LOCAL_SYMBOL),
        order=SimpleNamespace(
            account=ACCOUNT,
            orderId=ORDER_ID,
            permId=PERM_ID,
            clientId=300,
            totalQuantity=1,
            action="BUY",
            orderType="MKT",
            orderRef=ORDER_REF,
        ),
        orderStatus=SimpleNamespace(
            orderId=ORDER_ID,
            permId=PERM_ID,
            status=status,
            filled=filled,
            remaining=remaining,
        ),
        orderState=(
            SimpleNamespace(completedStatus="Filled", warningText="")
            if source_completed
            else None
        ),
    )


def ib_fill(*, with_commission: bool = True):
    return SimpleNamespace(
        contract=SimpleNamespace(conId=CON_ID, localSymbol=LOCAL_SYMBOL),
        execution=SimpleNamespace(
            execId="exec-1",
            acctNumber=ACCOUNT,
            orderRef=ORDER_REF,
            orderId=ORDER_ID,
            permId=PERM_ID,
            clientId=300,
            side="BOT",
            shares=1,
            price=28_650.25,
            cumQty=1,
            avgPrice=28_650.25,
            exchange="CME",
            time=datetime(2026, 7, 24, 10, 0, 1, tzinfo=timezone.utc),
        ),
        commissionReport=(
            SimpleNamespace(
                execId="exec-1",
                commission=0.85,
                currency="USD",
                realizedPNL=0.0,
            )
            if with_commission
            else SimpleNamespace(
                execId="",
                commission=0.0,
                currency="",
                realizedPNL=0.0,
            )
        ),
    )


class FakeIB:
    def __init__(self, *, open_trades=(), completed=(), fills=()) -> None:
        self._connected = False
        self._open = list(open_trades)
        self._completed = list(completed)
        self._fills = list(fills)
        self.connect_kwargs = None
        self.completed_api_only = None
        self.execution_filter = None

    async def connectAsync(self, **kwargs):
        self.connect_kwargs = kwargs
        self._connected = True
        return self

    def isConnected(self):
        return self._connected

    def disconnect(self):
        self._connected = False

    def managedAccounts(self):
        return [ACCOUNT]

    async def reqAllOpenOrdersAsync(self):
        return list(self._open)

    async def reqCompletedOrdersAsync(self, apiOnly):
        self.completed_api_only = apiOnly
        return list(self._completed)

    async def reqExecutionsAsync(self, execution_filter):
        self.execution_filter = execution_filter
        return list(self._fills)

    def fills(self):
        return list(self._fills)


class BrokerReconciliationContractAndAdapterTest(unittest.TestCase):
    def test_snapshot_round_trip_and_explicit_commission_completeness(self) -> None:
        value = broker_snapshot(
            open_orders=(order_fact(),),
            fills=(fill_fact(with_commission=False),),
        )
        self.assertEqual(
            BrokerReconciliationSnapshotV1.from_dict(value.to_dict()),
            value,
        )
        self.assertEqual(value.commission_complete_count, 0)
        self.assertFalse(value.fills[0].commission_complete)

    def test_ib_mapping_preserves_order_ref_execution_and_commission(self) -> None:
        order = order_fact_from_ib_trade(
            ib_trade(),
            expected_account_id=ACCOUNT,
            source=BrokerOrderSource.OPEN,
            observed_at_utc=T2,
        )
        fill = fill_fact_from_ib(
            ib_fill(),
            expected_account_id=ACCOUNT,
            observed_at_utc=T2,
        )
        self.assertIsNotNone(order)
        self.assertIsNotNone(fill)
        self.assertEqual(order.order_ref, ORDER_REF)
        self.assertEqual(order.order_type, "MARKET")
        self.assertEqual(fill.exec_id, "exec-1")
        self.assertTrue(fill.commission_complete)
        self.assertEqual(fill.commission.currency, "USD")

    def test_reader_is_read_only_and_requests_all_three_fact_sets(self) -> None:
        fake = FakeIB(
            open_trades=(ib_trade(),),
            completed=(
                ib_trade(
                    status="Filled",
                    filled=1,
                    remaining=0,
                    source_completed=True,
                ),
            ),
            fills=(ib_fill(),),
        )
        reader = IBAsyncBrokerReconciliationReader(
            IBBrokerReconciliationConnectionSettings(
                host="127.0.0.1",
                port=7497,
                client_id=300,
                account_id=ACCOUNT,
                commission_wait_seconds=0.0,
            ),
            ib_factory=lambda: fake,
            clock=lambda: datetime(
                2026, 7, 24, 10, 0, 2, tzinfo=timezone.utc
            ),
        )
        snapshot = asyncio.run(reader.read_snapshot(account_id=ACCOUNT))
        self.assertTrue(fake.connect_kwargs["readonly"])
        self.assertEqual(
            fake.connect_kwargs["fetchFields"],
            StartupFetchNONE,
        )
        self.assertFalse(fake.completed_api_only)
        self.assertEqual(fake.execution_filter.acctCode, ACCOUNT)
        self.assertEqual(len(snapshot.open_orders), 1)
        self.assertEqual(len(snapshot.completed_orders), 1)
        self.assertEqual(len(snapshot.fills), 1)
        asyncio.run(reader.close())


class BrokerAttemptNormalizationTest(unittest.TestCase):
    def test_exact_live_order_is_one_live_observation(self) -> None:
        current = submitting_snapshot()
        result = reconcile_broker_attempt_snapshot(
            broker_snapshot=broker_snapshot(
                open_orders=(
                    order_fact(order_ref=current.attempt.order_ref),
                )
            ),
            current=current,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.LIVE,
        )
        self.assertEqual(result.observation.broker_order_id, ORDER_ID)

    def test_completed_fill_and_execution_are_exact(self) -> None:
        current = submitting_snapshot()
        result = reconcile_broker_attempt_snapshot(
            broker_snapshot=broker_snapshot(
                completed_orders=(
                    order_fact(
                        order_ref=current.attempt.order_ref,
                        status="Filled",
                        source=BrokerOrderSource.COMPLETED,
                        filled=1,
                        remaining=0,
                        completed_status="Filled",
                    ),
                ),
                fills=(
                    fill_fact(order_ref=current.attempt.order_ref),
                ),
            ),
            current=current,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.FILLED,
        )
        self.assertEqual(result.observation.remaining_qty, 0)
        self.assertEqual([item.exec_id for item in result.fills], ["exec-1"])
        self.assertTrue(result.commission_complete)

    def test_same_order_in_open_and_completed_is_not_ambiguous(self) -> None:
        current = submitting_snapshot()
        reference = current.attempt.order_ref
        result = reconcile_broker_attempt_snapshot(
            broker_snapshot=broker_snapshot(
                open_orders=(order_fact(order_ref=reference),),
                completed_orders=(
                    order_fact(
                        order_ref=reference,
                        status="Filled",
                        source=BrokerOrderSource.COMPLETED,
                        filled=1,
                        remaining=0,
                        completed_status="Filled",
                    ),
                ),
            ),
            current=current,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.FILLED,
        )

    def test_two_order_identities_for_one_ref_are_ambiguous(self) -> None:
        current = submitting_snapshot()
        result = reconcile_broker_attempt_snapshot(
            broker_snapshot=broker_snapshot(
                open_orders=(
                    order_fact(order_ref=current.attempt.order_ref),
                    order_fact(
                        order_ref=current.attempt.order_ref,
                        order_id=ORDER_ID + 1,
                        perm_id=PERM_ID + 1,
                    ),
                )
            ),
            current=current,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.AMBIGUOUS,
        )

    def test_complete_snapshot_without_match_is_not_found(self) -> None:
        current = submitting_snapshot()
        result = reconcile_broker_attempt_snapshot(
            broker_snapshot=broker_snapshot(),
            current=current,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.NOT_FOUND,
        )

    def test_full_execution_quantity_can_prove_fill_without_order_row(self) -> None:
        current = submitting_snapshot()
        result = reconcile_broker_attempt_snapshot(
            broker_snapshot=broker_snapshot(
                fills=(fill_fact(order_ref=current.attempt.order_ref),)
            ),
            current=current,
        )
        self.assertEqual(
            result.observation.outcome,
            BrokerObservationOutcome.FILLED,
        )
        self.assertEqual(
            result.observation.broker_status,
            "FILLED_FROM_EXECUTIONS",
        )


if __name__ == "__main__":
    unittest.main()
