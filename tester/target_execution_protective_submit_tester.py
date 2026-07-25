from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone

from ibmd.execution.application.protective_submit import (
    PaperProtectiveSubmitCoordinator,
    PaperProtectiveSubmitError,
    PaperProtectiveSubmitPolicy,
    require_paper_protective_gate,
)
from ibmd.foundation.identity import new_id
from ibmd.ib_gateway.fake_paper_orders import ScriptedPaperOrderGateway
from ibmd.ib_gateway.ib_async_paper_orders import build_paper_protective_order
from ibmd.ib_gateway.paper_orders import (
    PaperOrderRoute,
    PaperProtectiveOrderRequest,
)
from ibmd.public_contracts.broker_execution import BrokerOrderSide
from ibmd.public_contracts.broker_reconciliation import (
    BrokerOrderFactV1,
    BrokerOrderSource,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    PositionContractV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import (
    BrokerPositionRowV1,
    BrokerPositionSnapshotV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodePolicyV1,
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
    ProtectiveOrderType,
    ProtectiveOrderV1,
)

ACCOUNT = "DU000000"
STRATEGY = "IBMarketData.rolling"
DEPLOYMENT = "paper-drill-protective"
INSTRUMENT = "MNQ"
CON_ID = 793_356_225
LOCAL_SYMBOL = "MNQU6"
T0 = "2026-07-27T10:00:00Z"
T1 = "2026-07-27T10:00:01Z"
T2 = "2026-07-27T10:00:02Z"
T3 = "2026-07-27T10:00:03Z"


def clock(value: str):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return lambda: parsed


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


def episode_and_protection():
    policy = PositionEpisodePolicyV1(
        price_tick=0.25,
        stop_required=True,
        take_profit_enabled=True,
        stop_loss_points=150.0,
        take_profit_points=75.0,
        time_in_force="DAY",
        stop_outside_rth=True,
        take_profit_outside_rth=False,
        price_watchdog_enabled=True,
        stale_feed_market_close_enabled=False,
        price_stale_max_seconds=600,
    )
    episode_id = new_id("position_episode")
    set_id = new_id("protection_set")
    stop_id = new_id("protective_order")
    tp_id = new_id("protective_order")
    episode = PositionEpisodeV1(
        position_episode_id=episode_id,
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        source_command_id=new_id("strategy_command"),
        source_operation_id=new_id("broker_operation"),
        source_attempt_id=new_id("broker_attempt"),
        source_exec_ids=("entry-exec-1",),
        side=StrategyPositionSide.LONG,
        quantity=1,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        entry_average_price=28_600.0,
        broker_snapshot_id=new_id("position_snapshot"),
        opened_at_utc=T0,
        status=PositionEpisodeStatus.OPEN,
        strategy_policy_hash="a" * 64,
        protective_policy_hash=policy.content_hash,
        protective_policy=policy,
    )
    oca = f"IBMD_OCA_{set_id.rsplit('_', 1)[-1]}"
    stop = ProtectiveOrderV1(
        protective_order_id=stop_id,
        protection_set_id=set_id,
        position_episode_id=episode_id,
        kind=ProtectiveOrderKind.STOP_LOSS,
        state=ProtectiveOrderState.PLANNED,
        planned_sequence=1,
        order_ref=f"IBMD:{set_id}:SL",
        side=BrokerOrderSide.SELL,
        order_type=ProtectiveOrderType.STOP,
        quantity=1,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        stop_price=28_450.0,
        limit_price=None,
        time_in_force="DAY",
        outside_rth=True,
        oca_group=oca,
        filled_qty=0,
        remaining_qty=1,
        broker_order_id=None,
        broker_perm_id=None,
        broker_status=None,
        broker_terminal_proven=False,
        created_at_utc=T0,
        updated_at_utc=T0,
        terminal_at_utc=None,
        last_broker_proof_at_utc=None,
        failure_reason=None,
    )
    tp = ProtectiveOrderV1(
        protective_order_id=tp_id,
        protection_set_id=set_id,
        position_episode_id=episode_id,
        kind=ProtectiveOrderKind.TAKE_PROFIT,
        state=ProtectiveOrderState.PLANNED,
        planned_sequence=2,
        order_ref=f"IBMD:{set_id}:TP",
        side=BrokerOrderSide.SELL,
        order_type=ProtectiveOrderType.LIMIT,
        quantity=1,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        stop_price=None,
        limit_price=28_675.0,
        time_in_force="DAY",
        outside_rth=False,
        oca_group=oca,
        filled_qty=0,
        remaining_qty=1,
        broker_order_id=None,
        broker_perm_id=None,
        broker_status=None,
        broker_terminal_proven=False,
        created_at_utc=T0,
        updated_at_utc=T0,
        terminal_at_utc=None,
        last_broker_proof_at_utc=None,
        failure_reason=None,
    )
    protection = ProtectionStateV1(
        protection_set_id=set_id,
        position_episode_id=episode_id,
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=ProtectionSetStatus.PLANNED,
        orders=(stop, tp),
        created_at_utc=T0,
        updated_at_utc=T0,
        terminal_at_utc=None,
        blocking_reason="stop_not_submitted",
    )
    return episode, protection


def strategy_position(episode: PositionEpisodeV1) -> StrategyPositionV1:
    return StrategyPositionV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        position_episode_id=episode.position_episode_id,
        side=StrategyPositionSide.LONG,
        quantity=1,
        contracts=(
            PositionContractV1(
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                signed_quantity=1,
                contract_is_active=True,
            ),
        ),
        projection_status=StrategyPositionStatus.OPEN,
        broker_snapshot_id=new_id("position_snapshot"),
        updated_at_utc=T0,
        source_freshness_seconds=1.0,
    )


def blocked_readiness() -> ExecutionReadinessV1:
    return ExecutionReadinessV1(
        account_id=ACCOUNT,
        strategy_id=STRATEGY,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        status=ExecutionReadinessStatus.BLOCKED,
        command_intake_enabled=False,
        broker_actions_enabled=True,
        reconciliation_complete=True,
        clock_healthy=True,
        blocking_reasons=("protection:stop_not_proven",),
        updated_at_utc=T0,
    )


def position_snapshot() -> BrokerPositionSnapshotV1:
    return BrokerPositionSnapshotV1.complete(
        snapshot_id=new_id("position_snapshot"),
        account_id=ACCOUNT,
        captured_at_utc=T1,
        published_at_utc=T1,
        source_session_id=new_id("ib_session"),
        rows=(
            BrokerPositionRowV1(
                con_id=CON_ID,
                local_symbol=LOCAL_SYMBOL,
                symbol=INSTRUMENT,
                sec_type="FUT",
                exchange="CME",
                currency="USD",
                signed_quantity=1,
                average_cost=57_200.0,
            ),
        ),
    )


def order_fact(
    order: ProtectiveOrderV1,
    *,
    broker_order_id: int,
    captured_at: str,
) -> BrokerOrderFactV1:
    return BrokerOrderFactV1(
        account_id=ACCOUNT,
        order_ref=order.order_ref,
        broker_order_id=broker_order_id,
        broker_perm_id=9_000 + broker_order_id,
        client_id=340,
        con_id=CON_ID,
        local_symbol=LOCAL_SYMBOL,
        side=order.side,
        order_type=(
            "STP"
            if order.order_type == ProtectiveOrderType.STOP
            else "LMT"
        ),
        requested_qty=1,
        filled_qty=0,
        remaining_qty=1,
        status="Submitted",
        source=BrokerOrderSource.OPEN,
        observed_at_utc=captured_at,
        completed_status=None,
        warning_text=None,
    )


def reconciliation_snapshot(
    *orders: BrokerOrderFactV1,
    captured_at: str,
) -> BrokerReconciliationSnapshotV1:
    return BrokerReconciliationSnapshotV1(
        source_session_id=new_id("ib_session"),
        account_id=ACCOUNT,
        captured_at_utc=captured_at,
        open_orders=tuple(orders),
        completed_orders=(),
        fills=(),
        requests_complete=True,
    )


class MemoryRepository:
    def __init__(self, episode, protection, position, readiness) -> None:
        self.episode = episode
        self.protection = protection
        self.position = position
        self.readiness = readiness
        self.published = []

    def read_episode(self, position_episode_id):
        return (
            self.episode
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def read_protection_by_episode(self, position_episode_id):
        return (
            self.protection
            if position_episode_id == self.episode.position_episode_id
            else None
        )

    def read_position(self, **_scope):
        return self.position

    def read_readiness(self, **_scope):
        return self.readiness

    def publish_state_and_readiness(self, *, current, updated, readiness):
        self.assert_current(current)
        self.protection = updated
        self.readiness = readiness
        self.published.append((updated, readiness))
        return updated

    def assert_current(self, current):
        if current.to_dict() != self.protection.to_dict():
            raise AssertionError("repository current state mismatch")


class PositionSource:
    def __init__(self, snapshot) -> None:
        self.snapshot = snapshot

    def read_latest_complete(self):
        return self.snapshot


class BrokerSource:
    def __init__(self, snapshots) -> None:
        self.snapshots = list(snapshots)
        self.reads = 0

    async def read_snapshot(self, *, account_id):
        self.assert_account(account_id)
        self.reads += 1
        if not self.snapshots:
            raise RuntimeError("no scripted broker snapshot remains")
        return self.snapshots.pop(0)

    @staticmethod
    def assert_account(account_id):
        if account_id != ACCOUNT:
            raise AssertionError("unexpected account")


def policy() -> PaperProtectiveSubmitPolicy:
    return PaperProtectiveSubmitPolicy(
        account_id=ACCOUNT,
        environment="paper",
        confirmed_paper_account_id=ACCOUNT,
        strategy_id=STRATEGY,
        strategy_version=1,
        deployment_id=DEPLOYMENT,
        instrument_id=INSTRUMENT,
        order_route=route(),
        position_max_age_seconds=10.0,
        proof_max_age_seconds=15.0,
        reconciliation_read_attempts=2,
        reconciliation_poll_seconds=0.0,
    )


class PaperProtectiveGatewayContractTest(unittest.TestCase):
    def test_stop_and_take_profit_orders_are_explicit(self) -> None:
        episode, protection = episode_and_protection()
        _ = episode
        stop = protection.stop_order
        tp = protection.take_profit_order
        stop_order = build_paper_protective_order(
            PaperProtectiveOrderRequest(
                account_id=ACCOUNT,
                broker_order_id=7001,
                order_ref=stop.order_ref,
                kind=stop.kind,
                side=stop.side,
                order_type=stop.order_type,
                quantity=stop.quantity,
                route=route(),
                stop_price=stop.stop_price,
                limit_price=None,
                time_in_force=stop.time_in_force,
                outside_rth=stop.outside_rth,
                oca_group=stop.oca_group,
            )
        )
        self.assertEqual(stop_order.orderType, "STP")
        self.assertEqual(stop_order.auxPrice, 28_450.0)
        self.assertEqual(stop_order.tif, "DAY")
        self.assertTrue(stop_order.outsideRth)
        self.assertEqual(stop_order.ocaGroup, stop.oca_group)
        self.assertEqual(stop_order.ocaType, 1)

        tp_order = build_paper_protective_order(
            PaperProtectiveOrderRequest(
                account_id=ACCOUNT,
                broker_order_id=7002,
                order_ref=tp.order_ref,
                kind=tp.kind,
                side=tp.side,
                order_type=tp.order_type,
                quantity=tp.quantity,
                route=route(),
                stop_price=None,
                limit_price=tp.limit_price,
                time_in_force=tp.time_in_force,
                outside_rth=tp.outside_rth,
                oca_group=tp.oca_group,
            )
        )
        self.assertEqual(tp_order.orderType, "LMT")
        self.assertEqual(tp_order.lmtPrice, 28_675.0)
        self.assertEqual(tp_order.tif, "DAY")
        self.assertFalse(tp_order.outsideRth)
        self.assertEqual(tp_order.ocaGroup, tp.oca_group)

    def test_live_account_gate_is_rejected(self) -> None:
        invalid = PaperProtectiveSubmitPolicy(
            account_id="U123",
            environment="live",
            confirmed_paper_account_id="U123",
            strategy_id=STRATEGY,
            strategy_version=1,
            deployment_id=DEPLOYMENT,
            instrument_id=INSTRUMENT,
            order_route=route(),
        )
        with self.assertRaises(PaperProtectiveSubmitError):
            require_paper_protective_gate(invalid)


class PaperProtectiveCoordinatorTest(unittest.TestCase):
    def test_stop_is_persisted_before_submit_then_tp_requires_live_stop(self) -> None:
        episode, protection = episode_and_protection()
        repo = MemoryRepository(
            episode,
            protection,
            strategy_position(episode),
            blocked_readiness(),
        )
        position_source = PositionSource(position_snapshot())
        stop_fact = order_fact(
            protection.stop_order,
            broker_order_id=7001,
            captured_at=T2,
        )
        broker = BrokerSource(
            (reconciliation_snapshot(stop_fact, captured_at=T2),)
        )

        def before_stop(request):
            self.assertEqual(request.kind, ProtectiveOrderKind.STOP_LOSS)
            self.assertEqual(
                repo.protection.stop_order.state,
                ProtectiveOrderState.SUBMITTING,
            )
            self.assertEqual(repo.protection.stop_order.broker_order_id, 7001)
            self.assertEqual(
                repo.protection.take_profit_order.state,
                ProtectiveOrderState.PLANNED,
            )

        stop_gateway = ScriptedPaperOrderGateway(
            broker_order_id=7001,
            before_submit=before_stop,
            clock=clock(T1),
        )
        first = asyncio.run(
            PaperProtectiveSubmitCoordinator(
                policy=policy(),
                protection_source=repo,
                protection_repository=repo,
                execution_state_source=repo,
                position_snapshot_source=position_source,
                order_gateway=stop_gateway,
                broker_snapshot_source=broker,
                clock=clock(T1),
            ).run_once(position_episode_id=episode.position_episode_id)
        )
        self.assertTrue(first.submission_performed)
        self.assertEqual(len(stop_gateway.protective_requests), 1)
        self.assertEqual(
            first.after.stop_order.state,
            ProtectiveOrderState.LIVE,
        )
        self.assertEqual(first.after.status, ProtectionSetStatus.STOP_LIVE)
        self.assertEqual(repo.readiness.status, ExecutionReadinessStatus.READY)
        self.assertTrue(repo.readiness.command_intake_enabled)

        current = repo.protection
        stop_current_fact = order_fact(
            current.stop_order,
            broker_order_id=7001,
            captured_at=T3,
        )
        tp_fact = order_fact(
            current.take_profit_order,
            broker_order_id=7002,
            captured_at=T3,
        )
        tp_gateway = ScriptedPaperOrderGateway(
            broker_order_id=7002,
            clock=clock(T3),
        )
        second = asyncio.run(
            PaperProtectiveSubmitCoordinator(
                policy=policy(),
                protection_source=repo,
                protection_repository=repo,
                execution_state_source=repo,
                position_snapshot_source=position_source,
                order_gateway=tp_gateway,
                broker_snapshot_source=BrokerSource(
                    (
                        reconciliation_snapshot(
                            stop_current_fact,
                            tp_fact,
                            captured_at=T3,
                        ),
                    )
                ),
                clock=clock(T3),
            ).run_once(position_episode_id=episode.position_episode_id)
        )
        self.assertTrue(second.submission_performed)
        self.assertEqual(len(tp_gateway.protective_requests), 1)
        self.assertEqual(
            tp_gateway.protective_requests[0].kind,
            ProtectiveOrderKind.TAKE_PROFIT,
        )
        self.assertEqual(second.after.status, ProtectionSetStatus.PROTECTED)
        self.assertEqual(
            second.after.take_profit_order.state,
            ProtectiveOrderState.LIVE,
        )

        repeat_gateway = ScriptedPaperOrderGateway(broker_order_id=7003)
        third = asyncio.run(
            PaperProtectiveSubmitCoordinator(
                policy=policy(),
                protection_source=repo,
                protection_repository=repo,
                execution_state_source=repo,
                position_snapshot_source=position_source,
                order_gateway=repeat_gateway,
                broker_snapshot_source=BrokerSource(()),
                clock=clock(T3),
            ).run_once(position_episode_id=episode.position_episode_id)
        )
        self.assertFalse(third.submission_performed)
        self.assertEqual(repeat_gateway.protective_requests, [])
        self.assertEqual(third.after.status, ProtectionSetStatus.PROTECTED)

    def test_unknown_stop_never_resubmits(self) -> None:
        episode, protection = episode_and_protection()
        repo = MemoryRepository(
            episode,
            protection,
            strategy_position(episode),
            blocked_readiness(),
        )
        empty = reconciliation_snapshot(captured_at=T2)
        gateway = ScriptedPaperOrderGateway(
            broker_order_id=7101,
            submit_error=RuntimeError("disconnect after possible submit"),
            clock=clock(T1),
        )
        first = asyncio.run(
            PaperProtectiveSubmitCoordinator(
                policy=policy(),
                protection_source=repo,
                protection_repository=repo,
                execution_state_source=repo,
                position_snapshot_source=PositionSource(position_snapshot()),
                order_gateway=gateway,
                broker_snapshot_source=BrokerSource((empty, empty)),
                clock=clock(T1),
            ).run_once(position_episode_id=episode.position_episode_id)
        )
        self.assertTrue(first.submission_performed)
        self.assertEqual(
            first.after.stop_order.state,
            ProtectiveOrderState.UNKNOWN_OUTCOME,
        )
        self.assertEqual(first.after.status, ProtectionSetStatus.UNPROTECTED)

        repeated_gateway = ScriptedPaperOrderGateway(broker_order_id=7102)
        second = asyncio.run(
            PaperProtectiveSubmitCoordinator(
                policy=policy(),
                protection_source=repo,
                protection_repository=repo,
                execution_state_source=repo,
                position_snapshot_source=PositionSource(position_snapshot()),
                order_gateway=repeated_gateway,
                broker_snapshot_source=BrokerSource((empty, empty)),
                clock=clock(T3),
            ).run_once(position_episode_id=episode.position_episode_id)
        )
        self.assertFalse(second.submission_performed)
        self.assertEqual(repeated_gateway.protective_requests, [])
        self.assertEqual(
            second.after.stop_order.state,
            ProtectiveOrderState.UNKNOWN_OUTCOME,
        )


if __name__ == "__main__":
    unittest.main()
