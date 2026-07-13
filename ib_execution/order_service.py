import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional

from ib_async import CommissionReport, Contract, Fill, IB, Trade

from ib_execution.ib_order_api import (
    BracketOrders,
    CancelOrderReceipt,
    IBOrderApi,
    PlaceOrderReceipt,
)
from core.ib_account import normalize_account_id
from ib_execution.broker_state_service import get_broker_state_service
from ib_execution.order_monitor import AcceptanceResult, DoneResult, IBError, OrderMonitor

log = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]
WaitMode = Literal["none", "accept", "done"]


class OrderRejectedError(RuntimeError):
    def __init__(self, *, order_id: int, status: str, error: Optional[IBError]) -> None:
        msg = f"Order rejected: order_id={order_id}, status={status}"
        if error is not None:
            msg += f", ib_code={error.code}, ib_msg={error.message}"
        super().__init__(msg)
        self.order_id = order_id
        self.status = status
        self.error = error


class OrderTimeoutError(TimeoutError):
    def __init__(self, *, order_id: int, stage: str, status: str) -> None:
        super().__init__(f"Order timeout at stage={stage}: order_id={order_id}, status={status}")
        self.order_id = order_id
        self.stage = stage
        self.status = status


@dataclass(slots=True)
class TradeFillInfo:
    exec_id: str
    time: Optional[datetime]
    price: float
    size: float
    commission: Optional[float]
    realized_pnl: Optional[float]


@dataclass(slots=True)
class OrderPlacement:
    receipt: PlaceOrderReceipt
    acceptance: Optional[AcceptanceResult] = None
    done: Optional[DoneResult] = None
    fills: list[TradeFillInfo] = field(default_factory=list)
    fills_count: int = 0
    total_commission: float = 0.0
    realized_pnl: float = 0.0
    avg_fill_price: Optional[float] = None


class OrderService:
    """Верхний слой постановки ордеров: qualify, build, place, wait, cancel."""

    def __init__(
            self,
            ib: IB,
            *,
            account_id: str,
            api: Optional[IBOrderApi] = None,
            monitor: Optional[OrderMonitor] = None,
    ) -> None:
        self._ib = ib
        self._account_id = normalize_account_id(account_id)
        self._api = api or IBOrderApi(
            ib,
            account_id=self._account_id,
        )
        self._monitor = monitor or OrderMonitor(ib)
        self._broker_state = get_broker_state_service(
            ib,
            account_id=self._account_id,
        )

    @property
    def ib(self) -> IB:
        return self._ib

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def api(self) -> IBOrderApi:
        return self._api

    @property
    def monitor(self) -> OrderMonitor:
        return self._monitor

    @property
    def broker_state(self):
        return self._broker_state

    async def qualify(self, contract: Contract) -> Contract:
        if getattr(contract, "conId", 0):
            return contract

        result = await self._ib.qualifyContractsAsync(contract)
        if not result:
            raise RuntimeError(f"qualifyContractsAsync returned empty for contract={contract!r}")
        return result[0]

    async def place(
            self,
            *,
            contract: Contract,
            order,
            order_ref: str,
            wait: WaitMode = "accept",
            accept_timeout: float = 5.0,
            done_timeout: float = 60.0,
            poll_interval: float = 0.10,
    ) -> OrderPlacement:
        contract_q = await self.qualify(contract)
        receipt = await self._api.place_order(contract_q, order, order_ref=order_ref)
        placement = OrderPlacement(receipt=receipt)

        if wait == "none":
            return placement

        placement.acceptance = await self._monitor.wait_for_accept(
            receipt.trade,
            timeout=accept_timeout,
            poll_interval=poll_interval,
        )
        self._raise_for_unaccepted(placement.acceptance)

        if wait == "done":
            placement.done = await self._monitor.wait_for_done(
                receipt.trade,
                timeout=done_timeout,
                poll_interval=poll_interval,
            )
            self._raise_for_undone(placement.done)
            self._hydrate_fill_statistics(placement, receipt.trade.fills)

        return placement

    async def buy_market(
            self,
            *,
            contract: Contract,
            quantity: int,
            order_ref: str,
            time_in_force: str = "DAY",
            wait: WaitMode = "done",
            accept_timeout: float = 5.0,
            done_timeout: float = 60.0,
    ) -> OrderPlacement:
        order = self._api.build_market(action="BUY", quantity=int(quantity), time_in_force=time_in_force)
        return await self.place(
            contract=contract,
            order=order,
            order_ref=order_ref,
            wait=wait,
            accept_timeout=accept_timeout,
            done_timeout=done_timeout,
        )

    async def sell_market(
            self,
            *,
            contract: Contract,
            quantity: int,
            order_ref: str,
            time_in_force: str = "DAY",
            wait: WaitMode = "done",
            accept_timeout: float = 5.0,
            done_timeout: float = 60.0,
    ) -> OrderPlacement:
        order = self._api.build_market(action="SELL", quantity=int(quantity), time_in_force=time_in_force)
        return await self.place(
            contract=contract,
            order=order,
            order_ref=order_ref,
            wait=wait,
            accept_timeout=accept_timeout,
            done_timeout=done_timeout,
        )

    async def buy_limit(
            self,
            *,
            contract: Contract,
            quantity: int,
            limit_price: float,
            order_ref: str,
            ttl_seconds: Optional[int] = None,
            time_in_force: str = "DAY",
            wait: WaitMode = "accept",
    ) -> OrderPlacement:
        order = self._api.build_limit(
            action="BUY",
            quantity=int(quantity),
            limit_price=float(limit_price),
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        return await self.place(contract=contract, order=order, order_ref=order_ref, wait=wait)

    async def sell_limit(
            self,
            *,
            contract: Contract,
            quantity: int,
            limit_price: float,
            order_ref: str,
            ttl_seconds: Optional[int] = None,
            time_in_force: str = "DAY",
            wait: WaitMode = "accept",
    ) -> OrderPlacement:
        order = self._api.build_limit(
            action="SELL",
            quantity=int(quantity),
            limit_price=float(limit_price),
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        return await self.place(contract=contract, order=order, order_ref=order_ref, wait=wait)

    async def place_bracket_limit(
            self,
            *,
            contract: Contract,
            action: Side,
            quantity: int,
            limit_price: float,
            take_profit_price: Optional[float],
            stop_loss_price: Optional[float],
            order_ref: str,
            ttl_seconds: Optional[int] = None,
            time_in_force: str = "DAY",
            accept_timeout: float = 5.0,
            atomic: bool = True,
    ) -> list[OrderPlacement]:
        bracket: BracketOrders = self._api.build_bracket_limit(
            action=action,
            quantity=int(quantity),
            limit_price=float(limit_price),
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            ttl_seconds=ttl_seconds,
            time_in_force=time_in_force,
        )
        self._api.assign_bracket_ids(bracket)
        receipts = await self._api.place_bracket(
            contract=await self.qualify(contract),
            bracket=bracket,
            order_ref=order_ref,
        )
        return await self._collect_atomic_acceptance_results(
            receipts,
            accept_timeout=accept_timeout,
            atomic=atomic,
        )

    async def cancel_order_id(self, order_id: int) -> CancelOrderReceipt:
        order_id = int(order_id)
        self._monitor.note_cancel_requested(order_id)

        try:
            return await self._api.cancel_order(order_id)
        except Exception:
            self._monitor.clear_cancel_requested(order_id)
            raise

    async def cancel_order_ids(self, order_ids: list[int]) -> list[CancelOrderReceipt]:
        receipts: list[CancelOrderReceipt] = []
        for order_id in order_ids:
            receipts.append(await self.cancel_order_id(int(order_id)))
        return receipts

    def open_order_ids(self, *, order_ref: Optional[str] = None) -> list[int]:
        ids: list[int] = []
        for trade in list(self._ib.openTrades()):
            order = trade.order
            order_id = int(getattr(order, "orderId", 0) or 0)
            if not order_id:
                continue
            if order_ref is not None and str(getattr(order, "orderRef", "") or "") != str(order_ref):
                continue
            ids.append(order_id)
        return sorted(set(ids))

    async def cancel_all_open_orders(self, *, order_ref: Optional[str] = None) -> list[CancelOrderReceipt]:
        ids = self.open_order_ids(order_ref=order_ref)
        if not ids:
            return []
        return await self.cancel_order_ids(ids)

    async def global_cancel(self) -> None:
        for order_id in self.open_order_ids():
            self._monitor.note_cancel_requested(order_id)
        self._ib.reqGlobalCancel()
        await asyncio.sleep(0)

    @staticmethod
    def _raise_for_unaccepted(acceptance: AcceptanceResult) -> None:
        if acceptance.accepted:
            return
        if acceptance.timed_out:
            raise OrderTimeoutError(order_id=acceptance.order_id, stage="accept", status=acceptance.status)
        raise OrderRejectedError(
            order_id=acceptance.order_id,
            status=acceptance.status,
            error=acceptance.error,
        )

    @staticmethod
    def _raise_for_undone(done: DoneResult) -> None:
        if done.done:
            return
        raise OrderTimeoutError(order_id=done.order_id, stage="done", status=done.status)

    def _hydrate_fill_statistics(self, placement: OrderPlacement, fills: list[Fill]) -> None:
        placement.fills = self._collect_fill_infos(list(fills))
        placement.fills_count = len(fills)
        placement.total_commission, placement.realized_pnl = self._aggregate_commission_and_pnl(list(fills))
        placement.avg_fill_price = self._avg_fill_price(list(fills))

    async def _collect_atomic_acceptance_results(
            self,
            receipts: list[PlaceOrderReceipt],
            *,
            accept_timeout: float,
            atomic: bool,
    ) -> list[OrderPlacement]:
        results: list[OrderPlacement] = []

        for receipt in receipts:
            acceptance = await self._monitor.wait_for_accept(receipt.trade, timeout=accept_timeout)
            results.append(OrderPlacement(receipt=receipt, acceptance=acceptance))

        if atomic:
            await self._raise_for_atomic_failures(results)

        return results

    async def _raise_for_atomic_failures(self, results: list[OrderPlacement]) -> None:
        bad = [placement for placement in results if not (placement.acceptance and placement.acceptance.accepted)]

        if not bad:
            return

        for placement in results:
            await self.cancel_order_id(placement.receipt.order_id)

        first_bad = bad[0]
        acceptance = first_bad.acceptance

        if acceptance and acceptance.timed_out:
            raise OrderTimeoutError(order_id=acceptance.order_id, stage="accept", status=acceptance.status)

        raise OrderRejectedError(
            order_id=first_bad.receipt.order_id,
            status=acceptance.status if acceptance else "",
            error=acceptance.error if acceptance else None,
        )

    @staticmethod
    def _collect_fill_infos(fills: list[Fill]) -> list[TradeFillInfo]:
        result: list[TradeFillInfo] = []

        for fill in fills:
            execution = getattr(fill, "execution", None)

            if execution is None:
                continue

            commission_report: Optional[CommissionReport] = getattr(fill, "commissionReport", None)

            result.append(
                TradeFillInfo(
                    exec_id=str(getattr(execution, "execId", "")),
                    time=getattr(execution, "time", None),
                    price=float(getattr(execution, "price", 0.0) or 0.0),
                    size=float(getattr(execution, "shares", 0.0) or 0.0),
                    commission=(
                        float(commission_report.commission)
                        if commission_report is not None and commission_report.commission is not None
                        else None
                    ),
                    realized_pnl=(
                        float(commission_report.realizedPNL)
                        if commission_report is not None and commission_report.realizedPNL is not None
                        else None
                    ),
                )
            )

        return result

    @staticmethod
    def _aggregate_commission_and_pnl(fills: list[Fill]) -> tuple[float, float]:
        total_commission = 0.0
        realized_pnl = 0.0

        for fill in fills:
            commission_report: Optional[CommissionReport] = getattr(fill, "commissionReport", None)

            if commission_report is None:
                continue

            if commission_report.commission:
                total_commission += float(commission_report.commission)

            if commission_report.realizedPNL:
                realized_pnl += float(commission_report.realizedPNL)

        return total_commission, realized_pnl

    @staticmethod
    def _avg_fill_price(fills: list[Fill]) -> Optional[float]:
        if not fills:
            return None

        total_qty = 0.0
        total_notional = 0.0

        for fill in fills:
            execution = getattr(fill, "execution", None)

            if execution is None:
                continue

            shares = float(getattr(execution, "shares", 0.0) or 0.0)
            price = float(getattr(execution, "price", 0.0) or 0.0)
            total_qty += shares
            total_notional += shares * price

        if total_qty <= 0:
            return None

        return total_notional / total_qty
