import asyncio
import logging

from core.ib_account import normalize_account_id
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Mapping, Optional, Sequence

from ib_async import (
    IB,
    Contract,
    LimitOrder,
    MarketOrder,
    Order,
    StopLimitOrder,
    StopOrder,
    Trade,
)

log = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]


@dataclass(slots=True)
class PlaceOrderRequest:
    contract: Contract
    order: Order
    order_ref: str


@dataclass(slots=True)
class PlaceOrderReceipt:
    order_id: int
    order_ref: str
    placed_at_utc: datetime
    trade: Trade


@dataclass(slots=True)
class CancelOrderReceipt:
    order_id: int
    cancel_requested_at_utc: datetime


@dataclass(slots=True)
class BracketOrders:
    parent: Order
    take_profit: Optional[Order]
    stop_loss: Optional[Order]


class IBOrderApi:
    """Тонкий адаптер IB: сборка ордеров, placeOrder и cancelOrder без торговой логики."""

    def __init__(
            self,
            ib: IB,
            *,
            account_id: str,
            logger: Optional[logging.Logger] = None,
    ) -> None:
        self._ib = ib
        self._account_id = normalize_account_id(account_id)
        self._log = logger or log

    @property
    def ib(self) -> IB:
        return self._ib

    def next_order_id(self) -> int:
        """Резервирует следующий orderId у IB client."""
        return int(self._ib.client.getReqId())

    async def place_order(self, contract: Contract, order: Order, *, order_ref: str) -> PlaceOrderReceipt:
        """Отправляет ордер в IB и возвращает receipt без ожидания статуса."""
        if not order_ref:
            raise ValueError("order_ref must be a non-empty string")

        order.orderRef = order_ref

        existing_account = str(getattr(order, "account", "") or "").strip()
        if existing_account and existing_account != self._account_id:
            raise RuntimeError(
                f"Order account mismatch: configured={self._account_id}, "
                f"order.account={existing_account}, order_ref={order_ref}"
            )
        order.account = self._account_id

        placed_at = datetime.now(timezone.utc)
        trade: Trade = self._ib.placeOrder(contract, order)

        await asyncio.sleep(0)

        return PlaceOrderReceipt(
            order_id=int(trade.order.orderId),
            order_ref=order_ref,
            placed_at_utc=placed_at,
            trade=trade,
        )

    async def cancel_order(self, order_id: int) -> CancelOrderReceipt:
        """Запрашивает отмену ордера по orderId."""
        self._ib.client.cancelOrder(int(order_id))
        await asyncio.sleep(0)
        return CancelOrderReceipt(
            order_id=int(order_id),
            cancel_requested_at_utc=datetime.now(timezone.utc),
        )

    async def cancel_orders(self, order_ids: Sequence[int]) -> list[CancelOrderReceipt]:
        receipts: list[CancelOrderReceipt] = []
        for order_id in order_ids:
            receipts.append(await self.cancel_order(int(order_id)))
        return receipts

    def _apply_order_kwargs(self, order: Order, order_kwargs: Optional[Mapping[str, Any]]) -> None:
        if not order_kwargs:
            return

        for key, value in order_kwargs.items():
            if not hasattr(order, key):
                raise AttributeError(f"IB Order has no attribute {key!r}")
            setattr(order, key, value)

    @staticmethod
    def _format_good_till_date_utc(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y%m%d-%H:%M:%S")

    def _apply_time_in_force(
            self,
            order: Order,
            *,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
    ) -> None:
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")

        if ttl_seconds is not None and good_till is not None:
            raise ValueError("Use either ttl_seconds or good_till, not both")

        if ttl_seconds is not None:
            good_till = datetime.now(timezone.utc) + timedelta(seconds=int(ttl_seconds))

        if good_till is not None:
            order.tif = "GTD"
            order.goodTillDate = self._format_good_till_date_utc(good_till)
            return

        if time_in_force is not None:
            order.tif = str(time_in_force)

    def build_market(
            self,
            *,
            action: Side,
            quantity: int,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = MarketOrder(action, int(quantity))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_limit(
            self,
            *,
            action: Side,
            quantity: int,
            limit_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = LimitOrder(action, int(quantity), float(limit_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_stop(
            self,
            *,
            action: Side,
            quantity: int,
            stop_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = StopOrder(action, int(quantity), float(stop_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_stop_limit(
            self,
            *,
            action: Side,
            quantity: int,
            stop_price: float,
            limit_price: float,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        order = StopLimitOrder(action, int(quantity), float(limit_price), float(stop_price))
        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def build_trailing_stop(
            self,
            *,
            action: Side,
            quantity: int,
            trailing_percent: Optional[float] = None,
            trailing_amount: Optional[float] = None,
            trail_stop_price: Optional[float] = None,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Order:
        """Собирает TRAIL order через базовый ib_async.Order."""
        if trailing_percent is not None and trailing_amount is not None:
            raise ValueError("Use either trailing_percent or trailing_amount, not both")

        order = Order()
        order.action = action
        order.totalQuantity = int(quantity)
        order.orderType = "TRAIL"

        if trailing_percent is not None:
            order.trailingPercent = float(trailing_percent)

        if trailing_amount is not None:
            order.auxPrice = float(trailing_amount)

        if trail_stop_price is not None:
            order.trailStopPrice = float(trail_stop_price)

        self._apply_time_in_force(order, time_in_force=time_in_force, ttl_seconds=ttl_seconds, good_till=good_till)
        self._apply_order_kwargs(order, order_kwargs)
        return order

    def apply_oca_group(
            self,
            orders: Sequence[Order],
            *,
            oca_group: str,
            oca_type: int = 1,
    ) -> None:
        """Присваивает группе ордеров OCA-параметры."""
        if not oca_group:
            raise ValueError("oca_group must be a non-empty string")

        for order in orders:
            order.ocaGroup = oca_group
            order.ocaType = int(oca_type)

    def build_bracket_limit(
            self,
            *,
            action: Side,
            quantity: int,
            limit_price: float,
            take_profit_price: Optional[float] = None,
            stop_loss_price: Optional[float] = None,
            time_in_force: Optional[str] = None,
            ttl_seconds: Optional[int] = None,
            good_till: Optional[datetime] = None,
            order_kwargs: Optional[Mapping[str, Any]] = None,
            take_profit_order_kwargs: Optional[Mapping[str, Any]] = None,
            stop_loss_order_kwargs: Optional[Mapping[str, Any]] = None,
            oca_group: Optional[str] = None,
            oca_type: int = 1,
    ) -> BracketOrders:
        """Собирает parent LIMIT + optional TP LIMIT + optional SL STOP."""
        parent = self.build_limit(
            action=action,
            quantity=quantity,
            limit_price=limit_price,
            time_in_force=time_in_force,
            ttl_seconds=ttl_seconds,
            good_till=good_till,
            order_kwargs=order_kwargs,
        )
        parent.transmit = False

        exit_action: Side = "SELL" if action == "BUY" else "BUY"
        children: list[Order] = []

        take_profit: Optional[Order] = None
        if take_profit_price is not None:
            take_profit = self.build_limit(
                action=exit_action,
                quantity=quantity,
                limit_price=float(take_profit_price),
                time_in_force=time_in_force,
                ttl_seconds=ttl_seconds,
                good_till=good_till,
                order_kwargs=take_profit_order_kwargs,
            )
            take_profit.transmit = False
            children.append(take_profit)

        stop_loss: Optional[Order] = None
        if stop_loss_price is not None:
            stop_loss = self.build_stop(
                action=exit_action,
                quantity=quantity,
                stop_price=float(stop_loss_price),
                time_in_force=time_in_force,
                ttl_seconds=ttl_seconds,
                good_till=good_till,
                order_kwargs=stop_loss_order_kwargs,
            )
            stop_loss.transmit = False
            children.append(stop_loss)

        if len(children) >= 2:
            if oca_group is None:
                oca_group = f"OCA_BRACKET_{self.next_order_id()}"
            self.apply_oca_group(children, oca_group=oca_group, oca_type=oca_type)

        if children:
            children[-1].transmit = True
        else:
            parent.transmit = True

        return BracketOrders(parent=parent, take_profit=take_profit, stop_loss=stop_loss)

    def assign_bracket_ids(self, bracket: BracketOrders) -> None:
        parent_id = self.next_order_id()
        bracket.parent.orderId = int(parent_id)

        children: list[Order] = []
        if bracket.take_profit is not None:
            children.append(bracket.take_profit)
        if bracket.stop_loss is not None:
            children.append(bracket.stop_loss)

        for child in children:
            child.orderId = int(self.next_order_id())
            child.parentId = int(parent_id)

    async def place_bracket(
            self,
            *,
            contract: Contract,
            bracket: BracketOrders,
            order_ref: str,
    ) -> list[PlaceOrderReceipt]:
        self.assign_bracket_ids(bracket)

        orders: list[Order] = [bracket.parent]
        if bracket.take_profit is not None:
            orders.append(bracket.take_profit)
        if bracket.stop_loss is not None:
            orders.append(bracket.stop_loss)

        receipts: list[PlaceOrderReceipt] = []
        for order in orders:
            receipts.append(await self.place_order(contract, order, order_ref=order_ref))
        return receipts

    async def place_oca_group(
            self,
            *,
            contract: Contract,
            orders: Sequence[Order],
            order_ref: str,
    ) -> list[PlaceOrderReceipt]:
        receipts: list[PlaceOrderReceipt] = []
        for order in orders:
            receipts.append(await self.place_order(contract, order, order_ref=order_ref))
        return receipts
