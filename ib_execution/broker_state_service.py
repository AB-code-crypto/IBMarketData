from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any


DEFAULT_POSITION_REFRESH_INTERVAL_SECONDS = 2.0
DEFAULT_OPEN_ORDER_REFRESH_INTERVAL_SECONDS = 1.0
DEFAULT_EXECUTION_REFRESH_INTERVAL_SECONDS = 5.0


class BrokerStateService:
    """Throttled broker-state gateway shared by all execution components.

    One IB client must not run several concurrent reqPositions/reqOpenOrders/
    reqExecutions calls from independent recovery modules. This service
    serializes and caches those snapshots for short, explicit intervals.
    """

    def __init__(
            self,
            ib,
            *,
            account_id: str,
            position_refresh_interval_seconds: float = DEFAULT_POSITION_REFRESH_INTERVAL_SECONDS,
            open_order_refresh_interval_seconds: float = DEFAULT_OPEN_ORDER_REFRESH_INTERVAL_SECONDS,
            execution_refresh_interval_seconds: float = DEFAULT_EXECUTION_REFRESH_INTERVAL_SECONDS,
    ) -> None:
        self.ib = ib
        self.account_id = str(account_id).strip()

        self.position_refresh_interval_seconds = max(
            0.0,
            float(position_refresh_interval_seconds),
        )
        self.open_order_refresh_interval_seconds = max(
            0.0,
            float(open_order_refresh_interval_seconds),
        )
        self.execution_refresh_interval_seconds = max(
            0.0,
            float(execution_refresh_interval_seconds),
        )

        self._positions_lock = asyncio.Lock()
        self._open_orders_lock = asyncio.Lock()
        self._executions_lock = asyncio.Lock()

        self._positions: list[Any] = []
        self._positions_updated_monotonic = 0.0
        self._open_orders_updated_monotonic = 0.0
        self._executions_updated_monotonic = 0.0

    def _require_connected(self) -> None:
        try:
            connected = bool(self.ib.isConnected())
        except Exception:
            connected = False

        if not connected:
            raise ConnectionError("Not connected")

    @staticmethod
    async def _await_if_needed(value):
        if inspect.isawaitable(value):
            return await value
        return value

    @staticmethod
    def _cache_is_fresh(
            updated_monotonic: float,
            interval_seconds: float,
    ) -> bool:
        if updated_monotonic <= 0.0:
            return False

        return (
            time.monotonic() - float(updated_monotonic)
            < float(interval_seconds)
        )

    async def get_positions(
            self,
            *,
            force: bool = False,
    ) -> list[Any]:
        if (
                not force
                and self._cache_is_fresh(
                    self._positions_updated_monotonic,
                    self.position_refresh_interval_seconds,
                )
        ):
            return list(self._positions)

        async with self._positions_lock:
            if (
                    not force
                    and self._cache_is_fresh(
                        self._positions_updated_monotonic,
                        self.position_refresh_interval_seconds,
                    )
            ):
                return list(self._positions)

            self._require_connected()

            if hasattr(self.ib, "reqPositionsAsync"):
                result = await self._await_if_needed(
                    self.ib.reqPositionsAsync()
                )
            elif hasattr(self.ib, "reqPositions"):
                result = await self._await_if_needed(
                    self.ib.reqPositions()
                )
            elif hasattr(self.ib, "positions"):
                result = self.ib.positions()
            else:
                result = []

            self._positions = list(result or [])
            self._positions_updated_monotonic = time.monotonic()
            return list(self._positions)

    async def refresh_open_orders(
            self,
            *,
            force: bool = False,
    ) -> tuple[bool, str | None]:
        if (
                not force
                and self._cache_is_fresh(
                    self._open_orders_updated_monotonic,
                    self.open_order_refresh_interval_seconds,
                )
        ):
            return True, None

        async with self._open_orders_lock:
            if (
                    not force
                    and self._cache_is_fresh(
                        self._open_orders_updated_monotonic,
                        self.open_order_refresh_interval_seconds,
                    )
            ):
                return True, None

            try:
                self._require_connected()
            except Exception as exc:
                return False, f"{type(exc).__name__}: {exc}"

            errors: list[str] = []

            for method_name in (
                "reqAllOpenOrdersAsync",
                "reqOpenOrdersAsync",
                "reqAllOpenOrders",
                "reqOpenOrders",
            ):
                method = getattr(self.ib, method_name, None)
                if method is None:
                    continue

                try:
                    await self._await_if_needed(method())
                    # ib_async updates openTrades from events after the request.
                    await asyncio.sleep(0.25)
                    self._open_orders_updated_monotonic = time.monotonic()
                    return True, None
                except Exception as exc:
                    errors.append(
                        f"{method_name}: {type(exc).__name__}: {exc}"
                    )

            if not errors:
                return (
                    False,
                    "IB object has no reqAllOpenOrders/reqOpenOrders method",
                )

            return False, "; ".join(errors)

    async def refresh_executions(
            self,
            *,
            force: bool = False,
    ) -> bool:
        if (
                not force
                and self._cache_is_fresh(
                    self._executions_updated_monotonic,
                    self.execution_refresh_interval_seconds,
                )
        ):
            return True

        async with self._executions_lock:
            if (
                    not force
                    and self._cache_is_fresh(
                        self._executions_updated_monotonic,
                        self.execution_refresh_interval_seconds,
                    )
            ):
                return True

            try:
                self._require_connected()

                if hasattr(self.ib, "reqExecutionsAsync"):
                    await self._await_if_needed(
                        self.ib.reqExecutionsAsync()
                    )
                elif hasattr(self.ib, "reqExecutions"):
                    await self._await_if_needed(
                        self.ib.reqExecutions()
                    )
                else:
                    return False

                self._executions_updated_monotonic = time.monotonic()
                return True

            except Exception:
                return False

    def invalidate(self) -> None:
        self._positions_updated_monotonic = 0.0
        self._open_orders_updated_monotonic = 0.0
        self._executions_updated_monotonic = 0.0


def get_broker_state_service(
        ib,
        *,
        account_id: str,
) -> BrokerStateService:
    account = str(account_id).strip()
    if not account:
        raise RuntimeError("BrokerStateService requires account_id")

    attribute_name = "_ibmd_broker_state_service"
    existing = getattr(ib, attribute_name, None)

    if existing is not None:
        if str(existing.account_id) != account:
            raise RuntimeError(
                "IB client already has BrokerStateService for another account: "
                f"existing={existing.account_id}, requested={account}"
            )
        return existing

    service = BrokerStateService(
        ib,
        account_id=account,
    )
    setattr(ib, attribute_name, service)
    return service
