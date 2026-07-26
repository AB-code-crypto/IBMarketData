from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, NoReturn

from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.time import format_utc, utc_now
from ibmd.ib_gateway.paper_cancellations import (
    PaperOrderCancelReceipt,
    PaperOrderCancelRequest,
    PaperOrderCancellationGateway,
)
from ibmd.ib_gateway.paper_orders import (
    PaperMarketOrderRequest,
    PaperOrderGateway,
    PaperOrderSubmissionReceipt,
    PaperProtectiveOrderRequest,
)

RESTART_PROBE_EXIT_CODE = 86


class RestartProbeError(RuntimeError):
    pass


def require_restart_probe_checkpoint(
    *,
    environment: str,
    deployment_id: str,
    data_root: str | Path,
    checkpoint_file: str | Path | None,
) -> Path:
    if str(environment or "").strip().lower() != "paper":
        raise RestartProbeError(
            "restart probe requires IBMD_ENVIRONMENT=paper"
        )
    deployment = str(deployment_id or "").strip()
    if "paper-drill" not in deployment.lower():
        raise RestartProbeError(
            "restart probe requires a dedicated deployment_id containing "
            "'paper-drill'"
        )
    if checkpoint_file is None:
        raise RestartProbeError(
            "--drill-crash-checkpoint-file is required with the restart probe"
        )
    root = Path(data_root).resolve()
    allowed = (root / "runtime" / "paper_restart_acceptance").resolve()
    resolved = Path(checkpoint_file).resolve()
    try:
        resolved.relative_to(allowed)
    except ValueError as exc:
        raise RestartProbeError(
            "restart-probe checkpoint must be inside "
            f"{allowed}"
        ) from exc
    if resolved.suffix.lower() != ".json":
        raise RestartProbeError(
            "restart-probe checkpoint must use a .json suffix"
        )
    if resolved.exists():
        raise RestartProbeError(
            f"restart-probe checkpoint already exists: {resolved}"
        )
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def _validate_exit_code(exit_code: int) -> int:
    parsed = int(exit_code)
    if parsed <= 0 or parsed > 255:
        raise RestartProbeError(
            "restart-probe exit_code must be in 1..255"
        )
    return parsed


class CrashAfterSuccessfulSubmitGateway:
    """Terminate after a confirmed paper submit and before reconciliation."""

    def __init__(
        self,
        *,
        inner: PaperOrderGateway,
        checkpoint_file: str | Path,
        market_mutation_kind: str = "MARKET_ENTRY",
        terminate: Callable[[int], NoReturn] = os._exit,
        exit_code: int = RESTART_PROBE_EXIT_CODE,
    ) -> None:
        self.inner = inner
        self.checkpoint_file = Path(checkpoint_file).resolve()
        self.market_mutation_kind = str(market_mutation_kind or "").strip()
        self.terminate = terminate
        self.exit_code = _validate_exit_code(exit_code)
        if not self.market_mutation_kind:
            raise RestartProbeError("market_mutation_kind is required")
        if self.checkpoint_file.exists():
            raise RestartProbeError(
                f"restart-probe checkpoint already exists: "
                f"{self.checkpoint_file}"
            )
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)

    async def allocate_order_id(self, *, account_id: str) -> int:
        return await self.inner.allocate_order_id(account_id=account_id)

    @staticmethod
    def _route_payload(request) -> dict:
        route = request.route
        return {
            "instrument_id": route.instrument_id,
            "con_id": route.con_id,
            "local_symbol": route.local_symbol,
            "last_trade_date": route.last_trade_date,
            "sec_type": route.sec_type,
            "exchange": route.exchange,
            "currency": route.currency,
            "trading_class": route.trading_class,
            "multiplier": route.multiplier,
        }

    def _abort_after_checkpoint(
        self,
        *,
        mutation_kind: str,
        request_payload: dict,
        receipt: PaperOrderSubmissionReceipt,
    ) -> NoReturn:
        checkpoint = {
            "schema_name": "PaperRestartSubmitCheckpoint",
            "schema_version": 1,
            "mutation_kind": mutation_kind,
            "checkpoint_at_utc": format_utc(utc_now()),
            "process_id": os.getpid(),
            "expected_exit_code": self.exit_code,
            "request": request_payload,
            "receipt": {
                "broker_order_id": receipt.broker_order_id,
                "order_ref": receipt.order_ref,
                "submitted_at_utc": receipt.submitted_at_utc,
            },
            "reconciliation_started": False,
            "automatic_retry_enabled": False,
        }
        atomic_write_json(self.checkpoint_file, checkpoint)
        self.terminate(self.exit_code)
        raise AssertionError("restart probe terminator unexpectedly returned")

    async def submit_market_order(
        self,
        request: PaperMarketOrderRequest,
    ) -> PaperOrderSubmissionReceipt:
        receipt = await self.inner.submit_market_order(request)
        self._abort_after_checkpoint(
            mutation_kind=self.market_mutation_kind,
            request_payload={
                "account_id": request.account_id,
                "broker_order_id": request.broker_order_id,
                "order_ref": request.order_ref,
                "side": request.side.value,
                "quantity": request.quantity,
                "order_type": "MARKET",
                "route": self._route_payload(request),
            },
            receipt=receipt,
        )

    async def submit_protective_order(
        self,
        request: PaperProtectiveOrderRequest,
    ) -> PaperOrderSubmissionReceipt:
        receipt = await self.inner.submit_protective_order(request)
        self._abort_after_checkpoint(
            mutation_kind=request.kind.value,
            request_payload={
                "account_id": request.account_id,
                "broker_order_id": request.broker_order_id,
                "order_ref": request.order_ref,
                "kind": request.kind.value,
                "side": request.side.value,
                "quantity": request.quantity,
                "order_type": request.order_type.value,
                "stop_price": request.stop_price,
                "limit_price": request.limit_price,
                "time_in_force": request.time_in_force,
                "outside_rth": request.outside_rth,
                "oca_group": request.oca_group,
                "route": self._route_payload(request),
            },
            receipt=receipt,
        )

    async def close(self) -> None:
        await self.inner.close()


class CrashAfterSuccessfulCancelGateway:
    """Terminate after a confirmed paper cancel and before reconciliation."""

    def __init__(
        self,
        *,
        inner: PaperOrderCancellationGateway,
        checkpoint_file: str | Path,
        cancel_mutation_kind: str = "CANCEL_ORDER",
        terminate: Callable[[int], NoReturn] = os._exit,
        exit_code: int = RESTART_PROBE_EXIT_CODE,
    ) -> None:
        self.inner = inner
        self.checkpoint_file = Path(checkpoint_file).resolve()
        self.cancel_mutation_kind = str(cancel_mutation_kind or "").strip()
        self.terminate = terminate
        self.exit_code = _validate_exit_code(exit_code)
        if not self.cancel_mutation_kind:
            raise RestartProbeError("cancel_mutation_kind is required")
        if self.checkpoint_file.exists():
            raise RestartProbeError(
                f"restart-probe checkpoint already exists: "
                f"{self.checkpoint_file}"
            )
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)

    def _abort_after_checkpoint(
        self,
        *,
        request: PaperOrderCancelRequest,
        receipt: PaperOrderCancelReceipt,
    ) -> NoReturn:
        checkpoint = {
            "schema_name": "PaperRestartCancelCheckpoint",
            "schema_version": 1,
            "mutation_kind": self.cancel_mutation_kind,
            "checkpoint_at_utc": format_utc(utc_now()),
            "process_id": os.getpid(),
            "expected_exit_code": self.exit_code,
            "request": {
                "account_id": request.account_id,
                "broker_order_id": request.broker_order_id,
                "order_ref": request.order_ref,
            },
            "receipt": {
                "broker_order_id": receipt.broker_order_id,
                "order_ref": receipt.order_ref,
                "cancel_requested_at_utc": (
                    receipt.cancel_requested_at_utc
                ),
            },
            "reconciliation_started": False,
            "automatic_retry_enabled": False,
        }
        atomic_write_json(self.checkpoint_file, checkpoint)
        self.terminate(self.exit_code)
        raise AssertionError("restart probe terminator unexpectedly returned")

    async def cancel_order(
        self,
        request: PaperOrderCancelRequest,
    ) -> PaperOrderCancelReceipt:
        receipt = await self.inner.cancel_order(request)
        self._abort_after_checkpoint(request=request, receipt=receipt)

    async def close(self) -> None:
        await self.inner.close()
