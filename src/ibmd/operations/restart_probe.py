from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, NoReturn

from ibmd.foundation.atomic_json import atomic_write_json
from ibmd.foundation.time import format_utc, utc_now
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


class CrashAfterSuccessfulSubmitGateway:
    """Paper-only wrapper that terminates after a confirmed broker submit call.

    The wrapped coordinator has already persisted its durable SUBMITTING state
    before invoking this gateway. A successful inner submit is recorded to an
    atomic checkpoint, then the child process terminates without returning to
    reconciliation. The next ordinary invocation must adopt the same order.
    """

    def __init__(
        self,
        *,
        inner: PaperOrderGateway,
        checkpoint_file: str | Path,
        terminate: Callable[[int], NoReturn] = os._exit,
        exit_code: int = RESTART_PROBE_EXIT_CODE,
    ) -> None:
        self.inner = inner
        self.checkpoint_file = Path(checkpoint_file).resolve()
        self.terminate = terminate
        self.exit_code = int(exit_code)
        if self.exit_code <= 0 or self.exit_code > 255:
            raise RestartProbeError(
                "restart-probe exit_code must be in 1..255"
            )
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
            mutation_kind="MARKET_ENTRY",
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
