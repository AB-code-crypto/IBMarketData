from pathlib import Path

path = Path("apps/run_execution_liquidation_v2.py")
text = path.read_text(encoding="utf-8")


def replace_once(old: str, new: str) -> None:
    global text
    if text.count(old) != 1:
        raise SystemExit(f"expected exactly one liquidation patch target: {old[:80]!r}")
    text = text.replace(old, new, 1)


replace_once(
    '''from ibmd.public_contracts.liquidation import LiquidationReason
''',
    '''from ibmd.operations.restart_probe import (
    CrashAfterSuccessfulCancelGateway,
    CrashAfterSuccessfulSubmitGateway,
    RestartProbeError,
    require_restart_probe_checkpoint,
)
from ibmd.public_contracts.liquidation import LiquidationReason
''',
)
replace_once(
    '''    parser.add_argument(
        "--commission-wait-seconds",
        type=float,
        default=2.0,
    )
    return parser
''',
    '''    parser.add_argument(
        "--commission-wait-seconds",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--drill-crash-after-broker-action",
        action="store_true",
        help=(
            "paper-drill only: terminate after one successful cancelOrder or "
            "liquidation MARKET submit and before reconciliation"
        ),
    )
    parser.add_argument(
        "--drill-crash-checkpoint-file",
        type=Path,
        default=None,
        help="atomic checkpoint written immediately before the intentional exit",
    )
    return parser
''',
)
replace_once(
    '''    cancellation_gateway = IBAsyncPaperOrderCancellationGateway(
        IBPaperCancellationConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=cancel_client_id,
            account_id=settings.ib_account_id,
        )
    )
    reconciliation = IBAsyncBrokerReconciliationReader(
''',
    '''    cancellation_gateway = IBAsyncPaperOrderCancellationGateway(
        IBPaperCancellationConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=cancel_client_id,
            account_id=settings.ib_account_id,
        )
    )
    if arguments.drill_crash_after_broker_action:
        checkpoint = require_restart_probe_checkpoint(
            environment=settings.environment,
            deployment_id=settings.deployment_id,
            data_root=settings.data_root,
            checkpoint_file=arguments.drill_crash_checkpoint_file,
        )
        order_gateway = CrashAfterSuccessfulSubmitGateway(
            inner=order_gateway,
            checkpoint_file=checkpoint,
            market_mutation_kind="LIQUIDATION_MARKET_CLOSE",
        )
        cancellation_gateway = CrashAfterSuccessfulCancelGateway(
            inner=cancellation_gateway,
            checkpoint_file=checkpoint,
        )
    elif arguments.drill_crash_checkpoint_file is not None:
        raise RestartProbeError(
            "--drill-crash-checkpoint-file requires "
            "--drill-crash-after-broker-action"
        )
    reconciliation = IBAsyncBrokerReconciliationReader(
''',
)
replace_once(
    '''        LiquidationStoreError,
        PaperLiquidationError,
''',
    '''        LiquidationStoreError,
        PaperLiquidationError,
        RestartProbeError,
''',
)
path.write_text(text, encoding="utf-8")
