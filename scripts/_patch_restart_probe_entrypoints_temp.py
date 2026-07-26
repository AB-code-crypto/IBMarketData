from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(
            f"expected exactly one patch target in {path}: {old[:80]!r}"
        )
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


submit = Path("apps/run_execution_submit_v2.py")
replace_once(
    submit,
    '''from ibmd.ib_gateway import (
    BrokerOrderSubmitError,
    BrokerReconciliationReadError,
    IBAsyncBrokerReconciliationReader,
    IBAsyncPaperOrderGateway,
    IBBrokerReconciliationConnectionSettings,
    IBPaperOrderConnectionSettings,
    PaperOrderRoute,
)
''',
    '''from ibmd.ib_gateway import (
    BrokerOrderSubmitError,
    BrokerReconciliationReadError,
    IBAsyncBrokerReconciliationReader,
    IBAsyncPaperOrderGateway,
    IBBrokerReconciliationConnectionSettings,
    IBPaperOrderConnectionSettings,
    PaperOrderRoute,
)
from ibmd.operations.restart_probe import (
    CrashAfterSuccessfulSubmitGateway,
    RestartProbeError,
    require_restart_probe_checkpoint,
)
''',
)
replace_once(
    submit,
    '''    parser.add_argument("--reconciliation-poll-seconds", type=float, default=1.0)
    return parser
''',
    '''    parser.add_argument("--reconciliation-poll-seconds", type=float, default=1.0)
    parser.add_argument(
        "--drill-crash-after-submit",
        action="store_true",
        help=(
            "paper-drill only: terminate after a successful MARKET broker call "
            "and before reconciliation"
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
    submit,
    '''    submit_gateway = IBAsyncPaperOrderGateway(
        IBPaperOrderConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=submit_client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=float(
                arguments.connect_timeout_seconds
            ),
        )
    )
    reconciliation_source = IBAsyncBrokerReconciliationReader(
''',
    '''    submit_gateway = IBAsyncPaperOrderGateway(
        IBPaperOrderConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=submit_client_id,
            account_id=settings.ib_account_id,
            connect_timeout_seconds=float(
                arguments.connect_timeout_seconds
            ),
        )
    )
    if arguments.drill_crash_after_submit:
        checkpoint = require_restart_probe_checkpoint(
            environment=settings.environment,
            deployment_id=settings.deployment_id,
            data_root=settings.data_root,
            checkpoint_file=arguments.drill_crash_checkpoint_file,
        )
        submit_gateway = CrashAfterSuccessfulSubmitGateway(
            inner=submit_gateway,
            checkpoint_file=checkpoint,
        )
    elif arguments.drill_crash_checkpoint_file is not None:
        raise RestartProbeError(
            "--drill-crash-checkpoint-file requires "
            "--drill-crash-after-submit"
        )
    reconciliation_source = IBAsyncBrokerReconciliationReader(
''',
)
replace_once(
    submit,
    '''        PaperSubmitError,
        ValueError,
''',
    '''        PaperSubmitError,
        RestartProbeError,
        ValueError,
''',
)

protective = Path("apps/run_execution_protective_submit_v2.py")
replace_once(
    protective,
    '''from ibmd.ib_gateway.paper_orders import (
    BrokerOrderSubmitError,
    PaperOrderRoute,
)
''',
    '''from ibmd.ib_gateway.paper_orders import (
    BrokerOrderSubmitError,
    PaperOrderRoute,
)
from ibmd.operations.restart_probe import (
    CrashAfterSuccessfulSubmitGateway,
    RestartProbeError,
    require_restart_probe_checkpoint,
)
''',
)
replace_once(
    protective,
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
        "--drill-crash-after-submit",
        action="store_true",
        help=(
            "paper-drill only: terminate after a successful protective broker "
            "call and before reconciliation"
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
    protective,
    '''    gateway = IBAsyncPaperOrderGateway(
        IBPaperOrderConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=submit_client_id,
            account_id=settings.ib_account_id,
        )
    )
    reconciliation = IBAsyncBrokerReconciliationReader(
''',
    '''    gateway = IBAsyncPaperOrderGateway(
        IBPaperOrderConnectionSettings(
            host=settings.ib_host,
            port=settings.ib_port,
            client_id=submit_client_id,
            account_id=settings.ib_account_id,
        )
    )
    if arguments.drill_crash_after_submit:
        checkpoint = require_restart_probe_checkpoint(
            environment=settings.environment,
            deployment_id=settings.deployment_id,
            data_root=settings.data_root,
            checkpoint_file=arguments.drill_crash_checkpoint_file,
        )
        gateway = CrashAfterSuccessfulSubmitGateway(
            inner=gateway,
            checkpoint_file=checkpoint,
        )
    elif arguments.drill_crash_checkpoint_file is not None:
        raise RestartProbeError(
            "--drill-crash-checkpoint-file requires "
            "--drill-crash-after-submit"
        )
    reconciliation = IBAsyncBrokerReconciliationReader(
''',
)
replace_once(
    protective,
    '''        ProtectiveSubmissionDomainError,
        ValueError,
''',
    '''        ProtectiveSubmissionDomainError,
        RestartProbeError,
        ValueError,
''',
)
