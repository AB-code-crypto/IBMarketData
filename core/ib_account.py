from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class IbAccountValidation:
    expected_account_id: str
    managed_accounts: tuple[str, ...]


def normalize_account_id(value: str) -> str:
    account_id = str(value or "").strip()
    if not account_id:
        raise RuntimeError(
            "IB_ACCOUNT_ID is required. Add the exact TWS account id to .env, "
            "for example: IB_ACCOUNT_ID=U1234567"
        )
    return account_id


def read_managed_accounts(ib) -> list[str]:
    method = getattr(ib, "managedAccounts", None)
    if method is None:
        return []

    try:
        values = list(method() or [])
    except Exception:
        return []

    result: list[str] = []
    for value in values:
        account_id = str(value or "").strip()
        if account_id and account_id not in result:
            result.append(account_id)
    return result


async def validate_ib_account_access(
        ib,
        *,
        expected_account_id: str,
        timeout_seconds: float = 5.0,
        poll_interval_seconds: float = 0.10,
) -> IbAccountValidation:
    expected = normalize_account_id(expected_account_id)
    loop_time = asyncio.get_running_loop().time
    deadline = loop_time() + float(timeout_seconds)

    while True:
        managed_accounts = read_managed_accounts(ib)

        if managed_accounts:
            if expected not in managed_accounts:
                raise RuntimeError(
                    "Configured IB account is not available in this TWS session: "
                    f"expected={expected}, managed_accounts={managed_accounts}"
                )

            return IbAccountValidation(
                expected_account_id=expected,
                managed_accounts=tuple(managed_accounts),
            )

        if loop_time() >= deadline:
            raise RuntimeError(
                "TWS connection is active, but managed account list was not received "
                f"within {timeout_seconds:g}s; expected_account={expected}"
            )

        await asyncio.sleep(float(poll_interval_seconds))


def position_belongs_to_account(position, *, expected_account_id: str) -> bool:
    expected = normalize_account_id(expected_account_id)
    actual = str(getattr(position, "account", "") or "").strip()
    return actual == expected
