from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    actual = text.count(old)
    if actual != 1:
        raise SystemExit(
            f"{path}: expected one occurrence, found {actual}: {old!r}"
        )
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/execution/domain/ib_reconciliation.py",
    "            broker_order_id=order.broker_order_id,\n"
    "            broker_perm_id=order.broker_perm_id,\n",
    "            broker_order_id=(\n"
    "                order.broker_order_id\n"
    "                if order.broker_order_id is not None\n"
    "                else current.attempt.broker_order_id\n"
    "            ),\n"
    "            broker_perm_id=(\n"
    "                order.broker_perm_id\n"
    "                if order.broker_perm_id is not None\n"
    "                else current.attempt.broker_perm_id\n"
    "            ),\n",
)
replace_once(
    "src/ibmd/execution/domain/protective_submission.py",
    "            broker_order_id=order_fact.broker_order_id,\n"
    "            broker_perm_id=order_fact.broker_perm_id,\n",
    "            broker_order_id=(\n"
    "                order_fact.broker_order_id\n"
    "                if order_fact.broker_order_id is not None\n"
    "                else order.broker_order_id\n"
    "            ),\n"
    "            broker_perm_id=(\n"
    "                order_fact.broker_perm_id\n"
    "                if order_fact.broker_perm_id is not None\n"
    "                else order.broker_perm_id\n"
    "            ),\n",
)
replace_once(
    "src/ibmd/execution/domain/liquidation_reconciliation.py",
    "            broker_order_id=order.broker_order_id,\n"
    "            broker_perm_id=order.broker_perm_id,\n",
    "            broker_order_id=(\n"
    "                order.broker_order_id\n"
    "                if order.broker_order_id is not None\n"
    "                else current.attempt.broker_order_id\n"
    "            ),\n"
    "            broker_perm_id=(\n"
    "                order.broker_perm_id\n"
    "                if order.broker_perm_id is not None\n"
    "                else current.attempt.broker_perm_id\n"
    "            ),\n",
)
replace_once(
    "tester/target_ib_completed_order_zero_id_tester.py",
    "    broker_perm_id: int | None = 9_001,\n"
    ") -> BrokerOrderFactV1:\n"
    "    return BrokerOrderFactV1(\n"
    "        account_id=ACCOUNT,\n",
    "    broker_perm_id: int | None = 9_001,\n"
    "    account_id: str = ACCOUNT,\n"
    ") -> BrokerOrderFactV1:\n"
    "    return BrokerOrderFactV1(\n"
    "        account_id=account_id,\n",
)
replace_once(
    "tester/target_ib_completed_order_zero_id_tester.py",
    "        fact = completed_fact(\n"
    "            order_ref=current.attempt.order_ref,\n"
    "            side=current.attempt.side,\n"
    "            order_type=current.attempt.order_type,\n"
    "            broker_perm_id=9_101,\n"
    "        )\n",
    "        fact = completed_fact(\n"
    "            order_ref=current.attempt.order_ref,\n"
    "            side=current.attempt.side,\n"
    "            order_type=current.attempt.order_type,\n"
    "            broker_perm_id=9_101,\n"
    "            account_id=current.operation.account_id,\n"
    "        )\n",
)
replace_once(
    "tester/target_ib_completed_order_zero_id_tester.py",
    "        account_id=ACCOUNT,\n"
    "        captured_at_utc=T3,\n"
    "        open_orders=(),\n"
    "        completed_orders=tuple(facts),\n",
    "        account_id=(facts[0].account_id if facts else ACCOUNT),\n"
    "        captured_at_utc=T3,\n"
    "        open_orders=(),\n"
    "        completed_orders=tuple(facts),\n",
)

print("completed observation id fallback patch applied")
