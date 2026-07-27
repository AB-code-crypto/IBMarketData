from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_exact(path: str, old: str, new: str, *, count: int = 1) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    actual = text.count(old)
    if actual != count:
        raise SystemExit(
            f"{path}: expected {count} occurrence(s), found {actual}: {old!r}"
        )
    target.write_text(text.replace(old, new, count), encoding="utf-8")


def replace_all_exact(path: str, old: str, new: str, *, minimum: int = 1) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    actual = text.count(old)
    if actual < minimum:
        raise SystemExit(
            f"{path}: expected at least {minimum} occurrence(s), found {actual}: {old!r}"
        )
    target.write_text(text.replace(old, new), encoding="utf-8")


replace_exact(
    "src/ibmd/public_contracts/broker_reconciliation.py",
    "class BrokerOrderFactV1:\n    account_id: str\n    order_ref: str | None\n    broker_order_id: int\n",
    "class BrokerOrderFactV1:\n    account_id: str\n    order_ref: str | None\n    broker_order_id: int | None\n",
)
replace_exact(
    "src/ibmd/public_contracts/broker_reconciliation.py",
    "        object.__setattr__(self, \"account_id\", _text(self.account_id, \"account_id\"))\n"
    "        object.__setattr__(self, \"order_ref\", _text(self.order_ref, \"order_ref\", optional=True))\n"
    "        object.__setattr__(self, \"broker_order_id\", _int(self.broker_order_id, \"broker_order_id\", 1))\n"
    "        object.__setattr__(self, \"broker_perm_id\", _opt_positive_int(self.broker_perm_id, \"broker_perm_id\"))\n",
    "        object.__setattr__(self, \"account_id\", _text(self.account_id, \"account_id\"))\n"
    "        object.__setattr__(self, \"order_ref\", _text(self.order_ref, \"order_ref\", optional=True))\n"
    "        object.__setattr__(\n"
    "            self,\n"
    "            \"broker_order_id\",\n"
    "            _opt_positive_int(self.broker_order_id, \"broker_order_id\"),\n"
    "        )\n"
    "        object.__setattr__(self, \"broker_perm_id\", _opt_positive_int(self.broker_perm_id, \"broker_perm_id\"))\n",
)
replace_exact(
    "src/ibmd/public_contracts/broker_reconciliation.py",
    "        if not isinstance(self.source, BrokerOrderSource):\n"
    "            raise BrokerReconciliationContractError(f\"invalid order source: {self.source!r}\")\n"
    "        object.__setattr__(self, \"observed_at_utc\", _utc(self.observed_at_utc, \"observed_at_utc\"))\n",
    "        if not isinstance(self.source, BrokerOrderSource):\n"
    "            raise BrokerReconciliationContractError(f\"invalid order source: {self.source!r}\")\n"
    "        if (\n"
    "            self.source == BrokerOrderSource.OPEN\n"
    "            and self.broker_order_id is None\n"
    "        ):\n"
    "            raise BrokerReconciliationContractError(\n"
    "                \"open broker order fact requires broker_order_id\"\n"
    "            )\n"
    "        if (\n"
    "            self.order_ref is None\n"
    "            and self.broker_order_id is None\n"
    "            and self.broker_perm_id is None\n"
    "        ):\n"
    "            raise BrokerReconciliationContractError(\n"
    "                \"broker order fact requires order_ref, order id or perm id\"\n"
    "            )\n"
    "        object.__setattr__(self, \"observed_at_utc\", _utc(self.observed_at_utc, \"observed_at_utc\"))\n",
)
replace_exact(
    "src/ibmd/public_contracts/broker_reconciliation.py",
    "    @property\n"
    "    def broker_identity(self) -> tuple[int, int | None]:\n"
    "        return self.broker_order_id, self.broker_perm_id\n",
    "    @property\n"
    "    def broker_identity(self) -> tuple[str, object, int | None]:\n"
    "        if self.broker_perm_id is not None:\n"
    "            return \"PERM\", self.broker_perm_id, None\n"
    "        if self.broker_order_id is not None:\n"
    "            return \"ORDER\", self.broker_order_id, self.client_id\n"
    "        if self.order_ref is not None:\n"
    "            return \"REF\", self.order_ref, None\n"
    "        raise BrokerReconciliationContractError(\n"
    "            \"broker order fact has no usable identity\"\n"
    "        )\n",
)
replace_exact(
    "src/ibmd/public_contracts/broker_reconciliation.py",
    "            object.__setattr__(self, field, tuple(sorted(values, key=lambda item: (item.order_ref or \"\", item.broker_order_id))))\n",
    "            object.__setattr__(\n"
    "                self,\n"
    "                field,\n"
    "                tuple(\n"
    "                    sorted(\n"
    "                        values,\n"
    "                        key=lambda item: (\n"
    "                            item.order_ref or \"\",\n"
    "                            item.broker_order_id or 0,\n"
    "                            item.broker_perm_id or 0,\n"
    "                        ),\n"
    "                    )\n"
    "                ),\n"
    "            )\n",
)

replace_exact(
    "src/ibmd/ib_gateway/broker_reconciliation_mapping.py",
    "        broker_order_id=_positive_int(order_id, field_name=\"orderId\"),\n",
    "        broker_order_id=(\n"
    "            None\n"
    "            if order_id <= 0\n"
    "            else _positive_int(order_id, field_name=\"orderId\")\n"
    "        ),\n",
)
replace_exact(
    "src/ibmd/ib_gateway/broker_reconciliation_mapping.py",
    "    by_identity: dict[tuple[int, int | None], BrokerOrderFactV1] = {}\n",
    "    by_identity: dict[tuple[object, ...], BrokerOrderFactV1] = {}\n",
)

same_identity_old = (
    "    if left.broker_perm_id is not None and right.broker_perm_id is not None:\n"
    "        return left.broker_perm_id == right.broker_perm_id\n"
    "    return left.broker_order_id == right.broker_order_id\n"
)
same_identity_new = (
    "    if left.broker_perm_id is not None and right.broker_perm_id is not None:\n"
    "        return left.broker_perm_id == right.broker_perm_id\n"
    "    if left.broker_order_id is not None and right.broker_order_id is not None:\n"
    "        return left.broker_order_id == right.broker_order_id\n"
    "    return (\n"
    "        left.order_ref is not None\n"
    "        and left.order_ref == right.order_ref\n"
    "    )\n"
)
for path in (
    "src/ibmd/execution/domain/ib_reconciliation.py",
    "src/ibmd/execution/domain/protective_submission.py",
    "src/ibmd/execution/domain/liquidation_reconciliation.py",
):
    replace_exact(path, same_identity_old, same_identity_new)

for path, prefix in (
    ("src/ibmd/execution/domain/ib_reconciliation.py", "attempt"),
    ("src/ibmd/execution/domain/liquidation_reconciliation.py", "attempt"),
):
    replace_exact(
        path,
        f"        {prefix}.broker_order_id is not None\n"
        f"        and order.broker_order_id != {prefix}.broker_order_id\n",
        f"        {prefix}.broker_order_id is not None\n"
        "        and order.broker_order_id is not None\n"
        f"        and order.broker_order_id != {prefix}.broker_order_id\n",
    )
replace_exact(
    "src/ibmd/execution/domain/protective_submission.py",
    "        expected.broker_order_id is not None\n"
    "        and order_fact.broker_order_id != expected.broker_order_id\n",
    "        expected.broker_order_id is not None\n"
    "        and order_fact.broker_order_id is not None\n"
    "        and order_fact.broker_order_id != expected.broker_order_id\n",
)

replace_exact(
    "src/ibmd/execution/domain/broker_attempt.py",
    "        broker_order_id=observation.broker_order_id,\n"
    "        broker_perm_id=observation.broker_perm_id,\n",
    "        broker_order_id=(\n"
    "            observation.broker_order_id\n"
    "            if observation.broker_order_id is not None\n"
    "            else snapshot.attempt.broker_order_id\n"
    "        ),\n"
    "        broker_perm_id=(\n"
    "            observation.broker_perm_id\n"
    "            if observation.broker_perm_id is not None\n"
    "            else snapshot.attempt.broker_perm_id\n"
    "        ),\n",
)

replace_all_exact(
    "src/ibmd/execution/domain/protection.py",
    "            broker_order_id=observation.broker_order_id,\n"
    "            broker_perm_id=observation.broker_perm_id,\n",
    "            broker_order_id=(\n"
    "                observation.broker_order_id\n"
    "                if observation.broker_order_id is not None\n"
    "                else order.broker_order_id\n"
    "            ),\n"
    "            broker_perm_id=(\n"
    "                observation.broker_perm_id\n"
    "                if observation.broker_perm_id is not None\n"
    "                else order.broker_perm_id\n"
    "            ),\n",
    minimum=2,
)

replace_exact(
    "src/ibmd/execution/domain/liquidation.py",
    "    if observation.broker_order_id != attempt.broker_order_id:\n"
    "        raise LiquidationDomainError(\n"
    "            \"broker observation order id differs from liquidation attempt\"\n"
    "        )\n",
    "    if (\n"
    "        observation.broker_order_id is not None\n"
    "        and observation.broker_order_id != attempt.broker_order_id\n"
    "    ):\n"
    "        raise LiquidationDomainError(\n"
    "            \"broker observation order id differs from liquidation attempt\"\n"
    "        )\n",
)
replace_all_exact(
    "src/ibmd/execution/domain/liquidation.py",
    "            broker_perm_id=observation.broker_perm_id,\n",
    "            broker_perm_id=(\n"
    "                observation.broker_perm_id\n"
    "                if observation.broker_perm_id is not None\n"
    "                else attempt.broker_perm_id\n"
    "            ),\n",
    minimum=2,
)

print("completed-order zero-id production patch applied")
