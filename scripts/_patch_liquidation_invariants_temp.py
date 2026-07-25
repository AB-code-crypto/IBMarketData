from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"expected one replacement in {path}, got {count}: {old[:100]!r}"
        )
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/public_contracts/protection.py",
    '''        closing_operation = (\n            None\n            if self.closing_operation_id is None\n            else validate_id(\n                self.closing_operation_id,\n                expected_kind="broker_operation",\n            )\n        )\n''',
    '''        if self.closing_operation_id is None:\n            closing_operation = None\n        else:\n            closing_candidate = str(self.closing_operation_id).strip()\n            try:\n                closing_operation = validate_id(\n                    closing_candidate,\n                    expected_kind="broker_operation",\n                )\n            except ValueError:\n                closing_operation = validate_id(\n                    closing_candidate,\n                    expected_kind="liquidation_operation",\n                )\n''',
)

replace_once(
    "src/ibmd/execution/domain/liquidation_completion.py",
    '''    updated_episode = replace(\n        episode,\n        status=PositionEpisodeStatus.CLOSED,\n        closed_at_utc=observed,\n        closing_operation_id=None,\n    )\n''',
    '''    updated_episode = replace(\n        episode,\n        status=PositionEpisodeStatus.CLOSED,\n        closed_at_utc=observed,\n        closing_operation_id=(\n            liquidation.operation.liquidation_operation_id\n        ),\n    )\n''',
)

replace_once(
    "src/ibmd/execution/domain/liquidation.py",
    '''    if not readiness.broker_actions_enabled:\n        raise LiquidationDomainError(\n            "liquidation requires broker_actions_enabled=true"\n        )\n''',
    '''    # A liquidation request is a durable safety fact, not a broker call.\n    # It must be recorded even while broker actions are temporarily disabled.\n''',
)

replace_once(
    "src/ibmd/execution/domain/liquidation.py",
    '''    if observation.outcome == BrokerObservationOutcome.LIVE:\n        live_attempt = replace(\n''',
    '''    cumulative_filled = (\n        snapshot.operation.liquidation_filled_quantity\n        - attempt.filled_qty\n        + int(observation.filled_qty)\n    )\n    if cumulative_filled < 0:\n        raise LiquidationDomainError(\n            "liquidation cumulative fill quantity became negative"\n        )\n    if observation.outcome == BrokerObservationOutcome.LIVE:\n        live_attempt = replace(\n''',
)

replace_once(
    "src/ibmd/execution/domain/liquidation.py",
    '''        operation = replace(\n            snapshot.operation,\n            state=LiquidationOperationState.LIVE,\n            broker_remaining_quantity=int(observation.remaining_qty),\n''',
    '''        operation = replace(\n            snapshot.operation,\n            state=LiquidationOperationState.LIVE,\n            broker_remaining_quantity=int(observation.remaining_qty),\n            liquidation_filled_quantity=cumulative_filled,\n''',
)

replace_once(
    "src/ibmd/execution/domain/liquidation.py",
    '''            liquidation_filled_quantity=(\n                snapshot.operation.liquidation_filled_quantity\n                + terminal_attempt.filled_qty\n            ),\n''',
    '''            liquidation_filled_quantity=cumulative_filled,\n''',
)

replace_once(
    "src/ibmd/execution/domain/liquidation.py",
    '''            broker_remaining_quantity=terminal_attempt.remaining_qty,\n            next_action=LiquidationNextAction.OPERATOR_REQUIRED,\n''',
    '''            broker_remaining_quantity=terminal_attempt.remaining_qty,\n            liquidation_filled_quantity=cumulative_filled,\n            next_action=LiquidationNextAction.OPERATOR_REQUIRED,\n''',
)

replace_once(
    "src/ibmd/execution/domain/liquidation_position.py",
    '''    side = (\n        StrategyPositionSide.LONG\n        if signed > 0.0\n        else StrategyPositionSide.SHORT\n    )\n''',
    '''    if quantity > episode.quantity:\n        return LiquidationBrokerPositionProof(\n            state="INCIDENT",\n            snapshot_id=snapshot.snapshot_id,\n            freshness_seconds=freshness.age_seconds,\n            quantity=0,\n            side=None,\n            reason=(\n                "broker_quantity_exceeds_owned_episode:"\n                f"episode={episode.quantity}, broker={quantity}"\n            ),\n        )\n    side = (\n        StrategyPositionSide.LONG\n        if signed > 0.0\n        else StrategyPositionSide.SHORT\n    )\n''',
)

replace_once(
    "src/ibmd/execution/application/liquidation.py",
    '''        completion = None\n        if proof.state == "FLAT" and updated.operation.next_action not in {\n            LiquidationNextAction.RECONCILE_EXITS,\n            LiquidationNextAction.CANCEL_TAKE_PROFIT,\n            LiquidationNextAction.CANCEL_STOP,\n        }:\n''',
    '''        completion = None\n        close_outcome_unresolved = (\n            updated.attempt is not None\n            and updated.attempt.state.value\n            in {"SUBMITTING", "LIVE", "UNKNOWN_OUTCOME"}\n        )\n        unresolved_protection = any(\n            item.state.value\n            in {"SUBMITTING", "LIVE", "CANCEL_REQUESTED", "UNKNOWN_OUTCOME"}\n            for item in protection.orders\n        )\n        if (\n            proof.state == "FLAT"\n            and not close_outcome_unresolved\n            and not unresolved_protection\n        ):\n''',
)
