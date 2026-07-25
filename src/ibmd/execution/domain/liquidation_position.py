from __future__ import annotations

from dataclasses import dataclass

from ibmd.public_contracts.execution import StrategyPositionSide
from ibmd.public_contracts.positions import (
    BrokerPositionSnapshotStatus,
    BrokerPositionSnapshotV1,
)
from ibmd.public_contracts.protection import PositionEpisodeV1


class LiquidationPositionError(ValueError):
    pass


@dataclass(frozen=True)
class LiquidationBrokerPositionProof:
    state: str
    snapshot_id: str
    freshness_seconds: float
    quantity: int
    side: StrategyPositionSide | None
    reason: str | None

    def __post_init__(self) -> None:
        if self.state not in {"OPEN", "FLAT", "INCIDENT"}:
            raise LiquidationPositionError(
                f"invalid liquidation broker position state: {self.state!r}"
            )
        if self.quantity < 0:
            raise LiquidationPositionError("quantity must be non-negative")
        if self.state == "OPEN":
            if self.quantity <= 0 or self.side not in {
                StrategyPositionSide.LONG,
                StrategyPositionSide.SHORT,
            }:
                raise LiquidationPositionError(
                    "OPEN liquidation proof requires side and positive quantity"
                )
        else:
            if self.quantity != 0 or self.side is not None:
                raise LiquidationPositionError(
                    f"{self.state} liquidation proof cannot have position quantity"
                )
        if self.state == "INCIDENT" and not self.reason:
            raise LiquidationPositionError("INCIDENT proof requires a reason")


def prove_liquidation_broker_position(
    *,
    snapshot: BrokerPositionSnapshotV1,
    episode: PositionEpisodeV1,
    observed_at_utc: str,
    max_age_seconds: float,
) -> LiquidationBrokerPositionProof:
    if not isinstance(snapshot, BrokerPositionSnapshotV1):
        raise LiquidationPositionError(
            "snapshot must be BrokerPositionSnapshotV1"
        )
    if not isinstance(episode, PositionEpisodeV1):
        raise LiquidationPositionError("episode must be PositionEpisodeV1")
    if snapshot.status != BrokerPositionSnapshotStatus.COMPLETE:
        raise LiquidationPositionError(
            "broker position snapshot is not COMPLETE"
        )
    if snapshot.account_id != episode.account_id:
        raise LiquidationPositionError(
            "broker position snapshot account differs from episode"
        )
    max_age = float(max_age_seconds)
    if max_age <= 0.0:
        raise LiquidationPositionError(
            "position max age seconds must be positive"
        )
    freshness = snapshot.freshness(
        observed_at_utc=observed_at_utc,
        max_age_seconds=max_age,
    )
    if not freshness.is_fresh:
        raise LiquidationPositionError(
            "broker position snapshot is stale for liquidation: "
            f"age={freshness.age_seconds:.6f}s"
        )

    relevant = [
        row
        for row in snapshot.rows
        if row.symbol.upper() == episode.instrument_id.upper()
        or str(row.local_symbol or "").upper().startswith(
            episode.instrument_id.upper()
        )
    ]
    nonzero = [
        row for row in relevant if abs(float(row.signed_quantity)) > 1e-9
    ]
    if not nonzero:
        return LiquidationBrokerPositionProof(
            state="FLAT",
            snapshot_id=snapshot.snapshot_id,
            freshness_seconds=freshness.age_seconds,
            quantity=0,
            side=None,
            reason=None,
        )
    if len(nonzero) != 1:
        summary = [
            (row.con_id, row.local_symbol, row.signed_quantity, row.sec_type)
            for row in nonzero
        ]
        return LiquidationBrokerPositionProof(
            state="INCIDENT",
            snapshot_id=snapshot.snapshot_id,
            freshness_seconds=freshness.age_seconds,
            quantity=0,
            side=None,
            reason=f"multiple_broker_positions_for_instrument:{summary}",
        )
    row = nonzero[0]
    if (
        row.sec_type != "FUT"
        or row.con_id != episode.con_id
        or str(row.local_symbol or "") != episode.local_symbol
    ):
        return LiquidationBrokerPositionProof(
            state="INCIDENT",
            snapshot_id=snapshot.snapshot_id,
            freshness_seconds=freshness.age_seconds,
            quantity=0,
            side=None,
            reason=(
                "broker_contract_differs_from_episode:"
                f"{row.con_id}:{row.local_symbol}:{row.sec_type}"
            ),
        )
    signed = float(row.signed_quantity)
    quantity = int(abs(signed))
    if quantity <= 0 or abs(abs(signed) - quantity) > 1e-9:
        return LiquidationBrokerPositionProof(
            state="INCIDENT",
            snapshot_id=snapshot.snapshot_id,
            freshness_seconds=freshness.age_seconds,
            quantity=0,
            side=None,
            reason=f"fractional_or_invalid_futures_quantity:{signed}",
        )
    if quantity > episode.quantity:
        return LiquidationBrokerPositionProof(
            state="INCIDENT",
            snapshot_id=snapshot.snapshot_id,
            freshness_seconds=freshness.age_seconds,
            quantity=0,
            side=None,
            reason=(
                "broker_quantity_exceeds_owned_episode:"
                f"episode={episode.quantity}, broker={quantity}"
            ),
        )
    side = (
        StrategyPositionSide.LONG
        if signed > 0.0
        else StrategyPositionSide.SHORT
    )
    if side != episode.side:
        return LiquidationBrokerPositionProof(
            state="INCIDENT",
            snapshot_id=snapshot.snapshot_id,
            freshness_seconds=freshness.age_seconds,
            quantity=0,
            side=None,
            reason=(
                "broker_position_side_differs_from_episode:"
                f"episode={episode.side.value}, broker={side.value}"
            ),
        )
    return LiquidationBrokerPositionProof(
        state="OPEN",
        snapshot_id=snapshot.snapshot_id,
        freshness_seconds=freshness.age_seconds,
        quantity=quantity,
        side=side,
        reason=None,
    )
