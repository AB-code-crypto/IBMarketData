from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"expected one replacement in {path}, got {count}: {old[:120]!r}"
        )
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/execution/domain/reverse_finalization.py",
    '''        object.__setattr__(
            self,
            "opening_started_at_utc",
            format_utc(parse_utc(self.opening_started_at_utc)),
        )
''',
    '''        object.__setattr__(
            self,
            "opening_started_at_utc",
            format_utc(parse_utc(self.opening_started_at_utc)),
        )
        if parse_utc(self.opening_started_at_utc) < parse_utc(
            self.closing_completed_at_utc
        ):
            raise ReverseFinalizationError(
                "reverse opening cannot precede completion of the source close"
            )
        allocations = tuple(self.allocations)
        if any(not isinstance(item, ReverseFillAllocationV1) for item in allocations):
            raise ReverseFinalizationError(
                "allocations must contain ReverseFillAllocationV1 values"
            )
        if tuple(item.sequence_no for item in allocations) != tuple(
            range(1, len(allocations) + 1)
        ):
            raise ReverseFinalizationError(
                "reverse allocation sequence numbers must be contiguous"
            )
        if len({item.reverse_allocation_id for item in allocations}) != len(
            allocations
        ) or len({item.exec_id for item in allocations}) != len(allocations):
            raise ReverseFinalizationError(
                "reverse allocation identities and execIds must be unique"
            )
        operation_id = self.new_plan.episode.source_operation_id
        attempt_id = self.new_plan.episode.source_attempt_id
        closing_id = self.closed_episode.position_episode_id
        opening_id = self.new_plan.episode.position_episode_id
        if any(
            item.source_operation_id != operation_id
            or item.source_attempt_id != attempt_id
            or item.closing_position_episode_id != closing_id
            or item.opening_position_episode_id != opening_id
            for item in allocations
        ):
            raise ReverseFinalizationError(
                "reverse allocations disagree with finalization identities"
            )
        if self.closed_protection.position_episode_id != closing_id:
            raise ReverseFinalizationError(
                "closed protection belongs to another source episode"
            )
        if (
            self.new_plan.protection.position_episode_id != opening_id
            or self.new_plan.strategy_position.position_episode_id != opening_id
        ):
            raise ReverseFinalizationError(
                "new protection/position does not reference the opening episode"
            )
        if self.closed_episode.closing_operation_id != operation_id:
            raise ReverseFinalizationError(
                "closed source episode does not reference the reverse operation"
            )
        if sum(item.close_quantity for item in allocations) != self.closed_episode.quantity:
            raise ReverseFinalizationError(
                "reverse allocations do not close the source episode quantity"
            )
        if sum(item.open_quantity for item in allocations) != self.new_plan.episode.quantity:
            raise ReverseFinalizationError(
                "reverse allocations do not open the target episode quantity"
            )
        opening_exec_ids = tuple(
            item.exec_id for item in allocations if item.open_quantity > 0
        )
        if opening_exec_ids != self.new_plan.episode.source_exec_ids:
            raise ReverseFinalizationError(
                "opening episode execIds differ from reverse allocations"
            )
        expected_commission_complete = all(
            item.commission_complete for item in allocations
        )
        if self.commission_complete != expected_commission_complete:
            raise ReverseFinalizationError(
                "reverse commission completeness differs from allocations"
            )
        object.__setattr__(self, "allocations", allocations)
''',
)

replace_once(
    "src/ibmd/execution/adapters/sqlite_reverse_finalization.py",
    '''def _finalization(payload: str) -> ReversePositionFinalizationV1:
''',
    '''def _commission_material(value: ReversePositionFinalizationV1) -> str:
    payload = _payload(value)
    payload["commission_complete"] = False
    for item in payload["allocations"]:
        item["commission_complete"] = False
    return canonical_json_text(payload)


def _finalization(payload: str) -> ReversePositionFinalizationV1:
''',
)

replace_once(
    "src/ibmd/execution/adapters/sqlite_reverse_finalization.py",
    '''    def publish_finalization(
        self,
        *,
        current_episode: PositionEpisodeV1,
''',
    '''    def refresh_commission_completion(
        self,
        *,
        current: ReversePositionFinalizationV1,
        updated: ReversePositionFinalizationV1,
    ) -> ReversePositionFinalizationV1:
        operation_id = current.new_plan.episode.source_operation_id
        if updated.new_plan.episode.source_operation_id != operation_id:
            raise ReverseFinalizationStoreError(
                "reverse commission refresh operation identity changed"
            )
        if current.commission_complete and not updated.commission_complete:
            raise ReverseFinalizationStoreError(
                "reverse commission completeness cannot regress"
            )
        if _commission_material(current) != _commission_material(updated):
            raise ReverseFinalizationStoreError(
                "reverse commission refresh changed economic finalization facts"
            )
        current_by_id = {
            item.reverse_allocation_id: item for item in current.allocations
        }
        updated_by_id = {
            item.reverse_allocation_id: item for item in updated.allocations
        }
        if set(current_by_id) != set(updated_by_id):
            raise ReverseFinalizationStoreError(
                "reverse commission refresh allocation identities changed"
            )
        for allocation_id, stored in current_by_id.items():
            incoming = updated_by_id[allocation_id]
            if stored.commission_complete and not incoming.commission_complete:
                raise ReverseFinalizationStoreError(
                    "reverse allocation commission completeness cannot regress"
                )
        if current == updated:
            return current
        with self._writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT payload_json FROM internal_reverse_finalizations "
                    "WHERE source_operation_id=? LIMIT 1",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise ReverseFinalizationStoreError(
                        "reverse finalization disappeared before commission refresh"
                    )
                stored = _finalization(str(row["payload_json"]))
                if stored.to_dict() if hasattr(stored, "to_dict") else False:
                    pass
                if canonical_json_text(_payload(stored)) != canonical_json_text(
                    _payload(current)
                ):
                    raise ReverseFinalizationStoreError(
                        "reverse finalization changed concurrently"
                    )
                for allocation in updated.allocations:
                    previous = current_by_id[allocation.reverse_allocation_id]
                    if previous.commission_complete == allocation.commission_complete:
                        continue
                    cursor = connection.execute(
                        "UPDATE internal_reverse_fill_allocations "
                        "SET commission_complete=?, payload_json=? "
                        "WHERE reverse_allocation_id=?",
                        (
                            int(allocation.commission_complete),
                            canonical_json_text(allocation.to_dict()),
                            allocation.reverse_allocation_id,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ReverseFinalizationStoreError(
                            "reverse allocation disappeared during commission refresh"
                        )
                cursor = connection.execute(
                    "UPDATE internal_reverse_finalizations "
                    "SET commission_complete=?, payload_json=? "
                    "WHERE source_operation_id=?",
                    (
                        int(updated.commission_complete),
                        canonical_json_text(_payload(updated)),
                        operation_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise ReverseFinalizationStoreError(
                        "reverse finalization disappeared during commission refresh"
                    )
                connection.commit()
                return updated
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, ReverseFinalizationStoreError):
                    raise
                raise ReverseFinalizationStoreError(
                    "cannot refresh reverse commission completion: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                connection.close()

    def publish_finalization(
        self,
        *,
        current_episode: PositionEpisodeV1,
''',
)
