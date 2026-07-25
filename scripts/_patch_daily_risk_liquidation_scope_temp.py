from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(f"expected one replacement in {path}: {old[:100]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/ibmd/execution/application/daily_risk.py",
    '''    def read_latest_liquidation_operation(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> LiquidationOperationV1 | None: ...
''',
    '''    def read_liquidation_operation(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
        position_episode_id: str | None,
    ) -> LiquidationOperationV1 | None: ...
''',
)

replace_once(
    "src/ibmd/execution/application/daily_risk.py",
    '''        fills = self.owned_fill_source.read_owned_fills(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        mark = (
''',
    '''        current_state = self.repository.read_latest_state(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
        )
        fills = self.owned_fill_source.read_owned_fills(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        mark = (
''',
)

replace_once(
    "src/ibmd/execution/application/daily_risk.py",
    '''        liquidation = self.owned_fill_source.read_latest_liquidation_operation(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        current_state = self.repository.read_latest_state(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
        )
''',
    '''        needs_cleanup_state = (
            current_state is not None
            and current_state.status.value in {"TRIGGERED", "CLOSING"}
        )
        liquidation = (
            self.owned_fill_source.read_liquidation_operation(
                account_id=self.policy.account_id,
                strategy_id=self.policy.strategy_id,
                deployment_id=self.policy.deployment_id,
                instrument_id=self.policy.instrument_id,
                position_episode_id=(
                    None if episode is None else episode.position_episode_id
                ),
            )
            if episode is not None or needs_cleanup_state
            else None
        )
''',
)

replace_once(
    "src/ibmd/execution/adapters/sqlite_daily_risk_sources.py",
    '''    def read_latest_liquidation_operation(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> LiquidationOperationV1 | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_liquidation_operations_v1
                WHERE account_id = ?
                  AND strategy_id = ?
                  AND deployment_id = ?
                  AND instrument_id = ?
                ORDER BY updated_at_ts DESC, liquidation_operation_id DESC
                LIMIT 1
                """,
                (
                    str(account_id),
                    str(strategy_id),
                    str(deployment_id),
                    str(instrument_id),
                ),
            ).fetchone()
''',
    '''    def read_liquidation_operation(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
        position_episode_id: str | None,
    ) -> LiquidationOperationV1 | None:
        episode_id = str(position_episode_id or "").strip()
        episode_clause = " AND position_episode_id = ?" if episode_id else ""
        parameters = [
            str(account_id),
            str(strategy_id),
            str(deployment_id),
            str(instrument_id),
        ]
        if episode_id:
            parameters.append(episode_id)
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT payload_json
                FROM public_liquidation_operations_v1
                WHERE account_id = ?
                  AND strategy_id = ?
                  AND deployment_id = ?
                  AND instrument_id = ?
                """ + episode_clause + """
                ORDER BY updated_at_ts DESC, liquidation_operation_id DESC
                LIMIT 1
                """,
                tuple(parameters),
            ).fetchone()
''',
)

replace_once(
    "src/ibmd/execution/domain/daily_risk.py",
    '''        if liquidation_scope != expected_scope:
            raise DailyRiskDomainError(
                "liquidation operation belongs to another daily-risk scope"
            )
''',
    '''        if liquidation_scope != expected_scope:
            raise DailyRiskDomainError(
                "liquidation operation belongs to another daily-risk scope"
            )
        if (
            episode is not None
            and liquidation.position_episode_id != episode.position_episode_id
        ):
            raise DailyRiskDomainError(
                "liquidation operation belongs to another position episode"
            )
''',
)

replace_once(
    "tester/target_execution_daily_risk_tester.py",
    '''    def read_latest_liquidation_operation(self, **_scope):
        return self.liquidation
''',
    '''    def read_liquidation_operation(self, **_scope):
        return self.liquidation
''',
)
