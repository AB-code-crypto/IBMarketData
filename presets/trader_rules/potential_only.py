# Preset: potential_only
#
# Минимальный базовый режим:
#   - вход туда, куда говорит potential / signal.direction;
#   - regime не используется;
#   - ma_zone не используется;
#   - session windows не используются;
#   - order_type = MARKET.
#
# Позиционная логика всё равно работает в trade_store.py:
#   UNKNOWN -> NO_ACTION
#   FLAT -> OPEN_POSITION
#   same side -> NO_ACTION
#   opposite side -> REVERSE_POSITION.

# REQUIRE_MARKET_FEATURES:
#   False — ib_trader может принимать решение без regime/ma_zone.
#           Используется для режима potential_only, когда торгуем только по signal.direction / potential.
#
#   True  — regime и ma_zone обязательны.
#           Если хотя бы одно значение не прочитано из job DB, сигнал отклоняется:
#           decision_action = NO_ACTION
#           decision_reason = market_features_unknown.
#
# Практическое правило:
#   - если ACTIVE_RULES пустой и торгуем только по potential -> False;
#   - если хотя бы одно активное правило использует regime или ma_zone -> True.

REQUIRE_MARKET_FEATURES = False

ACTIVE_RULES: list[dict] = []
