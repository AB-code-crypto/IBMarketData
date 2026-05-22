# Первый вариант настроек ib_trader:
# Входим туда, куда говорит потенциал / signal.direction.
# Режим, зоны, session windows и другие правила сейчас не учитываются.
#
# Важно:
# - позиция всё равно учитывается в trade_store.py:
#   UNKNOWN -> NO_ACTION
#   FLAT -> OPEN_POSITION
#   same side -> NO_ACTION
#   opposite side -> REVERSE_POSITION
# - order_type по умолчанию остаётся MARKET.

REQUIRE_MARKET_FEATURES = False

ACTIVE_RULES: list[dict] = []
