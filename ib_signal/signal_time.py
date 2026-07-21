"""CT-hour groups used to keep historical candidates in comparable sessions."""

ALL_CT_HOURS = list(range(24))

# Existing strategy behaviour: futures search all hours except the no-trade and
# clearing hours. This is an hour-of-day filter, not a signal-window mode.
FUT_SEARCH_HOUR_GROUPS_CT = {
    hour: ([] if hour in {15, 16} else list(ALL_CT_HOURS))
    for hour in ALL_CT_HOURS
}

FX_SEARCH_HOUR_GROUPS_CT = {
    **{hour: [17, 18, 19, 20, 21, 22, 23, 0, 1] for hour in [17, 18, 19, 20, 21, 22, 23, 0, 1]},
    **{hour: [2, 3, 4, 5, 6] for hour in [2, 3, 4, 5, 6]},
    **{hour: [7, 8, 9, 10] for hour in [7, 8, 9, 10]},
    **{hour: [11, 12, 13, 14, 15, 16] for hour in [11, 12, 13, 14, 15, 16]},
}

CRYPTO_SEARCH_HOUR_GROUPS_CT = {
    hour: list(ALL_CT_HOURS)
    for hour in ALL_CT_HOURS
}

SEARCH_HOUR_GROUPS_BY_SEC_TYPE = {
    "FUT": FUT_SEARCH_HOUR_GROUPS_CT,
    "CASH": FX_SEARCH_HOUR_GROUPS_CT,
    "CRYPTO": CRYPTO_SEARCH_HOUR_GROUPS_CT,
}


def resolve_allowed_hours(current_hour_ct: int, sec_type: str) -> list[int]:
    sec_type_value = str(sec_type).upper()
    groups = SEARCH_HOUR_GROUPS_BY_SEC_TYPE.get(sec_type_value)
    if groups is None:
        raise ValueError(f"Неподдерживаемый secType для candidate search: {sec_type!r}")
    hour = int(current_hour_ct)
    if hour not in groups:
        raise ValueError(f"CT hour должен быть в диапазоне 0..23: {hour}")
    return list(groups[hour])
