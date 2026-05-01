from core.time_utils import format_utc_ts


def analyze_history_coverage(target_start_ts, target_end_ts, existing_min_ts, existing_max_ts, bar_size_seconds):
    # Анализируем покрытие истории по конкретному контракту.
    #
    # В БД хранится время НАЧАЛА бара, а не его правой границы.
    # Поэтому реальный хвост покрытия в БД:
    # existing_max_ts + размер_бара.
    if target_end_ts <= target_start_ts:
        raise ValueError("Целевой интервал истории должен быть положительным")

    if existing_min_ts is None or existing_max_ts is None:
        return {
            "is_full": False,
            "db_min_ts": None,
            "db_max_ts": None,
            "loaded_until_ts": None,
            "segments": [
                {
                    "kind": "full",
                    "start_ts": target_start_ts,
                    "end_ts": target_end_ts,
                }
            ],
        }

    loaded_until_ts = existing_max_ts + bar_size_seconds
    segments = []

    if existing_min_ts > target_start_ts:
        segments.append(
            {
                "kind": "head",
                "start_ts": target_start_ts,
                "end_ts": existing_min_ts,
            }
        )

    if loaded_until_ts < target_end_ts:
        segments.append(
            {
                "kind": "tail",
                "start_ts": loaded_until_ts,
                "end_ts": target_end_ts,
            }
        )

    return {
        "is_full": len(segments) == 0,
        "db_min_ts": existing_min_ts,
        "db_max_ts": existing_max_ts,
        "loaded_until_ts": loaded_until_ts,
        "segments": segments,
    }


def describe_missing_segments(segments):
    if not segments:
        return "пропусков нет"

    parts = []

    for segment in segments:
        if segment["kind"] == "full":
            title = "весь интервал"
        elif segment["kind"] == "head":
            title = "начальный участок"
        elif segment["kind"] == "tail":
            title = "конечный участок"
        else:
            raise ValueError(f"Неизвестный тип сегмента: {segment['kind']}")

        parts.append(
            f"{title} {format_utc_ts(segment['start_ts'])} -> {format_utc_ts(segment['end_ts'])}"
        )

    return "; ".join(parts)
