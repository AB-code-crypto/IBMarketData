from enum import Enum


class SignalWindowMode(str, Enum):
    ROLLING = "ROLLING"
    GRID = "GRID"

    def __str__(self) -> str:
        return self.value


def parse_signal_window_mode(value: SignalWindowMode | str) -> SignalWindowMode:
    # Нормализует режим сигнального окна.
    #
    # В боевом config лучше задавать сразу SignalWindowMode.ROLLING / GRID.
    # Но для тестера и временных конфигов оставляем возможность передать строку.
    if isinstance(value, SignalWindowMode):
        return value

    normalized_value = str(value).strip().upper()

    try:
        return SignalWindowMode(normalized_value)
    except ValueError as exc:
        allowed_values = ", ".join(mode.value for mode in SignalWindowMode)
        raise ValueError(
            f"Неизвестный режим signal_window_mode: {value!r}. "
            f"Допустимые значения: {allowed_values}"
        ) from exc
