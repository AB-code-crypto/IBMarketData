from enum import Enum


class SignalWindowMode(str, Enum):
    ROLLING = "ROLLING"
    GRID = "GRID"

    def __str__(self) -> str:
        return self.value
