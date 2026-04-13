from __future__ import annotations

from collections.abc import Sequence
from typing import TypeVar

T = TypeVar("T")


def maybe_use_closed_candles(candles: Sequence[T], *, enabled: bool) -> list[T]:
    """Prefer the latest fully closed candle by dropping the freshest bar when enabled."""
    data = list(candles)
    if not enabled or len(data) <= 1:
        return data
    return data[:-1]
