from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

try:
    import pandas_ta as ta  # type: ignore
except ImportError:  # pragma: no cover - optional dependency fallback
    ta = None


@dataclass
class IndicatorSnapshot:
    adx_value: float
    bb_width: float
    bb_width_previous: float
    volatility: float

    @property
    def bb_width_expanding(self) -> bool:
        return self.bb_width > self.bb_width_previous


def ohlcv_to_dataframe(ohlcv: list[list[float]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    numeric_columns = ["open", "high", "low", "close", "volume"]
    frame[numeric_columns] = frame[numeric_columns].astype(float)
    return frame


def _true_range(frame: pd.DataFrame) -> pd.Series:
    high = frame["high"]
    low = frame["low"]
    close = frame["close"]
    tr_components = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    )
    return tr_components.max(axis=1).fillna(0.0)


def _manual_atr(frame: pd.DataFrame, length: int) -> pd.Series:
    true_range = _true_range(frame)
    return true_range.ewm(alpha=1 / length, adjust=False).mean().fillna(0.0)


def compute_atr(frame: pd.DataFrame, length: int = 14) -> pd.Series:
    return _manual_atr(frame, length)


def compute_obv(frame: pd.DataFrame) -> pd.Series:
    close = frame["close"]
    volume = frame["volume"].fillna(0.0)
    direction = close.diff().fillna(0.0).map(lambda value: 1.0 if value > 0 else -1.0 if value < 0 else 0.0)
    obv = (direction * volume).cumsum().astype(float)
    if not obv.empty:
        obv.iloc[0] = 0.0
    return obv


def compute_supertrend(frame: pd.DataFrame, length: int = 10, multiplier: float = 3.0) -> pd.DataFrame:
    atr = compute_atr(frame, length)
    hl2 = (frame["high"] + frame["low"]) / 2.0
    basic_upper = hl2 + multiplier * atr
    basic_lower = hl2 - multiplier * atr

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()

    for index in range(1, len(frame)):
        previous_close = float(frame["close"].iloc[index - 1])
        if basic_upper.iloc[index] < final_upper.iloc[index - 1] or previous_close > final_upper.iloc[index - 1]:
            final_upper.iloc[index] = basic_upper.iloc[index]
        else:
            final_upper.iloc[index] = final_upper.iloc[index - 1]

        if basic_lower.iloc[index] > final_lower.iloc[index - 1] or previous_close < final_lower.iloc[index - 1]:
            final_lower.iloc[index] = basic_lower.iloc[index]
        else:
            final_lower.iloc[index] = final_lower.iloc[index - 1]

    direction = pd.Series(index=frame.index, dtype="int64")
    supertrend = pd.Series(index=frame.index, dtype="float64")

    if len(frame) == 0:
        return pd.DataFrame(columns=["supertrend", "direction", "upper_band", "lower_band", "atr"])

    direction.iloc[0] = 1
    supertrend.iloc[0] = float(final_lower.iloc[0])

    for index in range(1, len(frame)):
        close = float(frame["close"].iloc[index])
        previous_upper = float(final_upper.iloc[index - 1])
        previous_lower = float(final_lower.iloc[index - 1])
        previous_direction = int(direction.iloc[index - 1])

        if close > previous_upper:
            current_direction = 1
        elif close < previous_lower:
            current_direction = -1
        else:
            current_direction = previous_direction

        direction.iloc[index] = current_direction
        supertrend.iloc[index] = float(final_lower.iloc[index] if current_direction == 1 else final_upper.iloc[index])

    return pd.DataFrame(
        {
            "supertrend": supertrend.fillna(0.0),
            "direction": direction.fillna(0).astype(int),
            "upper_band": final_upper.fillna(0.0),
            "lower_band": final_lower.fillna(0.0),
            "atr": atr.fillna(0.0),
        }
    )


def _manual_adx(frame: pd.DataFrame, length: int) -> pd.Series:
    high = frame["high"]
    low = frame["low"]
    close = frame["close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    atr = _manual_atr(frame, length)

    plus_di = 100 * plus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr
    denominator = (plus_di + minus_di).replace(0, float("nan"))
    dx = ((plus_di - minus_di).abs() / denominator) * 100
    dx = dx.astype(float)
    return dx.ewm(alpha=1 / length, adjust=False).mean().fillna(0.0)


def _adx(frame: pd.DataFrame, length: int) -> pd.Series:
    if ta is not None:
        adx_frame = ta.adx(frame["high"], frame["low"], frame["close"], length=length)
        column_name = f"ADX_{length}"
        if adx_frame is not None and column_name in adx_frame:
            return adx_frame[column_name].fillna(0.0)
    return _manual_adx(frame, length)


def compute_bollinger_bands(frame: pd.DataFrame, length: int = 20, std: float = 2.0) -> pd.DataFrame:
    close = frame["close"]
    basis = close.rolling(length).mean()
    deviation = close.rolling(length).std(ddof=0)
    upper = basis + deviation * std
    lower = basis - deviation * std
    width = (upper - lower) / basis.replace(0, float("nan"))
    return pd.DataFrame(
        {
            "basis": basis.astype(float).fillna(0.0),
            "upper": upper.astype(float).fillna(0.0),
            "lower": lower.astype(float).fillna(0.0),
            "width": width.astype(float).fillna(0.0),
        }
    )


def _bollinger_width(frame: pd.DataFrame, length: int, std: float) -> pd.Series:
    return compute_bollinger_bands(frame, length=length, std=std)["width"]


def _realized_volatility(frame: pd.DataFrame, length: int) -> pd.Series:
    returns = frame["close"].pct_change().fillna(0.0)
    return returns.rolling(length).std(ddof=0).fillna(0.0)


def compute_indicator_snapshot(
    ohlcv: list[list[float]],
    adx_length: int = 14,
    bb_length: int = 20,
    bb_std: float = 2.0,
) -> IndicatorSnapshot:
    frame = ohlcv_to_dataframe(ohlcv)
    if len(frame) < max(adx_length * 3, bb_length + 2):
        raise ValueError("Not enough OHLCV data to compute indicators reliably")

    adx_series = _adx(frame, adx_length)
    bb_width_series = _bollinger_width(frame, bb_length, bb_std)
    volatility_series = _realized_volatility(frame, bb_length)

    return IndicatorSnapshot(
        adx_value=float(adx_series.iloc[-1]),
        bb_width=float(bb_width_series.iloc[-1]),
        bb_width_previous=float(bb_width_series.iloc[-2]),
        volatility=float(volatility_series.iloc[-1]),
    )
