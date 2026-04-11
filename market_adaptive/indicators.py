from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

try:
    import pandas_ta as ta  # type: ignore
except ImportError:  # pragma: no cover - optional dependency fallback
    ta = None


@dataclass
class IndicatorSnapshot:
    adx_value: float
    adx_previous: float
    adx_pre_previous: float
    plus_di_value: float
    minus_di_value: float
    bb_width: float
    bb_width_previous: float
    volatility: float

    @property
    def bb_width_expanding(self) -> bool:
        return self.bb_width > self.bb_width_previous

    @property
    def di_gap(self) -> float:
        return abs(self.plus_di_value - self.minus_di_value)

    @property
    def adx_rising(self) -> bool:
        return self.adx_value > self.adx_previous > self.adx_pre_previous

    @property
    def adx_trend_label(self) -> str:
        if self.adx_rising:
            return "rising"
        if self.adx_value < self.adx_previous < self.adx_pre_previous:
            return "falling"
        return "flat"


@dataclass
class OBVConfirmationSnapshot:
    current_obv: float
    sma_value: float
    increment_value: float
    increment_mean: float
    increment_std: float
    zscore: float

    @property
    def above_sma(self) -> bool:
        return self.current_obv > self.sma_value

    @property
    def below_sma(self) -> bool:
        return self.current_obv < self.sma_value

    def buy_confirmed(self, *, zscore_threshold: float) -> bool:
        return self.above_sma and self.zscore > zscore_threshold

    def sell_confirmed(self, *, zscore_threshold: float) -> bool:
        return self.below_sma and self.zscore < -zscore_threshold


@dataclass
class VolumeProfileSnapshot:
    poc_price: float
    value_area_low: float
    value_area_high: float
    total_volume: float
    value_area_volume: float
    low_price: float
    high_price: float
    bin_size: float
    bin_count: int

    def contains_price(self, price: float) -> bool:
        current_price = float(price)
        return self.value_area_low <= current_price <= self.value_area_high

    def above_poc(self, price: float) -> bool:
        return float(price) > self.poc_price

    def above_value_area(self, price: float) -> bool:
        return float(price) > self.value_area_high


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


def compute_rsi(frame: pd.DataFrame, length: int = 14) -> pd.Series:
    close = frame["close"].astype(float)
    delta = close.diff().fillna(0.0)
    gains = delta.clip(lower=0.0)
    losses = (-delta).clip(lower=0.0)
    average_gain = gains.ewm(alpha=1 / max(1, int(length)), adjust=False).mean()
    average_loss = losses.ewm(alpha=1 / max(1, int(length)), adjust=False).mean()

    relative_strength = average_gain / average_loss.replace(0.0, float("nan"))
    rsi = 100.0 - (100.0 / (1.0 + relative_strength))
    rsi = rsi.where(average_loss > 0.0, 100.0)
    rsi = rsi.where((average_gain > 0.0) | (average_loss > 0.0), 50.0)
    return rsi.astype(float).fillna(50.0)


def compute_kdj(
    frame: pd.DataFrame,
    *,
    length: int = 9,
    k_smoothing: int = 3,
    d_smoothing: int = 3,
) -> pd.DataFrame:
    effective_length = max(2, int(length))
    lowest_low = frame["low"].rolling(effective_length, min_periods=1).min()
    highest_high = frame["high"].rolling(effective_length, min_periods=1).max()
    denominator = (highest_high - lowest_low).replace(0.0, float("nan"))
    rsv = ((frame["close"] - lowest_low) / denominator) * 100.0
    rsv = rsv.clip(lower=0.0, upper=100.0).fillna(50.0)

    k_alpha = 1 / max(1, int(k_smoothing))
    d_alpha = 1 / max(1, int(d_smoothing))
    k_value = rsv.ewm(alpha=k_alpha, adjust=False).mean().fillna(50.0)
    d_value = k_value.ewm(alpha=d_alpha, adjust=False).mean().fillna(50.0)
    j_value = (3.0 * k_value - 2.0 * d_value).astype(float)

    return pd.DataFrame(
        {
            "k": k_value.astype(float),
            "d": d_value.astype(float),
            "j": j_value.astype(float),
            "rsv": rsv.astype(float),
        }
    )


def compute_obv(frame: pd.DataFrame) -> pd.Series:
    close = frame["close"]
    volume = frame["volume"].fillna(0.0)
    direction = close.diff().fillna(0.0).map(lambda value: 1.0 if value > 0 else -1.0 if value < 0 else 0.0)
    obv = (direction * volume).cumsum().astype(float)
    if not obv.empty:
        obv.iloc[0] = 0.0
    return obv


def compute_obv_slope_angle(frame: pd.DataFrame, *, window: int = 8, obv: pd.Series | None = None) -> float:
    if len(frame) == 0:
        return 0.0

    effective_window = max(2, min(int(window), len(frame)))
    recent_obv = (obv if obv is not None else compute_obv(frame)).tail(effective_window).reset_index(drop=True)
    average_volume = float(frame["volume"].tail(effective_window).mean())
    if average_volume <= 0:
        return 0.0

    normalized_obv = recent_obv / average_volume
    x_values = list(range(len(normalized_obv)))
    x_mean = sum(x_values) / len(x_values)
    y_mean = float(normalized_obv.mean())
    numerator = sum((x - x_mean) * (float(y) - y_mean) for x, y in zip(x_values, normalized_obv))
    denominator = sum((x - x_mean) ** 2 for x in x_values)
    if denominator <= 0:
        return 0.0

    slope = numerator / denominator
    return float(math.degrees(math.atan(slope)))


def compute_obv_confirmation_snapshot(
    frame: pd.DataFrame,
    *,
    obv: pd.Series | None = None,
    sma_period: int = 50,
    zscore_window: int = 100,
) -> OBVConfirmationSnapshot:
    if len(frame) == 0:
        return OBVConfirmationSnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    obv_series = (obv if obv is not None else compute_obv(frame)).astype(float)
    sma_window = max(1, int(sma_period))
    z_window = max(2, int(zscore_window))

    obv_sma = obv_series.rolling(sma_window, min_periods=1).mean().astype(float)
    increments = obv_series.diff().fillna(0.0).astype(float)
    increment_mean = increments.rolling(z_window, min_periods=2).mean().fillna(0.0)
    increment_std = increments.rolling(z_window, min_periods=2).std(ddof=0).fillna(0.0)
    zscore = ((increments - increment_mean) / increment_std.replace(0.0, float("nan"))).replace([float("inf"), -float("inf")], 0.0).fillna(0.0)

    return OBVConfirmationSnapshot(
        current_obv=float(obv_series.iloc[-1]),
        sma_value=float(obv_sma.iloc[-1]),
        increment_value=float(increments.iloc[-1]),
        increment_mean=float(increment_mean.iloc[-1]),
        increment_std=float(increment_std.iloc[-1]),
        zscore=float(zscore.iloc[-1]),
    )


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


def _manual_dmi(frame: pd.DataFrame, length: int) -> pd.DataFrame:
    high = frame["high"]
    low = frame["low"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    atr = _manual_atr(frame, length).replace(0.0, float("nan"))

    plus_di = (100 * plus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr).fillna(0.0)
    minus_di = (100 * minus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr).fillna(0.0)
    denominator = (plus_di + minus_di).replace(0, float("nan"))
    dx = (((plus_di - minus_di).abs() / denominator) * 100).astype(float).fillna(0.0)
    adx = dx.ewm(alpha=1 / length, adjust=False).mean().fillna(0.0)
    return pd.DataFrame(
        {
            "adx": adx.astype(float),
            "plus_di": plus_di.astype(float),
            "minus_di": minus_di.astype(float),
        }
    )


def _dmi(frame: pd.DataFrame, length: int) -> pd.DataFrame:
    manual = _manual_dmi(frame, length)
    if ta is not None:
        adx_frame = ta.adx(frame["high"], frame["low"], frame["close"], length=length)
        adx_column = f"ADX_{length}"
        if adx_frame is not None and adx_column in adx_frame:
            manual["adx"] = adx_frame[adx_column].fillna(0.0).astype(float)
    return manual


def _manual_adx(frame: pd.DataFrame, length: int) -> pd.Series:
    return _manual_dmi(frame, length)["adx"]


def _adx(frame: pd.DataFrame, length: int) -> pd.Series:
    return _dmi(frame, length)["adx"]


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


def _clamp_ratio(value: float, *, minimum: float = 0.1, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, float(value)))


def recent_frame(frame: pd.DataFrame, lookback_hours: int = 24) -> pd.DataFrame:
    if len(frame) == 0:
        return frame
    cutoff = frame["timestamp"].iloc[-1] - pd.Timedelta(hours=max(1, int(lookback_hours)))
    recent = frame[frame["timestamp"] >= cutoff].copy()
    return recent if not recent.empty else frame.copy()


def compute_volume_profile(
    frame: pd.DataFrame,
    *,
    lookback_hours: int = 24,
    value_area_pct: float = 0.70,
    bin_count: int = 24,
) -> VolumeProfileSnapshot | None:
    scoped_frame = recent_frame(frame, lookback_hours=lookback_hours)
    if scoped_frame.empty:
        return None

    low_price = float(scoped_frame["low"].min())
    high_price = float(scoped_frame["high"].max())
    total_volume = float(scoped_frame["volume"].clip(lower=0.0).sum())
    if total_volume <= 0:
        return None

    effective_bin_count = max(8, int(bin_count))
    price_range = high_price - low_price
    if price_range <= 0:
        price_range = max(abs(high_price), 1.0) * 0.001
    bin_size = price_range / effective_bin_count
    profile = [0.0 for _ in range(effective_bin_count)]

    for candle in scoped_frame.itertuples(index=False):
        candle_low = float(min(candle.low, candle.high))
        candle_high = float(max(candle.low, candle.high))
        candle_volume = max(0.0, float(candle.volume))
        if candle_volume <= 0:
            continue

        start_index = int((candle_low - low_price) / bin_size) if bin_size > 0 else 0
        end_index = int((candle_high - low_price) / bin_size) if bin_size > 0 else 0
        start_index = max(0, min(effective_bin_count - 1, start_index))
        end_index = max(0, min(effective_bin_count - 1, end_index))
        touched_bins = max(1, end_index - start_index + 1)
        distributed_volume = candle_volume / touched_bins
        for index in range(start_index, end_index + 1):
            profile[index] += distributed_volume

    poc_index = max(range(effective_bin_count), key=lambda index: profile[index])
    target_volume = total_volume * _clamp_ratio(value_area_pct)
    selected_bins = {poc_index}
    value_area_volume = profile[poc_index]
    left_index = poc_index - 1
    right_index = poc_index + 1

    while value_area_volume < target_volume and (left_index >= 0 or right_index < effective_bin_count):
        left_volume = profile[left_index] if left_index >= 0 else -1.0
        right_volume = profile[right_index] if right_index < effective_bin_count else -1.0
        if right_volume > left_volume:
            selected_bins.add(right_index)
            value_area_volume += right_volume
            right_index += 1
        else:
            selected_bins.add(left_index)
            value_area_volume += left_volume
            left_index -= 1

    value_area_low_index = min(selected_bins)
    value_area_high_index = max(selected_bins)

    def _bin_left(index: int) -> float:
        return low_price + index * bin_size

    def _bin_right(index: int) -> float:
        return low_price + (index + 1) * bin_size

    poc_price = low_price + (poc_index + 0.5) * bin_size
    return VolumeProfileSnapshot(
        poc_price=float(poc_price),
        value_area_low=float(_bin_left(value_area_low_index)),
        value_area_high=float(_bin_right(value_area_high_index)),
        total_volume=total_volume,
        value_area_volume=float(value_area_volume),
        low_price=low_price,
        high_price=high_price,
        bin_size=float(bin_size),
        bin_count=effective_bin_count,
    )


def compute_indicator_snapshot(
    ohlcv: list[list[float]],
    adx_length: int = 14,
    bb_length: int = 20,
    bb_std: float = 2.0,
) -> IndicatorSnapshot:
    frame = ohlcv_to_dataframe(ohlcv)
    if len(frame) < max(adx_length * 3, bb_length + 2):
        raise ValueError("Not enough OHLCV data to compute indicators reliably")

    dmi_frame = _dmi(frame, adx_length)
    adx_series = dmi_frame["adx"]
    bb_width_series = _bollinger_width(frame, bb_length, bb_std)
    volatility_series = _realized_volatility(frame, bb_length)

    return IndicatorSnapshot(
        adx_value=float(adx_series.iloc[-1]),
        adx_previous=float(adx_series.iloc[-2]),
        adx_pre_previous=float(adx_series.iloc[-3]),
        plus_di_value=float(dmi_frame["plus_di"].iloc[-1]),
        minus_di_value=float(dmi_frame["minus_di"].iloc[-1]),
        bb_width=float(bb_width_series.iloc[-1]),
        bb_width_previous=float(bb_width_series.iloc[-2]),
        volatility=float(volatility_series.iloc[-1]),
    )
