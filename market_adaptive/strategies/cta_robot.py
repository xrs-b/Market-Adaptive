from __future__ import annotations

import pandas as pd

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.indicators import ohlcv_to_dataframe
from market_adaptive.strategies.base import BaseStrategyRobot


class CTARobot(BaseStrategyRobot):
    strategy_name = "cta"
    activation_status = "trend"

    def __init__(self, client, database, config: CTAConfig, execution_config: ExecutionConfig, notifier=None) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config

    def execute_active_cycle(self) -> str:
        ohlcv = self.client.fetch_ohlcv(
            symbol=self.config.symbol,
            timeframe=self.config.timeframe,
            limit=self.config.lookback_limit,
        )
        frame = ohlcv_to_dataframe(ohlcv)
        frame["ema_fast"] = frame["close"].ewm(span=self.config.fast_ema, adjust=False).mean()
        frame["ema_slow"] = frame["close"].ewm(span=self.config.slow_ema, adjust=False).mean()

        latest = frame.iloc[-1]
        previous = frame.iloc[-2]
        bullish_cross = previous["ema_fast"] <= previous["ema_slow"] and latest["ema_fast"] > latest["ema_slow"]
        bearish_cross = previous["ema_fast"] >= previous["ema_slow"] and latest["ema_fast"] < latest["ema_slow"]

        if bullish_cross:
            self.client.place_market_order(self.symbol, "buy", self.execution_config.cta_order_size)
            return "cta:market_buy"
        if bearish_cross:
            self.client.place_market_order(self.symbol, "sell", self.execution_config.cta_order_size)
            return "cta:market_sell"
        return "cta:no_cross"
