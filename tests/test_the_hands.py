from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from market_adaptive.config import CTAConfig, ExecutionConfig, GridConfig, SentimentConfig
from market_adaptive.coordination import StrategyRuntimeContext
from market_adaptive.db import DatabaseInitializer, MarketStatusRecord
from market_adaptive.indicators import OBVConfirmationSnapshot
from market_adaptive.sentiment import SentimentAnalyst
from market_adaptive.strategies import CTARobot, GridRobot, HandsCoordinator
from market_adaptive.strategies.cta_robot import ManagedPosition, TrendSignal
from market_adaptive.strategies.order_flow_sentinel import OrderFlowAssessment
from market_adaptive.strategies.mtf_engine import MultiTimeframeSignalEngine


class DummyClient:
    def __init__(self) -> None:
        self.market_orders = []
        self.limit_orders = []
        self.cancel_all_calls = []
        self.cancel_order_calls = []
        self.close_all_calls = []
        self.futures_settings_calls = []
        self.last_price = 100.0
        self.ohlcv = []
        self.ohlcv_by_timeframe = {}
        self.positions = []
        self.latest_long_short_ratio = None
        self.raise_on_limit_order_at = None
        self._limit_order_seq = 0
        self.fetch_order_responses = {}
        self.fetch_order_calls = []
        self.order_book = {
            "bids": [[100.0 - index * 0.1, 1.6] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 1.0] for index in range(20)],
        }
        self.total_equity = 95_000.0
        self.min_order_amount = 0.0
        self.contract_value = 1.0

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        del symbol, since
        payload = self.ohlcv_by_timeframe.get(timeframe, self.ohlcv)
        return payload[-limit:]

    def fetch_last_price(self, symbol: str) -> float:
        del symbol
        return self.last_price

    def fetch_order_book(self, symbol: str, limit: int | None = None):
        del symbol
        if limit is None:
            return self.order_book
        return {
            "bids": list(self.order_book.get("bids", []))[:limit],
            "asks": list(self.order_book.get("asks", []))[:limit],
        }

    def place_market_order(self, symbol: str, side: str, amount: float, **kwargs):
        payload = {"symbol": symbol, "side": side, "amount": amount, **kwargs}
        self.market_orders.append(payload)
        return payload

    def place_limit_order(self, symbol: str, side: str, amount: float, price: float, **kwargs):
        if self.raise_on_limit_order_at is not None and len(self.limit_orders) >= self.raise_on_limit_order_at:
            raise RuntimeError("limit order placement failed")
        self._limit_order_seq += 1
        payload = {"id": f"order-{self._limit_order_seq}", "symbol": symbol, "side": side, "amount": amount, "price": price, "status": "open", **kwargs}
        self.limit_orders.append(payload)
        return payload

    def cancel_order(self, order_id: str, symbol: str):
        self.cancel_order_calls.append((order_id, symbol))
        self.limit_orders = [order for order in self.limit_orders if order.get("id") != order_id]
        return {"id": order_id, "symbol": symbol, "status": "canceled"}

    def cancel_all_orders(self, symbol: str):
        self.cancel_all_calls.append(symbol)
        self.limit_orders = []
        return []

    def close_all_positions(self, symbol: str):
        self.close_all_calls.append(symbol)
        return []

    def fetch_positions(self, symbols=None):
        del symbols
        return list(self.positions)

    def fetch_open_orders(self, symbol: str):
        del symbol
        return list(self.limit_orders)

    def fetch_order(self, order_id: str, symbol: str):
        del symbol
        self.fetch_order_calls.append(order_id)
        responses = self.fetch_order_responses.get(order_id)
        if isinstance(responses, list):
            if not responses:
                return None
            if len(responses) == 1:
                return responses[0]
            return responses.pop(0)
        return responses

    def ensure_futures_settings(self, symbol: str, leverage: int, margin_mode: str | None = None) -> None:
        self.futures_settings_calls.append(
            {"symbol": symbol, "leverage": leverage, "margin_mode": margin_mode}
        )

    def fetch_total_equity(self, quote_currency: str = "USDT") -> float:
        del quote_currency
        return self.total_equity

    def get_min_order_amount(self, symbol: str) -> float:
        del symbol
        return self.min_order_amount

    def get_position_liquidation_price(self, position: dict) -> float | None:
        liquidation_price = position.get("liquidationPrice") or position.get("info", {}).get("liqPx")
        if liquidation_price in (None, "", 0, "0"):
            return None
        return abs(float(liquidation_price))

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        del symbol
        return round(float(amount), 8)

    def price_to_precision(self, symbol: str, price: float) -> float:
        del symbol
        return round(float(price), 8)

    def get_contract_value(self, symbol: str) -> float:
        del symbol
        return 1.0

    def estimate_notional(self, symbol: str, amount: float, price: float) -> float:
        del symbol
        return abs(float(amount)) * abs(float(price)) * float(self.contract_value)

    def fetch_latest_long_short_account_ratio(self, symbol: str, timeframe: str = "5m", limit: int = 1):
        del symbol, timeframe, limit
        return self.latest_long_short_ratio


class DummyRiskManager:
    def __init__(self, *, allow_grid: bool = True, grid_reason: str | None = None) -> None:
        self.allow_grid = allow_grid
        self.grid_reason = grid_reason
        self.cta_profiles = []
        self.grid_profiles = []
        self.cleanup_requests = []
        self.position_size_calls = []

    def calculate_position_size(self, symbol: str, risk_percent: float, stop_loss_atr: float, *, atr_value=None, last_price=None) -> float:
        self.position_size_calls.append(
            {
                "symbol": symbol,
                "risk_percent": risk_percent,
                "stop_loss_atr": stop_loss_atr,
                "atr_value": atr_value,
                "last_price": last_price,
            }
        )
        return 0.02

    def can_open_new_position(self, symbol: str, requested_notional: float, strategy_name: str | None = None, opening_side: str | None = None):
        del symbol, requested_notional, opening_side
        if strategy_name == "grid" and not self.allow_grid:
            return False, self.grid_reason
        return True, None

    def check_symbol_notional_limit(self, symbol: str, requested_notional: float):
        del symbol, requested_notional
        return True, None

    def check_directional_exposure_limit(self, requested_notional: float, opening_side: str | None):
        del requested_notional, opening_side
        return True, None

    def coordinate_strategy_cleanup(self, strategy_name: str, reason: str):
        self.cleanup_requests.append((strategy_name, reason))
        return None

    def update_cta_risk(self, profile) -> None:
        self.cta_profiles.append(profile)

    def update_grid_risk(self, profile) -> None:
        self.grid_profiles.append(profile)


class TheHandsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = DatabaseInitializer(Path(self.temp_dir.name) / "market_adaptive.sqlite3")
        self.database.initialize()
        self.client = DummyClient()
        self.execution = ExecutionConfig(cta_order_size=0.02, grid_order_size=0.03)
        self.cta_config = CTAConfig(
            symbol="BTC/USDT",
            major_timeframe="4h",
            swing_timeframe="1h",
            execution_timeframe="15m",
            lower_timeframe="15m",
            higher_timeframe="1h",
            atr_trailing_multiplier=1.0,
            stop_loss_atr=2.0,
            first_take_profit_size=0.25,
            second_take_profit_size=0.25,
        )
        self.grid_config = GridConfig(
            symbol="BTC/USDT",
            timeframe="1h",
            lookback_limit=80,
            bollinger_period=20,
            bollinger_std=2.0,
            levels=8,
            leverage=5,
            martingale_factor=1.5,
            trigger_window_seconds=300,
            trigger_limit_per_layer=3,
            layer_cooldown_seconds=300,
            rebalance_exposure_threshold=2.0,
            max_rebalance_orders=2,
            range_percent=0.03,
            liquidation_protection_ratio=0.05,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _insert_status(self, status: str, timestamp: str = "2026-04-10T03:00:00+00:00") -> None:
        self.database.insert_market_status(
            MarketStatusRecord(
                timestamp=timestamp,
                symbol="BTC/USDT",
                status=status,
                adx_value=25.0,
                volatility=0.02,
            )
        )

    def _set_ohlcv(self, timeframe: str, closes: list[float], step_ms: int) -> None:
        base = 1_700_000_000_000
        payload = []
        for index, close in enumerate(closes):
            payload.append([base + index * step_ms, close - 0.3, close + 0.4, close - 0.5, close, 100 + index * 3])
        self.client.ohlcv_by_timeframe[timeframe] = payload

    def _set_bullish_higher_timeframes(self, swing_last_close: float = 140.0, major_last_close: float = 220.0) -> None:
        swing_closes = [swing_last_close - 1.0 * (59 - index) for index in range(60)]
        major_closes = [major_last_close - 2.0 * (59 - index) for index in range(60)]
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("4h", major_closes, 14_400_000)

    def _set_order_book(self, *, bid_size: float, ask_size: float, ask_levels=None) -> None:
        bids = [[100.0 - index * 0.1, bid_size] for index in range(20)]
        asks = ask_levels or [[100.1 + index * 0.1, ask_size] for index in range(20)]
        self.client.order_book = {"bids": bids, "asks": asks}

    def _load_bullish_signal(self, lower_last_close: float = 100.0, higher_last_close: float = 140.0) -> None:
        lower_closes = []
        base_price = lower_last_close - 8.0
        pattern = [0.0, 0.4, -0.3, 0.5, -0.2, 0.3, -0.1, 0.2]
        for index in range(112):
            lower_closes.append(base_price + pattern[index % len(pattern)])
        lower_closes.extend(
            [
                lower_last_close - 5.6,
                lower_last_close - 4.8,
                lower_last_close - 5.2,
                lower_last_close - 4.6,
                lower_last_close - 7.0,
                lower_last_close - 6.5,
                lower_last_close - 6.0,
                lower_last_close,
            ]
        )
        base = 1_700_000_000_000
        payload = []
        for index, close in enumerate(lower_closes):
            volume = 100 + index * 2
            if index >= len(lower_closes) - 4:
                volume *= 8
            payload.append([base + index * 900_000, close - 0.3, close + 0.4, close - 0.5, close, volume])
        self.client.ohlcv_by_timeframe["15m"] = payload
        self._set_bullish_higher_timeframes(swing_last_close=higher_last_close)

    def _load_pullback_after_rally(self, latest_close: float) -> None:
        closes = [80 + index * 0.45 for index in range(56)] + [103.6, 104.8, 106.0, latest_close]
        self._set_ohlcv("15m", closes, 900_000)
        self._set_bullish_higher_timeframes()

    def _load_bullish_signal_inside_value_area(self, lower_last_close: float = 100.0) -> None:
        closes = []
        for index in range(40):
            closes.append(lower_last_close + 0.7 + (0.2 if index % 2 == 0 else -0.2))
        for index in range(14):
            closes.append(lower_last_close + 0.4 + (0.1 if index % 2 == 0 else -0.1))
        closes.extend(
            [
                lower_last_close - 0.2,
                lower_last_close - 0.3,
                lower_last_close - 0.1,
                lower_last_close,
                lower_last_close + 0.1,
                lower_last_close + 0.2,
            ]
        )
        self._set_ohlcv("15m", closes, 900_000)
        self._set_bullish_higher_timeframes()

    def _load_bullish_ready_without_execution_trigger(self) -> None:
        closes = [90 + index * 0.25 for index in range(55)] + [104.0, 103.4, 102.9, 102.4, 101.9]
        self._set_ohlcv("15m", closes, 900_000)
        self._set_bullish_higher_timeframes()

    def _load_execution_window_prices(self, *, closes: list[float]) -> None:
        self._set_ohlcv("15m", closes, 900_000)
        self._set_bullish_higher_timeframes()

    def _mock_execution_kdj(self, bars: int, golden_cross_bar_from_end: int) -> pd.DataFrame:
        k_values = [40.0] * bars
        d_values = [50.0] * bars
        cross_index = bars - golden_cross_bar_from_end - 1
        k_values[cross_index - 1] = 45.0
        d_values[cross_index - 1] = 50.0
        k_values[cross_index] = 55.0
        d_values[cross_index] = 50.0
        for index in range(cross_index + 1, bars):
            k_values[index] = 56.0
            d_values[index] = 51.0
        return pd.DataFrame({"k": k_values, "d": d_values})

    def _set_sentiment_ratio(self, ratio: float, timestamp: int = 1_712_722_800_000) -> None:
        self.client.latest_long_short_ratio = {
            "timestamp": timestamp,
            "longShortRatio": ratio,
        }

    def _build_sentiment_analyst(self, **overrides) -> SentimentAnalyst:
        config = SentimentConfig(enabled=True, **overrides)
        return SentimentAnalyst(self.client, config)

    def _load_sideways_grid_data(self, center: float = 100.0, width: float = 4.0, length: int = 60) -> None:
        closes = []
        pattern = [0.0, 1.0, -0.8, 0.7, -0.6, 0.4, -0.3, 0.2]
        for index in range(length):
            closes.append(center + pattern[index % len(pattern)] * width / 2.0)
        self._set_ohlcv("1h", closes, 3_600_000)

    def test_cta_robot_opens_long_only_in_trend(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.action, "cta:open_long")
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertEqual(self.client.market_orders[0]["side"], "buy")

    def test_cta_robot_uses_stop_loss_atr_for_initial_dynamic_stop(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        robot.run()

        self.assertIsNotNone(robot.position)
        signal = robot._build_trend_signal()
        assert signal is not None
        expected_multiplier = robot._resolve_dynamic_stop_loss_multiplier(signal)
        expected_stop = robot.position.entry_price - robot.position.atr_value * expected_multiplier
        self.assertAlmostEqual(robot.position.stop_price, expected_stop)

    def test_cta_robot_dynamic_stop_loss_multiplier_tightens_for_higher_score(self) -> None:
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        low_score_signal = TrendSignal(direction=1, raw_direction=1, major_direction=1, bullish_score=55.0)
        high_score_signal = TrendSignal(direction=1, raw_direction=1, major_direction=1, bullish_score=90.0)

        low_multiplier = robot._resolve_dynamic_stop_loss_multiplier(low_score_signal)
        high_multiplier = robot._resolve_dynamic_stop_loss_multiplier(high_score_signal)

        self.assertLess(high_multiplier, low_multiplier)
        self.assertLess(high_multiplier, self.cta_config.stop_loss_atr)

    def test_cta_robot_enters_bullish_ready_state_before_execution_trigger(self) -> None:
        self._insert_status("trend")
        self._load_bullish_ready_without_execution_trigger()
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.action, "cta:bullish_ready")
        self.assertEqual(len(self.client.market_orders), 0)
        self.assertIsNone(robot.position)

    def test_cta_memory_window_unlocks_later_breakout(self) -> None:
        self._load_execution_window_prices(closes=[100.0] * 56 + [100.4, 100.6, 100.8, 101.4])
        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=3)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertTrue(signal.execution_trigger.bullish_memory_active)
        self.assertEqual(signal.execution_trigger.bullish_cross_bars_ago, 3)
        self.assertTrue(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.fully_aligned)
        self.assertEqual(
            signal.execution_trigger.reason,
            "Triggered via Memory Window: KDJ crossed 3 bars ago + Price Breakout NOW",
        )

    def test_cta_memory_window_blocks_outside_window_breakout(self) -> None:
        self._load_execution_window_prices(closes=[100.0] * 56 + [100.4, 100.6, 100.8, 101.4])
        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=6)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertFalse(signal.execution_trigger.bullish_memory_active)
        self.assertEqual(signal.execution_trigger.bullish_cross_bars_ago, 6)
        self.assertTrue(signal.execution_trigger.prior_high_break)
        self.assertTrue(signal.fully_aligned)
        self.assertIn("breakout", signal.execution_trigger.reason)

    def test_cta_robot_skips_bullish_entry_while_price_is_still_inside_value_area(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal_inside_value_area(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertNotEqual(result.action, "cta:open_long")
        self.assertEqual(len(self.client.market_orders), 0)
        self.assertIsNone(robot.position)

    def test_cta_robot_high_conviction_entry_can_override_strict_obv_breakout_strength(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        strict_config = CTAConfig(
            symbol="BTC/USDT",
            lower_timeframe="15m",
            higher_timeframe="1h",
            atr_trailing_multiplier=1.0,
            stop_loss_atr=2.0,
            first_take_profit_size=0.25,
            second_take_profit_size=0.25,
            obv_zscore_threshold=999.0,
        )
        robot = CTARobot(self.client, self.database, strict_config, self.execution)

        result = robot.run()

        self.assertEqual(result.action, "cta:open_long")
        self.assertGreater(len(self.client.market_orders), 0)

    def test_cta_robot_blocks_bullish_entry_when_retail_sentiment_is_extreme(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        self._set_sentiment_ratio(3.1)
        robot = CTARobot(
            self.client,
            self.database,
            self.cta_config,
            self.execution,
            sentiment_analyst=self._build_sentiment_analyst(),
        )

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.action, "cta:sentiment_blocked")
        self.assertEqual(len(self.client.market_orders), 0)
        self.assertIsNone(robot.position)

    def test_cta_robot_can_halve_bullish_entry_when_sentiment_policy_requests_it(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        self._set_sentiment_ratio(3.1)
        robot = CTARobot(
            self.client,
            self.database,
            self.cta_config,
            self.execution,
            sentiment_analyst=self._build_sentiment_analyst(cta_buy_action="halve"),
        )

        result = robot.run()

        self.assertEqual(result.action, "cta:open_long_sentiment_halved")
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertAlmostEqual(self.client.market_orders[0]["amount"], 71.25)
        self.assertIsNotNone(robot.position)
        self.assertAlmostEqual(robot.position.initial_size, 71.25)

    def test_cta_robot_uses_dynamic_fixed_fraction_position_sizing(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertEqual(result.action, "cta:open_long")
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertAlmostEqual(self.client.market_orders[0]["amount"], 142.5)
        self.assertIsNotNone(robot.position)
        self.assertAlmostEqual(robot.position.risk_percent, self.cta_config.boosted_risk_percent_per_trade)

    def test_cta_robot_scales_early_bullish_starter_position_to_thirty_percent_of_normal_size(self) -> None:
        self._insert_status("trend")
        major_closes = [200 - 0.5 * index for index in range(60)]
        swing_closes = [100.0 + 0.2 * index for index in range(60)]
        execution_closes = [100.0] * 59 + [106.0]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        self.cta_config.early_entry_minimum_score = 0.0
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=10)

        import pandas as pd
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 58 + [104.0, 104.3],
                "upper_band": [110.0] * 60,
                "supertrend": [110.0] * 60,
                "atr": [2.0] * 60,
            }
        )
        swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 58 + [1, 1],
                "lower_band": [99.0] * 60,
                "upper_band": [109.0] * 60,
                "supertrend": [99.0] * 60,
                "atr": [1.0] * 60,
            }
        )

        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, swing_supertrend, swing_supertrend],
            ),
        ):
            signal = engine.build_signal()
        assert signal is not None

        self.assertTrue(signal.early_bullish)
        self.assertEqual(signal.execution_entry_mode, "early_bullish_starter_limit")
        self.assertAlmostEqual(signal.entry_size_multiplier, self.cta_config.early_bullish_starter_fraction)

    def test_cta_robot_blocks_early_bullish_when_swing_direction_just_flipped(self) -> None:
        self._insert_status("trend")
        major_closes = [200 - 0.5 * index for index in range(60)]
        swing_closes = [100.0 + 0.2 * index for index in range(60)]
        execution_closes = [100.0] * 59 + [106.0]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000)
        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=10)

        import pandas as pd
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 58 + [104.0, 104.3],
                "upper_band": [110.0] * 60,
                "supertrend": [110.0] * 60,
                "atr": [2.0] * 60,
            }
        )
        unstable_swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 59 + [1],
                "lower_band": [99.0] * 60,
                "upper_band": [109.0] * 60,
                "supertrend": [99.0] * 60,
                "atr": [1.0] * 60,
            }
        )

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, unstable_swing_supertrend, unstable_swing_supertrend],
            ),
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertFalse(signal.early_bullish)
        self.assertNotEqual(signal.execution_entry_mode, "early_bullish_starter_limit")

    def test_cta_robot_blocks_early_bullish_when_score_is_below_floor(self) -> None:
        self._insert_status("trend")
        self.cta_config.early_entry_minimum_score = 90.0
        major_closes = [200 - 0.5 * index for index in range(60)]
        swing_closes = [100.0 + 0.2 * index for index in range(60)]
        execution_closes = [100.0] * 59 + [106.0]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000)
        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=10)

        import pandas as pd
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 58 + [104.0, 104.3],
                "upper_band": [110.0] * 60,
                "supertrend": [110.0] * 60,
                "atr": [2.0] * 60,
            }
        )
        swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 58 + [1, 1],
                "lower_band": [99.0] * 60,
                "upper_band": [109.0] * 60,
                "supertrend": [99.0] * 60,
                "atr": [1.0] * 60,
            }
        )

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, swing_supertrend, swing_supertrend],
            ),
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertFalse(signal.early_bullish)
        self.assertNotEqual(signal.execution_entry_mode, "early_bullish_starter_limit")

    def test_cta_robot_blocks_entry_when_order_flow_imbalance_fails_last_second_confirmation(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        self._set_order_book(bid_size=1.2, ask_size=1.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertEqual(result.action, "cta:order_flow_blocked")
        self.assertEqual(len(self.client.market_orders), 0)
        self.assertEqual(len(self.client.limit_orders), 0)
        self.assertIsNone(robot.position)

    def test_cta_reward_risk_filter_blocks_thin_profit_setup(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        signal = robot._build_trend_signal()
        assert signal is not None
        assert signal.volume_profile is not None
        signal.volume_profile.high_price = signal.price + 10.0
        robot.config.minimum_expected_rr = 5.0

        result = robot._open_position(signal)

        self.assertEqual(result, "cta:reward_risk_blocked")

    def test_cta_robot_allows_high_quality_entry_when_only_adaptive_order_flow_guard_fails(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        signal = robot._build_trend_signal()
        assert signal is not None
        assert signal.volume_profile is not None
        signal.volume_profile.high_price = signal.price + 600.0
        assessment = OrderFlowAssessment(
            symbol="BTC/USDT",
            side="buy",
            depth_levels=20,
            bid_sum=36.0,
            ask_sum=20.0,
            imbalance_ratio=1.8,
            best_bid=100.0,
            best_ask=100.1,
            confirmation_passed=False,
            high_conviction=False,
            recommended_limit_price=None,
            expected_average_price=None,
            depth_boundary_price=None,
            reason="imbalance_decay_detected",
            history_mean=2.1,
            history_sigma=0.2,
            health_floor=2.3,
            confirmation_threshold=2.3,
            high_conviction_threshold=2.3,
            decay_detected=True,
        )

        with patch.object(robot.order_flow_sentinel, "assess_entry", return_value=assessment):
            result = robot._open_position(signal)

        self.assertTrue(result.startswith("cta:open_long"))
        self.assertIsNotNone(robot.position)
        self.assertGreater(len(self.client.market_orders), 0)

    def test_cta_reward_risk_filter_allows_when_target_space_is_large(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        robot.config.order_flow_enabled = False
        signal = robot._build_trend_signal()
        assert signal is not None
        assert signal.volume_profile is not None
        signal.volume_profile.high_price = signal.price + 600.0
        robot.config.minimum_expected_rr = 1.2

        result = robot._open_position(signal)

        self.assertTrue(result.startswith("cta:open_long"))
        self.assertIsNotNone(robot.position)

    def test_cta_breakout_reward_risk_uses_atr_extension_target(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        robot.config.order_flow_enabled = False
        robot.config.minimum_expected_rr = 1.2
        robot.config.breakout_rr_target_atr_multiplier = 3.0
        signal = robot._build_trend_signal()
        assert signal is not None
        assert signal.volume_profile is not None
        signal.execution_entry_mode = "breakout_confirmed"
        signal.volume_profile.high_price = signal.price + 405.0
        signal.atr = 327.0
        signal.bullish_score = 85.0
        signal.direction = 1

        result = robot._open_position(signal)

        self.assertTrue(result.startswith("cta:open_long"))
        self.assertIsNotNone(robot.position)

    def test_cta_relaxed_entry_uses_higher_reward_risk_floor(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        robot.config.order_flow_enabled = False
        signal = robot._build_trend_signal()
        assert signal is not None
        assert signal.volume_profile is not None
        signal.relaxed_entry = True
        signal.volume_profile.high_price = signal.price + 0.5
        robot.config.minimum_expected_rr = 0.0
        robot.config.relaxed_entry_minimum_expected_rr = 2.0

        result = robot._open_position(signal)

        self.assertEqual(result, "cta:reward_risk_blocked")

    def test_cta_starter_entry_uses_stricter_reward_risk_floor(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        robot.config.order_flow_enabled = False
        signal = robot._build_trend_signal()
        assert signal is not None
        assert signal.volume_profile is not None
        signal.execution_entry_mode = "starter_frontrun_limit"
        signal.volume_profile.high_price = signal.price + 0.5
        robot.config.minimum_expected_rr = 0.0
        robot.config.relaxed_entry_minimum_expected_rr = 1.0
        robot.config.starter_entry_minimum_expected_rr = 2.0
        robot.config.starter_quality_minimum_score = 0.0
        robot.config.scale_in_quality_minimum_score = 0.0

        result = robot._open_position(signal)

        self.assertEqual(result, "cta:reward_risk_blocked")

    def test_cta_relaxed_entry_blocks_when_not_near_breakout(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        robot.config.order_flow_enabled = False
        signal = robot._build_trend_signal()
        assert signal is not None
        signal.relaxed_entry = True
        signal.execution_breakout = False
        signal.execution_frontrun_near_breakout = False

        result = robot._open_position(signal)

        self.assertEqual(result, "cta:entry_location_blocked")

    def test_cta_starter_entry_blocks_when_not_near_breakout(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        robot.config.order_flow_enabled = False
        signal = robot._build_trend_signal()
        assert signal is not None
        signal.execution_entry_mode = "starter_frontrun_limit"
        signal.execution_breakout = False
        signal.execution_frontrun_near_breakout = False

        result = robot._open_position(signal)

        self.assertEqual(result, "cta:entry_location_blocked")

    def test_cta_robot_logs_trade_close_on_full_exit(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        robot.run()
        self.assertIsNotNone(robot.position)
        self.client.last_price = float(robot.position.entry_price) + 2.0

        with self.assertLogs("market_adaptive.strategies.cta_robot", level="INFO") as captured:
            robot._close_remaining_position("manual_test_exit")

        self.assertTrue(any("[TRADE_CLOSE]" in message for message in captured.output))
        self.assertTrue(any("manual_test_exit" in message for message in captured.output))

    def test_cta_robot_uses_aggressive_ioc_limit_for_high_conviction_entry(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        self._set_order_book(
            bid_size=2.4,
            ask_size=1.0,
            ask_levels=[
                [100.1, 0.005],
                [100.2, 0.005],
                [100.3, 0.050],
            ] + [[100.4 + index * 0.1, 1.0] for index in range(17)],
        )
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertEqual(result.action, "cta:open_long_limit")
        self.assertEqual(len(self.client.market_orders), 0)
        self.assertEqual(len(self.client.limit_orders), 1)
        self.assertEqual(self.client.limit_orders[0]["params"]["timeInForce"], "IOC")
        self.assertEqual(self.client.limit_orders[0]["params"]["executionMode"], "aggressive_limit")
        self.assertAlmostEqual(self.client.limit_orders[0]["price"], 100.3)
        self.assertIsNotNone(robot.position)

    def test_cta_robot_chases_partial_ioc_fill_with_market_order_and_uses_weighted_entry_price(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        self._set_order_book(
            bid_size=2.4,
            ask_size=1.0,
            ask_levels=[
                [100.1, 0.005],
                [100.2, 0.005],
                [100.3, 0.050],
            ] + [[100.4 + index * 0.1, 1.0] for index in range(17)],
        )
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        self.client.fetch_order_responses["order-1"] = [
            {
                "id": "order-1",
                "symbol": "BTC/USDT",
                "side": "buy",
                "amount": 0.02,
                "price": 100.22,
                "filled": 0.008,
                "average": 100.22,
                "remaining": 0.012,
                "status": "canceled",
                "info": {"accFillSz": "0.008", "avgPx": "100.22"},
            }
        ]

        original_market_order = self.client.place_market_order

        def partial_market_order(symbol: str, side: str, amount: float, **kwargs):
            payload = original_market_order(symbol, side, amount, **kwargs)
            payload.update({"filled": amount, "average": 100.6, "status": "closed", "remaining": 0.0})
            return payload

        self.client.place_market_order = partial_market_order

        result = robot.run()

        self.assertEqual(result.action, "cta:open_long_limit")
        self.assertEqual(len(self.client.limit_orders), 1)
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertAlmostEqual(self.client.market_orders[0]["amount"], 142.492)
        self.assertIsNotNone(robot.position)
        expected_price = ((0.008 * 100.22) + (142.492 * 100.6)) / 142.5
        self.assertAlmostEqual(robot.position.entry_price, expected_price)
        self.assertAlmostEqual(robot.position.initial_size, 142.5)

    def test_extract_filled_amount_returns_zero_for_zero_fill_ioc_orders(self) -> None:
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        filled = robot._extract_filled_amount(
            {
                "id": "order-1",
                "amount": 0.02,
                "filled": 0.0,
                "remaining": 0.02,
                "status": "open",
            },
            0.02,
            used_limit_order=True,
        )

        self.assertEqual(filled, 0.0)

    def test_cta_robot_returns_low_fill_ratio_when_ioc_limit_order_does_not_fill(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        self._set_order_book(
            bid_size=2.4,
            ask_size=1.0,
            ask_levels=[
                [100.1, 0.005],
                [100.2, 0.005],
                [100.3, 0.050],
            ] + [[100.4 + index * 0.1, 1.0] for index in range(17)],
        )
        self.client.fetch_order_responses["order-1"] = [
            {
                "id": "order-1",
                "symbol": "BTC/USDT",
                "side": "buy",
                "amount": 0.02,
                "price": 100.22,
                "filled": 0.0,
                "remaining": 0.02,
                "status": "canceled",
                "info": {"accFillSz": "0"},
            }
        ]
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertEqual(result.action, "cta:open_long_limit")
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertIsNotNone(robot.position)

    def test_cta_robot_returns_low_fill_ratio_when_ioc_limit_fill_stays_below_half(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        self._set_order_book(
            bid_size=2.4,
            ask_size=1.0,
            ask_levels=[
                [100.1, 0.005],
                [100.2, 0.005],
                [100.3, 0.050],
            ] + [[100.4 + index * 0.1, 1.0] for index in range(17)],
        )
        self.client.fetch_order_responses["order-1"] = [
            {
                "id": "order-1",
                "symbol": "BTC/USDT",
                "side": "buy",
                "amount": 0.02,
                "price": 100.22,
                "filled": 0.009,
                "average": 100.22,
                "remaining": 0.011,
                "status": "canceled",
                "info": {"accFillSz": "0.009", "avgPx": "100.22"},
            }
        ]

        original_market_order = self.client.place_market_order

        def partial_market_order(symbol: str, side: str, amount: float, **kwargs):
            payload = original_market_order(symbol, side, amount, **kwargs)
            payload.update({"filled": 0.0, "average": 100.6, "status": "open", "remaining": amount})
            return payload

        self.client.place_market_order = partial_market_order
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertEqual(result.action, "cta:low_fill_ratio")
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertIsNone(robot.position)

    def test_cta_robot_scales_out_and_all_outs_on_atr_stop(self) -> None:
        self._insert_status("trend")
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        self._load_bullish_signal(lower_last_close=100.0)
        first_result = robot.run()
        self.assertEqual(first_result.action, "cta:open_long")

        self._load_bullish_signal(lower_last_close=102.5)
        second_result = robot.run()
        self.assertEqual(second_result.action, "cta:take_profit_2pct")
        self.assertEqual(self.client.market_orders[1]["side"], "sell")
        self.assertTrue(self.client.market_orders[1]["reduce_only"])
        self.assertAlmostEqual(self.client.market_orders[1]["amount"], 35.625)

        self._load_bullish_signal(lower_last_close=106.0)
        third_result = robot.run()
        self.assertEqual(third_result.action, "cta:take_profit_5pct")
        self.assertAlmostEqual(self.client.market_orders[2]["amount"], 35.625)
        self.assertIsNotNone(robot.position)

        robot.position.stop_price = 104.0
        self._load_pullback_after_rally(latest_close=103.0)
        fourth_result = robot.run()
        self.assertEqual(fourth_result.action, "cta:atr_stop_all_out")
        self.assertAlmostEqual(self.client.market_orders[3]["amount"], 71.25)
        self.assertIsNone(robot.position)

    def test_cta_robot_signal_flip_reduces_first_then_exits_on_second_flip(self) -> None:
        self._insert_status("trend")
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        self._load_bullish_signal(lower_last_close=100.0)
        first_result = robot.run()
        self.assertEqual(first_result.action, "cta:open_long")
        self.assertIsNotNone(robot.position)

        robot._manage_position(TrendSignal(direction=-1, raw_direction=-1, major_direction=-1, price=99.0, atr=2.0))
        self.assertAlmostEqual(self.client.market_orders[1]["amount"], 71.25)
        self.assertTrue(robot._signal_flip_pending)
        self.assertIsNotNone(robot.position)

        actions, closed = robot._manage_position(TrendSignal(direction=-1, raw_direction=-1, major_direction=-1, price=98.0, atr=2.0))
        self.assertTrue(closed)
        self.assertIn("cta:signal_flip_exit", actions)
        self.assertAlmostEqual(self.client.market_orders[2]["amount"], 71.25)
        self.assertIsNone(robot.position)

    def test_grid_robot_uses_neutral_price_band_and_dynamic_per_level_sizing(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertIn("grid:placed_8_orders@100.00", result.action)
        self.assertIn("rebalances=0", result.action)
        self.assertEqual(len(self.client.limit_orders), 8)
        self.assertEqual(len(self.client.futures_settings_calls), 1)
        self.assertEqual(self.client.futures_settings_calls[0]["leverage"], 5)
        self.assertEqual(self.client.futures_settings_calls[0]["margin_mode"], "isolated")
        self.assertEqual(self.client.cancel_all_calls, [])

        buy_orders = [order for order in self.client.limit_orders if order["side"] == "buy"]
        sell_orders = [order for order in self.client.limit_orders if order["side"] == "sell"]
        self.assertEqual(len(buy_orders), 4)
        self.assertEqual(len(sell_orders), 4)
        self.assertTrue(all(order["amount"] == 47.5 for order in buy_orders + sell_orders))
        self.assertTrue(all(not order.get("reduce_only", False) for order in sell_orders))
        self.assertAlmostEqual(max(buy_orders, key=lambda order: order["price"])["price"], 98.75255730638212)
        self.assertAlmostEqual(min(sell_orders, key=lambda order: order["price"])["price"], 101.24744269361788)
        self.assertLess(max(buy_orders, key=lambda order: order["price"])["price"], 100.0)
        self.assertGreater(min(sell_orders, key=lambda order: order["price"])["price"], 100.0)


    def test_grid_robot_enforces_minimum_spacing_floor_of_point_seven_percent(self) -> None:
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)

        buy_prices, sell_prices = robot._derive_layer_prices(99.5, 100.0, 100.5)

        self.assertEqual(len(buy_prices), 4)
        self.assertEqual(len(sell_prices), 4)
        self.assertAlmostEqual(buy_prices[0], 99.3)
        self.assertAlmostEqual(sell_prices[0], 100.7)
        self.assertAlmostEqual(buy_prices[0] - buy_prices[1], 0.7)
        self.assertAlmostEqual(sell_prices[1] - sell_prices[0], 0.7)

    def test_grid_robot_uses_eight_level_grid_by_default(self) -> None:
        self.assertEqual(self.grid_config.levels, 8)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)
        context = robot._fallback_context(100.0, atr_value=0.0)

        self.assertEqual(len(context.buy_prices), 4)
        self.assertEqual(len(context.sell_prices), 4)

    def test_grid_robot_adapts_minimum_spacing_floor_to_half_atr(self) -> None:
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, atr_multiplier=1.0)
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 110.0, 90.0, 100.0, 120.0])

        context = robot._refresh_grid_context(100.0)

        self.assertIsNotNone(context)
        assert context is not None
        self.assertAlmostEqual(context.atr_value, 20.0, places=6)
        self.assertAlmostEqual(context.sell_prices[0] - context.center_price, 10.0, places=6)
        self.assertAlmostEqual(context.center_price - context.buy_prices[0], 10.0, places=6)

    def test_grid_robot_atr_spacing_floor_widens_dense_biased_side_only(self) -> None:
        class BullishOracle:
            def current_bias_value(self) -> float:
                return 0.5

            def get_hourly_atr(self, symbol: str) -> float:
                del symbol
                return 20.0

        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            market_oracle=BullishOracle(),
        )

        context = robot._refresh_grid_context(100.0)

        self.assertIsNotNone(context)
        assert context is not None
        buy_step = context.center_price - context.buy_prices[0]
        sell_step = context.sell_prices[0] - context.center_price
        self.assertAlmostEqual(buy_step, 10.0, places=6)
        self.assertAlmostEqual(sell_step, 25.0, places=6)

    def test_grid_robot_bullish_bias_skews_to_six_buy_two_sell_levels(self) -> None:
        class BullishOracle:
            def current_bias_value(self) -> float:
                return 0.5

            def get_hourly_atr(self, symbol: str) -> float:
                del symbol
                return 10.0

        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            market_oracle=BullishOracle(),
            use_dynamic_range=False,
        )

        context = robot._refresh_grid_context(100.0)

        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(len(context.buy_prices), 6)
        self.assertEqual(len(context.sell_prices), 2)

    def test_grid_robot_bullish_bias_uses_asymmetric_spacing(self) -> None:
        class BullishOracle:
            def current_bias_value(self) -> float:
                return 0.5

            def get_hourly_atr(self, symbol: str) -> float:
                del symbol
                return 10.0

        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            market_oracle=BullishOracle(),
            use_dynamic_range=False,
        )

        context = robot._refresh_grid_context(100.0)

        self.assertIsNotNone(context)
        assert context is not None
        buy_spacing = round(context.buy_prices[1] - context.buy_prices[0], 6)
        sell_spacing = round(context.sell_prices[1] - context.sell_prices[0], 6)
        self.assertAlmostEqual(abs(buy_spacing), 5.0)
        self.assertAlmostEqual(abs(sell_spacing), 5.0)
        self.assertLessEqual(abs(buy_spacing), abs(sell_spacing))

    def test_grid_robot_bullish_bias_shifts_center_upward_by_atr_ratio(self) -> None:
        class BullishOracle:
            def current_bias_value(self) -> float:
                return 0.5

            def get_hourly_atr(self, symbol: str) -> float:
                del symbol
                return 10.0

        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            market_oracle=BullishOracle(),
            use_dynamic_range=False,
        )

        context = robot._refresh_grid_context(100.0)

        self.assertIsNotNone(context)
        assert context is not None
        self.assertAlmostEqual(context.center_price, 100.03)

    def test_grid_robot_bearish_bias_skews_to_six_sell_two_buy_levels(self) -> None:
        class BearishOracle:
            def current_bias_value(self) -> float:
                return -0.5

            def get_hourly_atr(self, symbol: str) -> float:
                del symbol
                return 10.0

        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            market_oracle=BearishOracle(),
            use_dynamic_range=False,
        )

        context = robot._refresh_grid_context(100.0)

        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(len(context.buy_prices), 2)
        self.assertEqual(len(context.sell_prices), 6)

    def test_grid_robot_bearish_bias_uses_mirrored_asymmetric_spacing(self) -> None:
        class BearishOracle:
            def current_bias_value(self) -> float:
                return -0.5

            def get_hourly_atr(self, symbol: str) -> float:
                del symbol
                return 10.0

        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            market_oracle=BearishOracle(),
            use_dynamic_range=False,
        )

        context = robot._refresh_grid_context(100.0)

        self.assertIsNotNone(context)
        assert context is not None
        buy_spacing = round(context.buy_prices[1] - context.buy_prices[0], 6)
        sell_spacing = round(context.sell_prices[1] - context.sell_prices[0], 6)
        self.assertAlmostEqual(abs(buy_spacing), 5.0)
        self.assertAlmostEqual(abs(sell_spacing), 5.0)
        self.assertGreaterEqual(abs(buy_spacing), abs(sell_spacing))

    def test_grid_robot_bearish_bias_shifts_center_downward_by_atr_ratio(self) -> None:
        class BearishOracle:
            def current_bias_value(self) -> float:
                return -0.5

            def get_hourly_atr(self, symbol: str) -> float:
                del symbol
                return 10.0

        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            market_oracle=BearishOracle(),
            use_dynamic_range=False,
        )

        context = robot._refresh_grid_context(100.0)

        self.assertIsNotNone(context)
        assert context is not None
        self.assertAlmostEqual(context.center_price, 99.97)

    def test_grid_robot_allocates_forty_percent_equity_across_levels(self) -> None:
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)

        amount = robot._calculate_grid_order_amount(100.0)

        self.assertAlmostEqual(amount, 47.5)

    def test_grid_robot_fee_aware_close_price_exceeds_round_trip_fee_move(self) -> None:
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)
        context = robot._fallback_context(100.0, atr_value=0.0)

        buy_close = robot._calculate_fee_aware_close_price(entry_side="buy", entry_price=100.0, step_size=0.05, context=context)
        sell_close = robot._calculate_fee_aware_close_price(entry_side="sell", entry_price=100.0, step_size=0.05, context=context)

        self.assertGreater(buy_close - 100.0, 0.2)
        self.assertGreater(100.0 - sell_close, 0.2)
        self.assertGreater(robot._estimate_grid_level_net_profit(entry_side="buy", entry_price=100.0, close_price=buy_close, amount=1.0), 0.0)
        self.assertGreater(robot._estimate_grid_level_net_profit(entry_side="sell", entry_price=100.0, close_price=sell_close, amount=1.0), 0.0)

    def test_grid_robot_honors_risk_observe_block_and_stops_opening_orders(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        risk_manager = DummyRiskManager(allow_grid=False, grid_reason="grid_observe_lower_break|price=95.00|lower=97.00")
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, risk_manager=risk_manager)

        result = robot.run()

        self.assertEqual(result.action, "grid:risk_blocked")
        self.assertEqual(len(self.client.limit_orders), 0)
        self.assertEqual(len(risk_manager.grid_profiles), 1)

    def test_grid_robot_holds_existing_grid_when_snapshot_is_healthy(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        now = datetime(2026, 4, 10, 3, 0, tzinfo=timezone.utc)
        now_values = [now, now + timedelta(seconds=60)]
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            now_provider=lambda: now_values.pop(0),
            market_oracle=None,
            use_dynamic_range=False,
        )

        first = robot.run()
        second = robot.run()

        self.assertTrue(first.action.startswith("grid:placed_"))
        self.assertTrue(second.action.startswith("grid:hold_existing_grid"))

    def test_grid_robot_enforces_five_minute_hard_regrid_cooldown(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, now_provider=lambda: now, use_dynamic_range=False)
        robot._cached_context = robot._fallback_context(100.0, atr_value=10.0)
        robot.last_regrid_time = now.timestamp() - 299
        robot.current_grid_center = 100.0

        self.assertFalse(robot._should_regrid(robot._fallback_context(105.0, atr_value=10.0), 105.0, now))

    def test_grid_robot_regrids_once_center_shift_exceeds_configured_point_three_atr(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        now = datetime(2026, 4, 11, 10, 10, tzinfo=timezone.utc)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, now_provider=lambda: now, use_dynamic_range=False)
        robot._cached_context = robot._fallback_context(100.0, atr_value=10.0)
        robot.last_regrid_time = now.timestamp() - 301
        robot.current_grid_center = 100.0

        self.assertFalse(robot._should_regrid(robot._fallback_context(102.9, atr_value=10.0), 102.9, now))
        self.assertTrue(robot._should_regrid(robot._fallback_context(103.1, atr_value=10.0), 103.1, now))

    def test_grid_robot_hard_reanchors_once_price_drift_exceeds_one_point_two_atr(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        now = datetime(2026, 4, 11, 10, 10, tzinfo=timezone.utc)
        grid_config = GridConfig(regrid_trigger_atr_ratio=2.0, hard_reanchor_atr_ratio=1.2)
        robot = GridRobot(self.client, self.database, grid_config, self.execution, now_provider=lambda: now, use_dynamic_range=False)
        robot._cached_context = robot._fallback_context(100.0, atr_value=10.0)
        robot.last_regrid_time = now.timestamp() - 301
        robot.current_grid_center = 100.0

        self.assertFalse(robot._should_regrid(robot._fallback_context(112.0, atr_value=10.0), 112.0, now))
        self.assertTrue(robot._should_regrid(robot._fallback_context(112.1, atr_value=10.0), 112.1, now))

    def test_grid_robot_cools_down_repeatedly_triggered_buy_layer(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        now = datetime(2026, 4, 10, 3, 0, tzinfo=timezone.utc)
        now_values = [
            now,
            now + timedelta(minutes=2),
            now + timedelta(minutes=4),
            now + timedelta(minutes=5),
            now + timedelta(minutes=6),
            now + timedelta(minutes=7),
            now + timedelta(minutes=8),
            now + timedelta(minutes=9),
        ]
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            now_provider=lambda: now_values.pop(0),
            use_dynamic_range=False,
            market_oracle=None,
        )

        first_preview = robot._refresh_grid_context(self.client.last_price)
        assert first_preview is not None
        self.client.last_price = first_preview.buy_prices[0] - 0.1

        first_result = robot.run()
        first_count = len(self.client.limit_orders)

        second_preview = robot._refresh_grid_context(self.client.last_price)
        assert second_preview is not None
        self.client.last_price = second_preview.buy_prices[0] - 0.1
        second_result = robot.run()
        second_count = len(self.client.limit_orders) - first_count

        third_preview = robot._refresh_grid_context(self.client.last_price)
        assert third_preview is not None
        self.client.last_price = third_preview.buy_prices[0] - 0.1
        third_result = robot.run()
        third_count = len(self.client.limit_orders) - first_count - second_count

        fourth_preview = robot._refresh_grid_context(self.client.last_price)
        assert fourth_preview is not None
        self.client.last_price = fourth_preview.buy_prices[0] - 0.1
        fourth_result = robot.run()
        fourth_count = len(self.client.limit_orders) - first_count - second_count - third_count

        self.assertEqual(first_count, 8)
        self.assertEqual(second_count, 0)
        self.assertEqual(third_count, 0)
        self.assertEqual(fourth_count, 0)
        self.assertTrue(second_result.action.startswith("grid:hold_existing_grid"))
        self.assertTrue(third_result.action.startswith("grid:hold_existing_grid"))
        self.assertTrue(fourth_result.action.startswith("grid:hold_existing_grid"))
        self.assertIn("cooldown=0", first_result.action)

    def test_grid_robot_triggers_flash_crash_guard_and_pauses_orders(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self._set_ohlcv(
            "1m",
            [100.0, 100.1, 99.9],
            60_000,
        )
        self.client.ohlcv_by_timeframe["1m"][-1] = [
            self.client.ohlcv_by_timeframe["1m"][-1][0],
            100.0,
            118.0,
            98.0,
            100.0,
            120.0,
        ]
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        now = datetime(2026, 4, 10, 3, 0, tzinfo=timezone.utc)
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            now_provider=lambda: now,
            use_dynamic_range=False,
            market_oracle=None,
        )

        result = robot.run()

        self.assertEqual(result.action.split("|")[0], "grid:flash_crash_triggered")
        self.assertIn("atr=", result.action)
        self.assertIn("dynamic=", result.action)
        self.assertIn("regrid=false", result.action)
        self.assertEqual(len(self.client.limit_orders), 0)
        self.assertGreaterEqual(self.client.cancel_all_calls.count("BTC/USDT"), 1)

    def test_grid_robot_keeps_pause_during_spike_guard_cooldown(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self._set_ohlcv(
            "1m",
            [100.0, 100.1, 99.9],
            60_000,
        )
        self.client.ohlcv_by_timeframe["1m"][-1] = [
            self.client.ohlcv_by_timeframe["1m"][-1][0],
            100.0,
            118.0,
            98.0,
            100.0,
            120.0,
        ]
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        now = datetime(2026, 4, 10, 3, 0, tzinfo=timezone.utc)
        now_values = [now, now + timedelta(seconds=5)]
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            now_provider=lambda: now_values.pop(0),
            use_dynamic_range=False,
            market_oracle=None,
        )

        first = robot.run()
        second = robot.run()

        self.assertEqual(first.action.split("|")[0], "grid:flash_crash_triggered")
        self.assertEqual(second.action.split("|")[0], "grid:flash_crash_cooldown")
        self.assertIn("remaining=", second.action)
        self.assertEqual(len(self.client.limit_orders), 0)

    def test_grid_robot_batch_places_opening_orders_before_rebalance_orders(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = [{"contracts": 0.12, "side": "long"}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        result = robot.run()

        self.assertIn("grid:placed_10_orders@100.00", result.action)
        self.assertIn("openings=8", result.action)
        self.assertIn("rebalances=2", result.action)
        self.assertEqual(robot._placed_order_ids, [f"order-{index}" for index in range(1, 9)])
        self.assertEqual(len(self.client.cancel_order_calls), 0)

    def test_grid_robot_rolls_back_partial_batch_when_opening_order_placement_fails(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.raise_on_limit_order_at = 3
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)

        result = robot.run()

        self.assertEqual(result.action, "grid:batch_place_failed|reason=RuntimeError")
        self.assertEqual(self.client.cancel_order_calls, [("order-3", "BTC/USDT"), ("order-2", "BTC/USDT"), ("order-1", "BTC/USDT")])
        self.assertEqual(len(self.client.limit_orders), 0)
        self.assertEqual(robot._placed_order_ids, [])

    def test_grid_robot_batch_failure_skips_rebalance_orders(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = [{"contracts": 0.12, "side": "long"}]
        self.client.raise_on_limit_order_at = 1
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)

        result = robot.run()

        self.assertEqual(result.action, "grid:batch_place_failed|reason=RuntimeError")
        self.assertEqual([order["side"] for order in self.client.limit_orders], [])
        self.assertEqual(self.client.cancel_order_calls, [("order-1", "BTC/USDT")])

    def test_grid_robot_places_reduce_only_rebalance_orders_when_long_heavy(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = [{"contracts": 0.12, "side": "long"}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        result = robot.run()

        reduce_only_sells = [
            order for order in self.client.limit_orders if order["side"] == "sell" and order.get("reduce_only")
        ]
        self.assertEqual(len(reduce_only_sells), 2)
        self.assertTrue(all(order["amount"] > self.execution.grid_order_size for order in reduce_only_sells))
        self.assertIn("rebalances=2", result.action)

    def test_grid_robot_places_reduce_only_rebalance_orders_when_short_heavy(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = [{"contracts": 0.12, "side": "short"}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        result = robot.run()

        reduce_only_buys = [
            order for order in self.client.limit_orders if order["side"] == "buy" and order.get("reduce_only")
        ]
        self.assertEqual(len(reduce_only_buys), 2)
        self.assertTrue(all(order["amount"] > self.execution.grid_order_size for order in reduce_only_buys))
        self.assertIn("rebalances=2", result.action)

    def test_grid_robot_health_check_requires_complete_expected_ladder(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)
        now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=int(now.timestamp() * 1000))
        assert context is not None
        robot._cached_context = context
        self.client.limit_orders = [
            {
                "side": order.side,
                "price": order.price,
                "reduceOnly": False,
                "status": "open",
            }
            for order in robot._build_opening_orders(context, self.client.last_price, now)
        ]

        self.assertTrue(robot._has_active_grid_orders(context, now))

    def test_grid_robot_health_check_accepts_partial_openings_with_reduce_only_hedge(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)
        now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=int(now.timestamp() * 1000))
        assert context is not None
        robot._cached_context = context
        opening_orders = robot._build_opening_orders(context, self.client.last_price, now)

        surviving_openings = opening_orders[1:]
        filled_opening = opening_orders[0]
        self.client.limit_orders = [
            {
                "side": order.side,
                "price": order.price,
                "reduceOnly": False,
                "status": "open",
            }
            for order in surviving_openings
        ]
        self.client.limit_orders.append(
            {
                "side": "sell" if filled_opening.side == "buy" else "buy",
                "price": context.sell_prices[0],
                "reduceOnly": True,
                "status": "open",
            }
        )

        self.assertTrue(robot._has_active_grid_orders(context, now))

    def test_grid_robot_health_check_rejects_shifted_ladder_orders(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)
        now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=int(now.timestamp() * 1000))
        assert context is not None
        robot._cached_context = context
        opening_orders = robot._build_opening_orders(context, self.client.last_price, now)

        shifted_orders = [
            {
                "side": order.side,
                "price": order.price,
                "reduceOnly": False,
                "status": "open",
            }
            for order in opening_orders
        ]
        shifted_orders[0]["price"] = context.center_price - 0.01
        self.client.limit_orders = shifted_orders

        self.assertFalse(robot._has_active_grid_orders(context, now))

    def test_grid_robot_health_check_treats_string_false_reduce_only_as_opening_orders(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)
        now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=int(now.timestamp() * 1000))
        assert context is not None
        robot._cached_context = context
        opening_orders = robot._build_opening_orders(context, self.client.last_price, now)
        self.client.limit_orders = [
            {
                "id": f"order-{idx}",
                "side": order.side,
                "price": order.price,
                "reduceOnly": "false",
                "status": "open",
                "info": {"reduceOnly": "false"},
            }
            for idx, order in enumerate(opening_orders, start=1)
        ]

        self.assertTrue(robot._has_active_grid_orders(context, now))

    def test_grid_robot_regrid_cancels_pending_orders_without_flattening_positions(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, market_oracle=None, use_dynamic_range=False)
        self.client.limit_orders = [
            {"id": "order-1", "side": "buy", "price": 99.0, "reduceOnly": False},
            {"id": "order-2", "side": "sell", "price": 101.0, "reduceOnly": False},
        ]
        self.client.positions = [{"contracts": 1.0, "side": "long", "notional": 100.0}]

        robot._cancel_pending_grid_orders(list(self.client.limit_orders))

        self.assertEqual(self.client.cancel_order_calls, [("order-1", "BTC/USDT"), ("order-2", "BTC/USDT")])
        self.assertEqual(self.client.close_all_calls, [])

    def test_grid_robot_run_holds_partial_grid_with_reduce_only_hedge_instead_of_regridding(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        now = datetime(2026, 4, 10, 3, 0, tzinfo=timezone.utc)
        now_values = [now, now + timedelta(seconds=60)]
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            now_provider=lambda: now_values.pop(0),
            market_oracle=None,
            use_dynamic_range=False,
        )

        first = robot.run()
        surviving_openings = self.client.limit_orders[1:]
        self.client.limit_orders = list(surviving_openings)
        self.client.limit_orders.append(
            {
                "symbol": "BTC/USDT",
                "side": "sell",
                "amount": self.execution.grid_order_size,
                "price": surviving_openings[-1]["price"],
                "reduce_only": True,
                "reduceOnly": True,
                "status": "open",
            }
        )

        second = robot.run()

        self.assertTrue(first.action.startswith("grid:placed_"))
        self.assertTrue(second.action.startswith("grid:hold_existing_grid"))
        self.assertEqual(len(self.client.cancel_all_calls), 0)

    def test_grid_robot_logs_fetch_open_orders_failure_in_health_check(self) -> None:
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)
        now = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=int(now.timestamp() * 1000))
        assert context is not None
        robot._cached_context = context
        attempts = []

        def boom(symbol: str):
            attempts.append(symbol)
            raise RuntimeError(f"boom for {symbol}")

        self.client.fetch_open_orders = boom
        with self.assertLogs("market_adaptive.strategies.grid_robot", level="ERROR") as captured:
            healthy = robot._has_active_grid_orders(context, now)

        self.assertFalse(healthy)
        self.assertEqual(len(attempts), 3)
        log_output = "\n".join(captured.output)
        self.assertIn("fetch_open_orders failed | symbol=BTC/USDT purpose=health_check attempt=1/3", log_output)
        self.assertIn("timestamp=2026-04-11T10:00:00+00:00", log_output)
        self.assertIn("error_type=RuntimeError", log_output)
        self.assertIn("boom for BTC/USDT", log_output)

    def test_grid_robot_enters_health_check_degraded_mode_after_repeated_sync_failures(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        now = datetime(2026, 4, 10, 3, 0, tzinfo=timezone.utc)
        now_values = [now, now + timedelta(seconds=60)]
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            now_provider=lambda: now_values.pop(0),
            market_oracle=None,
            use_dynamic_range=False,
        )

        first = robot.run()

        def boom(symbol: str):
            raise RuntimeError(f"sync failed for {symbol}")

        self.client.fetch_open_orders = boom
        second = robot.run()

        self.assertTrue(first.action.startswith("grid:placed_"))
        self.assertTrue(second.action.startswith("grid:health_check_degraded"))
        self.assertEqual(len(self.client.cancel_all_calls), 0)
        self.assertEqual(robot._health_check_failed_streak, 3)
        self.assertIsNotNone(robot._health_check_degraded_until)

    def test_grid_robot_returns_order_sync_unavailable_before_regrid_when_validation_fails(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        now = datetime(2026, 4, 10, 3, 0, tzinfo=timezone.utc)
        now_values = [now, now + timedelta(seconds=360)]
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            now_provider=lambda: now_values.pop(0),
            market_oracle=None,
            use_dynamic_range=False,
        )

        first = robot.run()

        calls = []

        def flaky(symbol: str):
            calls.append(symbol)
            if len(calls) == 1:
                return []
            raise RuntimeError(f"validation failed for {symbol}")

        self.client.fetch_open_orders = flaky
        second = robot.run()

        self.assertTrue(first.action.startswith("grid:placed_"))
        self.assertTrue(second.action.startswith("grid:order_sync_unavailable"))
        self.assertEqual(len(self.client.cancel_all_calls), 0)

    def test_grid_robot_reduces_exposure_stepwise_instead_of_flattening(self) -> None:
        self.client.positions = [{"contracts": 0.12, "side": "long", "info": {"posSide": "long"}}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        result = robot.reduce_exposure_step("grid_liquidation_warning", 0.25)

        self.assertIn("grid:step_reduce", result)
        self.assertEqual(self.client.cancel_all_calls, ["BTC/USDT"])
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertEqual(self.client.market_orders[0]["side"], "sell")
        self.assertTrue(self.client.market_orders[0]["reduce_only"])
        self.assertAlmostEqual(self.client.market_orders[0]["amount"], 0.03)

    def test_grid_robot_suppresses_sell_openings_when_cta_long_trend_is_strong(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        runtime_context = StrategyRuntimeContext()
        runtime_context.publish_cta_state(
            symbol="BTC/USDT",
            side="long",
            size=1.0,
            trend_strength=2.0,
            strong_trend=True,
        )
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            runtime_context=runtime_context,
            use_dynamic_range=False,
            market_oracle=None,
        )
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert context is not None

        opening_orders = robot._build_opening_orders(context, self.client.last_price, datetime.now(timezone.utc))
        filtered_orders, reason = robot._apply_runtime_lockout(opening_orders)

        self.assertTrue(any(order.side == "sell" for order in opening_orders))
        self.assertTrue(all(order.side == "buy" for order in filtered_orders))
        self.assertEqual(reason, "cta_long_lockout:sell_suppressed")

    def test_grid_inventory_heavy_state_triggers_cta_protective_reduce_hook(self) -> None:
        runtime_context = StrategyRuntimeContext()
        grid = GridRobot(self.client, self.database, self.grid_config, self.execution, runtime_context=runtime_context)
        cta = CTARobot(self.client, self.database, self.cta_config, self.execution, runtime_context=runtime_context)
        cta.position = ManagedPosition(
            side="long",
            entry_price=100.0,
            initial_size=0.2,
            remaining_size=0.2,
            stop_price=95.0,
            best_price=100.0,
            atr_value=2.0,
            stop_distance=5.0,
        )
        self.client.positions = [{"contracts": 0.8, "side": "long", "notional": 80.0, "info": {"posSide": "long"}}]
        grid_context = grid._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert grid_context is not None

        grid._publish_grid_risk(grid_context)
        grid_state = runtime_context.snapshot_grid()
        action = cta._apply_runtime_coordination(
            TrendSignal(direction=1, raw_direction=1, major_direction=1, major_bias_score=2.0, price=100.0, atr=2.0)
        )

        self.assertTrue(grid_state.heavy_inventory)
        self.assertEqual(grid_state.inventory_bias_side, "long")
        self.assertTrue(grid_state.hedge_assist_requested)
        self.assertIsNotNone(action)
        self.assertIn("cta:coordination_reduce_", action)
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertTrue(self.client.market_orders[0]["reduce_only"])
        self.assertEqual(self.client.market_orders[0]["side"], "sell")
        self.assertLess(cta.position.remaining_size if cta.position is not None else 0.0, 0.2)

    def test_grid_heavy_inventory_threshold_is_configurable(self) -> None:
        runtime_context = StrategyRuntimeContext()
        grid = GridRobot(
            self.client,
            self.database,
            GridConfig(symbol="BTC/USDT", heavy_inventory_threshold=0.80),
            self.execution,
            runtime_context=runtime_context,
        )
        self.client.positions = [
            {"contracts": 0.8, "side": "long", "notional": 80.0, "info": {"posSide": "long"}},
            {"contracts": 0.2, "side": "short", "notional": 20.0, "info": {"posSide": "short"}},
        ]
        grid_context = grid._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert grid_context is not None

        grid._publish_grid_risk(grid_context)
        grid_state = runtime_context.snapshot_grid()

        self.assertAlmostEqual(grid_state.inventory_bias_ratio, 0.75)
        self.assertFalse(grid_state.heavy_inventory)
        self.assertFalse(grid_state.hedge_assist_requested)

    def test_cta_assist_trim_ratio_is_configurable(self) -> None:
        runtime_context = StrategyRuntimeContext()
        grid = GridRobot(self.client, self.database, self.grid_config, self.execution, runtime_context=runtime_context)
        cta = CTARobot(
            self.client,
            self.database,
            CTAConfig(symbol="BTC/USDT", cta_assist_trim_ratio=0.40),
            self.execution,
            runtime_context=runtime_context,
        )
        cta.position = ManagedPosition(
            side="long",
            entry_price=100.0,
            initial_size=0.2,
            remaining_size=0.2,
            stop_price=95.0,
            best_price=100.0,
            atr_value=2.0,
            stop_distance=5.0,
        )
        self.client.positions = [{"contracts": 0.8, "side": "long", "notional": 80.0, "info": {"posSide": "long"}}]
        grid_context = grid._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert grid_context is not None

        grid._publish_grid_risk(grid_context)
        action = cta._apply_runtime_coordination(
            TrendSignal(direction=1, raw_direction=1, major_direction=1, major_bias_score=2.0, price=100.0, atr=2.0)
        )

        self.assertIsNotNone(action)
        self.assertAlmostEqual(self.client.market_orders[0]["amount"], 0.08)
        self.assertAlmostEqual(cta.position.remaining_size if cta.position is not None else 0.0, 0.12)

    def test_active_hedge_mode_is_opt_in_and_requires_cta_position_by_default(self) -> None:
        runtime_context = StrategyRuntimeContext()
        runtime_context.publish_cta_state(
            symbol="BTC/USDT",
            side=None,
            size=0.0,
            trend_strength=0.0,
            strong_trend=False,
        )
        grid = GridRobot(
            self.client,
            self.database,
            GridConfig(
                symbol="BTC/USDT",
                heavy_inventory_threshold=0.80,
                active_hedge_mode_enabled=True,
                active_hedge_min_inventory_ratio=0.70,
            ),
            self.execution,
            runtime_context=runtime_context,
        )
        self.client.positions = [
            {"contracts": 0.8, "side": "long", "notional": 80.0, "info": {"posSide": "long"}},
            {"contracts": 0.2, "side": "short", "notional": 20.0, "info": {"posSide": "short"}},
        ]
        grid_context = grid._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert grid_context is not None

        grid._publish_grid_risk(grid_context)
        without_cta_position = runtime_context.snapshot_grid()
        self.assertFalse(without_cta_position.heavy_inventory)
        self.assertFalse(without_cta_position.hedge_assist_requested)

        runtime_context.publish_cta_state(
            symbol="BTC/USDT",
            side="long",
            size=0.2,
            trend_strength=1.0,
            strong_trend=False,
        )
        grid._publish_grid_risk(grid_context)
        with_cta_position = runtime_context.snapshot_grid()
        self.assertFalse(with_cta_position.heavy_inventory)
        self.assertTrue(with_cta_position.hedge_assist_requested)
        self.assertEqual(with_cta_position.hedge_assist_reason, "grid_active_hedge:long")

    def test_grid_robot_liquidation_trim_targets_worst_farthest_position_first(self) -> None:
        self.client.last_price = 100.0
        self.client.positions = [
            {"contracts": 0.08, "side": "long", "entryPrice": 98.0, "liquidationPrice": 70.0, "info": {"posSide": "long"}},
            {"contracts": 0.16, "side": "long", "entryPrice": 120.0, "liquidationPrice": 104.0, "info": {"posSide": "long"}},
        ]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        result = robot.reduce_exposure_step("grid_liquidation_warning", 0.25)

        self.assertIn("protective_trim", result)
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertAlmostEqual(self.client.market_orders[0]["amount"], 0.06)
        self.assertEqual(self.client.market_orders[0]["params"]["posSide"], "long")

    def test_grid_robot_expected_profit_respects_okx_contract_value(self) -> None:
        self.client.contract_value = 0.01
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        expected_profit = robot._estimate_grid_level_net_profit(
            entry_side="buy",
            entry_price=72346.76,
            close_price=72930.20,
            amount=6.54720625,
        )

        self.assertAlmostEqual(expected_profit, 28.687437940070147, places=6)

    def test_grid_robot_websocket_fill_places_reduce_only_counter_order_with_pos_side(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = [{"contracts": 0.02, "side": "long", "info": {"posSide": "long"}}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False, market_oracle=None)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert context is not None
        robot._cached_context = context

        robot._on_ws_orders({
            "id": "fill-1",
            "status": "closed",
            "filled": 0.01,
            "side": "buy",
            "price": 99.0,
            "reduceOnly": False,
            "info": {"state": "filled", "fillSz": "0.01", "fillPx": "99.0"},
        })

        self.assertEqual(len(self.client.limit_orders), 1)
        self.assertTrue(self.client.limit_orders[0]["reduce_only"])
        self.assertEqual(self.client.limit_orders[0]["params"]["posSide"], "long")
        self.assertEqual(self.client.limit_orders[0]["side"], "sell")

    def test_grid_robot_websocket_fill_skips_hedge_when_position_missing(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = []
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False, market_oracle=None)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert context is not None
        robot._cached_context = context

        with self.assertLogs("market_adaptive.strategies.grid_robot", level="WARNING") as captured:
            robot._on_ws_orders({
                "id": "fill-missing-position",
                "status": "closed",
                "filled": 0.01,
                "side": "buy",
                "price": 99.0,
                "reduceOnly": False,
                "info": {"state": "filled", "fillSz": "0.01", "fillPx": "99.0"},
            })

        self.assertEqual(len(self.client.limit_orders), 0)
        self.assertTrue(any("missing position" in message for message in captured.output))

    def test_grid_robot_websocket_fill_is_idempotent_per_cycle(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = [{"contracts": 0.02, "side": "long", "info": {"posSide": "long"}}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False, market_oracle=None)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert context is not None
        robot._cached_context = context
        payload = {
            "id": "fill-dup-1",
            "status": "closed",
            "filled": 0.01,
            "side": "buy",
            "price": 99.0,
            "reduceOnly": False,
            "info": {"state": "filled", "fillSz": "0.01", "fillPx": "99.0"},
        }

        robot._on_ws_orders(payload)
        robot._on_ws_orders(payload)

        self.assertEqual(len(self.client.limit_orders), 1)

    def test_grid_robot_websocket_fill_clamps_hedge_price_to_grid_bounds(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = [{"contracts": 0.02, "side": "short", "info": {"posSide": "short"}}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False, market_oracle=None)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert context is not None
        robot._cached_context = context

        robot._on_ws_orders({
            "id": "fill-clamp-1",
            "status": "closed",
            "filled": 0.01,
            "side": "sell",
            "price": context.lower_bound,
            "reduceOnly": False,
            "info": {"state": "filled", "fillSz": "0.01", "fillPx": str(context.lower_bound)},
        })

        self.assertEqual(len(self.client.limit_orders), 1)
        self.assertAlmostEqual(self.client.limit_orders[0]["price"], context.lower_bound)
        self.assertEqual(self.client.limit_orders[0]["side"], "buy")

    def test_grid_robot_websocket_fill_logs_placement_failure_and_rejection(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        self.client.positions = [{"contracts": 0.02, "side": "long", "info": {"posSide": "long"}}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False, market_oracle=None)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert context is not None
        robot._cached_context = context

        self.client.raise_on_limit_order_at = 0
        with self.assertLogs("market_adaptive.strategies.grid_robot", level="ERROR") as failure_logs:
            robot._on_ws_orders({
                "id": "fill-fail-1",
                "status": "closed",
                "filled": 0.01,
                "side": "buy",
                "price": 99.0,
                "reduceOnly": False,
                "info": {"state": "filled", "fillSz": "0.01", "fillPx": "99.0"},
            })
        self.assertTrue(any("hedge placement failed" in message for message in failure_logs.output))
        self.assertEqual(len(self.client.limit_orders), 0)

        self.client.raise_on_limit_order_at = None
        self.client.fetch_order_responses["order-1"] = {"id": "order-1", "status": "canceled", "info": {"state": "canceled"}}
        with self.assertLogs("market_adaptive.strategies.grid_robot", level="WARNING") as rejection_logs:
            robot._on_ws_orders({
                "id": "fill-reject-1",
                "status": "closed",
                "filled": 0.01,
                "side": "buy",
                "price": 99.0,
                "reduceOnly": False,
                "info": {"state": "filled", "fillSz": "0.01", "fillPx": "99.0"},
            })
        self.assertTrue(any("hedge_order_rejected" in message for message in rejection_logs.output))

    def test_grid_robot_higher_timeframe_trend_guard_blocks_new_grid(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        major_closes = [160.0 + 2.0 * index for index in range(60)]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self.grid_config.higher_timeframe_trend_distance_atr_threshold = 0.5
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, market_oracle=None, use_dynamic_range=False)

        result = robot.run()

        self.assertTrue(result.action.startswith("grid:higher_timeframe_trend_guard_blocked"))
        self.assertEqual(len(self.client.limit_orders), 0)

    def test_grid_robot_oracle_adx_label_block_returns_specific_reason(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)

        class OracleStub:
            def current_higher_adx_trend(self):
                return "rising"

        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, market_oracle=OracleStub(), use_dynamic_range=False)

        result = robot.run()

        self.assertTrue(result.action.startswith("grid:oracle_adx_trend_blocked|higher_adx_trend=rising"))
        self.assertEqual(len(self.client.limit_orders), 0)

    def test_grid_robot_trend_defense_reduces_exposure_on_confirmed_breakout(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=1.0)
        major_closes = [160.0 + 2.0 * index for index in range(60)]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self.grid_config.higher_timeframe_trend_guard_enabled = False
        self.grid_config.flash_crash_enabled = False
        self.grid_config.trend_defense_breakout_atr_ratio = 0.5
        self.grid_config.trend_defense_reduction_ratio = 0.5
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, market_oracle=None, use_dynamic_range=False)

        self.client.last_price = 100.0
        first = robot.run()
        self.assertTrue(first.action.startswith("grid:placed_"))

        self.client.positions = [{"contracts": 0.1, "side": "long", "entryPrice": 100.0, "info": {"posSide": "long"}}]
        self.client.last_price = 111.0
        result = robot.run()

        self.assertEqual(result.action.split("|")[0], "grid:trend_defense_triggered")
        self.assertEqual(self.client.cancel_all_calls, ["BTC/USDT"])
        self.assertEqual(len(self.client.market_orders), 1)
        self.assertEqual(self.client.market_orders[0]["side"], "sell")
        self.assertAlmostEqual(self.client.market_orders[0]["amount"], 0.05)

    def test_grid_robot_regime_cleanup_uses_limit_for_profitable_positions(self) -> None:
        self.client.last_price = 105.0
        self.client.positions = [{"contracts": 0.1, "side": "long", "entryPrice": 100.0, "info": {"posSide": "long"}}]
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        result = robot.cleanup_for_regime_switch("status_switch:sideways->trend")

        self.assertIn("grid:regime_cleanup", result)
        self.assertEqual(self.client.cancel_all_calls, ["BTC/USDT"])
        self.assertEqual(len(self.client.limit_orders), 1)
        self.assertEqual(self.client.limit_orders[0]["side"], "sell")
        self.assertTrue(self.client.limit_orders[0]["reduce_only"])

    def test_status_switch_triggers_flatten_before_inactive_cycle(self) -> None:
        self._insert_status("trend", "2026-04-10T03:00:00+00:00")
        cta = CTARobot(self.client, self.database, self.cta_config, self.execution)
        self._load_bullish_signal(lower_last_close=100.0)
        cta.run()
        self.assertIsNotNone(cta.position)

        self._insert_status("sideways", "2026-04-10T03:05:00+00:00")
        result = cta.run()

        self.assertFalse(result.active)
        self.assertIn("BTC/USDT", self.client.cancel_all_calls)
        self.assertIn("BTC/USDT", self.client.close_all_calls)
        self.assertIsNone(cta.position)

    def test_grid_status_switch_uses_risk_manager_cleanup_coordinator(self) -> None:
        self._insert_status("trend", "2026-04-10T03:00:00+00:00")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        risk_manager = DummyRiskManager()
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, risk_manager=risk_manager, market_oracle=None, use_dynamic_range=False)
        robot.run()

        self._insert_status("sideways", "2026-04-10T03:05:00+00:00")
        robot.run()

        self.assertEqual(risk_manager.cleanup_requests, [("grid", "status_switch:trend->sideways")])

    def test_cta_robot_accepts_trend_impulse_status_for_activation(self) -> None:
        self._insert_status("trend_impulse")
        self._load_bullish_signal(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.status, "trend_impulse")
        self.assertEqual(result.action, "cta:open_long")


    def test_cta_robot_blocks_starter_frontrun_when_swing_direction_is_not_confirmed(self) -> None:
        self._set_bullish_higher_timeframes()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        base = 1_700_000_000_000
        payload = []
        volumes = [100.0 + index for index in range(54)] + [160.0, 170.0, 180.0, 220.0, 235.0, 250.0]
        for index, close in enumerate(execution_closes):
            open_price = close - 0.35 if index >= len(execution_closes) - 3 else close - 0.2
            payload.append([base + index * 900_000, open_price, close + 0.4, open_price - 0.3, close, volumes[index]])
        self.client.ohlcv_by_timeframe["15m"] = payload
        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)

        import pandas as pd
        major_supertrend = pd.DataFrame(
            {
                "direction": [1] * 60,
                "lower_band": [80.0 + 0.5 * index for index in range(60)],
                "upper_band": [120.0 + 0.5 * index for index in range(60)],
                "supertrend": [80.0 + 0.5 * index for index in range(60)],
                "atr": [2.0] * 60,
            }
        )
        unstable_swing_supertrend = pd.DataFrame(
            {
                "direction": [1] * 59 + [-1],
                "lower_band": [100.0] * 60,
                "upper_band": [110.0] * 60,
                "supertrend": [100.0] * 60,
                "atr": [1.0] * 60,
            }
        )

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, unstable_swing_supertrend],
            ),
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertFalse(signal.execution_trigger.frontrun_ready)
        self.assertNotEqual(signal.execution_entry_mode, "starter_frontrun_limit")

    def test_cta_robot_blocks_starter_frontrun_when_score_is_below_floor(self) -> None:
        self.cta_config.starter_frontrun_minimum_score = 90.0
        self._set_bullish_higher_timeframes()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        base = 1_700_000_000_000
        payload = []
        volumes = [100.0 + index for index in range(54)] + [160.0, 170.0, 180.0, 220.0, 235.0, 250.0]
        for index, close in enumerate(execution_closes):
            open_price = close - 0.35 if index >= len(execution_closes) - 3 else close - 0.2
            payload.append([base + index * 900_000, open_price, close + 0.4, open_price - 0.3, close, volumes[index]])
        self.client.ohlcv_by_timeframe["15m"] = payload
        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertFalse(signal.execution_trigger.frontrun_ready)
        self.assertNotEqual(signal.execution_entry_mode, "starter_frontrun_limit")

    def test_cta_robot_uses_limit_entry_for_starter_frontrun_path_with_starter_size(self) -> None:
        self._insert_status("trend")
        self.cta_config.starter_frontrun_fraction = 0.2
        self.cta_config.starter_frontrun_minimum_score = 70.0
        self._set_bullish_higher_timeframes()
        execution_closes = [90 + index * 0.25 for index in range(54)] + [103.8, 104.3, 104.9, 105.4, 105.8, 105.99]
        base = 1_700_000_000_000
        payload = []
        volumes = [100.0 + index for index in range(54)] + [160.0, 170.0, 180.0, 220.0, 235.0, 250.0]
        for index, close in enumerate(execution_closes):
            open_price = close - 0.35 if index >= len(execution_closes) - 3 else close - 0.2
            payload.append([base + index * 900_000, open_price, close + 0.4, open_price - 0.3, close, volumes[index]])
        self.client.ohlcv_by_timeframe["15m"] = payload
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)

        robot.config.starter_quality_minimum_score = 0.0
        robot.config.scale_in_quality_minimum_score = 0.0
        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            result = robot.run()

        self.assertEqual(result.action, "cta:open_long_limit")
        self.assertEqual(len(self.client.limit_orders), 1)
        self.assertEqual(self.client.limit_orders[0]["params"]["executionMode"], "starter_frontrun")
        expected_normal_amount = robot._calculate_entry_amount(execution_closes[-1])
        self.assertAlmostEqual(
            self.client.limit_orders[0]["amount"],
            round(expected_normal_amount * self.cta_config.starter_frontrun_fraction, 8),
        )

    def test_mtf_engine_allows_starter_short_frontrun_for_strong_bearish_impulse(self) -> None:
        major_closes = [220 - 1.0 * index for index in range(60)]
        swing_closes = [160 - 0.8 * index for index in range(60)]
        execution_closes = [120 - index * 0.2 for index in range(54)] + [109.5, 109.1, 108.8, 108.4, 108.0, 107.7]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        base = 1_700_000_000_000
        payload = []
        volumes = [100.0 + index for index in range(54)] + [160.0, 170.0, 180.0, 220.0, 235.0, 250.0]
        for index, close in enumerate(execution_closes):
            open_price = close + 0.35 if index >= len(execution_closes) - 3 else close + 0.2
            payload.append([base + index * 900_000, open_price, open_price + 0.3, close - 0.4, close, volumes[index]])
        self.client.ohlcv_by_timeframe["15m"] = payload
        self.cta_config.starter_frontrun_minimum_score = 70.0
        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)

        import pandas as pd
        bearish_kdj = pd.DataFrame({
            "k": [60.0] * 58 + [45.0, 40.0],
            "d": [50.0] * 58 + [50.0, 45.0],
        })
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 60,
                "upper_band": [130.0 - 0.2 * index for index in range(60)],
                "supertrend": [130.0 - 0.2 * index for index in range(60)],
                "atr": [2.0] * 60,
            }
        )
        swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 60,
                "upper_band": [120.0] * 60,
                "supertrend": [120.0] * 60,
                "atr": [1.0] * 60,
            }
        )
        bearish_obv = OBVConfirmationSnapshot(-100.0, 0.0, -10.0, 0.0, 1.0, -2.0)

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=bearish_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, swing_supertrend, swing_supertrend],
            ),
            patch("market_adaptive.strategies.mtf_engine.compute_obv_confirmation_snapshot", return_value=bearish_obv),
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.execution_entry_mode, "starter_short_frontrun_limit")
        self.assertTrue(signal.execution_trigger.frontrun_ready)
        self.assertTrue(signal.bearish_ready)

    def test_mtf_engine_blocks_starter_short_frontrun_when_score_is_below_floor(self) -> None:
        major_closes = [220 - 1.0 * index for index in range(60)]
        swing_closes = [160 - 0.8 * index for index in range(60)]
        execution_closes = [120 - index * 0.2 for index in range(54)] + [109.5, 109.1, 108.8, 108.4, 108.0, 107.7]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        base = 1_700_000_000_000
        payload = []
        volumes = [100.0 + index for index in range(54)] + [160.0, 170.0, 180.0, 220.0, 235.0, 250.0]
        for index, close in enumerate(execution_closes):
            open_price = close + 0.35 if index >= len(execution_closes) - 3 else close + 0.2
            payload.append([base + index * 900_000, open_price, open_price + 0.3, close - 0.4, close, volumes[index]])
        self.client.ohlcv_by_timeframe["15m"] = payload
        self.cta_config.starter_frontrun_minimum_score = 95.0
        engine = MultiTimeframeSignalEngine(self.client, self.cta_config)

        import pandas as pd
        bearish_kdj = pd.DataFrame({
            "k": [60.0] * 58 + [45.0, 40.0],
            "d": [50.0] * 58 + [50.0, 45.0],
        })
        major_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 60,
                "upper_band": [130.0 - 0.2 * index for index in range(60)],
                "supertrend": [130.0 - 0.2 * index for index in range(60)],
                "atr": [2.0] * 60,
            }
        )
        swing_supertrend = pd.DataFrame(
            {
                "direction": [-1] * 60,
                "lower_band": [100.0] * 60,
                "upper_band": [120.0] * 60,
                "supertrend": [120.0] * 60,
                "atr": [1.0] * 60,
            }
        )
        bearish_obv = OBVConfirmationSnapshot(-100.0, 0.0, -10.0, 0.0, 1.0, -2.0)

        with (
            patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=bearish_kdj),
            patch(
                "market_adaptive.strategies.mtf_engine.compute_supertrend",
                side_effect=[major_supertrend, swing_supertrend, swing_supertrend],
            ),
            patch("market_adaptive.strategies.mtf_engine.compute_obv_confirmation_snapshot", return_value=bearish_obv),
        ):
            signal = engine.build_signal()

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertNotEqual(signal.execution_entry_mode, "starter_short_frontrun_limit")
        self.assertFalse(signal.execution_trigger.frontrun_ready)

    def test_grid_robot_restores_balanced_levels_when_bearish_bias_is_not_strong_enough(self) -> None:
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, market_oracle=None, use_dynamic_range=False)
        robot._resolve_bias_value = lambda: -0.24

        profile = robot._resolve_grid_bias_profile(atr_value=10.0)
        buy_prices, sell_prices = robot._derive_layer_prices(96.0, 100.0, 104.0, bias_profile=profile)

        self.assertEqual(profile.center_shift, 0.0)
        self.assertEqual(profile.buy_levels, 0)
        self.assertEqual(profile.sell_levels, 0)
        self.assertEqual(len(buy_prices), 4)
        self.assertEqual(len(sell_prices), 4)

    def test_cta_robot_uses_limit_entry_for_weak_bull_bias_path(self) -> None:
        self._insert_status("trend")
        major_closes = [200 - 0.5 * index for index in range(60)]
        swing_closes = [100.0] * 20 + [100.2, 100.4, 100.7, 101.0, 101.3, 101.8, 102.2, 102.7, 103.1, 103.5, 103.9, 104.4, 104.8, 105.2, 105.7, 106.1, 106.5, 106.9, 107.2, 107.5, 107.9, 108.2, 108.5, 108.9, 109.2, 109.6, 110.0, 110.3, 110.7, 111.0, 111.4, 111.8, 112.2, 112.5, 112.9, 113.3, 113.6, 114.0, 114.4, 114.8]
        execution_closes = [100.0] * 56 + [100.2, 100.3, 100.4, 100.5]
        self._set_ohlcv("4h", major_closes, 14_400_000)
        self._set_ohlcv("1h", swing_closes, 3_600_000)
        self._set_ohlcv("15m", execution_closes, 900_000)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)
        mocked_kdj = self._mock_execution_kdj(bars=60, golden_cross_bar_from_end=2)

        with patch("market_adaptive.strategies.mtf_engine.compute_kdj", return_value=mocked_kdj):
            result = robot.run()

        self.assertEqual(result.action, "cta:no_signal")
        self.assertEqual(len(self.client.limit_orders), 0)
        self.assertIsNone(robot.position)

    def test_hands_coordinator_runs_both_robots(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0, width=4.0, length=120)
        self.client.ohlcv_by_timeframe["1h"] = []
        for i in range(80):
            ts = 60_000 + i * 3_600_000
            self.client.ohlcv_by_timeframe["1h"].append([ts, 100.0, 105.0, 95.0, 100.0, 120.0])
        coordinator = HandsCoordinator(
            cta_robot=CTARobot(self.client, self.database, self.cta_config, self.execution),
            grid_robot=GridRobot(self.client, self.database, self.grid_config, self.execution, market_oracle=None, use_dynamic_range=False),
        )

        summary = coordinator.run_once()

        self.assertFalse(summary.cta.active)
        self.assertTrue(summary.grid.active)


if __name__ == "__main__":
    unittest.main()
