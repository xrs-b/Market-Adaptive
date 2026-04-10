from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from market_adaptive.config import CTAConfig, ExecutionConfig, GridConfig, SentimentConfig
from market_adaptive.db import DatabaseInitializer, MarketStatusRecord
from market_adaptive.sentiment import SentimentAnalyst
from market_adaptive.strategies import CTARobot, GridRobot, HandsCoordinator


class DummyClient:
    def __init__(self) -> None:
        self.market_orders = []
        self.limit_orders = []
        self.cancel_all_calls = []
        self.close_all_calls = []
        self.futures_settings_calls = []
        self.last_price = 100.0
        self.ohlcv = []
        self.ohlcv_by_timeframe = {}
        self.positions = []
        self.latest_long_short_ratio = None

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        del symbol, since
        payload = self.ohlcv_by_timeframe.get(timeframe, self.ohlcv)
        return payload[-limit:]

    def fetch_last_price(self, symbol: str) -> float:
        del symbol
        return self.last_price

    def place_market_order(self, symbol: str, side: str, amount: float, **kwargs):
        payload = {"symbol": symbol, "side": side, "amount": amount, **kwargs}
        self.market_orders.append(payload)
        return payload

    def place_limit_order(self, symbol: str, side: str, amount: float, price: float, **kwargs):
        payload = {"symbol": symbol, "side": side, "amount": amount, "price": price, **kwargs}
        self.limit_orders.append(payload)
        return payload

    def cancel_all_orders(self, symbol: str):
        self.cancel_all_calls.append(symbol)
        return []

    def close_all_positions(self, symbol: str):
        self.close_all_calls.append(symbol)
        return []

    def fetch_positions(self, symbols=None):
        del symbols
        return list(self.positions)

    def ensure_futures_settings(self, symbol: str, leverage: int, margin_mode: str | None = None) -> None:
        self.futures_settings_calls.append(
            {"symbol": symbol, "leverage": leverage, "margin_mode": margin_mode}
        )

    def get_position_liquidation_price(self, position: dict) -> float | None:
        liquidation_price = position.get("liquidationPrice") or position.get("info", {}).get("liqPx")
        if liquidation_price in (None, "", 0, "0"):
            return None
        return abs(float(liquidation_price))

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        del symbol
        return round(float(amount), 8)

    def get_contract_value(self, symbol: str) -> float:
        del symbol
        return 1.0

    def estimate_notional(self, symbol: str, amount: float, price: float) -> float:
        del symbol
        return abs(float(amount)) * abs(float(price))

    def fetch_latest_long_short_account_ratio(self, symbol: str, timeframe: str = "5m", limit: int = 1):
        del symbol, timeframe, limit
        return self.latest_long_short_ratio


class DummyRiskManager:
    def __init__(self, *, allow_grid: bool = True, grid_reason: str | None = None) -> None:
        self.allow_grid = allow_grid
        self.grid_reason = grid_reason
        self.cta_profiles = []
        self.grid_profiles = []

    def calculate_position_size(self, symbol: str, risk_percent: float, stop_loss_atr: float, *, atr_value=None, last_price=None) -> float:
        del symbol, risk_percent, stop_loss_atr, atr_value, last_price
        return 0.02

    def can_open_new_position(self, symbol: str, requested_notional: float, strategy_name: str | None = None):
        del symbol, requested_notional
        if strategy_name == "grid" and not self.allow_grid:
            return False, self.grid_reason
        return True, None

    def check_symbol_notional_limit(self, symbol: str, requested_notional: float):
        del symbol, requested_notional
        return True, None

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
            lower_timeframe="15m",
            higher_timeframe="1h",
            atr_trailing_multiplier=1.0,
            stop_loss_atr=2.0,
            first_take_profit_size=0.5,
            second_take_profit_size=0.25,
        )
        self.grid_config = GridConfig(
            symbol="BTC/USDT",
            timeframe="1h",
            lookback_limit=80,
            bollinger_period=20,
            bollinger_std=2.0,
            levels=10,
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

    def _load_bullish_signal(self, lower_last_close: float = 100.0, higher_last_close: float = 140.0) -> None:
        lower_closes = [lower_last_close - 0.4 * (59 - index) for index in range(60)]
        higher_closes = [higher_last_close - 1.0 * (59 - index) for index in range(60)]
        self._set_ohlcv("15m", lower_closes, 900_000)
        self._set_ohlcv("1h", higher_closes, 3_600_000)

    def _load_pullback_after_rally(self, latest_close: float) -> None:
        closes = [80 + index * 0.45 for index in range(56)] + [103.6, 104.8, 106.0, latest_close]
        self._set_ohlcv("15m", closes, 900_000)
        higher_closes = [140 - 1.0 * (59 - index) for index in range(60)]
        self._set_ohlcv("1h", higher_closes, 3_600_000)

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
        expected_stop = robot.position.entry_price - robot.position.atr_value * self.cta_config.stop_loss_atr
        self.assertAlmostEqual(robot.position.stop_price, expected_stop)

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
        self.assertAlmostEqual(self.client.market_orders[0]["amount"], 0.01)
        self.assertIsNotNone(robot.position)
        self.assertAlmostEqual(robot.position.initial_size, 0.01)

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
        self.assertAlmostEqual(self.client.market_orders[1]["amount"], 0.01)

        self._load_bullish_signal(lower_last_close=106.0)
        third_result = robot.run()
        self.assertEqual(third_result.action, "cta:take_profit_5pct")
        self.assertAlmostEqual(self.client.market_orders[2]["amount"], 0.005)
        self.assertIsNotNone(robot.position)

        robot.position.stop_price = 104.0
        self._load_pullback_after_rally(latest_close=103.0)
        fourth_result = robot.run()
        self.assertEqual(fourth_result.action, "cta:atr_stop_all_out")
        self.assertAlmostEqual(self.client.market_orders[3]["amount"], 0.005)
        self.assertIsNone(robot.position)

    def test_grid_robot_uses_neutral_price_band_and_martingale_buys(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertIn("grid:placed_10_orders@100.00", result.action)
        self.assertIn("rebalances=0", result.action)
        self.assertEqual(len(self.client.limit_orders), 10)
        self.assertEqual(len(self.client.futures_settings_calls), 1)
        self.assertEqual(self.client.futures_settings_calls[0]["leverage"], 5)
        self.assertEqual(self.client.futures_settings_calls[0]["margin_mode"], "isolated")
        self.assertEqual(self.client.cancel_all_calls, ["BTC/USDT"])

        buy_orders = [order for order in self.client.limit_orders if order["side"] == "buy"]
        sell_orders = [order for order in self.client.limit_orders if order["side"] == "sell"]
        self.assertEqual(len(buy_orders), 5)
        self.assertEqual(len(sell_orders), 5)
        self.assertGreater(buy_orders[-1]["amount"], buy_orders[0]["amount"])
        self.assertTrue(all(not order.get("reduce_only", False) for order in sell_orders))
        self.assertTrue(all(order["price"] >= 97.0 for order in buy_orders))
        self.assertTrue(all(order["price"] <= 103.0 for order in sell_orders))
        self.assertLess(max(buy_orders, key=lambda order: order["price"])["price"], 100.0)
        self.assertGreater(min(sell_orders, key=lambda order: order["price"])["price"], 100.0)

    def test_grid_robot_honors_risk_observe_block_and_stops_opening_orders(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        risk_manager = DummyRiskManager(allow_grid=False, grid_reason="grid_observe_lower_break|price=95.00|lower=97.00")
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, risk_manager=risk_manager)

        result = robot.run()

        self.assertEqual(result.action, "grid:risk_blocked")
        self.assertEqual(len(self.client.limit_orders), 0)
        self.assertEqual(len(risk_manager.grid_profiles), 1)

    def test_grid_robot_cools_down_repeatedly_triggered_buy_layer(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        now = datetime(2026, 4, 10, 3, 0, tzinfo=timezone.utc)
        now_values = [
            now,
            now + timedelta(minutes=2),
            now + timedelta(minutes=4),
            now + timedelta(minutes=5),
        ]
        robot = GridRobot(
            self.client,
            self.database,
            self.grid_config,
            self.execution,
            now_provider=lambda: now_values.pop(0),
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

        self.assertEqual(first_count, 10)
        self.assertEqual(second_count, 10)
        self.assertEqual(third_count, 10)
        self.assertEqual(fourth_count, 9)
        self.assertIn("cooldown=1", fourth_result.action)
        self.assertIn("cooldown=0", first_result.action)
        self.assertIn("cooldown=0", second_result.action)
        self.assertIn("cooldown=0", third_result.action)

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

    def test_hands_coordinator_runs_both_robots(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        coordinator = HandsCoordinator(
            cta_robot=CTARobot(self.client, self.database, self.cta_config, self.execution),
            grid_robot=GridRobot(self.client, self.database, self.grid_config, self.execution),
        )

        summary = coordinator.run_once()

        self.assertFalse(summary.cta.active)
        self.assertTrue(summary.grid.active)


if __name__ == "__main__":
    unittest.main()
