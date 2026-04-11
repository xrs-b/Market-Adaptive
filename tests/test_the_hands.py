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
        self.order_book = {
            "bids": [[100.0 - index * 0.1, 1.6] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 1.0] for index in range(20)],
        }

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
        payload = {"symbol": symbol, "side": side, "amount": amount, "price": price, "status": "open", **kwargs}
        self.limit_orders.append(payload)
        return payload

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

    def price_to_precision(self, symbol: str, price: float) -> float:
        del symbol
        return round(float(price), 8)

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
                lower_last_close - 4.0,
                lower_last_close - 3.2,
                lower_last_close - 2.4,
                lower_last_close - 1.6,
                lower_last_close - 0.8,
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
                lower_last_close + 0.55,
            ]
        )
        self._set_ohlcv("15m", closes, 900_000)
        self._set_bullish_higher_timeframes()

    def _load_bullish_ready_without_execution_trigger(self) -> None:
        closes = [90 + index * 0.25 for index in range(55)] + [104.0, 103.4, 102.9, 102.4, 101.9]
        self._set_ohlcv("15m", closes, 900_000)
        self._set_bullish_higher_timeframes()

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

    def test_cta_robot_enters_bullish_ready_state_before_execution_trigger(self) -> None:
        self._insert_status("trend")
        self._load_bullish_ready_without_execution_trigger()
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.action, "cta:bullish_ready")
        self.assertEqual(len(self.client.market_orders), 0)
        self.assertIsNone(robot.position)

    def test_cta_robot_skips_bullish_entry_while_price_is_still_inside_value_area(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal_inside_value_area(lower_last_close=100.0)
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution)

        result = robot.run()

        self.assertTrue(result.active)
        self.assertEqual(result.action, "cta:range_filter_blocked")
        self.assertEqual(len(self.client.market_orders), 0)
        self.assertIsNone(robot.position)

    def test_cta_robot_requires_configured_obv_breakout_strength(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        strict_config = CTAConfig(
            symbol="BTC/USDT",
            lower_timeframe="15m",
            higher_timeframe="1h",
            atr_trailing_multiplier=1.0,
            stop_loss_atr=2.0,
            first_take_profit_size=0.5,
            second_take_profit_size=0.25,
            obv_zscore_threshold=999.0,
        )
        robot = CTARobot(self.client, self.database, strict_config, self.execution)

        result = robot.run()

        self.assertEqual(result.action, "cta:range_filter_blocked")
        self.assertEqual(len(self.client.market_orders), 0)

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

    def test_cta_robot_uses_boosted_risk_percent_for_fully_aligned_mtf_entry(self) -> None:
        self._insert_status("trend")
        self._load_bullish_signal(lower_last_close=100.0)
        risk_manager = DummyRiskManager()
        robot = CTARobot(self.client, self.database, self.cta_config, self.execution, risk_manager=risk_manager)

        result = robot.run()

        self.assertEqual(result.action, "cta:open_long")
        self.assertEqual(len(risk_manager.position_size_calls), 1)
        self.assertAlmostEqual(
            risk_manager.position_size_calls[0]["risk_percent"],
            self.cta_config.boosted_risk_percent_per_trade,
        )
        self.assertIsNotNone(robot.position)
        self.assertAlmostEqual(robot.position.risk_percent, self.cta_config.boosted_risk_percent_per_trade)

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

    def test_cta_robot_uses_order_flow_limit_price_for_high_conviction_entry(self) -> None:
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
        self.assertGreater(self.client.limit_orders[0]["price"], 100.1)
        self.assertIsNotNone(robot.position)

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
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False)

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

        self.assertEqual(first_count, 10)
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
        self.assertEqual(len(self.client.cancel_all_calls), 1)

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
        self.assertEqual(len(self.client.cancel_all_calls), 1)
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
        self.assertEqual(len(self.client.cancel_all_calls), 1)

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

    def test_grid_robot_websocket_fill_places_reduce_only_counter_order_with_pos_side(self) -> None:
        self._insert_status("sideways")
        self._load_sideways_grid_data(center=100.0)
        robot = GridRobot(self.client, self.database, self.grid_config, self.execution, use_dynamic_range=False, market_oracle=None)
        context = robot._refresh_grid_context(self.client.last_price, anchor_timestamp_ms=1)
        assert context is not None
        robot._cached_context = context

        robot._on_ws_orders({
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
