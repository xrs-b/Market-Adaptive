from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from market_adaptive.config import CTAConfig, DiscordNotificationConfig, ExecutionConfig, GridConfig, MarketOracleConfig
from market_adaptive.db import DatabaseInitializer, MarketStatusRecord
from market_adaptive.notifiers.discord_notifier import DiscordNotifier
from market_adaptive.oracles.market_oracle import MarketOracle
from market_adaptive.strategies import CTARobot, GridRobot
from market_adaptive.strategies.cta_robot import CTANearMissSample, ManagedPosition
from market_adaptive.testsupport import DummyNotifier


class DummyClient:
    def __init__(self) -> None:
        self.market_orders = []
        self.limit_orders = []
        self.cancel_all_calls = []
        self.close_all_calls = []
        self.last_price = 100.0
        self.market_order_price = None
        self.total_equity = 1000.0
        self.ohlcv = []
        self.ohlcv_by_timeframe = {}
        self.order_book = {
            "bids": [[100.0 - index * 0.1, 1.6] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 1.0] for index in range(20)],
        }

    def fetch_ohlcv(self, symbol: str, timeframe: str = "15m", limit: int = 200, since=None):
        payload = self.ohlcv_by_timeframe.get(timeframe, self.ohlcv)
        return payload[-limit:]

    def fetch_last_price(self, symbol: str) -> float:
        return self.last_price

    def fetch_order_book(self, symbol: str, limit: int | None = None):
        del symbol
        if limit is None:
            return self.order_book
        return {
            "bids": list(self.order_book.get("bids", []))[:limit],
            "asks": list(self.order_book.get("asks", []))[:limit],
        }

    def fetch_positions(self, symbols=None):
        del symbols
        return []

    def place_market_order(self, symbol: str, side: str, amount: float, **kwargs):
        payload = {"symbol": symbol, "side": side, "amount": amount, **kwargs}
        if self.market_order_price is not None:
            payload["average"] = self.market_order_price
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

    def fetch_open_orders(self, symbol: str):
        del symbol
        return list(self.limit_orders)

    def price_to_precision(self, symbol: str, price: float) -> float:
        del symbol
        return float(price)

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        del symbol
        return float(amount)

    def fetch_total_equity(self, quote_currency: str = "USDT") -> float:
        del quote_currency
        return self.total_equity

    def get_contract_value(self, symbol: str) -> float:
        del symbol
        return 1.0

    def estimate_notional(self, symbol: str, amount: float, price: float) -> float:
        del symbol
        return abs(float(amount)) * abs(float(price))


class CapturingDiscordNotifier(DiscordNotifier):
    def __init__(self) -> None:
        super().__init__(DiscordNotificationConfig(enabled=True, webhook_url="https://example.invalid/webhook"))
        self.payloads: list[dict] = []

    async def _post_payload(self, payload: dict) -> bool:
        self.payloads.append(payload)
        return True

    def submit_and_wait(self, coro) -> bool:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
            return True
        finally:
            loop.close()

    def _submit_coroutine(self, coro) -> bool:
        if getattr(coro, "cr_code", None) is not None and coro.cr_code.co_name in {"_flush_grid_trade_bucket_after_delay", "_flush_grid_profit_bucket_after_delay", "_flush_cleanup_bucket_after_delay"}:
            coro.close()
            return True
        return self.submit_and_wait(coro)


class NotificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = DatabaseInitializer(Path(self.temp_dir.name) / "market_adaptive.sqlite3")
        self.database.initialize()
        self.notifier = DummyNotifier()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _set_bullish_ohlcv(self, client: DummyClient, lower_last_close: float = 100.0) -> None:
        base = 1_700_000_000_000
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
        higher_closes = [140 - 1.0 * (59 - index) for index in range(60)]
        major_closes = [220 - 2.0 * (59 - index) for index in range(60)]
        client.ohlcv_by_timeframe["15m"] = []
        for index, close in enumerate(lower_closes):
            volume = 100 + index * 3
            if index >= len(lower_closes) - 4:
                volume *= 8
            client.ohlcv_by_timeframe["15m"].append(
                [base + index * 900_000, close - 0.3, close + 0.4, close - 0.5, close, volume]
            )
        client.ohlcv_by_timeframe["1h"] = [
            [base + index * 3_600_000, close - 0.5, close + 0.8, close - 0.7, close, 200 + index * 5]
            for index, close in enumerate(higher_closes)
        ]
        client.ohlcv_by_timeframe["4h"] = [
            [base + index * 14_400_000, close - 0.8, close + 1.0, close - 1.1, close, 260 + index * 7]
            for index, close in enumerate(major_closes)
        ]

    def test_market_oracle_notifies_on_status_change(self) -> None:
        client = DummyClient()
        oracle = MarketOracle(client, self.database, MarketOracleConfig(), notifier=self.notifier)
        oracle.collect_market_snapshot = lambda: type('Snapshot', (), {
            'symbol': 'BTC/USDT', 'strongest_adx': 30.0, 'strongest_volatility': 0.02,
            'higher_timeframe': '1h',
            'lower_timeframe': '15m',
            'higher': type('I', (), {
                'adx_value': 30.0,
                'adx_rising': True,
                'adx_previous': 28.0,
                'adx_trend_label': 'rising',
                'di_gap': 14.0,
                'plus_di_value': 32.0,
                'minus_di_value': 18.0,
                'bb_width_expanding': True,
                'bb_width': 0.18,
                'bb_width_previous': 0.16,
            })(),
            'lower': type('I', (), {
                'adx_value': 18.0,
                'adx_rising': False,
                'adx_previous': 19.0,
                'adx_trend_label': 'flat',
                'di_gap': 3.0,
                'plus_di_value': 21.0,
                'minus_di_value': 18.0,
                'bb_width_expanding': False,
                'bb_width': 0.08,
                'bb_width_previous': 0.09,
            })(),
        })()

        oracle.run_once()

        self.assertEqual(len(self.notifier.messages), 1)
        self.assertEqual(self.notifier.messages[0][0], '市场状态已切换')

    def test_cta_robot_trade_execution_uses_trade_notifier_without_generic_action_message(self) -> None:
        client = DummyClient()
        notifier = self.notifier
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'trend', 28.0, 0.02))
        self._set_bullish_ohlcv(client, lower_last_close=100.0)

        robot = CTARobot(client, db, CTAConfig(), ExecutionConfig(), notifier=notifier)
        result = robot.run()

        self.assertEqual(result.action, 'cta:open_long')
        self.assertFalse(any(title == '策略动作通知' for title, _ in notifier.messages))
        self.assertEqual(len(notifier.trade_calls), 1)
        self.assertEqual(notifier.trade_calls[0]['strategy'], 'cta')
        self.assertEqual(notifier.trade_calls[0]['signal'], 'cta_open_long')
        self.assertEqual(notifier.trade_calls[0]['symbol'], 'BTC/USDT')
        self.assertEqual(client.market_orders[0]['side'], 'buy')

    def test_grid_robot_does_not_notify_on_regular_grid_placement(self) -> None:
        client = DummyClient()
        notifier = self.notifier
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'sideways', 10.0, 0.01))
        client.ohlcv_by_timeframe['1h'] = [
            [1_700_000_000_000 + i * 3_600_000, 100.0, 101.0, 99.0, 100.0, 120.0]
            for i in range(80)
        ]
        robot = GridRobot(client, db, GridConfig(), ExecutionConfig(), notifier=notifier, market_oracle=None, use_dynamic_range=False)

        result = robot.run()

        self.assertTrue(result.action.startswith('grid:placed_'))
        self.assertFalse(any(title == '策略动作通知' for title, _ in notifier.messages))

    def test_grid_robot_notifies_on_cleanup(self) -> None:
        client = DummyClient()
        notifier = self.notifier
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'trend', 28.0, 0.02))
        robot = GridRobot(client, db, GridConfig(), ExecutionConfig(), notifier=notifier)
        robot.run()
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:05:00+00:00', 'BTC/USDT', 'sideways', 10.0, 0.01))
        robot.run()
        notifier.flush_strategy_cleanup_notifications()

        self.assertTrue(any(title == '策略切换清理' for title, _ in notifier.messages))

    def test_grid_robot_notifies_on_flash_crash_trigger(self) -> None:
        client = DummyClient()
        notifier = self.notifier
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'sideways', 10.0, 0.01))
        client.ohlcv_by_timeframe['1h'] = [
            [1_700_000_000_000 + i * 3_600_000, 100.0, 105.0, 95.0, 100.0, 120.0]
            for i in range(80)
        ]
        client.ohlcv_by_timeframe['1m'] = [
            [1_700_000_000_000, 100.0, 100.1, 99.9, 100.0, 100.0],
            [1_700_000_060_000, 100.0, 118.0, 98.0, 100.0, 120.0],
        ]
        robot = GridRobot(client, db, GridConfig(), ExecutionConfig(), notifier=notifier, market_oracle=None, use_dynamic_range=False)

        result = robot.run()

        self.assertEqual(result.action.split('|')[0], 'grid:flash_crash_triggered')
        self.assertTrue(any(title == 'Grid 风险预警' for title, _ in notifier.messages))
        self.assertTrue(any('flash_crash_triggered' in body for _, body in notifier.messages))

    def test_discord_notifier_builds_embed_and_aggregates_grid_fills(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_trade("buy", 100.0, 0.2, "grid", "grid_fill_websocket", symbol="BTC/USDT")
        notifier.notify_trade("buy", 102.0, 0.1, "grid", "grid_fill_websocket", symbol="BTC/USDT")

        self.assertEqual(len(notifier.payloads), 0)
        bucket_key = "grid::grid_fill_websocket::BUY::BTC/USDT"
        notifier.submit_and_wait(notifier._flush_grid_trade_bucket_after_delay(bucket_key, 0.0))

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]["embeds"][0]
        self.assertEqual(embed["color"], 0x00FF00)
        self.assertEqual(embed["title"], "Grid 对冲成交汇总")
        field_map = {field["name"]: field["value"] for field in embed["fields"]}
        self.assertEqual(field_map["交易对"], "BTC/USDT")
        self.assertEqual(field_map["成交笔数"], "2")
        self.assertEqual(field_map["累计成交额"], "30.2000 USDT")

    def test_discord_notifier_builds_cta_open_trade_payload(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_trade("buy", 100.0, 0.2, "cta", "cta_open_long", symbol="BTC/USDT")

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]["embeds"][0]
        self.assertEqual(embed["title"], "CTA 开仓成交")
        field_map = {field["name"]: field["value"] for field in embed["fields"]}
        self.assertEqual(field_map["交易对"], "BTC/USDT")
        self.assertEqual(field_map["策略"], "CTA 策略")
        self.assertEqual(field_map["触发信号"], "cta_open_long")

    def test_discord_notifier_merges_strategy_cleanup_for_same_status_switch(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_strategy_cleanup(
            strategy="cta",
            symbol="BTC/USDT",
            reason="status_switch:trend->sideways",
            result="cta:flatten_all",
            overview="CTA 策略已完成状态切换清理。",
        )
        notifier.notify_strategy_cleanup(
            strategy="grid",
            symbol="BTC/USDT",
            reason="status_switch:trend->sideways",
            result="grid:regime_cleanup|market_long:0.10000000",
            overview="网格策略已完成状态切换清理。",
        )

        self.assertEqual(len(notifier.payloads), 0)
        notifier.submit_and_wait(notifier._flush_cleanup_bucket_after_delay("BTC/USDT::status_switch:trend->sideways", 0.0))

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]["embeds"][0]
        self.assertEqual(embed["title"], "策略切换清理")
        field_map = {field["name"]: field["value"] for field in embed["fields"]}
        self.assertEqual(field_map["状态切换"], "trend → sideways")
        self.assertIn("CTA 策略、网格策略", field_map["清理策略"])
        self.assertEqual(field_map["触发说明"], "市场状态由 trend 切换到 sideways")
        self.assertIn("CTA 策略：cta:flatten_all", field_map["清理结果"])
        self.assertIn("网格策略：grid:regime_cleanup", field_map["清理结果"])

    def test_discord_notifier_keeps_single_cleanup_notification_when_only_one_strategy_runs(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_strategy_cleanup(
            strategy="grid",
            symbol="BTC/USDT",
            reason="status_switch:sideways->trend",
            result="grid:regime_cleanup_idle",
            overview="网格策略已完成状态切换清理。",
        )
        notifier.submit_and_wait(notifier._flush_cleanup_bucket_after_delay("BTC/USDT::status_switch:sideways->trend", 0.0))

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]["embeds"][0]
        self.assertEqual(embed["title"], "策略切换清理")
        field_map = {field["name"]: field["value"] for field in embed["fields"]}
        self.assertEqual(field_map["清理策略"], "网格策略")
        self.assertIn("grid:regime_cleanup_idle", field_map["清理结果"])

    def test_grid_websocket_fill_calls_trade_notifier(self) -> None:
        client = DummyClient()
        notifier = DummyNotifier()
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'sideways', 10.0, 0.01))
        client.ohlcv_by_timeframe['1h'] = [
            [1_700_000_000_000 + i * 3_600_000, 100.0, 101.0, 99.0, 100.0, 120.0]
            for i in range(80)
        ]
        robot = GridRobot(client, db, GridConfig(), ExecutionConfig(), notifier=notifier, market_oracle=None, use_dynamic_range=False)
        robot._cached_context = robot._fallback_context(100.0, 2.0)
        client.fetch_positions = lambda symbols=None: [{"contracts": 1.0, "side": "long", "entryPrice": 100.0}]
        client.price_to_precision = lambda symbol, price: float(price)
        client.amount_to_precision = lambda symbol, amount: float(amount)
        client.fetch_order = lambda order_id, symbol: {"status": "open", "id": order_id, "symbol": symbol}
        client.limit_orders.clear()

        robot._on_ws_orders({"status": "filled", "filled": 0.5, "side": "buy", "average": 100.0, "id": "fill-1"})

        self.assertEqual(len(notifier.trade_calls), 1)
        self.assertEqual(notifier.trade_calls[0]["strategy"], "grid")
        self.assertEqual(notifier.trade_calls[0]["signal"], "grid_fill_websocket")


    def test_grid_reduce_only_fill_notifies_realized_profit(self) -> None:
        client = DummyClient()
        client.total_equity = 1500.0
        notifier = DummyNotifier()
        db = self.database
        db.insert_market_status(MarketStatusRecord('2026-04-10T00:00:00+00:00', 'BTC/USDT', 'sideways', 10.0, 0.01))
        client.ohlcv_by_timeframe['1h'] = [
            [1_700_000_000_000 + i * 3_600_000, 100.0, 101.0, 99.0, 100.0, 120.0]
            for i in range(80)
        ]
        robot = GridRobot(client, db, GridConfig(), ExecutionConfig(), notifier=notifier, market_oracle=None, use_dynamic_range=False)
        robot._cached_context = robot._fallback_context(100.0, 2.0)
        client.fetch_positions = lambda symbols=None: [{"contracts": 1.0, "side": "long", "entryPrice": 100.0}]
        client.fetch_order = lambda order_id, symbol: {"status": "open", "id": order_id, "symbol": symbol}

        def place_limit_order(symbol: str, side: str, amount: float, price: float, **kwargs):
            payload = {"symbol": symbol, "side": side, "amount": amount, "price": price, "id": "hedge-1", **kwargs}
            client.limit_orders.append(payload)
            return payload

        client.place_limit_order = place_limit_order

        robot._on_ws_orders({"status": "filled", "filled": 0.5, "side": "buy", "average": 100.0, "id": "fill-1"})
        robot._on_ws_orders({"status": "filled", "filled": 0.5, "side": "sell", "average": 101.0, "id": "hedge-1", "reduceOnly": True})

        self.assertEqual(len(notifier.profit_calls), 1)
        self.assertAlmostEqual(notifier.profit_calls[0]['pnl'], 0.5)
        self.assertAlmostEqual(notifier.profit_calls[0]['roi'], 1.0)
        self.assertAlmostEqual(notifier.profit_calls[0]['balance'], 1500.0)
        self.assertEqual(notifier.profit_calls[0]['strategy'], 'grid')

    def test_discord_notifier_localizes_profit_payload_and_timestamp(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_profit(pnl=12.34, roi=5.67, balance=1234.5)

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]["embeds"][0]
        self.assertEqual(embed["title"], "CTA 已实现盈利")
        field_map = {field["name"]: field["value"] for field in embed["fields"]}
        self.assertIn("本次盈亏", field_map)
        self.assertEqual(field_map["策略"], "CTA 策略")
        self.assertEqual(field_map["交易对"], "未知")
        self.assertNotIn("timestamp", embed)
        footer_lines = embed["footer"]["text"].splitlines()
        self.assertRegex(footer_lines[0], r"^通知时间：\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

    def test_discord_notifier_aggregates_grid_profit_notifications(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_profit(pnl=1.2, roi=1.0, balance=1001.0, strategy='grid', symbol='BTC/USDT', side='SELL', exit_price=101.0, size=0.1)
        notifier.notify_profit(pnl=0.8, roi=0.5, balance=1001.8, strategy='grid', symbol='BTC/USDT', side='SELL', exit_price=102.0, size=0.2)

        self.assertEqual(len(notifier.payloads), 0)
        notifier.submit_and_wait(notifier._flush_grid_profit_bucket_after_delay('grid::BTC/USDT', 0.0))

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]['embeds'][0]
        self.assertEqual(embed['title'], '网格已实现盈利')
        field_map = {field['name']: field['value'] for field in embed['fields']}
        self.assertEqual(field_map['平仓笔数'], '2')
        self.assertEqual(field_map['统计窗口'], '60秒')
        self.assertEqual(field_map['累计已实现盈亏'], '+2.0000 USDT')


    def test_discord_notifier_builds_cta_near_miss_report_payload(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_cta_near_miss_report(
            symbol='BTC/USDT',
            window_seconds=3600.0,
            samples=[
                CTANearMissSample(
                    symbol='BTC/USDT',
                    captured_at=1_700_000_000.0,
                    execution_trigger_reason='Triggered via Memory Window: KDJ crossed 1 bars ago + Price Breakout NOW',
                    execution_memory_active=True,
                    execution_memory_bars_ago=1,
                    execution_breakout=True,
                    execution_golden_cross=False,
                    obv_zscore=0.85,
                    obv_threshold=1.0,
                    obv_gap=0.15,
                    price=70960.6,
                )
            ],
        )

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]['embeds'][0]
        self.assertEqual(embed['title'], 'CTA 近失报告')
        self.assertEqual(embed['color'], 0xFFFF00)
        field_map = {field['name']: field['value'] for field in embed['fields']}
        self.assertEqual(field_map['统计窗口'], '1 小时')
        self.assertIn('OBV Z-Score 0.85 / 阈值 1.00 / 差距 0.15', field_map['最接近样本'])
        self.assertIn('Triggered via Memory Window', field_map['样本详情'])

    def test_discord_notifier_builds_signal_profiler_summary_payload(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_signal_profiler_summary(
            symbol='BTC/USDT',
            summary_interval=10,
            summary={
                'window_cycles': 10,
                'total_cycles': 20,
                'passed_regime': 8,
                'passed_swing': 5,
                'passed_trigger': 2,
                'regime_pass_rate_pct': 80.0,
                'swing_pass_rate_pct': 50.0,
                'trigger_pass_rate_pct': 20.0,
                'top_blockers': [('Blocked_By_OBV_STRENGTH_NOT_CONFIRMED', 4), ('PASSED', 2)],
                'dominant_blocking_layer': 'OBV',
                'dominant_blocking_label': 'OBV（执行过滤层）',
                'dominant_blocking_count': 4,
                'latest_blocker_reason': 'Blocked_By_BELOW_POC',
                'latest_execution_obv_zscore': 0.73,
                'latest_execution_obv_threshold': 1.0,
                'latest_execution_price': 70250.5,
                'latest_grid_center_gap': -55.2,
            },
        )

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]['embeds'][0]
        self.assertEqual(embed['title'], 'CTA 信号漏斗摘要｜主阻塞：OBV（执行过滤层）')
        self.assertEqual(embed['color'], 0xFFFF00)
        field_map = {field['name']: field['value'] for field in embed['fields']}
        self.assertEqual(field_map['当前主阻塞层'], 'OBV（执行过滤层）（4 次）')
        self.assertEqual(field_map['统计窗口'], '最近 10 个 CTA 周期')
        self.assertEqual(field_map['通过 Trigger'], '2/10 (20.0%)')
        self.assertEqual(field_map['最近价格'], '70250.5000')
        self.assertEqual(field_map['距离网格中心'], '-55.2000')
        self.assertIn('Blocked_By_OBV_STRENGTH_NOT_CONFIRMED × 4', field_map['主要拦截原因'])
        self.assertEqual(field_map['最近一次结果'], 'Blocked_By_BELOW_POC')

    def test_discord_notifier_marks_unavailable_signal_profiler_values_truthfully(self) -> None:
        notifier = CapturingDiscordNotifier()

        notifier.notify_signal_profiler_summary(
            symbol='BTC/USDT',
            summary_interval=10,
            summary={
                'window_cycles': 10,
                'total_cycles': 20,
                'passed_regime': 8,
                'passed_swing': 5,
                'passed_trigger': 2,
                'regime_pass_rate_pct': 80.0,
                'swing_pass_rate_pct': 50.0,
                'trigger_pass_rate_pct': 20.0,
                'top_blockers': [('Blocked_By_OBV_STRENGTH_NOT_CONFIRMED', 4)],
                'dominant_blocking_layer': 'OBV',
                'dominant_blocking_label': 'OBV（执行过滤层）',
                'dominant_blocking_count': 4,
                'latest_blocker_reason': 'Blocked_By_BELOW_POC',
                'latest_execution_obv_zscore': 0.73,
                'latest_execution_obv_threshold': 1.0,
                'latest_execution_price': None,
                'latest_grid_center_gap': None,
            },
        )

        self.assertEqual(len(notifier.payloads), 1)
        embed = notifier.payloads[0]['embeds'][0]
        field_map = {field['name']: field['value'] for field in embed['fields']}
        self.assertEqual(field_map['最近价格'], '暂无有效样本')
        self.assertEqual(field_map['距离网格中心'], '未进入执行层')

    def test_cta_take_profit_notifies_realized_profit(self) -> None:
        client = DummyClient()
        client.market_order_price = 102.0
        client.total_equity = 1234.5
        notifier = DummyNotifier()
        robot = CTARobot(client, self.database, CTAConfig(), ExecutionConfig(), notifier=notifier)
        robot.position = ManagedPosition(
            side='long',
            entry_price=100.0,
            initial_size=1.0,
            remaining_size=1.0,
            stop_price=95.0,
            best_price=100.0,
            atr_value=1.0,
            stop_distance=5.0,
        )

        reduced = robot._reduce_position(0.5)

        self.assertTrue(reduced)
        self.assertEqual(len(notifier.profit_calls), 1)
        self.assertAlmostEqual(notifier.profit_calls[0]['pnl'], 1.0)
        self.assertAlmostEqual(notifier.profit_calls[0]['roi'], 2.0)
        self.assertAlmostEqual(notifier.profit_calls[0]['balance'], 1234.5)
        self.assertEqual(notifier.profit_calls[0]['strategy'], 'cta')
        self.assertEqual(notifier.profit_calls[0]['symbol'], 'BTC/USDT')
        self.assertEqual(notifier.profit_calls[0]['side'], 'LONG')

    def test_cta_full_close_notifies_realized_loss_for_short(self) -> None:
        client = DummyClient()
        client.market_order_price = 102.0
        client.total_equity = 987.6
        notifier = DummyNotifier()
        robot = CTARobot(client, self.database, CTAConfig(), ExecutionConfig(), notifier=notifier)
        robot.position = ManagedPosition(
            side='short',
            entry_price=100.0,
            initial_size=1.5,
            remaining_size=1.5,
            stop_price=105.0,
            best_price=100.0,
            atr_value=1.0,
            stop_distance=5.0,
        )

        robot._close_remaining_position(reason='atr_stop')

        self.assertEqual(robot.position, None)
        self.assertEqual(len(notifier.profit_calls), 1)
        self.assertAlmostEqual(notifier.profit_calls[0]['pnl'], -3.0)
        self.assertAlmostEqual(notifier.profit_calls[0]['roi'], -2.0)
        self.assertAlmostEqual(notifier.profit_calls[0]['balance'], 987.6)
        self.assertEqual(notifier.profit_calls[0]['strategy'], 'cta')
        self.assertEqual(notifier.profit_calls[0]['symbol'], 'BTC/USDT')
        self.assertEqual(notifier.profit_calls[0]['side'], 'SHORT')


if __name__ == '__main__':
    unittest.main()
