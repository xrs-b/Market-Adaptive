from __future__ import annotations

import unittest

from market_adaptive.config import CTAConfig
from market_adaptive.strategies.order_flow_sentinel import OrderFlowSentinel


class DummyClient:
    def __init__(self, order_book: dict) -> None:
        self.order_book = order_book

    def fetch_order_book(self, symbol: str, limit: int | None = None) -> dict:
        del symbol, limit
        return self.order_book

    def price_to_precision(self, symbol: str, price: float) -> float:
        del symbol
        return round(float(price), 4)


class OrderFlowSentinelTests(unittest.TestCase):
    def test_assess_entry_computes_top20_imbalance_and_confirmation(self) -> None:
        order_book = {
            "bids": [[100.0 - index * 0.1, 3.0] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 1.5] for index in range(20)],
        }
        sentinel = OrderFlowSentinel(DummyClient(order_book), CTAConfig())

        assessment = sentinel.assess_entry("BTC/USDT", "buy", amount=0.02)

        self.assertAlmostEqual(assessment.bid_sum, 60.0)
        self.assertAlmostEqual(assessment.ask_sum, 30.0)
        self.assertAlmostEqual(assessment.imbalance_ratio, 2.0)
        self.assertTrue(assessment.confirmation_passed)
        self.assertTrue(assessment.high_conviction)

    def test_history_health_floor_can_block_borderline_confirmation(self) -> None:
        order_book = {
            "bids": [[100.0 - index * 0.1, 1.0] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 1.0] for index in range(20)],
        }
        config = CTAConfig(order_flow_confirmation_ratio=1.5, order_flow_health_sigma_multiplier=0.0)
        sentinel = OrderFlowSentinel(DummyClient(order_book), config)
        sentinel._imbalance_history.extend([2.0, 2.1, 2.2])

        assessment = sentinel.assess_entry("BTC/USDT", "buy", amount=0.02)

        self.assertFalse(assessment.confirmation_passed)
        self.assertEqual(assessment.reason, "imbalance_below_2.10")

    def test_decay_detection_blocks_even_when_current_ratio_is_above_threshold(self) -> None:
        order_book = {
            "bids": [[100.0 - index * 0.1, 1.8] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 1.0] for index in range(20)],
        }
        config = CTAConfig(order_flow_confirmation_ratio=1.5, order_flow_decay_lookback=3)
        sentinel = OrderFlowSentinel(DummyClient(order_book), config)
        sentinel._imbalance_history.extend([2.2, 2.0, 1.9])

        assessment = sentinel.assess_entry("BTC/USDT", "buy", amount=0.02)

        self.assertFalse(assessment.confirmation_passed)
        self.assertEqual(assessment.reason, "imbalance_decay_detected")

    def test_high_conviction_limit_price_tracks_depth_but_respects_slippage_cap(self) -> None:
        config = CTAConfig(
            order_flow_confirmation_ratio=1.5,
            order_flow_high_conviction_ratio=2.0,
            order_flow_limit_buffer_bps=5.0,
            order_flow_max_slippage_bps=20.0,
        )
        order_book = {
            "bids": [[100.0 - index * 0.1, 4.0] for index in range(20)],
            "asks": [
                [100.1, 0.005],
                [100.2, 0.005],
                [100.3, 0.050],
            ]
            + [[100.4 + index * 0.1, 1.0] for index in range(17)],
        }
        sentinel = OrderFlowSentinel(DummyClient(order_book), config)

        assessment = sentinel.assess_entry("BTC/USDT", "buy", amount=0.02)

        self.assertTrue(assessment.use_limit_order)
        assert assessment.depth_boundary_price is not None
        assert assessment.recommended_limit_price is not None
        self.assertGreater(assessment.depth_boundary_price, assessment.best_ask)
        self.assertGreater(assessment.recommended_limit_price, assessment.best_ask)
        self.assertLessEqual(
            assessment.recommended_limit_price,
            round(float(assessment.best_ask) * 1.002, 4),
        )

    def test_low_imbalance_blocks_entry(self) -> None:
        order_book = {
            "bids": [[100.0 - index * 0.1, 1.0] for index in range(20)],
            "asks": [[100.1 + index * 0.1, 1.0] for index in range(20)],
        }
        sentinel = OrderFlowSentinel(DummyClient(order_book), CTAConfig(order_flow_confirmation_ratio=1.5))

        assessment = sentinel.assess_entry("BTC/USDT", "buy", amount=0.02)

        self.assertFalse(assessment.entry_allowed)
        self.assertEqual(assessment.reason, "imbalance_below_1.50")


if __name__ == "__main__":
    unittest.main()
