from __future__ import annotations

import unittest

from market_adaptive.config import SentimentConfig
from market_adaptive.sentiment import SentimentAnalyst


class DummySentimentClient:
    def __init__(self, payload=None) -> None:
        self.payload = payload
        self.calls = []

    def fetch_latest_long_short_account_ratio(self, symbol: str, timeframe: str = "5m", limit: int = 1):
        self.calls.append((symbol, timeframe, limit))
        return self.payload


class SentimentAnalystTests(unittest.TestCase):
    def test_default_policy_blocks_cta_buy_when_ratio_is_extreme(self) -> None:
        client = DummySentimentClient({"timestamp": 1712722800000, "longShortRatio": 3.0})
        analyst = SentimentAnalyst(client, SentimentConfig(enabled=True, extreme_bullish_ratio=2.5))

        decision = analyst.evaluate_cta_buy("BTC/USDT")

        self.assertTrue(decision.blocked)
        self.assertEqual(decision.size_multiplier, 0.0)
        self.assertIsNotNone(decision.snapshot)
        self.assertEqual(client.calls[0], ("BTC/USDT", "5m", 1))

    def test_halve_policy_reduces_buy_size_instead_of_blocking(self) -> None:
        client = DummySentimentClient({"timestamp": 1712722800000, "longShortRatio": 3.0})
        analyst = SentimentAnalyst(
            client,
            SentimentConfig(enabled=True, symbol="ETH/USDT", cta_buy_action="halve", extreme_bullish_ratio=2.5),
        )

        decision = analyst.evaluate_cta_buy("BTC/USDT")

        self.assertFalse(decision.blocked)
        self.assertEqual(decision.size_multiplier, 0.5)
        self.assertIsNotNone(decision.snapshot)
        self.assertEqual(decision.snapshot.symbol, "ETH/USDT")
        self.assertEqual(client.calls[0], ("ETH/USDT", "5m", 1))


if __name__ == "__main__":
    unittest.main()
