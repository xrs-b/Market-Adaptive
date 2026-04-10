from __future__ import annotations

import unittest

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import ExecutionConfig, OKXConfig


class OKXClientSymbolTests(unittest.TestCase):
    def test_normalize_symbol_maps_plain_pair_to_okx_swap_symbol(self) -> None:
        client = OKXClient(
            OKXConfig(api_key="", api_secret="", passphrase="", default_type="swap"),
            ExecutionConfig(),
        )
        self.assertEqual(client._normalize_symbol("BTC/USDT"), "BTC/USDT:USDT")
        self.assertEqual(client._normalize_symbol("BTC/USDT:USDT"), "BTC/USDT:USDT")


if __name__ == "__main__":
    unittest.main()
