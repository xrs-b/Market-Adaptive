from __future__ import annotations

from typing import Any

import ccxt

from market_adaptive.config import OKXConfig


class OKXClient:
    """Thin OKX wrapper for shared account and market-data calls."""

    def __init__(self, config: OKXConfig) -> None:
        self.config = config
        self.exchange = self._build_exchange()

    def _build_exchange(self) -> ccxt.okx:
        exchange = ccxt.okx(
            {
                "apiKey": self.config.api_key,
                "secret": self.config.api_secret,
                "password": self.config.passphrase,
                "timeout": self.config.timeout_ms,
                "enableRateLimit": True,
                "headers": self.config.headers,
                "options": {
                    "defaultType": self.config.default_type,
                },
            }
        )
        if self.config.sandbox:
            exchange.set_sandbox_mode(True)
        return exchange

    def fetch_balance(self) -> dict[str, Any]:
        return self.exchange.fetch_balance()

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 200,
        since: int | None = None,
    ) -> list[list[Any]]:
        return self.exchange.fetch_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            since=since,
            limit=limit,
        )
