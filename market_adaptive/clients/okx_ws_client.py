from __future__ import annotations

import importlib
import inspect
from typing import Any, Iterable, Mapping

from market_adaptive.config import OKXConfig


class CCXTProUnavailableError(RuntimeError):
    """Raised when ccxt.pro is requested but not installed."""


class OKXCCXTProWebsocketClient:
    """Thin ccxt.pro-backed OKX websocket client.

    A fresh instance should be created per long-lived stream so each worker can reconnect
    independently without taking the rest of the runtime down.
    """

    def __init__(self, config: OKXConfig) -> None:
        self.config = config
        self.exchange = self._build_exchange()
        self._markets_loaded = False

    def _build_exchange(self) -> Any:
        try:
            ccxt_pro = importlib.import_module("ccxt.pro")
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on optional package
            raise CCXTProUnavailableError(
                "ccxt.pro is required for OKX websocket streams. "
                "Install ccxt.pro or disable websocket runtime / order-flow monitoring."
            ) from exc

        exchange = ccxt_pro.okx(
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

    async def watch_ticker(self, symbol: str) -> Mapping[str, Any]:
        await self._ensure_markets_loaded()
        return await self.exchange.watch_ticker(self._normalize_symbol(symbol))

    async def watch_order_book(self, symbol: str, limit: int | None = None) -> Mapping[str, Any]:
        await self._ensure_markets_loaded()
        normalized_symbol = self._normalize_symbol(symbol)
        if limit is None:
            return await self.exchange.watch_order_book(normalized_symbol)
        return await self.exchange.watch_order_book(normalized_symbol, limit)

    async def watch_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        await self._ensure_markets_loaded()
        normalized_symbol = None if symbol is None else self._normalize_symbol(symbol)
        return await self.exchange.watch_orders(normalized_symbol, since=since, limit=limit)

    async def watch_positions(
        self,
        symbols: Iterable[str] | None = None,
        since: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        await self._ensure_markets_loaded()
        normalized_symbols = None
        if symbols is not None:
            normalized_symbols = [self._normalize_symbol(symbol) for symbol in symbols]
        return await self.exchange.watch_positions(normalized_symbols, since=since, limit=limit)

    async def watch_balance(self) -> dict[str, Any]:
        await self._ensure_markets_loaded()
        return await self.exchange.watch_balance()

    async def close(self) -> None:
        result = self.exchange.close()
        if inspect.isawaitable(result):
            await result

    async def _ensure_markets_loaded(self) -> None:
        if self._markets_loaded:
            return
        result = self.exchange.load_markets()
        if inspect.isawaitable(result):
            await result
        self._markets_loaded = True

    def _normalize_symbol(self, symbol: str) -> str:
        if self.config.default_type != "swap":
            return symbol
        if ":" in symbol or "/" not in symbol:
            return symbol
        base, quote = symbol.split("/", 1)
        return f"{base}/{quote}:{quote}"


def build_okx_websocket_client(config: OKXConfig) -> OKXCCXTProWebsocketClient:
    return OKXCCXTProWebsocketClient(config)
