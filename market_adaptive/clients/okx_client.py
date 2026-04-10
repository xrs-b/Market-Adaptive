from __future__ import annotations

from typing import Any

import ccxt

from market_adaptive.config import ExecutionConfig, OKXConfig


class OKXClient:
    """Thin OKX wrapper for shared account, market-data and order execution calls."""

    def __init__(self, config: OKXConfig, execution_config: ExecutionConfig | None = None) -> None:
        self.config = config
        self.execution_config = execution_config or ExecutionConfig()
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

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        return self.exchange.fetch_ticker(symbol)

    def fetch_last_price(self, symbol: str) -> float:
        ticker = self.fetch_ticker(symbol)
        last_price = ticker.get("last") or ticker.get("close")
        if last_price is None:
            raise ValueError(f"No last price available for {symbol}")
        return float(last_price)

    def fetch_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        return self.exchange.fetch_open_orders(symbol)

    def cancel_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        return self.exchange.cancel_order(order_id, symbol)

    def cancel_all_orders(self, symbol: str) -> list[dict[str, Any]]:
        responses: list[dict[str, Any]] = []
        for order in self.fetch_open_orders(symbol):
            order_id = order.get("id")
            if order_id is None:
                continue
            responses.append(self.cancel_order(str(order_id), symbol))
        return responses

    def fetch_positions(self, symbol: str) -> list[dict[str, Any]]:
        try:
            return self.exchange.fetch_positions([symbol])
        except TypeError:
            return self.exchange.fetch_positions([symbol], params={"type": self.config.default_type})

    def place_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        *,
        reduce_only: bool = False,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self._merge_order_params(params, reduce_only=reduce_only)
        return self.exchange.create_order(symbol, "market", side, amount, None, payload)

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        *,
        reduce_only: bool = False,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self._merge_order_params(params, reduce_only=reduce_only)
        return self.exchange.create_order(symbol, "limit", side, amount, price, payload)

    def close_all_positions(self, symbol: str) -> list[dict[str, Any]]:
        responses: list[dict[str, Any]] = []
        for position in self.fetch_positions(symbol):
            contracts = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos")
            if contracts in (None, "", 0, "0"):
                continue
            size = abs(float(contracts))
            if size <= 0:
                continue
            side = str(position.get("side") or "").lower()
            if side == "long" or float(contracts) > 0:
                close_side = "sell"
            else:
                close_side = "buy"
            responses.append(
                self.place_market_order(
                    symbol,
                    close_side,
                    size,
                    reduce_only=True,
                )
            )
        return responses

    def _merge_order_params(self, params: dict[str, Any] | None, *, reduce_only: bool) -> dict[str, Any]:
        payload = {
            "tdMode": self.execution_config.td_mode,
        }
        if reduce_only:
            payload["reduceOnly"] = True
        if params:
            payload.update(params)
        return payload
