from __future__ import annotations

from typing import Any, Iterable

import ccxt

from market_adaptive.config import ExecutionConfig, OKXConfig


class OKXClient:
    """Thin OKX wrapper for shared account, market-data and order execution calls."""

    def __init__(self, config: OKXConfig, execution_config: ExecutionConfig | None = None) -> None:
        self.config = config
        self.execution_config = execution_config or ExecutionConfig()
        self.exchange = self._build_exchange()
        self._hedged_mode: bool | None = None

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

    def fetch_total_equity(self, quote_currency: str = "USDT") -> float:
        balance = self.fetch_balance()
        total = balance.get("total", {})
        if isinstance(total, dict) and quote_currency in total and total[quote_currency] is not None:
            return float(total[quote_currency])

        info = balance.get("info") or {}
        details = info.get("data") or []
        for item in details:
            details_list = item.get("details") or []
            for detail in details_list:
                ccy = detail.get("ccy")
                eq = detail.get("eq") or detail.get("cashBal")
                if ccy == quote_currency and eq not in (None, ""):
                    return float(eq)
        raise ValueError(f"Unable to determine total equity for {quote_currency}")

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 200,
        since: int | None = None,
    ) -> list[list[Any]]:
        return self.exchange.fetch_ohlcv(
            symbol=self._normalize_symbol(symbol),
            timeframe=timeframe,
            since=since,
            limit=limit,
        )

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        return self.exchange.fetch_ticker(self._normalize_symbol(symbol))

    def fetch_last_price(self, symbol: str) -> float:
        ticker = self.fetch_ticker(symbol)
        last_price = ticker.get("last") or ticker.get("close")
        if last_price is None:
            raise ValueError(f"No last price available for {symbol}")
        return float(last_price)

    def fetch_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        return self.exchange.fetch_open_orders(self._normalize_symbol(symbol))

    def cancel_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        return self.exchange.cancel_order(order_id, self._normalize_symbol(symbol))

    def cancel_all_orders(self, symbol: str) -> list[dict[str, Any]]:
        responses: list[dict[str, Any]] = []
        for order in self.fetch_open_orders(symbol):
            order_id = order.get("id")
            if order_id is None:
                continue
            responses.append(self.cancel_order(str(order_id), symbol))
        return responses

    def cancel_all_orders_for_symbols(self, symbols: Iterable[str]) -> list[dict[str, Any]]:
        responses: list[dict[str, Any]] = []
        for symbol in sorted(set(symbols)):
            responses.extend(self.cancel_all_orders(symbol))
        return responses

    def fetch_positions(self, symbols: list[str] | None = None) -> list[dict[str, Any]]:
        normalized = None if symbols is None else [self._normalize_symbol(symbol) for symbol in symbols]
        try:
            return self.exchange.fetch_positions(normalized)
        except TypeError:
            return self.exchange.fetch_positions(normalized, params={"type": self.config.default_type})

    def fetch_total_unrealized_pnl(self, symbols: list[str] | None = None) -> float:
        total = 0.0
        for position in self.fetch_positions(symbols):
            unrealized = (
                position.get("unrealizedPnl")
                or position.get("info", {}).get("upl")
                or position.get("info", {}).get("uplLastPx")
                or 0.0
            )
            total += float(unrealized)
        return total

    def place_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        *,
        reduce_only: bool = False,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self._merge_order_params(side=side, params=params, reduce_only=reduce_only)
        return self.exchange.create_order(self._normalize_symbol(symbol), "market", side, amount, None, payload)

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
        payload = self._merge_order_params(side=side, params=params, reduce_only=reduce_only)
        return self.exchange.create_order(self._normalize_symbol(symbol), "limit", side, amount, price, payload)

    def close_all_positions(self, symbol: str) -> list[dict[str, Any]]:
        responses: list[dict[str, Any]] = []
        for position in self.fetch_positions([symbol]):
            contracts = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos")
            if contracts in (None, "", 0, "0"):
                continue
            size = abs(float(contracts))
            if size <= 0:
                continue
            side = str(position.get("side") or "").lower()
            pos_side = str(position.get("info", {}).get("posSide") or side)
            if side == "short" or pos_side == "short":
                close_side = "buy"
            elif side == "long" or pos_side == "long":
                close_side = "sell"
            elif float(contracts) > 0:
                close_side = "sell"
            else:
                close_side = "buy"
            responses.append(
                self.place_market_order(
                    symbol,
                    close_side,
                    size,
                    reduce_only=True,
                    params={"posSide": pos_side},
                )
            )
        return responses

    def close_all_positions_for_symbols(self, symbols: Iterable[str]) -> list[dict[str, Any]]:
        responses: list[dict[str, Any]] = []
        for symbol in sorted(set(symbols)):
            responses.extend(self.close_all_positions(symbol))
        return responses

    def _merge_order_params(
        self,
        side: str,
        params: dict[str, Any] | None,
        *,
        reduce_only: bool,
    ) -> dict[str, Any]:
        payload = {
            "tdMode": self.execution_config.td_mode,
        }
        if self._is_hedged_mode():
            payload["posSide"] = "long" if side == "buy" else "short"
        if reduce_only:
            payload["reduceOnly"] = True
        if params:
            payload.update(params)
        return payload

    def _normalize_symbol(self, symbol: str) -> str:
        if self.config.default_type != "swap":
            return symbol
        if ":" in symbol or "/" not in symbol:
            return symbol
        base, quote = symbol.split("/", 1)
        return f"{base}/{quote}:{quote}"

    def _is_hedged_mode(self) -> bool:
        if self._hedged_mode is not None:
            return self._hedged_mode
        try:
            info = self.exchange.fetch_position_mode(self._normalize_symbol("BTC/USDT"))
            self._hedged_mode = bool(info.get("hedged"))
        except Exception:
            self._hedged_mode = False
        return self._hedged_mode
