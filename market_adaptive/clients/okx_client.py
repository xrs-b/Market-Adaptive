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
        self._futures_settings_cache: dict[str, tuple[int, str]] = {}

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

    def fetch_account_risk_snapshot(self, symbols: list[str] | None = None) -> dict[str, float]:
        equity = self.fetch_total_equity()
        balance = self.fetch_balance()
        positions = self.fetch_positions(symbols)

        balance_info = balance.get("info") or {}
        account_data = (balance_info.get("data") or [{}])[0]
        margin_ratio = self._extract_margin_ratio(balance, positions)
        maintenance_margin = self._extract_maintenance_margin(account_data, positions)
        total_notional = 0.0
        for position in positions:
            total_notional += self.position_notional(position.get("symbol") or symbols[0] if symbols else "BTC/USDT", position)
        if margin_ratio <= 0 and equity > 0 and maintenance_margin > 0:
            margin_ratio = maintenance_margin / equity

        return {
            "equity": equity,
            "margin_ratio": max(0.0, margin_ratio),
            "maintenance_margin": max(0.0, maintenance_margin),
            "total_notional": max(0.0, total_notional),
        }

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

    def fetch_order_book(self, symbol: str, limit: int | None = None) -> dict[str, Any]:
        return self.exchange.fetch_order_book(self._normalize_symbol(symbol), limit=limit)

    def fetch_last_price(self, symbol: str) -> float:
        ticker = self.fetch_ticker(symbol)
        last_price = ticker.get("last") or ticker.get("close")
        if last_price is None:
            raise ValueError(f"No last price available for {symbol}")
        return float(last_price)

    def fetch_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        return self.exchange.fetch_open_orders(self._normalize_symbol(symbol))

    def cancel_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        try:
            return self.exchange.cancel_order(order_id, self._normalize_symbol(symbol))
        except (ccxt.OrderNotFound, ccxt.InvalidOrder, ccxt.BadRequest) as exc:
            if self._is_idempotent_cancel_error(exc):
                return {"id": order_id, "status": "already_closed"}
            raise

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

    def fetch_symbol_position_notional(self, symbol: str) -> float:
        total = 0.0
        for position in self.fetch_positions([symbol]):
            total += self.position_notional(symbol, position)
        return total

    def fetch_symbol_open_order_notional(self, symbol: str) -> float:
        total = 0.0
        last_price = None
        for order in self.fetch_open_orders(symbol):
            reduce_only = bool(order.get("reduceOnly") or order.get("info", {}).get("reduceOnly"))
            if reduce_only:
                continue
            amount = order.get("remaining") or order.get("amount") or order.get("info", {}).get("sz") or 0.0
            price = order.get("price")
            if price in (None, ""):
                if last_price is None:
                    last_price = self.fetch_last_price(symbol)
                price = last_price
            total += self.estimate_notional(symbol, float(amount), float(price))
        return total

    def fetch_long_short_account_ratio_history(
        self,
        symbol: str,
        timeframe: str = "5m",
        limit: int = 1,
        since: int | None = None,
        until: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if until is not None:
            params["until"] = until
        return self.exchange.fetch_long_short_ratio_history(
            self._normalize_symbol(symbol),
            timeframe=timeframe,
            since=since,
            limit=limit,
            params=params,
        )

    def fetch_latest_long_short_account_ratio(
        self,
        symbol: str,
        timeframe: str = "5m",
        limit: int = 1,
    ) -> dict[str, Any] | None:
        history = self.fetch_long_short_account_ratio_history(symbol, timeframe=timeframe, limit=max(1, limit))
        if not history:
            return None
        return max(history, key=lambda item: int(item.get("timestamp") or 0))

    def ensure_futures_settings(self, symbol: str, leverage: int, margin_mode: str | None = None) -> None:
        normalized_symbol = self._normalize_symbol(symbol)
        normalized_margin_mode = self._normalize_margin_mode(margin_mode)
        normalized_leverage = self._normalize_leverage(leverage)
        cached = self._futures_settings_cache.get(normalized_symbol)
        if cached == (normalized_leverage, normalized_margin_mode):
            return

        if hasattr(self.exchange, "set_margin_mode"):
            try:
                self.exchange.set_margin_mode(normalized_margin_mode, normalized_symbol)
            except ccxt.ExchangeError as exc:
                if not self._is_idempotent_setting_error(exc):
                    raise

        leverage_params = {"mgnMode": normalized_margin_mode}
        if self._is_hedged_mode():
            for pos_side in ("long", "short"):
                self.exchange.set_leverage(
                    normalized_leverage,
                    normalized_symbol,
                    params={**leverage_params, "posSide": pos_side},
                )
        else:
            self.exchange.set_leverage(normalized_leverage, normalized_symbol, params=leverage_params)

        self._futures_settings_cache[normalized_symbol] = (normalized_leverage, normalized_margin_mode)

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

    def fetch_market(self, symbol: str) -> dict[str, Any]:
        self.exchange.load_markets()
        return self.exchange.market(self._normalize_symbol(symbol))

    def get_contract_value(self, symbol: str) -> float:
        market = self.fetch_market(symbol)
        contract_size = market.get("contractSize")
        if contract_size not in (None, "", 0, 0.0):
            return float(contract_size)
        contract_size = market.get("info", {}).get("ctVal")
        if contract_size not in (None, "", 0, 0.0):
            return float(contract_size)
        return 1.0

    def get_min_order_amount(self, symbol: str) -> float:
        market = self.fetch_market(symbol)
        amount_min = market.get("limits", {}).get("amount", {}).get("min")
        if amount_min not in (None, ""):
            return float(amount_min)
        amount_min = market.get("info", {}).get("minSz") or market.get("info", {}).get("lotSz")
        if amount_min not in (None, ""):
            return float(amount_min)
        return 0.0

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        try:
            return float(self.exchange.amount_to_precision(self._normalize_symbol(symbol), amount))
        except Exception:
            return float(amount)

    def price_to_precision(self, symbol: str, price: float) -> float:
        try:
            return float(self.exchange.price_to_precision(self._normalize_symbol(symbol), price))
        except Exception:
            return float(price)

    def estimate_notional(self, symbol: str, amount: float, price: float) -> float:
        contract_value = self.get_contract_value(symbol)
        return abs(float(amount)) * abs(float(price)) * contract_value

    def position_notional(self, symbol: str, position: dict[str, Any]) -> float:
        notional = position.get("notional") or position.get("info", {}).get("notionalUsd")
        if notional not in (None, ""):
            return abs(float(notional))
        contracts = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos") or 0.0
        price = (
            position.get("markPrice")
            or position.get("entryPrice")
            or position.get("info", {}).get("markPx")
            or position.get("info", {}).get("avgPx")
            or 0.0
        )
        return self.estimate_notional(symbol, float(contracts), float(price or 0.0))

    def get_position_liquidation_price(self, position: dict[str, Any]) -> float | None:
        liquidation_price = position.get("liquidationPrice")
        if liquidation_price not in (None, "", 0, "0"):
            return abs(float(liquidation_price))

        info = position.get("info", {})
        for key in ("liqPx", "liquidationPrice"):
            if info.get(key) not in (None, "", 0, "0"):
                return abs(float(info.get(key)))
        return None

    def _merge_order_params(
        self,
        side: str,
        params: dict[str, Any] | None,
        *,
        reduce_only: bool,
    ) -> dict[str, Any]:
        payload = {
            "tdMode": self._normalize_margin_mode(self.execution_config.td_mode),
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

    @staticmethod
    def _normalize_margin_mode(margin_mode: str | None) -> str:
        normalized = str(margin_mode or "isolated").strip().lower()
        return "cross" if normalized == "cross" else "isolated"

    @staticmethod
    def _normalize_leverage(leverage: int | float) -> int:
        return 5 if int(leverage) == 5 else 3

    @staticmethod
    def _is_idempotent_setting_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "no need" in message
            or "already" in message
            or "same" in message
            or 'setmarginmode() params["lever"] should be between 1 and 125' in message
        )

    @staticmethod
    def _is_idempotent_cancel_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "ordernotfound" in message
            or "does not exist" in message
            or "has been filled" in message
            or "already canceled" in message
            or "already cancelled" in message
            or "order cancellation failed as the order has been filled" in message
        )

    def _is_hedged_mode(self) -> bool:
        if self._hedged_mode is not None:
            return self._hedged_mode
        try:
            info = self.exchange.fetch_position_mode(self._normalize_symbol("BTC/USDT"))
            self._hedged_mode = bool(info.get("hedged"))
        except Exception:
            self._hedged_mode = False
        return self._hedged_mode

    @staticmethod
    def _safe_float(value: Any) -> float:
        if value in (None, ""):
            return 0.0
        text = str(value).strip()
        if text.endswith("%"):
            return float(text[:-1]) / 100.0
        return float(text)

    def _extract_margin_ratio(self, balance: dict[str, Any], positions: list[dict[str, Any]]) -> float:
        info = balance.get("info") or {}
        data = info.get("data") or []
        candidates: list[float] = []
        for item in data:
            for key in ("mgnRatio", "marginRatio", "riskRate"):
                if item.get(key) not in (None, ""):
                    candidates.append(self._safe_float(item.get(key)))
            for detail in item.get("details") or []:
                for key in ("mgnRatio", "marginRatio", "riskRate"):
                    if detail.get(key) not in (None, ""):
                        candidates.append(self._safe_float(detail.get(key)))
        for position in positions:
            info = position.get("info", {})
            for key in ("mgnRatio", "marginRatio", "riskRate"):
                if info.get(key) not in (None, ""):
                    candidates.append(self._safe_float(info.get(key)))
        return max(candidates) if candidates else 0.0

    def _extract_maintenance_margin(self, account_data: dict[str, Any], positions: list[dict[str, Any]]) -> float:
        for key in ("mmr", "maintMargin", "maintenanceMargin"):
            if account_data.get(key) not in (None, ""):
                return self._safe_float(account_data.get(key))

        total = 0.0
        for position in positions:
            info = position.get("info", {})
            for key in ("mmr", "maintMargin", "maintenanceMargin"):
                if info.get(key) not in (None, ""):
                    total += self._safe_float(info.get(key))
                    break
        return total
