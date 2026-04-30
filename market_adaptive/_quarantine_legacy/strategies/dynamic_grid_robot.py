from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from market_adaptive.clients.okx_ws_client import build_okx_websocket_client
from market_adaptive.indicators import compute_atr, ohlcv_to_dataframe

BLUE = "\033[94m"
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


@dataclass
class DynamicGridConfig:
    symbol: str = "BTC/USDT"
    leverage: int = 3
    levels: int = 10
    atr_timeframe: str = "1h"
    atr_period: int = 14
    atr_multiplier: float = 2.5
    base_order_amount: float = 0.01
    flash_crash_atr_multiplier: float = 1.5
    flash_crash_lookback_seconds: int = 60
    flash_crash_cooldown_seconds: int = 300
    stop_buffer_ratio: float = 0.01
    max_directional_exposure_ratio: float = 0.50
    td_mode: str = "isolated"


@dataclass
class DynamicGridLevel:
    index: int
    side: str
    price: float
    amount: float


@dataclass
class DynamicGridState:
    mark_price: float = 0.0
    atr_value: float = 0.0
    lower_bound: float = 0.0
    upper_bound: float = 0.0
    step_size: float = 0.0
    paused_until: datetime | None = None
    running: bool = False
    stop_reason: str | None = None
    open_orders: dict[str, dict[str, Any]] = field(default_factory=dict)


class DynamicGridRobot:
    """Async ATR-driven neutral grid robot with websocket-driven order handling.

    Designed for integration into the main controller without replacing the legacy GridRobot.
    """

    def __init__(
        self,
        *,
        client: Any,
        okx_config: Any,
        config: DynamicGridConfig,
        market_oracle: Any | None = None,
        atr_provider: Callable[[], float | Awaitable[float]] | None = None,
        equity_provider: Callable[[], float | Awaitable[float]] | None = None,
        ws_client_factory: Callable[[Any], Any] | None = None,
        logger: logging.Logger | logging.LoggerAdapter | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self.client = client
        self.okx_config = okx_config
        self.config = config
        self.market_oracle = market_oracle
        self.atr_provider = atr_provider
        self.equity_provider = equity_provider
        self.ws_client_factory = ws_client_factory or build_okx_websocket_client
        self.logger = logger or logging.LoggerAdapter(logging.getLogger(__name__), {"robot": "grid"})
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))

        self.state = DynamicGridState()
        self._price_window: deque[tuple[datetime, float]] = deque()
        self._tasks: list[asyncio.Task[Any]] = []
        self._ticker_ws: Any | None = None
        self._orders_ws: Any | None = None
        self._lock = asyncio.Lock()

    async def run_forever(self) -> None:
        self.state.running = True
        await self.initialize_grid()
        self._ticker_ws = self.ws_client_factory(self.okx_config)
        self._orders_ws = self.ws_client_factory(self.okx_config)
        self._tasks = [
            asyncio.create_task(self._ticker_loop(), name="dynamic-grid:ticker"),
            asyncio.create_task(self._orders_loop(), name="dynamic-grid:orders"),
        ]
        try:
            await asyncio.gather(*self._tasks)
        finally:
            await self.stop()

    async def stop(self) -> None:
        self.state.running = False
        tasks = list(self._tasks)
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for ws_client in (self._ticker_ws, self._orders_ws):
            if ws_client is not None:
                try:
                    await ws_client.close()
                except Exception:
                    pass
        self._ticker_ws = None
        self._orders_ws = None

    async def initialize_grid(self) -> None:
        async with self._lock:
            self.client.ensure_futures_settings(
                self.config.symbol,
                leverage=self.config.leverage,
                margin_mode=self.config.td_mode,
            )
            mark_price = await self._fetch_mark_price()
            atr_value = await self._resolve_hourly_atr()
            self._set_grid_geometry(mark_price, atr_value)
            await self._cancel_all_orders()
            await self._place_symmetric_grid()
            self._log_atr(
                f"Dynamic grid initialized | mark={mark_price:.2f} atr_1h={atr_value:.2f} "
                f"bounds={self.state.lower_bound:.2f}-{self.state.upper_bound:.2f} step={self.state.step_size:.2f}"
            )

    async def handle_ticker(self, payload: dict[str, Any]) -> None:
        mark_price = self._extract_mark_price(payload)
        if mark_price <= 0:
            return
        async with self._lock:
            self.state.mark_price = mark_price
            now = self.now_provider().astimezone(timezone.utc)
            self._push_price_point(now, mark_price)

            if self._hard_stop_triggered(mark_price):
                reason = (
                    f"dynamic_grid_hard_stop|price={mark_price:.2f}|"
                    f"bounds={self.state.lower_bound:.2f}-{self.state.upper_bound:.2f}"
                )
                await self._panic_stop(reason)
                return

            if self._cooldown_active(now):
                return

            if await self._flash_crash_triggered(now):
                self.state.paused_until = now + timedelta(seconds=self.config.flash_crash_cooldown_seconds)
                await self._cancel_all_orders()
                self._log_risk(
                    "Flash crash protection triggered | "
                    f"range_1m={self._window_range():.2f} atr={self.state.atr_value:.2f} "
                    f"threshold={self.state.atr_value * self.config.flash_crash_atr_multiplier:.2f} "
                    f"cooldown={self.config.flash_crash_cooldown_seconds}s"
                )
                return

    async def handle_order_update(self, payload: list[dict[str, Any]] | dict[str, Any]) -> None:
        events = payload if isinstance(payload, list) else [payload]
        for event in events:
            if not self._is_filled_event(event):
                continue
            async with self._lock:
                await self._handle_filled_order(event)

    async def _ticker_loop(self) -> None:
        assert self._ticker_ws is not None
        while self.state.running:
            payload = await self._ticker_ws.watch_ticker(self.config.symbol)
            await self.handle_ticker(dict(payload))

    async def _orders_loop(self) -> None:
        assert self._orders_ws is not None
        while self.state.running:
            payload = await self._orders_ws.watch_orders(self.config.symbol)
            await self.handle_order_update(payload)

    async def _handle_filled_order(self, order: dict[str, Any]) -> None:
        side = str(order.get("side") or "").lower()
        reduce_only = bool(order.get("reduceOnly") or order.get("info", {}).get("reduceOnly"))
        filled_price = float(order.get("average") or order.get("price") or order.get("info", {}).get("fillPx") or 0.0)
        filled_amount = float(order.get("filled") or order.get("amount") or order.get("info", {}).get("fillSz") or 0.0)
        if filled_price <= 0 or filled_amount <= 0 or side not in {"buy", "sell"}:
            return

        self._log_fill(
            f"Order filled | side={side} price={filled_price:.2f} amount={filled_amount:.8f} reduce_only={reduce_only}"
        )

        if reduce_only:
            return

        close_side = "sell" if side == "buy" else "buy"
        close_price = filled_price + self.state.step_size if side == "buy" else filled_price - self.state.step_size
        close_price = self.client.price_to_precision(self.config.symbol, close_price)
        close_amount = self._normalize_amount(filled_amount)
        if close_amount <= 0 or close_price <= 0:
            return

        await asyncio.to_thread(
            self.client.place_limit_order,
            self.config.symbol,
            close_side,
            close_amount,
            close_price,
            reduce_only=True,
        )
        self._log_fill(
            f"Counter order placed | side={close_side} price={close_price:.2f} amount={close_amount:.8f}"
        )

    async def _place_symmetric_grid(self) -> None:
        levels = self._build_grid_levels()
        for level in levels:
            if not await self._can_open_direction(level.side, level.price, level.amount):
                continue
            await asyncio.to_thread(
                self.client.place_limit_order,
                self.config.symbol,
                level.side,
                level.amount,
                level.price,
            )

    def _build_grid_levels(self) -> list[DynamicGridLevel]:
        if self.state.step_size <= 0:
            return []
        half_levels = max(1, self.config.levels // 2)
        levels: list[DynamicGridLevel] = []
        for index in range(1, half_levels + 1):
            buy_price = self.client.price_to_precision(
                self.config.symbol,
                self.state.mark_price - self.state.step_size * index,
            )
            sell_price = self.client.price_to_precision(
                self.config.symbol,
                self.state.mark_price + self.state.step_size * index,
            )
            amount = self._normalize_amount(self.config.base_order_amount)
            levels.append(DynamicGridLevel(index=index, side="buy", price=buy_price, amount=amount))
            levels.append(DynamicGridLevel(index=index, side="sell", price=sell_price, amount=amount))
        levels.sort(key=lambda item: (item.side, item.price))
        return levels

    def _set_grid_geometry(self, mark_price: float, atr_value: float) -> None:
        distance = float(self.config.atr_multiplier) * float(atr_value)
        lower_bound = mark_price - distance
        upper_bound = mark_price + distance
        total_levels = max(2, int(self.config.levels))
        step_size = (upper_bound - lower_bound) / total_levels
        self.state.mark_price = mark_price
        self.state.atr_value = atr_value
        self.state.lower_bound = lower_bound
        self.state.upper_bound = upper_bound
        self.state.step_size = step_size

    async def _resolve_hourly_atr(self) -> float:
        if self.atr_provider is not None:
            value = self.atr_provider()
            if asyncio.iscoroutine(value):
                value = await value
            return float(value)
        if self.market_oracle is not None and hasattr(self.market_oracle, "get_hourly_atr"):
            value = self.market_oracle.get_hourly_atr(self.config.symbol)
            if asyncio.iscoroutine(value):
                value = await value
            return float(value)
        ohlcv = await asyncio.to_thread(
            self.client.fetch_ohlcv,
            self.config.symbol,
            self.config.atr_timeframe,
            max(self.config.atr_period * 4, 80),
        )
        frame = ohlcv_to_dataframe(ohlcv)
        atr_series = compute_atr(frame, length=self.config.atr_period)
        return float(atr_series.iloc[-1])

    async def _fetch_mark_price(self) -> float:
        ticker = await asyncio.to_thread(self.client.fetch_ticker, self.config.symbol)
        mark_price = self._extract_mark_price(ticker)
        if mark_price <= 0:
            raise ValueError(f"Unable to determine mark price for {self.config.symbol}")
        return mark_price

    def _extract_mark_price(self, payload: dict[str, Any]) -> float:
        info = payload.get("info", {}) if isinstance(payload, dict) else {}
        for candidate in (
            payload.get("mark"),
            payload.get("last"),
            payload.get("close"),
            info.get("markPx"),
            info.get("markPrice"),
            info.get("last"),
        ):
            if candidate not in (None, ""):
                return float(candidate)
        return 0.0

    async def _can_open_direction(self, side: str, price: float, amount: float) -> bool:
        equity = await self._fetch_equity()
        if equity <= 0:
            return True
        long_notional, short_notional = await self._fetch_directional_exposure()
        order_notional = abs(float(price) * float(amount) * float(self.client.get_contract_value(self.config.symbol)))
        max_notional = equity * float(self.config.max_directional_exposure_ratio)
        if side == "buy":
            allowed = long_notional + order_notional <= max_notional + 1e-12
        else:
            allowed = short_notional + order_notional <= max_notional + 1e-12
        if not allowed:
            self._log_risk(
                f"Directional exposure blocked | side={side} current={long_notional if side == 'buy' else short_notional:.2f} "
                f"requested={order_notional:.2f} limit={max_notional:.2f}"
            )
        return allowed

    async def _fetch_equity(self) -> float:
        if self.equity_provider is not None:
            value = self.equity_provider()
            if asyncio.iscoroutine(value):
                value = await value
            return float(value)
        return float(await asyncio.to_thread(self.client.fetch_total_equity, "USDT"))

    async def _fetch_directional_exposure(self) -> tuple[float, float]:
        positions = await asyncio.to_thread(self.client.fetch_positions, [self.config.symbol])
        long_notional = 0.0
        short_notional = 0.0
        for position in positions or []:
            notional = abs(float(self.client.position_notional(self.config.symbol, position)))
            side = str(position.get("side") or position.get("info", {}).get("posSide") or "").lower()
            if side == "short":
                short_notional += notional
            else:
                long_notional += notional
        return long_notional, short_notional

    async def _cancel_all_orders(self) -> None:
        await asyncio.to_thread(self.client.cancel_all_orders, self.config.symbol)

    async def _panic_stop(self, reason: str) -> None:
        await self._cancel_all_orders()
        await asyncio.to_thread(self.client.close_all_positions, self.config.symbol)
        self.state.stop_reason = reason
        self.state.running = False
        self._log_risk(f"Hard stop triggered | {reason}")

    async def _flash_crash_triggered(self, now: datetime) -> bool:
        if self.state.atr_value <= 0:
            return False
        self._prune_price_window(now)
        return self._window_range() >= self.state.atr_value * float(self.config.flash_crash_atr_multiplier)

    def _hard_stop_triggered(self, price: float) -> bool:
        if self.state.lower_bound <= 0 or self.state.upper_bound <= 0:
            return False
        lower_stop = self.state.lower_bound * (1.0 - float(self.config.stop_buffer_ratio))
        upper_stop = self.state.upper_bound * (1.0 + float(self.config.stop_buffer_ratio))
        return price <= lower_stop or price >= upper_stop

    def _push_price_point(self, now: datetime, price: float) -> None:
        self._price_window.append((now, price))
        self._prune_price_window(now)

    def _prune_price_window(self, now: datetime) -> None:
        window = timedelta(seconds=max(1, int(self.config.flash_crash_lookback_seconds)))
        while self._price_window and now - self._price_window[0][0] > window:
            self._price_window.popleft()

    def _window_range(self) -> float:
        if not self._price_window:
            return 0.0
        prices = [price for _, price in self._price_window]
        return max(prices) - min(prices)

    def _cooldown_active(self, now: datetime) -> bool:
        return self.state.paused_until is not None and now < self.state.paused_until

    def _normalize_amount(self, amount: float) -> float:
        normalized = float(self.client.amount_to_precision(self.config.symbol, amount))
        minimum = float(self.client.get_min_order_amount(self.config.symbol))
        if normalized <= 0:
            return 0.0
        if minimum > 0 and normalized < minimum:
            normalized = minimum
        return normalized

    def _is_filled_event(self, order: dict[str, Any]) -> bool:
        status = str(order.get("status") or order.get("info", {}).get("state") or "").lower()
        filled = float(order.get("filled") or order.get("info", {}).get("fillSz") or 0.0)
        return status in {"closed", "filled"} and filled > 0

    def _log_atr(self, message: str) -> None:
        self.logger.info("%s%s%s", BLUE, message, RESET)

    def _log_fill(self, message: str) -> None:
        self.logger.info("%s%s%s", GREEN, message, RESET)

    def _log_risk(self, message: str) -> None:
        self.logger.warning("%s%s%s", RED, message, RESET)
