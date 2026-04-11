from __future__ import annotations

import asyncio
import logging
import threading
from collections import defaultdict, deque
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil, inf
from typing import Callable

from market_adaptive.clients.okx_ws_client import CCXTProUnavailableError, build_okx_websocket_client
from market_adaptive.config import ExecutionConfig, GridConfig
from market_adaptive.indicators import compute_atr, compute_bollinger_bands, ohlcv_to_dataframe
from market_adaptive.risk import GridRiskProfile
from market_adaptive.strategies.base import BaseStrategyRobot, StrategyRunResult

logger = logging.getLogger(__name__)


@dataclass
class GridContext:
    anchor_timestamp_ms: int
    lower_bound: float
    middle_band: float
    upper_bound: float
    buy_prices: list[float]
    sell_prices: list[float]
    center_price: float = 0.0
    atr_value: float = 0.0


@dataclass
class GridOrderPlan:
    layer_key: str
    index: int
    side: str
    price: float
    amount: float
    reduce_only: bool = False


@dataclass
class GridPositionCandidate:
    side: str
    pos_side: str
    size: float
    notional: float
    entry_price: float
    current_price: float
    price_distance_ratio: float
    liquidation_price: float | None
    liquidation_distance_ratio: float | None
    profitable: bool

    @property
    def close_side(self) -> str:
        return "sell" if self.side == "long" else "buy"


class GridRobot(BaseStrategyRobot):
    strategy_name = "grid"
    activation_status = "sideways"

    def __init__(
        self,
        client,
        database,
        config: GridConfig,
        execution_config: ExecutionConfig,
        notifier=None,
        risk_manager=None,
        now_provider: Callable[[], datetime] | None = None,
        market_oracle=None,
        use_dynamic_range: bool | None = None,
        atr_multiplier: float | None = None,
    ) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config
        self.risk_manager = risk_manager
        self.market_oracle = market_oracle
        self.use_dynamic_range = bool(config.use_dynamic_range if use_dynamic_range is None else use_dynamic_range)
        self.atr_multiplier = float(config.atr_multiplier if atr_multiplier is None else atr_multiplier)
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._cached_context: GridContext | None = None
        self._layer_triggers: dict[str, deque[datetime]] = defaultdict(deque)
        self._layer_cooldowns: dict[str, datetime] = {}
        self._layer_reference_prices: dict[str, float] = {}
        self._flash_crash_until: datetime | None = None
        self._halted = False
        self._price_window: deque[tuple[datetime, float]] = deque()
        self._ws_thread: threading.Thread | None = None
        self._ws_loop: asyncio.AbstractEventLoop | None = None
        self._ws_stop_event: asyncio.Event | None = None

    def should_notify_action(self, action: str) -> bool:
        if action in {
            "grid:risk_blocked",
            "grid:insufficient_data",
            "grid:no_orders",
            "grid:flash_crash_cooldown",
            "grid:hold_existing_grid",
            "grid:adx_trend_not_ready",
            "grid:halted",
        }:
            return False
        return super().should_notify_action(action)

    def start_background_websocket(self) -> None:
        if not bool(getattr(self.config, "websocket_order_sync_enabled", True)):
            return
        if self._ws_thread is not None and self._ws_thread.is_alive():
            return

        def runner() -> None:
            loop = asyncio.new_event_loop()
            self._ws_loop = loop
            self._ws_stop_event = asyncio.Event()
            asyncio.set_event_loop(loop)
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(self._ws_main())
            loop.close()
            self._ws_loop = None
            self._ws_stop_event = None

        self._ws_thread = threading.Thread(target=runner, daemon=True, name="grid-ws")
        self._ws_thread.start()

    def stop_background_websocket(self, timeout: float = 5.0) -> None:
        if self._ws_loop is not None and self._ws_stop_event is not None:
            self._ws_loop.call_soon_threadsafe(self._ws_stop_event.set)
        if self._ws_thread is not None:
            self._ws_thread.join(timeout=timeout)
            if not self._ws_thread.is_alive():
                self._ws_thread = None

    async def _ws_main(self) -> None:
        try:
            ticker_client = build_okx_websocket_client(self.client.config if hasattr(self.client, 'config') else self.market_oracle.client.config)
            orders_client = build_okx_websocket_client(self.client.config if hasattr(self.client, 'config') else self.market_oracle.client.config)
        except CCXTProUnavailableError as exc:
            logger.warning("Grid websocket sync disabled: %s", exc)
            return

        try:
            assert self._ws_stop_event is not None
            while not self._ws_stop_event.is_set():
                ticker_task = asyncio.create_task(ticker_client.watch_ticker(self.symbol))
                orders_task = asyncio.create_task(orders_client.watch_orders(self.symbol))
                stop_task = asyncio.create_task(self._ws_stop_event.wait())
                done, pending = await asyncio.wait(
                    {ticker_task, orders_task, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if stop_task in done:
                    break
                if ticker_task in done and not ticker_task.cancelled():
                    with suppress(Exception):
                        self._on_ws_ticker(ticker_task.result())
                if orders_task in done and not orders_task.cancelled():
                    with suppress(Exception):
                        self._on_ws_orders(orders_task.result())
        finally:
            with suppress(Exception):
                await ticker_client.close()
            with suppress(Exception):
                await orders_client.close()

    def flatten_and_cancel_all(self, reason: str) -> None:
        coordinated_result = None
        if self.risk_manager is not None and hasattr(self.risk_manager, "coordinate_strategy_cleanup"):
            coordinated_result = self.risk_manager.coordinate_strategy_cleanup(self.strategy_name, reason)

        if coordinated_result is None:
            super().flatten_and_cancel_all(reason)
        self._publish_grid_risk(None)

    def cleanup_for_regime_switch(self, reason: str) -> str:
        self.client.cancel_all_orders(self.symbol)
        current_price = float(self.client.fetch_last_price(self.symbol))
        positions = self._load_position_candidates(current_price)
        actions: list[str] = []

        for candidate in positions:
            close_amount = self._normalize_amount(candidate.size)
            if close_amount <= 0:
                continue
            if candidate.profitable and current_price > 0:
                self.client.place_limit_order(
                    self.symbol,
                    candidate.close_side,
                    close_amount,
                    current_price,
                    reduce_only=True,
                    params={"reason": reason, "posSide": candidate.pos_side},
                )
                actions.append(f"limit_{candidate.side}:{close_amount:.8f}@{current_price:.2f}")
                continue

            self.client.place_market_order(
                self.symbol,
                candidate.close_side,
                close_amount,
                reduce_only=True,
                params={"reason": reason, "posSide": candidate.pos_side},
            )
            actions.append(f"market_{candidate.side}:{close_amount:.8f}")

        result = "grid:regime_cleanup_idle" if not actions else "grid:regime_cleanup|" + "+".join(actions)
        if self.notifier is not None:
            self.notifier.send(
                "Strategy Cleanup",
                f"strategy={self.strategy_name} | symbol={self.symbol} | reason={reason} | result={result}",
            )
        return result

    def execute_active_cycle(self) -> str:
        now = self.now_provider().astimezone(timezone.utc)
        self._ensure_futures_settings()
        current_price = float(self.client.fetch_last_price(self.symbol))
        self._push_price_point(now, current_price)

        if self._halted:
            return "grid:halted"
        if not self._oracle_allows_dynamic_grid():
            return "grid:adx_trend_not_ready"
        if self._flash_crash_active(now):
            self.client.cancel_all_orders(self.symbol)
            self._publish_grid_risk(None)
            remaining_seconds = max(0, int((self._flash_crash_until - now).total_seconds())) if self._flash_crash_until else 0
            logger.warning("Grid flash crash cooldown | symbol=%s remaining=%ss", self.symbol, remaining_seconds)
            cached = self._cached_context
            if cached is None:
                return f"grid:flash_crash_cooldown|remaining={remaining_seconds}s|dynamic={str(self.use_dynamic_range).lower()}|regrid=false"
            return (
                f"grid:flash_crash_cooldown|remaining={remaining_seconds}s|atr={cached.atr_value:.2f}|"
                f"dynamic={str(self.use_dynamic_range).lower()}|regrid=false|center={cached.center_price:.2f}|"
                f"bounds={cached.lower_bound:.2f}-{cached.upper_bound:.2f}"
            )

        context = self._refresh_grid_context(current_price, anchor_timestamp_ms=int(now.timestamp() * 1000))
        if context is None:
            self._publish_grid_risk(None)
            return "grid:insufficient_data"

        if self._hard_stop_triggered(current_price, context):
            self.client.cancel_all_orders(self.symbol)
            self.client.close_all_positions(self.symbol)
            self._halted = True
            self._publish_grid_risk(None)
            logger.error(
                "Grid hard stop triggered | symbol=%s price=%.2f bounds=%.2f-%.2f",
                self.symbol,
                current_price,
                context.lower_bound,
                context.upper_bound,
            )
            return "grid:hard_stop_triggered"

        flash_crash_action = self._apply_flash_crash_guard(context, now)
        if flash_crash_action is not None:
            self._publish_grid_risk(None)
            return flash_crash_action

        self._publish_grid_risk(context)
        allow_new_openings = True
        risk_blocked_reason = None
        if self.risk_manager is not None:
            allow_new_openings, risk_blocked_reason = self.risk_manager.can_open_new_position(
                self.symbol,
                0.0,
                strategy_name=self.strategy_name,
            )

        needs_regrid = self._should_regrid(context, current_price)
        if not needs_regrid and self._has_active_grid_orders():
            return (
                f"grid:hold_existing_grid|center={context.center_price:.2f}|atr={context.atr_value:.2f}|"
                f"dynamic={str(self.use_dynamic_range).lower()}|regrid=false|"
                f"bounds={context.lower_bound:.2f}-{context.upper_bound:.2f}"
            )

        self.client.cancel_all_orders(self.symbol)
        net_position_size = self._fetch_net_position_size()
        opening_orders = self._build_opening_orders(context, current_price, now)
        rebalance_orders = self._build_rebalance_orders(context, net_position_size)

        placed_orders = 0
        opening_orders_placed = 0
        rebalance_orders_placed = 0
        cooled_layers = 0
        reserved_notional = 0.0

        for order in opening_orders:
            if order.side == "buy" and self._layer_is_cooling(order.layer_key, now, current_price, order.price):
                cooled_layers += 1
                continue
            if not allow_new_openings:
                continue
            if not self._directional_opening_allowed(order.side, order.price, order.amount):
                continue
            if self._try_place_limit_order(order, reserved_notional=reserved_notional):
                placed_orders += 1
                opening_orders_placed += 1
                reserved_notional += self._estimate_notional(order.amount, order.price)

        for order in rebalance_orders:
            if self._try_place_limit_order(order, reserved_notional=0.0):
                placed_orders += 1
                rebalance_orders_placed += 1

        if placed_orders <= 0:
            if not allow_new_openings and rebalance_orders_placed <= 0:
                return "grid:risk_blocked"
            return "grid:no_orders"

        self._cached_context = context
        action_parts = [
            f"grid:placed_{placed_orders}_orders@{current_price:.2f}",
            f"openings={opening_orders_placed}",
            f"rebalances={rebalance_orders_placed}",
            f"cooldown={cooled_layers}",
            f"atr={context.atr_value:.2f}",
            f"dynamic={str(self.use_dynamic_range).lower()}",
            f"regrid=true",
            f"center={context.center_price:.2f}",
            f"bounds={context.lower_bound:.2f}-{context.upper_bound:.2f}",
        ]
        if risk_blocked_reason is not None:
            action_parts.append(f"risk={risk_blocked_reason}")
        return "|".join(action_parts)

    def reduce_exposure_step(self, reason: str, reduction_step_pct: float) -> str:
        self.client.cancel_all_orders(self.symbol)
        reduction_ratio = min(1.0, max(0.01, float(reduction_step_pct)))
        current_price = float(self.client.fetch_last_price(self.symbol))
        candidates = self._load_position_candidates(current_price)
        if not candidates:
            return "grid:step_reduce_idle"

        if str(reason).startswith("grid_liquidation_warning"):
            protective_ratio = max(reduction_ratio, float(self.config.liquidation_protection_ratio))
            return self._protective_trim_positions(reason, candidates, protective_ratio)

        actions: list[str] = []
        for candidate in candidates:
            reduce_amount = min(candidate.size, self._normalize_amount(candidate.size * reduction_ratio))
            if reduce_amount <= 0:
                continue
            self.client.place_market_order(
                self.symbol,
                candidate.close_side,
                reduce_amount,
                reduce_only=True,
                params={"reason": reason, "posSide": candidate.pos_side},
            )
            actions.append(f"{candidate.side}:{reduce_amount:.8f}")

        return "grid:step_reduce_idle" if not actions else "grid:step_reduce|" + "+".join(actions)

    def _protective_trim_positions(
        self,
        reason: str,
        candidates: list[GridPositionCandidate],
        reduction_ratio: float,
    ) -> str:
        ranked = sorted(
            candidates,
            key=lambda candidate: (
                0 if candidate.liquidation_distance_ratio is not None else 1,
                candidate.liquidation_distance_ratio if candidate.liquidation_distance_ratio is not None else inf,
                0 if not candidate.profitable else 1,
                -candidate.price_distance_ratio,
                -candidate.notional,
            ),
        )
        target_size = sum(candidate.size for candidate in ranked) * reduction_ratio
        remaining_target = max(0.0, target_size)
        actions: list[str] = []

        for candidate in ranked:
            if remaining_target <= 1e-12:
                break
            raw_reduce = min(candidate.size, remaining_target)
            reduce_amount = self._normalize_amount(raw_reduce)
            if reduce_amount <= 0:
                continue
            self.client.place_market_order(
                self.symbol,
                candidate.close_side,
                reduce_amount,
                reduce_only=True,
                params={"reason": reason, "posSide": candidate.pos_side},
            )
            actions.append(
                f"protective_trim:{candidate.side}:{reduce_amount:.8f}|distance={candidate.price_distance_ratio:.2%}"
            )
            remaining_target = max(0.0, remaining_target - reduce_amount)

        return "grid:step_reduce_idle" if not actions else "grid:step_reduce|" + "+".join(actions)

    def _ensure_futures_settings(self) -> None:
        if hasattr(self.client, "ensure_futures_settings"):
            self.client.ensure_futures_settings(
                self.symbol,
                leverage=self.config.leverage,
                margin_mode=self.execution_config.td_mode,
            )

    def _flash_crash_active(self, now: datetime) -> bool:
        if self._flash_crash_until is None:
            return False
        if now >= self._flash_crash_until:
            self._flash_crash_until = None
            return False
        return True

    def _apply_flash_crash_guard(self, context: GridContext, now: datetime) -> str | None:
        if not bool(getattr(self.config, "flash_crash_enabled", True)):
            return None
        one_minute_range = self._latest_flash_crash_range(now)
        atr_multiplier = float(getattr(self.config, "flash_crash_atr_multiplier", 1.5))
        threshold = float(context.atr_value) * atr_multiplier
        logger.warning(
            "Grid flash crash check | symbol=%s range_1m=%.2f atr=%.2f threshold=%.2f",
            self.symbol,
            one_minute_range,
            context.atr_value,
            threshold,
        )
        if one_minute_range <= 0 or threshold <= 0 or one_minute_range < threshold:
            return None
        cooldown_seconds = max(1, int(getattr(self.config, "flash_crash_cooldown_seconds", 300)))
        self._flash_crash_until = now + timedelta(seconds=cooldown_seconds)
        self.client.cancel_all_orders(self.symbol)
        logger.error(
            "Grid flash crash protection triggered | symbol=%s range_1m=%.2f atr=%.2f threshold=%.2f cooldown=%ss",
            self.symbol,
            one_minute_range,
            context.atr_value,
            threshold,
            cooldown_seconds,
        )
        action = (
            f"grid:flash_crash_triggered|range_1m={one_minute_range:.2f}|atr={context.atr_value:.2f}"
            f"|threshold={threshold:.2f}|cooldown={cooldown_seconds}s|dynamic={str(self.use_dynamic_range).lower()}"
            f"|regrid=false|center={context.center_price:.2f}|bounds={context.lower_bound:.2f}-{context.upper_bound:.2f}"
        )
        if self.notifier is not None:
            self.notifier.send(
                "Grid Risk Alert",
                (
                    f"symbol={self.symbol} | action=flash_crash_triggered | range_1m={one_minute_range:.2f} | "
                    f"atr={context.atr_value:.2f} | threshold={threshold:.2f} | cooldown={cooldown_seconds}s | "
                    f"center={context.center_price:.2f} | bounds={context.lower_bound:.2f}-{context.upper_bound:.2f}"
                ),
            )
        return action

    def _latest_flash_crash_range(self, now: datetime) -> float:
        self._prune_price_window(now)
        observed_range = self._window_range()
        if observed_range > 0:
            return observed_range
        timeframe = str(getattr(self.config, "flash_crash_timeframe", "1m") or "1m")
        ohlcv = self.client.fetch_ohlcv(symbol=self.symbol, timeframe=timeframe, limit=3)
        if not ohlcv:
            return 0.0
        latest = ohlcv[-1]
        if len(latest) < 4:
            return 0.0
        return max(0.0, float(latest[2]) - float(latest[3]))

    def _refresh_grid_context(self, current_price: float, anchor_timestamp_ms: int | None = None) -> GridContext | None:
        atr_value = self._resolve_atr_value()
        anchor_timestamp_ms = int(anchor_timestamp_ms if anchor_timestamp_ms is not None else self.now_provider().timestamp() * 1000)

        if self.use_dynamic_range and atr_value > 0:
            lower_bound = current_price - self.atr_multiplier * atr_value
            upper_bound = current_price + self.atr_multiplier * atr_value
            center_price = current_price
        else:
            lower_bound, upper_bound = self._resolve_active_bounds(current_price=current_price)
            center_price = current_price

        if lower_bound <= 0 or upper_bound <= 0 or lower_bound >= upper_bound:
            return self._fallback_context(current_price, atr_value)

        buy_prices, sell_prices = self._derive_layer_prices(lower_bound, center_price, upper_bound)
        context = GridContext(
            anchor_timestamp_ms=anchor_timestamp_ms,
            lower_bound=lower_bound,
            middle_band=center_price,
            upper_bound=upper_bound,
            buy_prices=buy_prices,
            sell_prices=sell_prices,
            center_price=center_price,
            atr_value=atr_value,
        )
        self._prune_layer_state(anchor_timestamp_ms)
        return context

    def _fallback_context(self, current_price: float, atr_value: float = 0.0) -> GridContext:
        lower_bound, upper_bound = self._resolve_active_bounds(current_price=current_price)
        buy_prices, sell_prices = self._derive_layer_prices(lower_bound, current_price, upper_bound)
        return GridContext(
            anchor_timestamp_ms=-1,
            lower_bound=lower_bound,
            middle_band=current_price,
            upper_bound=upper_bound,
            buy_prices=buy_prices,
            sell_prices=sell_prices,
            center_price=current_price,
            atr_value=atr_value,
        )

    def _resolve_active_bounds(
        self,
        *,
        current_price: float,
        bollinger_lower: float | None = None,
        bollinger_upper: float | None = None,
    ) -> tuple[float, float]:
        price_floor = current_price * (1 - self.config.range_percent)
        price_ceiling = current_price * (1 + self.config.range_percent)

        lower_bound = max(price_floor, float(bollinger_lower)) if bollinger_lower is not None else price_floor
        upper_bound = min(price_ceiling, float(bollinger_upper)) if bollinger_upper is not None else price_ceiling

        if lower_bound >= current_price:
            lower_bound = price_floor
        if upper_bound <= current_price:
            upper_bound = price_ceiling
        return lower_bound, upper_bound

    def _resolve_atr_value(self) -> float:
        if self.market_oracle is not None and hasattr(self.market_oracle, "get_hourly_atr"):
            try:
                return float(self.market_oracle.get_hourly_atr(self.symbol))
            except Exception:
                pass
        timeframe = str(getattr(self.config, "atr_timeframe", "1h") or "1h")
        ohlcv = self.client.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=max(self.config.atr_period * 4, 80))
        if not ohlcv:
            return 0.0
        frame = ohlcv_to_dataframe(ohlcv)
        atr_series = compute_atr(frame, length=self.config.atr_period)
        if atr_series.empty:
            return 0.0
        return float(atr_series.iloc[-1])

    def _should_regrid(self, context: GridContext, current_price: float) -> bool:
        previous = self._cached_context
        if previous is None:
            return True
        if previous.center_price <= 0:
            return True
        if abs(previous.atr_value - context.atr_value) > 1e-9:
            return True
        trigger_ratio = float(getattr(self.config, "regrid_trigger_atr_ratio", 0.50))
        trigger_distance = max(0.0, context.atr_value * trigger_ratio)
        if trigger_distance <= 0:
            trigger_distance = abs(previous.center_price) * 0.001
        return abs(current_price - previous.center_price) >= trigger_distance

    def _has_active_grid_orders(self) -> bool:
        if not hasattr(self.client, "fetch_open_orders"):
            return False
        try:
            orders = self.client.fetch_open_orders(self.symbol)
        except Exception:
            return False
        return any(not bool(order.get("reduceOnly") or order.get("info", {}).get("reduceOnly")) for order in orders)

    def _oracle_allows_dynamic_grid(self) -> bool:
        if self.market_oracle is not None and hasattr(self.market_oracle, "current_higher_adx_trend"):
            try:
                return self.market_oracle.current_higher_adx_trend() in {"flat", "falling"}
            except Exception:
                return True
        return True

    def _hard_stop_triggered(self, current_price: float, context: GridContext) -> bool:
        buffer_ratio = float(getattr(self.config, "hard_stop_buffer_ratio", 0.01))
        lower_stop = context.lower_bound * (1.0 - buffer_ratio)
        upper_stop = context.upper_bound * (1.0 + buffer_ratio)
        return current_price <= lower_stop or current_price >= upper_stop

    def _directional_opening_allowed(self, side: str, price: float, amount: float) -> bool:
        max_ratio = float(getattr(self.config, "max_directional_exposure_ratio", 0.50))
        if max_ratio <= 0 or not hasattr(self.client, "fetch_total_equity"):
            return True
        try:
            equity = float(self.client.fetch_total_equity("USDT"))
        except Exception:
            return True
        if equity <= 0:
            return True
        long_notional = 0.0
        short_notional = 0.0
        for position in self.client.fetch_positions([self.symbol]) or []:
            notional = self._position_notional(position)
            position_side = str(position.get("side") or position.get("info", {}).get("posSide") or "").lower()
            if position_side == "short":
                short_notional += notional
            else:
                long_notional += notional
        requested = self._estimate_notional(amount, price)
        current = long_notional if side == "buy" else short_notional
        return current + requested <= equity * max_ratio + 1e-12

    def _push_price_point(self, now: datetime, current_price: float) -> None:
        self._price_window.append((now, float(current_price)))
        self._prune_price_window(now)

    def _prune_price_window(self, now: datetime) -> None:
        window = timedelta(seconds=60)
        while self._price_window and now - self._price_window[0][0] > window:
            self._price_window.popleft()

    def _window_range(self) -> float:
        if not self._price_window:
            return 0.0
        prices = [price for _, price in self._price_window]
        return max(prices) - min(prices)

    def _on_ws_ticker(self, payload: dict) -> None:
        mark = payload.get("mark") or payload.get("last") or payload.get("close") or payload.get("info", {}).get("markPx")
        if mark in (None, ""):
            return
        now = self.now_provider().astimezone(timezone.utc)
        self._push_price_point(now, float(mark))

    def _on_ws_orders(self, payload: list[dict] | dict) -> None:
        events = payload if isinstance(payload, list) else [payload]
        context = self._cached_context
        if context is None or context.atr_value <= 0:
            return
        step_size = max(1e-12, (context.upper_bound - context.lower_bound) / max(1, self.config.levels))
        for order in events:
            status = str(order.get("status") or order.get("info", {}).get("state") or "").lower()
            filled = float(order.get("filled") or order.get("info", {}).get("fillSz") or 0.0)
            if status not in {"closed", "filled"} or filled <= 0:
                continue
            reduce_only = bool(order.get("reduceOnly") or order.get("info", {}).get("reduceOnly"))
            if reduce_only:
                continue
            side = str(order.get("side") or "").lower()
            fill_price = float(order.get("average") or order.get("price") or order.get("info", {}).get("fillPx") or 0.0)
            if side not in {"buy", "sell"} or fill_price <= 0:
                continue
            counter_side = "sell" if side == "buy" else "buy"
            counter_price = fill_price + step_size if side == "buy" else fill_price - step_size
            counter_price = self.client.price_to_precision(self.symbol, counter_price)
            counter_amount = self._normalize_amount(filled)
            if counter_price <= 0 or counter_amount <= 0:
                continue
            self.client.place_limit_order(
                self.symbol,
                counter_side,
                counter_amount,
                counter_price,
                reduce_only=True,
            )
            logger.info(
                "Grid websocket hedge order | fill_side=%s fill_price=%.2f counter_side=%s counter_price=%.2f amount=%.8f",
                side,
                fill_price,
                counter_side,
                counter_price,
                counter_amount,
            )

    def _derive_layer_prices(self, lower_bound: float, anchor_price: float, upper_bound: float) -> tuple[list[float], list[float]]:
        buy_levels = max(1, self.config.levels // 2)
        sell_levels = max(1, self.config.levels - buy_levels)

        buy_step = max(1e-12, (anchor_price - lower_bound) / buy_levels)
        sell_step = max(1e-12, (upper_bound - anchor_price) / sell_levels)

        buy_prices = [anchor_price - buy_step * (index + 1) for index in range(buy_levels)]
        sell_prices = [anchor_price + sell_step * (index + 1) for index in range(sell_levels)]
        return buy_prices, sell_prices

    def _build_opening_orders(self, context: GridContext, current_price: float, now: datetime) -> list[GridOrderPlan]:
        del current_price, now
        orders: list[GridOrderPlan] = []

        for index, price in enumerate(context.buy_prices, start=1):
            amount = self.execution_config.grid_order_size * (self.config.martingale_factor ** (index - 1))
            orders.append(
                GridOrderPlan(
                    layer_key=self._layer_key(context.anchor_timestamp_ms, "buy", index),
                    index=index,
                    side="buy",
                    price=price,
                    amount=self._normalize_amount(amount),
                )
            )

        for index, price in enumerate(context.sell_prices, start=1):
            orders.append(
                GridOrderPlan(
                    layer_key=self._layer_key(context.anchor_timestamp_ms, "sell", index),
                    index=index,
                    side="sell",
                    price=price,
                    amount=self._normalize_amount(self.execution_config.grid_order_size),
                )
            )

        return orders

    def _build_rebalance_orders(self, context: GridContext, net_position_size: float) -> list[GridOrderPlan]:
        base_amount = max(0.0, float(self.execution_config.grid_order_size))
        threshold = base_amount * max(0.0, float(self.config.rebalance_exposure_threshold))
        exposure_size = abs(net_position_size)
        if base_amount <= 0 or exposure_size <= threshold:
            return []

        if net_position_size > 0:
            rebalance_side = "sell"
            rebalance_prices = context.sell_prices
        else:
            rebalance_side = "buy"
            rebalance_prices = list(reversed(context.buy_prices))

        if not rebalance_prices:
            return []

        order_count = min(
            max(1, int(self.config.max_rebalance_orders)),
            len(rebalance_prices),
            max(1, ceil(exposure_size / base_amount)),
        )
        rebalance_amount = self._normalize_amount(exposure_size / order_count)
        if rebalance_amount <= 0:
            return []

        orders: list[GridOrderPlan] = []
        for index, price in enumerate(rebalance_prices[:order_count], start=1):
            orders.append(
                GridOrderPlan(
                    layer_key=self._layer_key(context.anchor_timestamp_ms, "rebalance", index),
                    index=index,
                    side=rebalance_side,
                    price=price,
                    amount=rebalance_amount,
                    reduce_only=True,
                )
            )
        return orders

    def _try_place_limit_order(self, order: GridOrderPlan, *, reserved_notional: float) -> int:
        amount = self._normalize_amount(order.amount)
        if amount <= 0:
            return 0

        if self.risk_manager is not None and not order.reduce_only:
            requested_notional = reserved_notional + self._estimate_notional(amount, order.price)
            opening_side = "long" if order.side == "buy" else "short"
            leverage_allowed, _reason = self.risk_manager.check_directional_exposure_limit(
                requested_notional,
                opening_side,
            )
            if not leverage_allowed:
                return 0
            limit_allowed, _reason = self.risk_manager.check_symbol_notional_limit(self.symbol, requested_notional)
            if not limit_allowed:
                return 0

        self.client.place_limit_order(
            self.symbol,
            order.side,
            amount,
            order.price,
            reduce_only=order.reduce_only,
        )
        return 1

    def _layer_is_cooling(self, layer_key: str, now: datetime, current_price: float, layer_price: float) -> bool:
        reference_price = self._layer_reference_prices.get(layer_key, layer_price)
        self._layer_reference_prices[layer_key] = layer_price
        if current_price > reference_price:
            return self._is_layer_on_cooldown(layer_key, now)

        history = self._layer_triggers[layer_key]
        history.append(now)
        window = timedelta(seconds=max(1, self.config.trigger_window_seconds))
        while history and now - history[0] > window:
            history.popleft()

        if len(history) >= max(1, self.config.trigger_limit_per_layer):
            cooldown_until = now + timedelta(seconds=max(1, self.config.layer_cooldown_seconds))
            existing = self._layer_cooldowns.get(layer_key)
            if existing is None or cooldown_until > existing:
                self._layer_cooldowns[layer_key] = cooldown_until

        return self._is_layer_on_cooldown(layer_key, now)

    def _is_layer_on_cooldown(self, layer_key: str, now: datetime) -> bool:
        cooldown_until = self._layer_cooldowns.get(layer_key)
        if cooldown_until is None:
            return False
        if now >= cooldown_until:
            self._layer_cooldowns.pop(layer_key, None)
            self._layer_triggers.pop(layer_key, None)
            return False
        return True

    def _fetch_net_position_size(self) -> float:
        if not hasattr(self.client, "fetch_positions"):
            return 0.0

        long_size = 0.0
        short_size = 0.0
        for position in self.client.fetch_positions([self.symbol]) or []:
            raw_size = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos") or 0.0
            size = abs(float(raw_size or 0.0))
            if size <= 0:
                continue
            side = str(position.get("side") or position.get("info", {}).get("posSide") or "").lower()
            if side not in {"long", "short"}:
                side = "long" if float(raw_size or 0.0) >= 0 else "short"
            if side == "short":
                short_size += size
            else:
                long_size += size

        return long_size - short_size

    def _load_position_candidates(self, current_price: float) -> list[GridPositionCandidate]:
        candidates: list[GridPositionCandidate] = []
        for position in self.client.fetch_positions([self.symbol]) or []:
            raw_size = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos") or 0.0
            size = abs(float(raw_size or 0.0))
            if size <= 0:
                continue

            side = str(position.get("side") or position.get("info", {}).get("posSide") or "").lower()
            if side not in {"long", "short"}:
                side = "long" if float(raw_size or 0.0) >= 0 else "short"
            pos_side = str(position.get("info", {}).get("posSide") or side)
            notional = self._position_notional(position)
            entry_price = self._extract_entry_price(position, current_price)
            price_distance_ratio = abs(current_price - entry_price) / max(abs(current_price), 1e-12)
            liquidation_price = self._extract_liquidation_price(position)
            liquidation_distance_ratio = None
            if liquidation_price is not None and liquidation_price > 0:
                liquidation_distance_ratio = abs(current_price - liquidation_price) / liquidation_price

            profitable = current_price >= entry_price if side == "long" else current_price <= entry_price
            candidates.append(
                GridPositionCandidate(
                    side=side,
                    pos_side=pos_side,
                    size=size,
                    notional=notional,
                    entry_price=entry_price,
                    current_price=current_price,
                    price_distance_ratio=price_distance_ratio,
                    liquidation_price=liquidation_price,
                    liquidation_distance_ratio=liquidation_distance_ratio,
                    profitable=profitable,
                )
            )
        return candidates

    def _estimate_notional(self, amount: float, price: float) -> float:
        if hasattr(self.client, "estimate_notional"):
            return float(self.client.estimate_notional(self.symbol, amount, price))
        contract_value = 1.0
        if hasattr(self.client, "get_contract_value"):
            contract_value = float(self.client.get_contract_value(self.symbol))
        return abs(float(amount)) * abs(float(price)) * contract_value

    def _position_notional(self, position: dict) -> float:
        if hasattr(self.client, "position_notional"):
            return abs(float(self.client.position_notional(self.symbol, position)))
        contracts = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos") or 0.0
        return self._estimate_notional(float(contracts or 0.0), self._extract_entry_price(position, self.client.fetch_last_price(self.symbol)))

    def _extract_entry_price(self, position: dict, fallback_price: float) -> float:
        raw = (
            position.get("entryPrice")
            or position.get("avgPrice")
            or position.get("markPrice")
            or position.get("info", {}).get("avgPx")
            or position.get("info", {}).get("avgPrice")
            or position.get("info", {}).get("markPx")
            or fallback_price
        )
        return abs(float(raw or fallback_price or 0.0))

    def _extract_liquidation_price(self, position: dict) -> float | None:
        if hasattr(self.client, "get_position_liquidation_price"):
            return self.client.get_position_liquidation_price(position)
        liquidation_price = position.get("liquidationPrice") or position.get("info", {}).get("liqPx")
        if liquidation_price in (None, "", 0, "0"):
            return None
        return abs(float(liquidation_price))

    def _normalize_amount(self, amount: float) -> float:
        normalized = max(0.0, float(amount))
        if hasattr(self.client, "amount_to_precision"):
            normalized = float(self.client.amount_to_precision(self.symbol, normalized))
        normalized = round(normalized, 12)
        return 0.0 if normalized < 1e-12 else normalized

    def _publish_grid_risk(self, context: GridContext | None) -> None:
        if self.risk_manager is None:
            return
        if context is None:
            self.risk_manager.update_grid_risk(None)
            return

        self.risk_manager.update_grid_risk(
            GridRiskProfile(
                symbol=self.symbol,
                lower_bound=context.lower_bound,
                upper_bound=context.upper_bound,
            )
        )

    def _prune_layer_state(self, active_anchor_timestamp_ms: int) -> None:
        active_prefix = f"{active_anchor_timestamp_ms}:"
        self._layer_triggers = defaultdict(
            deque,
            {key: history for key, history in self._layer_triggers.items() if key.startswith(active_prefix)},
        )
        self._layer_cooldowns = {
            key: cooldown for key, cooldown in self._layer_cooldowns.items() if key.startswith(active_prefix)
        }
        self._layer_reference_prices = {
            key: price for key, price in self._layer_reference_prices.items() if key.startswith(active_prefix)
        }

    @staticmethod
    def _layer_key(anchor_timestamp_ms: int, side: str, index: int) -> str:
        return f"{anchor_timestamp_ms}:{side}:{index}"
