from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Callable

from market_adaptive.config import ExecutionConfig, GridConfig
from market_adaptive.indicators import compute_bollinger_bands, ohlcv_to_dataframe
from market_adaptive.strategies.base import BaseStrategyRobot


@dataclass
class GridContext:
    anchor_timestamp_ms: int
    lower_bound: float
    middle_band: float
    upper_bound: float
    buy_prices: list[float]
    sell_prices: list[float]


@dataclass
class GridOrderPlan:
    layer_key: str
    index: int
    side: str
    price: float
    amount: float
    reduce_only: bool = False


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
    ) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config
        self.risk_manager = risk_manager
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._cached_context: GridContext | None = None
        self._layer_triggers: dict[str, deque[datetime]] = defaultdict(deque)
        self._layer_cooldowns: dict[str, datetime] = {}
        self._layer_reference_prices: dict[str, float] = {}

    def should_notify_action(self, action: str) -> bool:
        if action in {"grid:risk_blocked", "grid:insufficient_data", "grid:no_orders"}:
            return False
        return super().should_notify_action(action)

    def execute_active_cycle(self) -> str:
        now = self.now_provider().astimezone(timezone.utc)
        self._ensure_futures_settings()
        current_price = self.client.fetch_last_price(self.symbol)

        liquidation_guard_action = self._check_liquidation_guard(current_price)
        if liquidation_guard_action is not None:
            self.client.cancel_all_orders(self.symbol)
            self.client.close_all_positions(self.symbol)
            return liquidation_guard_action

        self.client.cancel_all_orders(self.symbol)
        context = self._refresh_grid_context(current_price)
        if context is None:
            return "grid:insufficient_data"

        allow_new_openings = True
        risk_blocked_reason = None
        if self.risk_manager is not None:
            allow_new_openings, risk_blocked_reason = self.risk_manager.can_open_new_position(
                self.symbol,
                0.0,
                strategy_name=self.strategy_name,
            )

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

        action_parts = [
            f"grid:placed_{placed_orders}_orders@{current_price:.2f}",
            f"openings={opening_orders_placed}",
            f"rebalances={rebalance_orders_placed}",
            f"cooldown={cooled_layers}",
            f"bounds={context.lower_bound:.2f}-{context.upper_bound:.2f}",
        ]
        if risk_blocked_reason is not None:
            action_parts.append(f"risk={risk_blocked_reason}")
        return "|".join(action_parts)

    def _ensure_futures_settings(self) -> None:
        if hasattr(self.client, "ensure_futures_settings"):
            self.client.ensure_futures_settings(
                self.symbol,
                leverage=self.config.leverage,
                margin_mode=self.execution_config.td_mode,
            )

    def _check_liquidation_guard(self, current_price: float) -> str | None:
        if not hasattr(self.client, "fetch_positions"):
            return None

        nearest_snapshot: tuple[float, float, str] | None = None
        for position in self.client.fetch_positions([self.symbol]) or []:
            raw_size = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos") or 0.0
            size = abs(float(raw_size or 0.0))
            if size <= 0:
                continue
            liquidation_price = self._extract_liquidation_price(position)
            if liquidation_price is None or liquidation_price <= 0:
                continue
            distance_ratio = abs(current_price - liquidation_price) / liquidation_price
            side = str(position.get("side") or position.get("info", {}).get("posSide") or "net").lower()
            snapshot = (distance_ratio, liquidation_price, side)
            if nearest_snapshot is None or snapshot[0] < nearest_snapshot[0]:
                nearest_snapshot = snapshot

        if nearest_snapshot is None:
            return None

        distance_ratio, liquidation_price, side = nearest_snapshot
        if distance_ratio > self.config.liquidation_protection_ratio:
            return None
        return (
            "grid:liquidation_guard_triggered"
            f"|price={current_price:.2f}"
            f"|liq={liquidation_price:.2f}"
            f"|distance={distance_ratio:.2%}"
            f"|side={side}"
        )

    def _extract_liquidation_price(self, position: dict) -> float | None:
        if hasattr(self.client, "get_position_liquidation_price"):
            return self.client.get_position_liquidation_price(position)

        liquidation_price = position.get("liquidationPrice")
        if liquidation_price not in (None, "", 0, "0"):
            return abs(float(liquidation_price))

        info = position.get("info", {})
        for key in ("liqPx", "liquidationPrice"):
            if info.get(key) not in (None, "", 0, "0"):
                return abs(float(info.get(key)))
        return None

    def _refresh_grid_context(self, current_price: float) -> GridContext | None:
        ohlcv = self.client.fetch_ohlcv(
            symbol=self.symbol,
            timeframe=self.config.timeframe,
            limit=self.config.lookback_limit,
        )
        minimum_bars = max(self.config.bollinger_period + 2, 22)
        if len(ohlcv) < minimum_bars:
            return self._fallback_context(current_price)

        anchor_timestamp_ms = int(ohlcv[-1][0])

        frame = ohlcv_to_dataframe(ohlcv)
        bands = compute_bollinger_bands(frame, length=self.config.bollinger_period, std=self.config.bollinger_std)
        latest = bands.iloc[-1]
        lower_bound = float(latest["lower"])
        middle_band = float(latest["basis"])
        upper_bound = float(latest["upper"])

        if lower_bound <= 0 or middle_band <= 0 or upper_bound <= 0 or not (lower_bound < middle_band < upper_bound):
            return self._fallback_context(current_price)

        active_lower_bound, active_upper_bound = self._resolve_active_bounds(
            current_price=current_price,
            bollinger_lower=lower_bound,
            bollinger_upper=upper_bound,
        )
        buy_prices, sell_prices = self._derive_layer_prices(active_lower_bound, current_price, active_upper_bound)
        context = GridContext(
            anchor_timestamp_ms=anchor_timestamp_ms,
            lower_bound=active_lower_bound,
            middle_band=current_price,
            upper_bound=active_upper_bound,
            buy_prices=buy_prices,
            sell_prices=sell_prices,
        )
        self._cached_context = context
        self._prune_layer_state(anchor_timestamp_ms)
        return context

    def _fallback_context(self, current_price: float) -> GridContext:
        lower_bound, upper_bound = self._resolve_active_bounds(current_price=current_price)
        buy_prices, sell_prices = self._derive_layer_prices(lower_bound, current_price, upper_bound)
        return GridContext(
            anchor_timestamp_ms=-1,
            lower_bound=lower_bound,
            middle_band=current_price,
            upper_bound=upper_bound,
            buy_prices=buy_prices,
            sell_prices=sell_prices,
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
            allowed, _reason = self.risk_manager.check_symbol_notional_limit(self.symbol, requested_notional)
            if not allowed:
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

    def _estimate_notional(self, amount: float, price: float) -> float:
        if hasattr(self.client, "estimate_notional"):
            return float(self.client.estimate_notional(self.symbol, amount, price))
        contract_value = 1.0
        if hasattr(self.client, "get_contract_value"):
            contract_value = float(self.client.get_contract_value(self.symbol))
        return abs(float(amount)) * abs(float(price)) * contract_value

    def _normalize_amount(self, amount: float) -> float:
        normalized = max(0.0, float(amount))
        if hasattr(self.client, "amount_to_precision"):
            normalized = float(self.client.amount_to_precision(self.symbol, normalized))
        normalized = round(normalized, 12)
        return 0.0 if normalized < 1e-12 else normalized

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
