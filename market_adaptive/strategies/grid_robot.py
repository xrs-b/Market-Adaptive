from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import defaultdict, deque
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil, inf
from typing import Callable

from market_adaptive.clients.okx_ws_client import CCXTProUnavailableError, build_okx_websocket_client
from market_adaptive.config import ExecutionConfig, GridConfig
from market_adaptive.coordination import StrategyRuntimeContext
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
class GridBiasProfile:
    bias_value: float = 0.0
    center_shift: float = 0.0
    buy_levels: int = 0
    sell_levels: int = 0
    buy_spacing_ratio: float = 0.0
    sell_spacing_ratio: float = 0.0

    @property
    def bullish(self) -> bool:
        return self.bias_value > 0

    @property
    def bearish(self) -> bool:
        return self.bias_value < 0

    @property
    def neutral(self) -> bool:
        return not self.bullish and not self.bearish


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
        runtime_context: StrategyRuntimeContext | None = None,
    ) -> None:
        super().__init__(client=client, database=database, symbol=config.symbol, notifier=notifier)
        self.config = config
        self.execution_config = execution_config
        self.risk_manager = risk_manager
        self.market_oracle = market_oracle
        self.use_dynamic_range = bool(config.use_dynamic_range if use_dynamic_range is None else use_dynamic_range)
        self.atr_multiplier = float(config.atr_multiplier if atr_multiplier is None else atr_multiplier)
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self.runtime_context = runtime_context
        self._cached_context: GridContext | None = None
        self._layer_triggers: dict[str, deque[datetime]] = defaultdict(deque)
        self._layer_cooldowns: dict[str, datetime] = {}
        self._layer_reference_prices: dict[str, float] = {}
        self._flash_crash_until: datetime | None = None
        self._halted = False
        self._price_window: deque[tuple[datetime, float]] = deque()
        self._last_grid_placed_at: datetime | None = None
        self._last_healthy_grid_seen_at: datetime | None = None
        self._health_check_failed_streak = 0
        self._placed_order_ids: list[str] = []
        self._health_check_degraded_until: datetime | None = None
        self._ws_thread: threading.Thread | None = None
        self._ws_loop: asyncio.AbstractEventLoop | None = None
        self._ws_stop_event: asyncio.Event | None = None
        self._ws_orders_placed_in_cycle: set[str] = set()
        self._ws_cycle_anchor_timestamp_ms: int | None = None
        self._pending_reduce_only_profits: dict[str, dict[str, float | str]] = {}
        self._reduce_only_filled_amounts: dict[str, float] = {}
        self.last_regrid_time = 0.0
        self.current_grid_center: float | None = None

    def should_notify_action(self, action: str) -> bool:
        if action in {
            "grid:risk_blocked",
            "grid:insufficient_data",
            "grid:no_orders",
            "grid:adx_trend_not_ready",
            "grid:halted",
        }:
            return False
        if action.startswith("grid:placed_"):
            return False
        if action.startswith("grid:hold_existing_grid"):
            return False
        if action.startswith("grid:health_check_degraded"):
            return False
        if action.startswith("grid:order_sync_unavailable"):
            return False
        if action.startswith("grid:flash_crash_cooldown"):
            return False
        if action.startswith("grid:flash_crash_triggered"):
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
            if hasattr(self.notifier, "notify_strategy_cleanup"):
                self.notifier.notify_strategy_cleanup(
                    strategy=self.strategy_name,
                    symbol=self.symbol,
                    reason=reason,
                    result=result,
                    overview="网格策略已完成状态切换清理。",
                )
            else:
                self.notifier.send(
                    "策略清理完成",
                    (
                        "网格策略已完成状态切换清理。\n"
                        f"策略：{self.strategy_name}\n"
                        f"交易对：{self.symbol}\n"
                        f"原因：{reason}\n"
                        f"结果：{result}"
                    ),
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

        needs_regrid = self._should_regrid(context, current_price, now)
        if self._health_check_degraded_active(now):
            return self._build_health_check_degraded_action(context, now)
        if not needs_regrid and self._has_active_grid_orders(context, now):
            return (
                f"grid:hold_existing_grid|center={context.center_price:.2f}|atr={context.atr_value:.2f}|"
                f"dynamic={str(self.use_dynamic_range).lower()}|regrid=false|"
                f"bounds={context.lower_bound:.2f}-{context.upper_bound:.2f}"
            )
        if self._health_check_degraded_active(now):
            return self._build_health_check_degraded_action(context, now)

        order_sync_snapshot = self._fetch_open_orders_with_retry(now, purpose="pre_regrid_validation")
        if order_sync_snapshot is None:
            logger.warning(
                "[grid_robot] order sync unavailable before regrid | symbol=%s failed_streak=%d degraded=%s",
                self.symbol,
                self._health_check_failed_streak,
                self._health_check_degraded_active(now),
            )
            return (
                f"grid:order_sync_unavailable|failed_streak={self._health_check_failed_streak}|"
                f"center={context.center_price:.2f}|atr={context.atr_value:.2f}|"
                f"dynamic={str(self.use_dynamic_range).lower()}|regrid=false|"
                f"bounds={context.lower_bound:.2f}-{context.upper_bound:.2f}"
            )

        self._cancel_pending_grid_orders(order_sync_snapshot)
        net_position_size = self._fetch_net_position_size()
        opening_orders = self._build_opening_orders(context, current_price, now)
        rebalance_orders = self._build_rebalance_orders(context, net_position_size)
        opening_orders, coordination_reason = self._apply_runtime_lockout(opening_orders)

        opening_candidates: list[GridOrderPlan] = []
        cooled_layers = 0
        for order in opening_orders:
            if order.side == "buy" and self._layer_is_cooling(order.layer_key, now, current_price, order.price):
                cooled_layers += 1
                continue
            if not allow_new_openings:
                continue
            if not self._directional_opening_allowed(order.side, order.price, order.amount):
                continue
            opening_candidates.append(order)

        batch_result = self._place_grid_batch_safely(opening_candidates)
        if batch_result["status"] == "failed":
            return f"grid:batch_place_failed|reason={batch_result['reason']}"

        opening_orders_placed = int(batch_result["placed_orders"])
        placed_orders = opening_orders_placed
        rebalance_orders_placed = 0

        for order in rebalance_orders:
            if self._try_place_limit_order(order, reserved_notional=0.0):
                placed_orders += 1
                rebalance_orders_placed += 1

        if placed_orders <= 0:
            if not allow_new_openings and rebalance_orders_placed <= 0:
                return "grid:risk_blocked"
            return "grid:no_orders"

        self._cached_context = context
        self._last_grid_placed_at = now
        self.last_regrid_time = time.time()
        self.current_grid_center = current_price
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
        if coordination_reason is not None:
            action_parts.append(f"coordination={coordination_reason}")
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
                "Grid 风险预警",
                (
                    f"交易对：{self.symbol}\n"
                    "异常类型：flash_crash_triggered\n"
                    f"1分钟波动：{one_minute_range:.2f}\n"
                    f"ATR：{context.atr_value:.2f}\n"
                    f"触发阈值：{threshold:.2f}\n"
                    f"保护冷却：{cooldown_seconds}秒\n"
                    f"网格中心：{context.center_price:.2f}\n"
                    f"价格边界：{context.lower_bound:.2f}-{context.upper_bound:.2f}"
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
        self._reset_ws_order_cycle(anchor_timestamp_ms)
        bias_profile = self._resolve_grid_bias_profile(atr_value)

        if self.use_dynamic_range and atr_value > 0:
            center_price = current_price + bias_profile.center_shift
            lower_bound = center_price - self.atr_multiplier * atr_value
            upper_bound = center_price + self.atr_multiplier * atr_value
        else:
            lower_bound, upper_bound = self._resolve_active_bounds(current_price=current_price)
            center_price = current_price + bias_profile.center_shift

        if lower_bound <= 0 or upper_bound <= 0 or lower_bound >= upper_bound:
            return self._fallback_context(current_price, atr_value)

        buy_prices, sell_prices = self._derive_layer_prices(
            lower_bound,
            center_price,
            upper_bound,
            bias_profile=bias_profile,
            atr_value=atr_value,
        )
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

    def _resolve_bias_value(self) -> float:
        if self.market_oracle is not None and hasattr(self.market_oracle, "current_bias_value"):
            try:
                return float(self.market_oracle.current_bias_value())
            except Exception:
                return 0.0
        return 0.0

    def _resolve_grid_bias_profile(self, atr_value: float) -> GridBiasProfile:
        if not bool(getattr(self.config, "directional_skew_enabled", True)):
            return GridBiasProfile()
        bias_value = self._resolve_bias_value()
        threshold = float(getattr(self.config, "directional_bias_threshold", 0.20))
        neutral_threshold = float(getattr(self.config, "sideways_neutral_bias_threshold", 0.12))
        bearish_threshold = float(getattr(self.config, "bearish_directional_bias_threshold", max(threshold, 0.30)))

        def _normalize_levels(buy_levels: int, sell_levels: int) -> tuple[int, int]:
            buy_levels = max(1, int(buy_levels))
            sell_levels = max(1, int(sell_levels))
            total_levels = buy_levels + sell_levels
            if total_levels != self.config.levels:
                if buy_levels >= sell_levels:
                    buy_levels = max(1, min(self.config.levels - 1, buy_levels))
                    sell_levels = max(1, self.config.levels - buy_levels)
                else:
                    sell_levels = max(1, min(self.config.levels - 1, sell_levels))
                    buy_levels = max(1, self.config.levels - sell_levels)
            return buy_levels, sell_levels

        if abs(bias_value) < neutral_threshold:
            return GridBiasProfile(bias_value=0.0)

        if bias_value >= threshold:
            buy_levels, sell_levels = _normalize_levels(
                getattr(self.config, "bullish_buy_levels", max(1, self.config.levels - 2)),
                getattr(self.config, "bullish_sell_levels", 2),
            )
            return GridBiasProfile(
                bias_value=bias_value,
                center_shift=max(0.0, atr_value * float(getattr(self.config, "bullish_center_shift_atr_ratio", 0.0))),
                buy_levels=buy_levels,
                sell_levels=sell_levels,
                buy_spacing_ratio=max(0.0, float(getattr(self.config, "bullish_buy_spacing_ratio", 0.0))),
                sell_spacing_ratio=max(0.0, float(getattr(self.config, "bullish_sell_spacing_ratio", 0.0))),
            )
        elif bias_value <= -bearish_threshold:
            buy_levels, sell_levels = _normalize_levels(
                getattr(self.config, "bearish_buy_levels", 2),
                getattr(self.config, "bearish_sell_levels", max(1, self.config.levels - 2)),
            )
            return GridBiasProfile(
                bias_value=bias_value,
                center_shift=-max(0.0, atr_value * float(getattr(self.config, "bearish_center_shift_atr_ratio", 0.0))),
                buy_levels=buy_levels,
                sell_levels=sell_levels,
                buy_spacing_ratio=max(0.0, float(getattr(self.config, "bearish_buy_spacing_ratio", 0.0))),
                sell_spacing_ratio=max(0.0, float(getattr(self.config, "bearish_sell_spacing_ratio", 0.0))),
            )
        else:
            return GridBiasProfile(bias_value=bias_value)

    def _hard_reanchor_triggered(self, context: GridContext, current_price: float) -> bool:
        previous = self._cached_context
        if previous is None:
            return False
        grid_center = self.current_grid_center if self.current_grid_center is not None else previous.center_price
        if grid_center <= 0:
            return False
        hard_reanchor_distance = max(0.0, float(context.atr_value) * float(getattr(self.config, "hard_reanchor_atr_ratio", 1.20)))
        if hard_reanchor_distance <= 0:
            return False
        price_shift = abs(current_price - grid_center)
        if price_shift > hard_reanchor_distance:
            logger.info(
                "[grid_robot] _should_regrid: hard re-anchor triggered price_shift=%.2f hard_distance=%.2f center=%.2f",
                price_shift,
                hard_reanchor_distance,
                grid_center,
            )
            return True
        return False

    def _should_regrid(self, context: GridContext, current_price: float, now: datetime) -> bool:
        previous = self._cached_context
        if previous is None:
            logger.info("[grid_robot] _should_regrid: no cached context, returning True")
            return True

        current_ts = now.timestamp()
        if self.last_regrid_time > 0:
            elapsed = current_ts - float(self.last_regrid_time)
            if elapsed < 300:
                logger.info("[grid_robot] _should_regrid: hard cooldown active elapsed=%.2fs", elapsed)
                return False

        grid_center = self.current_grid_center if self.current_grid_center is not None else previous.center_price
        if grid_center <= 0:
            logger.info("[grid_robot] _should_regrid: invalid previous center, returning True")
            return True

        trigger_distance = max(0.0, float(context.atr_value) * float(getattr(self.config, "regrid_trigger_atr_ratio", 0.30)))
        if trigger_distance <= 0:
            trigger_distance = abs(grid_center) * 0.001
        price_shift = abs(current_price - grid_center)
        hard_reanchor = self._hard_reanchor_triggered(context, current_price)
        logger.info(
            "[grid_robot] _should_regrid: price_shift=%.2f trigger_distance=%.2f center=%.2f hard_reanchor=%s",
            price_shift,
            trigger_distance,
            grid_center,
            hard_reanchor,
        )
        if self.current_grid_center is not None and price_shift <= trigger_distance and not hard_reanchor:
            logger.info("[grid_robot] _should_regrid: spatial lock active")
            return False

        previous_atr = max(abs(float(previous.atr_value)), 1e-12)
        atr_diff_ratio = abs(float(context.atr_value) - float(previous.atr_value)) / previous_atr
        atr_regrid_change_ratio = float(getattr(self.config, "atr_regrid_change_ratio", 0.10))
        if atr_diff_ratio > atr_regrid_change_ratio:
            logger.info("[grid_robot] _should_regrid: ATR change %.1f%% > threshold %.1f%%, triggering regrid", atr_diff_ratio * 100, atr_regrid_change_ratio * 100)
            return True

        if hard_reanchor:
            logger.info("[grid_robot] _should_regrid: hard re-anchor forces regrid")
            return True

        if price_shift > trigger_distance:
            logger.info("[grid_robot] _should_regrid: price shift triggers regrid")
            return True

        logger.info("[grid_robot] _should_regrid: no trigger, returning False")
        return False

    def _health_check_degraded_active(self, now: datetime) -> bool:
        if self._health_check_degraded_until is None:
            return False
        if now >= self._health_check_degraded_until:
            self._health_check_degraded_until = None
            return False
        return True

    def _build_health_check_degraded_action(self, context: GridContext, now: datetime) -> str:
        remaining_seconds = 0
        if self._health_check_degraded_until is not None:
            remaining_seconds = max(0, int((self._health_check_degraded_until - now).total_seconds()))
        logger.warning(
            "[grid_robot] health_check_degraded active | symbol=%s remaining=%ss failed_streak=%d",
            self.symbol,
            remaining_seconds,
            self._health_check_failed_streak,
        )
        return (
            f"grid:health_check_degraded|remaining={remaining_seconds}s|failed_streak={self._health_check_failed_streak}|"
            f"center={context.center_price:.2f}|atr={context.atr_value:.2f}|"
            f"dynamic={str(self.use_dynamic_range).lower()}|regrid=false|"
            f"bounds={context.lower_bound:.2f}-{context.upper_bound:.2f}"
        )

    def _fetch_open_orders_with_retry(self, now: datetime, *, purpose: str) -> list[dict] | None:
        if not hasattr(self.client, "fetch_open_orders"):
            return None

        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                orders = self.client.fetch_open_orders(self.symbol)
                if self._health_check_failed_streak > 0:
                    logger.info(
                        "[grid_robot] fetch_open_orders recovered | symbol=%s purpose=%s attempt=%d failed_streak=%d",
                        self.symbol,
                        purpose,
                        attempt,
                        self._health_check_failed_streak,
                    )
                self._health_check_failed_streak = 0
                self._health_check_degraded_until = None
                return orders
            except Exception as exc:
                last_error = exc
                self._health_check_failed_streak += 1
                timestamp = now.isoformat()
                logger.error(
                    "[grid_robot] fetch_open_orders failed | symbol=%s purpose=%s attempt=%d/3 timestamp=%s error_type=%s error=%s",
                    self.symbol,
                    purpose,
                    attempt,
                    timestamp,
                    type(exc).__name__,
                    exc,
                )
                if self._health_check_failed_streak >= 3:
                    self._health_check_degraded_until = now + timedelta(seconds=30)
                if attempt < 3:
                    time.sleep(0.1)

        if last_error is not None and self._health_check_failed_streak >= 3:
            logger.error(
                "[grid_robot] health check degraded after repeated order sync failures | symbol=%s purpose=%s degraded_until=%s error_type=%s",
                self.symbol,
                purpose,
                self._health_check_degraded_until.isoformat() if self._health_check_degraded_until else None,
                type(last_error).__name__,
            )
        return None

    def _as_exchange_bool(self, value) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"", "0", "false", "f", "no", "n", "off", "none", "null"}:
                return False
            if normalized in {"1", "true", "t", "yes", "y", "on"}:
                return True
        return bool(value)

    def _is_reduce_only_order(self, order: dict) -> bool:
        info = order.get("info", {}) or {}
        return any(
            self._as_exchange_bool(value)
            for value in (
                order.get("reduceOnly"),
                order.get("reduce_only"),
                info.get("reduceOnly"),
                info.get("reduce_only"),
            )
        )

    def _has_active_grid_orders(self, context: GridContext, now: datetime) -> bool:
        orders = self._fetch_open_orders_with_retry(now, purpose="health_check")
        if orders is None:
            return False

        active = [order for order in orders if not self._is_reduce_only_order(order)]
        reduce_only_orders = [order for order in orders if self._is_reduce_only_order(order)]
        buy_orders = [order for order in active if str(order.get("side") or "").lower() == "buy"]
        sell_orders = [order for order in active if str(order.get("side") or "").lower() == "sell"]
        reference_context = self._cached_context or context
        expected_buy_prices = [self._normalize_grid_price(price) for price in reference_context.buy_prices]
        expected_sell_prices = [self._normalize_grid_price(price) for price in reference_context.sell_prices]
        expected_count = len(expected_buy_prices) + len(expected_sell_prices)

        actual_buy_prices = [self._extract_order_price(order) for order in buy_orders]
        actual_sell_prices = [self._extract_order_price(order) for order in sell_orders]
        all_opening_prices = [price for price in actual_buy_prices + actual_sell_prices if price > 0]
        expected_opening_prices = [price for price in expected_buy_prices + expected_sell_prices if price > 0]
        expected_lower_bound = min(expected_opening_prices) if expected_opening_prices else reference_context.lower_bound
        expected_upper_bound = max(expected_opening_prices) if expected_opening_prices else reference_context.upper_bound
        within_bounds = bool(all_opening_prices) and all(
            expected_lower_bound - 1e-9 <= price <= expected_upper_bound + 1e-9
            for price in all_opening_prices
        )
        price_tolerance = self._grid_health_price_tolerance(reference_context)
        buy_missing, buy_unexpected = self._grid_price_mismatches(expected_buy_prices, actual_buy_prices, price_tolerance)
        sell_missing, sell_unexpected = self._grid_price_mismatches(expected_sell_prices, actual_sell_prices, price_tolerance)

        reasons: list[str] = []
        if not active:
            reasons.append("no_opening_orders")
        if not within_bounds:
            reasons.append("out_of_bounds")
        if buy_unexpected:
            reasons.append(f"buy_ladder unexpected={buy_unexpected}")
        if sell_unexpected:
            reasons.append(f"sell_ladder unexpected={sell_unexpected}")

        missing_buy = buy_missing
        missing_sell = sell_missing
        if missing_buy or missing_sell:
            reasons.append(f"partial_openings buy_missing={missing_buy} sell_missing={missing_sell}")

        log_level = logging.INFO if not reasons or reasons == [f"partial_openings buy_missing={missing_buy} sell_missing={missing_sell}"] else logging.WARNING
        logger.log(
            log_level,
            "[grid_robot] _has_active_grid_orders check: active=%d buy=%d sell=%d reduce_only=%d expected=%d bounds=[%.2f, %.2f] tol=%.8f reasons=%s",
            len(active),
            len(buy_orders),
            len(sell_orders),
            len(reduce_only_orders),
            expected_count,
            reference_context.lower_bound,
            reference_context.upper_bound,
            price_tolerance,
            ", ".join(reasons) if reasons else "healthy",
        )

        only_partial_openings = reasons == [f"partial_openings buy_missing={missing_buy} sell_missing={missing_sell}"]
        healthy = not reasons or only_partial_openings
        if healthy:
            self._last_healthy_grid_seen_at = now
            return True

        if self._last_healthy_grid_seen_at is not None and (now - self._last_healthy_grid_seen_at).total_seconds() <= 30:
            logger.info(
                "Grid order snapshot failed health check (%s), reusing last healthy snapshot for 30s grace.",
                ", ".join(reasons),
            )
            return True

        return False

    def _extract_order_price(self, order: dict) -> float:
        value = order.get("price") or order.get("info", {}).get("px") or 0.0
        return self._normalize_grid_price(value)

    def _normalize_grid_price(self, price: float) -> float:
        raw_price = float(price or 0.0)
        if raw_price <= 0:
            return 0.0
        if hasattr(self.client, "price_to_precision"):
            try:
                return float(self.client.price_to_precision(self.symbol, raw_price))
            except Exception:
                pass
        return raw_price

    def _grid_health_price_tolerance(self, context: GridContext) -> float:
        expected_prices = sorted(
            price
            for price in [*context.buy_prices, *context.sell_prices]
            if float(price or 0.0) > 0
        )
        if len(expected_prices) >= 2:
            min_gap = min(
                max(0.0, expected_prices[index + 1] - expected_prices[index])
                for index in range(len(expected_prices) - 1)
            )
            if min_gap > 0:
                return max(1e-8, min_gap * 0.35)
        span = max(0.0, float(context.upper_bound) - float(context.lower_bound))
        return max(1e-8, span / max(1, self.config.levels) * 0.35)

    def _grid_price_mismatches(self, expected_prices: list[float], actual_prices: list[float], tolerance: float) -> tuple[int, int]:
        remaining_actual = sorted(price for price in actual_prices if price > 0)
        missing = 0

        for expected in sorted(price for price in expected_prices if price > 0):
            match_index = next(
                (
                    index
                    for index, actual in enumerate(remaining_actual)
                    if abs(actual - expected) <= tolerance
                ),
                None,
            )
            if match_index is None:
                missing += 1
                continue
            remaining_actual.pop(match_index)

        unexpected = len(remaining_actual)
        return missing, unexpected

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

    def _apply_runtime_lockout(self, orders: list[GridOrderPlan]) -> tuple[list[GridOrderPlan], str | None]:
        if self.runtime_context is None:
            return orders, None
        cta_state = self.runtime_context.snapshot_cta()
        if not cta_state.strong_trend or cta_state.symbol not in {"", self.symbol}:
            return orders, None

        blocked_side = "sell" if cta_state.side == "long" else "buy" if cta_state.side == "short" else None
        if blocked_side is None:
            return orders, None
        filtered = [order for order in orders if order.side != blocked_side]
        if len(filtered) == len(orders):
            return orders, None
        return filtered, f"cta_{cta_state.side}_lockout:{blocked_side}_suppressed"

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
        self._reset_ws_order_cycle(context.anchor_timestamp_ms)
        step_size = max(1e-12, (context.upper_bound - context.lower_bound) / max(1, self.config.levels))
        for order in events:
            status = str(order.get("status") or order.get("info", {}).get("state") or "").lower()
            filled = float(order.get("filled") or order.get("info", {}).get("fillSz") or 0.0)
            if status not in {"closed", "filled"} or filled <= 0:
                continue
            reduce_only = self._is_reduce_only_order(order)
            side = str(order.get("side") or "").lower()
            fill_price = float(order.get("average") or order.get("price") or order.get("info", {}).get("fillPx") or 0.0)
            if side not in {"buy", "sell"} or fill_price <= 0:
                continue
            if reduce_only:
                self._notify_ws_reduce_only_fill(order, side=side, filled=filled, fill_price=fill_price)
                continue

            hedge_key = self._ws_hedge_dedup_key(order)
            if hedge_key is not None and hedge_key in self._ws_orders_placed_in_cycle:
                logger.info("Grid websocket hedge skipped duplicate | key=%s", hedge_key)
                continue

            close_pos_side = "long" if side == "buy" else "short"
            if not self._has_open_position_for_pos_side(close_pos_side):
                logger.warning(
                    "Grid websocket hedge skipped missing position | fill_side=%s pos_side=%s filled=%.8f",
                    side,
                    close_pos_side,
                    filled,
                )
                continue

            counter_side = "sell" if side == "buy" else "buy"
            counter_price = self._calculate_fee_aware_close_price(entry_side=side, entry_price=fill_price, step_size=step_size, context=context)
            counter_amount = self._normalize_amount(filled)
            if counter_price <= 0 or counter_amount <= 0:
                continue
            try:
                response = self.client.place_limit_order(
                    self.symbol,
                    counter_side,
                    counter_amount,
                    counter_price,
                    reduce_only=True,
                    params={"posSide": close_pos_side},
                )
            except Exception:
                logger.exception(
                    "Grid websocket hedge placement failed | fill_side=%s fill_price=%.2f counter_side=%s counter_price=%.2f amount=%.8f",
                    side,
                    fill_price,
                    counter_side,
                    counter_price,
                    counter_amount,
                )
                continue

            if hedge_key is not None:
                self._ws_orders_placed_in_cycle.add(hedge_key)
            self._track_pending_reduce_only_profit(
                response,
                entry_side=side,
                exit_side=counter_side,
                entry_price=fill_price,
                exit_price=counter_price,
                amount=counter_amount,
                pos_side=close_pos_side,
            )
            logger.info(
                "Grid websocket hedge order | fill_side=%s fill_price=%.2f counter_side=%s counter_price=%.2f amount=%.8f",
                side,
                fill_price,
                counter_side,
                counter_price,
                counter_amount,
            )
            if self.notifier is not None and hasattr(self.notifier, "notify_trade"):
                self.notifier.notify_trade(
                    side=side,
                    price=fill_price,
                    size=counter_amount,
                    strategy=self.strategy_name,
                    signal="grid_fill_websocket",
                    symbol=self.symbol,
                )
            self._confirm_ws_hedge_order(response, counter_side=counter_side, counter_price=counter_price, amount=counter_amount)

    def _reset_ws_order_cycle(self, anchor_timestamp_ms: int | None) -> None:
        if anchor_timestamp_ms is None:
            return
        if self._ws_cycle_anchor_timestamp_ms == anchor_timestamp_ms:
            return
        self._ws_cycle_anchor_timestamp_ms = anchor_timestamp_ms
        self._ws_orders_placed_in_cycle.clear()

    def _ws_hedge_dedup_key(self, order: dict) -> str | None:
        order_id = order.get("id") or order.get("orderId") or order.get("info", {}).get("ordId")
        if order_id in (None, ""):
            return None
        return str(order_id)

    def _has_open_position_for_pos_side(self, pos_side: str) -> bool:
        if not hasattr(self.client, "fetch_positions"):
            return True
        try:
            positions = self.client.fetch_positions([self.symbol]) or []
        except Exception:
            logger.exception("Grid websocket hedge position check failed | pos_side=%s", pos_side)
            return False

        for position in positions:
            raw_size = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos") or 0.0
            size = abs(float(raw_size or 0.0))
            if size <= 0:
                continue
            side = str(position.get("side") or position.get("info", {}).get("posSide") or "").lower()
            if side not in {"long", "short"}:
                side = "long" if float(raw_size or 0.0) >= 0 else "short"
            if side == str(pos_side).lower():
                return True
        return False


    def _track_pending_reduce_only_profit(
        self,
        response: dict | None,
        *,
        entry_side: str,
        exit_side: str,
        entry_price: float,
        exit_price: float,
        amount: float,
        pos_side: str,
    ) -> None:
        order_keys = self._extract_order_keys(response)
        if not order_keys:
            return
        payload = {
            "entry_side": str(entry_side),
            "exit_side": str(exit_side),
            "entry_price": float(entry_price),
            "exit_price": float(exit_price),
            "amount": float(amount),
            "pos_side": str(pos_side),
        }
        for key in order_keys:
            self._pending_reduce_only_profits[key] = payload.copy()

    def _notify_ws_reduce_only_fill(self, order: dict, *, side: str, filled: float, fill_price: float) -> None:
        if self.notifier is None or not hasattr(self.notifier, "notify_profit"):
            return
        order_keys = self._extract_order_keys(order)
        if not order_keys:
            return
        tracked = None
        tracked_key = None
        for key in order_keys:
            candidate = self._pending_reduce_only_profits.get(key)
            if candidate is not None:
                tracked = candidate
                tracked_key = key
                break
        if tracked is None:
            return

        cumulative_filled = max(0.0, float(filled))
        previous_cumulative = max(self._reduce_only_filled_amounts.get(tracked_key or order_keys[0], 0.0), 0.0)
        delta_amount = min(float(tracked.get("amount") or 0.0), max(0.0, cumulative_filled - previous_cumulative))
        target_amount = float(tracked.get("amount") or 0.0)
        if cumulative_filled >= target_amount - 1e-12 and target_amount > 0:
            delta_amount = max(0.0, target_amount - previous_cumulative)
        key_for_progress = tracked_key or order_keys[0]
        self._reduce_only_filled_amounts[key_for_progress] = max(previous_cumulative, cumulative_filled)
        if delta_amount <= 0:
            return

        entry_price = float(tracked.get("entry_price") or 0.0)
        if entry_price <= 0 or fill_price <= 0:
            return
        contract_value = 1.0
        if hasattr(self.client, "get_contract_value"):
            try:
                contract_value = abs(float(self.client.get_contract_value(self.symbol))) or 1.0
            except Exception:
                contract_value = 1.0

        entry_side = str(tracked.get("entry_side") or "").lower()
        price_delta = fill_price - entry_price if entry_side == "buy" else entry_price - fill_price
        pnl = float(price_delta) * float(delta_amount) * contract_value
        entry_notional = abs(entry_price) * float(delta_amount) * contract_value
        roi = (pnl / entry_notional * 100.0) if entry_notional > 0 else 0.0
        balance = 0.0
        if hasattr(self.client, "fetch_total_equity"):
            try:
                balance = float(self.client.fetch_total_equity("USDT"))
            except Exception:
                balance = 0.0
        self.notifier.notify_profit(
            pnl=pnl,
            roi=roi,
            balance=balance,
            strategy=self.strategy_name,
            symbol=self.symbol,
            side=side,
            exit_price=fill_price,
            size=delta_amount,
        )
        if cumulative_filled >= target_amount - 1e-12 and target_amount > 0:
            for key in order_keys:
                self._pending_reduce_only_profits.pop(key, None)
                self._reduce_only_filled_amounts.pop(key, None)

    @staticmethod
    def _extract_order_keys(order: dict | None) -> list[str]:
        if not isinstance(order, dict):
            return []
        info = order.get("info") or {}
        keys: list[str] = []
        for value in (
            order.get("id"),
            order.get("orderId"),
            order.get("clientOrderId"),
            info.get("ordId"),
            info.get("algoId"),
            info.get("clOrdId"),
        ):
            if value in (None, ""):
                continue
            normalized = str(value)
            if normalized not in keys:
                keys.append(normalized)
        return keys

    def _confirm_ws_hedge_order(self, response: dict | None, *, counter_side: str, counter_price: float, amount: float) -> None:
        if not hasattr(self.client, "fetch_order"):
            return
        order_id = response.get("id") if isinstance(response, dict) else None
        if order_id in (None, ""):
            return
        try:
            time.sleep(0.1)
            hedge_order = self.client.fetch_order(str(order_id), self.symbol)
        except Exception:
            logger.exception("Grid websocket hedge confirmation failed | order_id=%s", order_id)
            return
        if hedge_order is None:
            logger.warning("Grid websocket hedge_order_rejected | order_id=%s side=%s price=%.2f amount=%.8f status=missing", order_id, counter_side, counter_price, amount)
            return
        status = str(hedge_order.get("status") or hedge_order.get("info", {}).get("state") or "").lower()
        if status not in {"open", "closed", "filled", "partially_filled", "partiallyfilled", "live"}:
            logger.warning(
                "Grid websocket hedge_order_rejected | order_id=%s side=%s price=%.2f amount=%.8f status=%s",
                order_id,
                counter_side,
                counter_price,
                amount,
                status or "unknown",
            )

    def _derive_layer_prices(
        self,
        lower_bound: float,
        anchor_price: float,
        upper_bound: float,
        *,
        bias_profile: GridBiasProfile | None = None,
        atr_value: float = 0.0,
    ) -> tuple[list[float], list[float]]:
        bias_profile = bias_profile or GridBiasProfile()
        if bias_profile.bullish and bias_profile.buy_levels > 0 and bias_profile.sell_levels > 0:
            buy_levels = bias_profile.buy_levels
            sell_levels = bias_profile.sell_levels
        elif bias_profile.bearish and bias_profile.buy_levels > 0 and bias_profile.sell_levels > 0:
            buy_levels = bias_profile.buy_levels
            sell_levels = bias_profile.sell_levels
        else:
            buy_levels = max(1, self.config.levels // 2)
            sell_levels = max(1, self.config.levels - buy_levels)

        min_spacing_ratio = max(0.0, float(getattr(self.config, "min_spacing_ratio", 0.0)))
        atr_spacing_floor_multiplier = max(0.0, float(getattr(self.config, "atr_spacing_floor_multiplier", 0.0)))
        atr_spacing_floor = max(0.0, float(atr_value)) * atr_spacing_floor_multiplier

        buy_step = max(1e-12, (anchor_price - lower_bound) / buy_levels)
        sell_step = max(1e-12, (upper_bound - anchor_price) / sell_levels)
        if bias_profile.bullish:
            buy_spacing_ratio = bias_profile.buy_spacing_ratio
            sell_spacing_ratio = bias_profile.sell_spacing_ratio
        elif bias_profile.bearish:
            buy_spacing_ratio = bias_profile.buy_spacing_ratio
            sell_spacing_ratio = bias_profile.sell_spacing_ratio
        else:
            buy_spacing_ratio = 0.0
            sell_spacing_ratio = 0.0
        effective_buy_spacing_ratio = buy_spacing_ratio if buy_spacing_ratio > 0 else min_spacing_ratio
        effective_sell_spacing_ratio = sell_spacing_ratio if sell_spacing_ratio > 0 else min_spacing_ratio
        minimum_buy_step = max(anchor_price * effective_buy_spacing_ratio, atr_spacing_floor, 1e-12)
        minimum_sell_step = max(anchor_price * effective_sell_spacing_ratio, atr_spacing_floor, 1e-12)

        buy_step = max(buy_step, minimum_buy_step)
        sell_step = max(sell_step, minimum_sell_step)

        buy_prices = [anchor_price - buy_step * (index + 1) for index in range(buy_levels)]
        sell_prices = [anchor_price + sell_step * (index + 1) for index in range(sell_levels)]
        return buy_prices, sell_prices

    def _build_opening_orders(self, context: GridContext, current_price: float, now: datetime) -> list[GridOrderPlan]:
        del current_price, now
        orders: list[GridOrderPlan] = []
        per_level_amount = self._calculate_grid_order_amount(context.center_price)
        per_level_notional = self._estimate_notional(per_level_amount, context.center_price)

        for index, price in enumerate(context.buy_prices, start=1):
            expected_close_price = self._calculate_fee_aware_close_price(entry_side="buy", entry_price=price, step_size=abs(context.center_price - price), context=context)
            expected_net_profit = self._estimate_grid_level_net_profit(
                entry_side="buy",
                entry_price=price,
                close_price=expected_close_price,
                amount=per_level_amount,
            )
            logger.info(
                "Grid level plan | symbol=%s side=buy level=%d entry=%.2f expected_notional=%.2fUSDT expected_net_profit=%.2fUSDT",
                self.symbol,
                index,
                price,
                per_level_notional,
                expected_net_profit,
            )
            orders.append(
                GridOrderPlan(
                    layer_key=self._layer_key(context.anchor_timestamp_ms, "buy", index),
                    index=index,
                    side="buy",
                    price=price,
                    amount=self._normalize_amount(per_level_amount),
                )
            )

        for index, price in enumerate(context.sell_prices, start=1):
            expected_close_price = self._calculate_fee_aware_close_price(entry_side="sell", entry_price=price, step_size=abs(price - context.center_price), context=context)
            expected_net_profit = self._estimate_grid_level_net_profit(
                entry_side="sell",
                entry_price=price,
                close_price=expected_close_price,
                amount=per_level_amount,
            )
            logger.info(
                "Grid level plan | symbol=%s side=sell level=%d entry=%.2f expected_notional=%.2fUSDT expected_net_profit=%.2fUSDT",
                self.symbol,
                index,
                price,
                per_level_notional,
                expected_net_profit,
            )
            orders.append(
                GridOrderPlan(
                    layer_key=self._layer_key(context.anchor_timestamp_ms, "sell", index),
                    index=index,
                    side="sell",
                    price=price,
                    amount=self._normalize_amount(per_level_amount),
                )
            )

        return orders


    def _calculate_fee_aware_close_price(self, *, entry_side: str, entry_price: float, step_size: float, context: GridContext | None = None) -> float:
        fee_rate = max(0.0, float(getattr(self.config, "fee_rate", 0.001)))
        minimum_move = entry_price * ((1 + fee_rate) / max(1e-12, 1 - fee_rate) - 1)
        target_move = max(float(step_size), minimum_move) + max(1e-8, entry_price * 1e-6)
        if str(entry_side).lower() == "buy":
            raw_close_price = entry_price + target_move
            if context is not None:
                raw_close_price = min(raw_close_price, context.upper_bound)
        else:
            raw_close_price = entry_price - target_move
            if context is not None:
                raw_close_price = max(raw_close_price, context.lower_bound)
        return float(self.client.price_to_precision(self.symbol, raw_close_price))

    def _estimate_grid_level_net_profit(self, *, entry_side: str, entry_price: float, close_price: float, amount: float) -> float:
        amount = max(0.0, float(amount))
        entry_price = abs(float(entry_price))
        close_price = abs(float(close_price))
        if amount <= 0 or entry_price <= 0 or close_price <= 0:
            return 0.0
        fee_rate = max(0.0, float(getattr(self.config, "fee_rate", 0.001)))
        entry_notional = self._estimate_notional(amount, entry_price)
        close_notional = self._estimate_notional(amount, close_price)
        base_exposure = entry_notional / max(entry_price, 1e-12)
        gross_profit = (
            (close_price - entry_price) * base_exposure
            if str(entry_side).lower() == "buy"
            else (entry_price - close_price) * base_exposure
        )
        fees = (entry_notional + close_notional) * fee_rate
        return gross_profit - fees

    def _calculate_grid_order_amount(self, reference_price: float) -> float:
        if reference_price <= 0:
            return 0.0
        if not hasattr(self.client, "fetch_total_equity"):
            fallback_amount = float(self.execution_config.grid_order_size)
            logger.info(
                "Grid sizing fallback | symbol=%s reason=no_equity_api amount=%.8f price=%.2f",
                self.symbol,
                fallback_amount,
                reference_price,
            )
            return fallback_amount
        try:
            equity = float(self.client.fetch_total_equity("USDT"))
        except Exception:
            logger.exception("Grid sizing failed to fetch account equity; falling back to configured order size")
            fallback_amount = float(self.execution_config.grid_order_size)
            logger.info(
                "Grid sizing fallback | symbol=%s reason=equity_fetch_failed amount=%.8f price=%.2f",
                self.symbol,
                fallback_amount,
                reference_price,
            )
            return fallback_amount

        allocation_ratio = max(0.0, float(self.config.equity_allocation_ratio))
        total_grid_notional = equity * allocation_ratio
        per_level_notional = total_grid_notional / max(1, int(self.config.levels))
        unit_notional = self._estimate_notional(1.0, reference_price)
        if per_level_notional <= 0 or unit_notional <= 0:
            return 0.0
        amount = per_level_notional / unit_notional
        logger.info(
            "Grid sizing | symbol=%s equity=%.4f allocation_ratio=%.4f levels=%d total_grid_notional=%.4f per_level_notional=%.4f ref_price=%.2f raw_amount=%.8f",
            self.symbol,
            equity,
            allocation_ratio,
            int(self.config.levels),
            total_grid_notional,
            per_level_notional,
            reference_price,
            amount,
        )
        return amount

    def _cancel_pending_grid_orders(self, orders: list[dict]) -> None:
        cancel_order = getattr(self.client, "cancel_order", None)
        if not callable(cancel_order):
            self.client.cancel_all_orders(self.symbol)
            return
        for order in orders:
            order_id = order.get("id") or order.get("info", {}).get("ordId")
            if order_id in (None, ""):
                continue
            cancel_order(str(order_id), self.symbol)

    def _place_grid_batch_safely(self, orders: list[GridOrderPlan]) -> dict[str, int | str]:
        self._placed_order_ids = []
        if not orders:
            return {"status": "ok", "placed_orders": 0}

        batch_notional = 0.0
        for order in orders:
            amount = self._normalize_amount(order.amount)
            if amount <= 0:
                continue
            requested_notional = batch_notional + self._estimate_notional(amount, order.price)
            if self.risk_manager is not None and not order.reduce_only:
                opening_side = "long" if order.side == "buy" else "short"
                leverage_allowed, reason = self.risk_manager.check_directional_exposure_limit(
                    requested_notional,
                    opening_side,
                )
                if not leverage_allowed:
                    return {"status": "failed", "placed_orders": 0, "reason": str(reason or "directional_limit")}
                limit_allowed, reason = self.risk_manager.check_symbol_notional_limit(self.symbol, requested_notional)
                if not limit_allowed:
                    return {"status": "failed", "placed_orders": 0, "reason": str(reason or "symbol_limit")}
            batch_notional = requested_notional

        placed_orders = 0
        try:
            for order in orders:
                placed_orders += self._try_place_limit_order(order, reserved_notional=0.0, track_batch=True)
        except Exception as exc:
            self._rollback_grid_batch_orders()
            return {"status": "failed", "placed_orders": 0, "reason": exc.__class__.__name__}

        return {"status": "ok", "placed_orders": placed_orders}

    def _rollback_grid_batch_orders(self) -> None:
        for order_id in list(reversed(self._placed_order_ids)):
            try:
                self.client.cancel_order(order_id, self.symbol)
            except Exception:
                logger.exception("[grid_robot] failed to rollback grid batch order | symbol=%s order_id=%s", self.symbol, order_id)
        self._placed_order_ids = []

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

    def _try_place_limit_order(self, order: GridOrderPlan, *, reserved_notional: float, track_batch: bool = False) -> int:
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

        response = self.client.place_limit_order(
            self.symbol,
            order.side,
            amount,
            order.price,
            reduce_only=order.reduce_only,
        )
        if track_batch:
            order_id = response.get("id") if isinstance(response, dict) else None
            if order_id not in (None, ""):
                self._placed_order_ids.append(str(order_id))
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
        if context is None:
            if self.runtime_context is not None:
                self.runtime_context.publish_grid_inventory(
                    symbol=self.symbol,
                    net_position_size=0.0,
                    inventory_bias_side=None,
                    inventory_bias_ratio=0.0,
                    heavy_inventory=False,
                    hedge_assist_requested=False,
                    hedge_assist_reason=None,
                    hedge_assist_target_side=None,
                )
            if self.risk_manager is not None:
                self.risk_manager.update_grid_risk(None)
            return

        current_price = float(self.client.fetch_last_price(self.symbol))
        candidates = self._load_position_candidates(current_price)
        long_notional = sum(candidate.notional for candidate in candidates if candidate.side == "long")
        short_notional = sum(candidate.notional for candidate in candidates if candidate.side == "short")
        heavier_notional = max(long_notional, short_notional)
        lighter_notional = min(long_notional, short_notional)
        inventory_bias_side = None
        if heavier_notional > 1e-12:
            inventory_bias_side = "long" if long_notional >= short_notional else "short"
        inventory_bias_ratio = 0.0 if heavier_notional <= 1e-12 else (heavier_notional - lighter_notional) / heavier_notional
        heavy_inventory_threshold = float(getattr(self.config, "heavy_inventory_threshold", 0.60))
        heavy_inventory = heavier_notional > 1e-12 and inventory_bias_ratio >= heavy_inventory_threshold
        hedge_assist_requested = False
        hedge_assist_reason = None
        if heavy_inventory and inventory_bias_side is not None:
            hedge_assist_requested = True
            hedge_assist_reason = f"grid_inventory_heavy:{inventory_bias_side}"
        elif self._active_hedge_assist_allowed(inventory_bias_side, inventory_bias_ratio):
            hedge_assist_requested = True
            hedge_assist_reason = f"grid_active_hedge:{inventory_bias_side}"
        net_position_size = self._fetch_net_position_size()
        if self.runtime_context is not None:
            self.runtime_context.publish_grid_inventory(
                symbol=self.symbol,
                net_position_size=net_position_size,
                inventory_bias_side=inventory_bias_side,
                inventory_bias_ratio=inventory_bias_ratio,
                heavy_inventory=heavy_inventory,
                hedge_assist_requested=hedge_assist_requested,
                hedge_assist_reason=hedge_assist_reason,
                hedge_assist_target_side=inventory_bias_side,
            )
        if self.risk_manager is None:
            return

        self.risk_manager.update_grid_risk(
            GridRiskProfile(
                symbol=self.symbol,
                lower_bound=context.lower_bound,
                upper_bound=context.upper_bound,
            )
        )

    def _active_hedge_assist_allowed(self, inventory_bias_side: str | None, inventory_bias_ratio: float) -> bool:
        if inventory_bias_side is None:
            return False
        if not bool(getattr(self.config, "active_hedge_mode_enabled", False)):
            return False
        min_ratio = float(getattr(self.config, "active_hedge_min_inventory_ratio", 0.45))
        if inventory_bias_ratio < min_ratio:
            return False
        if self.runtime_context is None:
            return True
        if not bool(getattr(self.config, "active_hedge_requires_cta_position", True)):
            return True
        cta_state = self.runtime_context.snapshot_cta()
        if cta_state.symbol not in {"", self.symbol}:
            return False
        return cta_state.side == inventory_bias_side and float(cta_state.size) > 0.0

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
