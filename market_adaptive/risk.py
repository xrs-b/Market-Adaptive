from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from market_adaptive.config import RiskControlConfig, RuntimeConfig
from market_adaptive.db import DatabaseInitializer, SystemStateRecord

logger = logging.getLogger(__name__)


@dataclass
class LogicalPositionSnapshot:
    symbol: str
    side: str
    size: float
    strategy_name: str = "unknown"


@dataclass
class ExchangePositionSnapshot:
    symbol: str
    side: str
    size: float
    notional: float

    @property
    def is_flat(self) -> bool:
        return abs(self.size) <= 1e-12


@dataclass
class AccountRiskSnapshot:
    equity: float
    daily_start_equity: float
    daily_drawdown: float
    total_unrealized_pnl: float
    margin_ratio: float
    maintenance_margin: float
    position_notional: float
    open_order_notional: float
    total_notional: float
    new_openings_blocked: bool = False
    block_reason: str | None = None


@dataclass
class StrategyExposureSnapshot:
    equity: float
    gross_long_notional: float
    gross_short_notional: float
    net_long_notional: float
    net_short_notional: float


@dataclass
class CTARiskProfile:
    symbol: str
    side: str
    stop_price: float
    remaining_size: float
    atr_value: float
    stop_distance: float


@dataclass
class GridRiskProfile:
    symbol: str
    lower_bound: float
    upper_bound: float


@dataclass
class GridLiveRiskMetrics:
    symbol: str
    current_price: float
    lower_bound: float
    upper_bound: float
    total_deviation_ratio: float
    net_position_size: float
    nearest_liquidation_price: float | None = None
    nearest_liquidation_distance_ratio: float | None = None

    @property
    def below_lower_bound(self) -> bool:
        return self.current_price < self.lower_bound - 1e-12

    @property
    def above_upper_bound(self) -> bool:
        return self.current_price > self.upper_bound + 1e-12

    @property
    def has_exposure(self) -> bool:
        return abs(self.net_position_size) > 1e-12


@dataclass
class GridObserveState:
    symbol: str
    lower_bound: float
    upper_bound: float
    reason: str
    entered_at: datetime

    def contains(self, price: float) -> bool:
        return self.lower_bound - 1e-12 <= price <= self.upper_bound + 1e-12


@dataclass
class RiskControlManager:
    config: RiskControlConfig
    runtime_config: RuntimeConfig
    database: DatabaseInitializer
    client: Any
    shutdown_client: Any
    symbols: list[str]
    notifier: Any | None = None
    stop_callback: Callable[[], None] | None = None
    reduce_grid_exposure_callback: Callable[[str, float], None] | None = None
    flatten_cta_position_callback: Callable[[str], None] | None = None
    logical_position_provider: Callable[[], dict[str, LogicalPositionSnapshot | None]] | None = None
    local_position_reset_callback: Callable[[str, str], None] | None = None
    grid_cleanup_callback: Callable[[str], str] | None = None
    daily_start_equity: float | None = None
    daily_start_date: str | None = None
    latest_total_pnl: float = 0.0
    new_openings_blocked: bool = False
    block_reason: str | None = None
    circuit_breaker_triggered: bool = False
    latest_cta_risk: CTARiskProfile | None = None
    latest_grid_risk: GridRiskProfile | None = None
    _grid_last_reduction_at: datetime | None = field(default=None, init=False, repr=False)
    _grid_observe_state: GridObserveState | None = field(default=None, init=False, repr=False)
    _cta_exit_applied: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.symbols = sorted(set(self.symbols))
        self.timezone = ZoneInfo(self.runtime_config.timezone)

    def initialize(self) -> None:
        current_equity = self.client.fetch_total_equity()
        self._sync_daily_baseline(current_equity=current_equity)
        self._persist_system_status("ON")
        self._persist_opening_block(False, None)

    def update_cta_risk(self, profile: CTARiskProfile | None) -> None:
        self.latest_cta_risk = profile
        if profile is None:
            self._cta_exit_applied = False

    def update_grid_risk(self, profile: GridRiskProfile | None) -> None:
        self.latest_grid_risk = profile
        if profile is None:
            self._clear_grid_observe_mode("grid_profile_cleared")

    def monitor_once(self) -> AccountRiskSnapshot:
        current_equity = self.client.fetch_total_equity()
        self.latest_total_pnl = self.client.fetch_total_unrealized_pnl(self.symbols)
        self._sync_daily_baseline(current_equity=current_equity)

        risk_payload = self.client.fetch_account_risk_snapshot(self.symbols)
        margin_ratio = max(0.0, float(risk_payload.get("margin_ratio", 0.0)))
        maintenance_margin = max(0.0, float(risk_payload.get("maintenance_margin", 0.0)))
        position_notional = max(0.0, float(risk_payload.get("position_notional", 0.0)))
        open_order_notional = max(0.0, float(risk_payload.get("open_order_notional", 0.0)))
        total_notional = max(0.0, float(risk_payload.get("total_notional", position_notional + open_order_notional)))

        assert self.daily_start_equity is not None
        daily_drawdown = 0.0
        if self.daily_start_equity > 0:
            daily_drawdown = max(0.0, (self.daily_start_equity - current_equity) / self.daily_start_equity)

        margin_blocked = margin_ratio >= self.config.max_margin_ratio
        block_reason = None
        if self.circuit_breaker_triggered:
            margin_blocked = True
            block_reason = "circuit_breaker"
        elif margin_blocked:
            block_reason = f"margin_ratio={margin_ratio:.2%}"

        self._persist_opening_block(margin_blocked, block_reason)

        snapshot = AccountRiskSnapshot(
            equity=current_equity,
            daily_start_equity=self.daily_start_equity,
            daily_drawdown=daily_drawdown,
            total_unrealized_pnl=self.latest_total_pnl,
            margin_ratio=margin_ratio,
            maintenance_margin=maintenance_margin,
            position_notional=position_notional,
            open_order_notional=open_order_notional,
            total_notional=total_notional,
            new_openings_blocked=margin_blocked,
            block_reason=block_reason,
        )

        self._apply_cta_risk_controls()
        self._apply_grid_risk_controls(snapshot)

        logger.info(
            "Risk heartbeat | equity=%.4f daily_start=%.4f drawdown=%.2f%% unrealized_pnl=%.4f margin_ratio=%.2f%% position_notional=%.4f open_order_notional=%.4f total_notional=%.4f blocked=%s",
            snapshot.equity,
            snapshot.daily_start_equity,
            snapshot.daily_drawdown * 100,
            snapshot.total_unrealized_pnl,
            snapshot.margin_ratio * 100,
            snapshot.position_notional,
            snapshot.open_order_notional,
            snapshot.total_notional,
            snapshot.new_openings_blocked,
        )

        if daily_drawdown >= self.config.daily_loss_cutoff_pct:
            self.trigger_circuit_breaker(snapshot)

        return snapshot

    def monitor_cta_fast_once(self) -> str:
        profile = self.latest_cta_risk
        if profile is None:
            self._cta_exit_applied = False
            return "cta_fast_guard:idle"

        triggered, reason = self._evaluate_cta_stop(profile)
        if not triggered:
            self._cta_exit_applied = False
            return "cta_fast_guard:armed"
        if self._cta_exit_applied or self.flatten_cta_position_callback is None:
            return "cta_fast_guard:exit_pending"

        self.flatten_cta_position_callback(reason)
        self._cta_exit_applied = True
        return "cta_fast_guard:stop_hit"

    def calculate_position_size(
        self,
        symbol: str,
        risk_percent: float,
        stop_loss_atr: float,
        *,
        atr_value: float | None = None,
        last_price: float | None = None,
    ) -> float:
        if self.circuit_breaker_triggered or risk_percent <= 0 or stop_loss_atr <= 0:
            return 0.0

        if atr_value is None or atr_value <= 0:
            raise ValueError("atr_value must be provided and > 0 for ATR-based sizing")

        price = float(last_price) if last_price is not None else float(self.client.fetch_last_price(symbol))
        account_equity = float(self.client.fetch_total_equity())
        risk_amount = max(0.0, account_equity * float(risk_percent))
        stop_distance = max(0.0, float(atr_value) * float(stop_loss_atr))
        if risk_amount <= 0 or stop_distance <= 0:
            return 0.0

        contract_value = self.client.get_contract_value(symbol)
        per_unit_risk = stop_distance * contract_value
        if per_unit_risk <= 0:
            return 0.0

        amount = risk_amount / per_unit_risk
        amount = self.client.amount_to_precision(symbol, amount)
        amount = self._cap_amount_by_equity_multiple(symbol, amount, price, account_equity)
        amount = self._cap_amount_by_symbol_limit(symbol, amount, price)
        minimum_amount = self.client.get_min_order_amount(symbol)
        if amount < minimum_amount:
            return 0.0
        return self.client.amount_to_precision(symbol, amount)

    def can_open_new_position(
        self,
        symbol: str,
        requested_notional: float,
        strategy_name: str | None = None,
        opening_side: str | None = None,
    ) -> tuple[bool, str | None]:
        if self.circuit_breaker_triggered:
            return False, "circuit_breaker"

        snapshot = self.monitor_once()
        if snapshot.new_openings_blocked:
            return False, snapshot.block_reason

        strategy_block_reason = self._resolve_strategy_opening_block(snapshot, strategy_name)
        if strategy_block_reason is not None:
            return False, strategy_block_reason

        leverage_allowed, leverage_reason = self.check_directional_exposure_limit(requested_notional, opening_side)
        if not leverage_allowed:
            return False, leverage_reason

        return self.check_symbol_notional_limit(symbol, requested_notional)

    def check_symbol_notional_limit(self, symbol: str, requested_notional: float) -> tuple[bool, str | None]:
        symbol_limit = self.config.resolve_symbol_notional_limit(symbol)
        if symbol_limit <= 0:
            return True, None

        current_position_notional = self.client.fetch_symbol_position_notional(symbol)
        current_order_notional = self.client.fetch_symbol_open_order_notional(symbol)
        total_if_opened = current_position_notional + current_order_notional + max(0.0, float(requested_notional))
        if total_if_opened > symbol_limit + 1e-9:
            return False, f"symbol_limit={symbol_limit:.4f}"
        return True, None

    def check_directional_exposure_limit(
        self,
        requested_notional: float,
        opening_side: str | None,
    ) -> tuple[bool, str | None]:
        normalized_side = self._normalize_opening_side(opening_side)
        max_directional_leverage = float(self.config.max_directional_leverage)
        if normalized_side is None or requested_notional <= 0 or max_directional_leverage <= 0:
            return True, None

        account_equity = float(self.client.fetch_total_equity())
        if account_equity <= 0:
            return False, "equity_unavailable"

        exposure = self._build_strategy_exposure_snapshot(account_equity)
        if normalized_side == "long":
            projected_net_notional = exposure.net_long_notional + max(0.0, float(requested_notional))
        else:
            projected_net_notional = exposure.net_short_notional + max(0.0, float(requested_notional))

        projected_leverage = projected_net_notional / account_equity
        if projected_leverage > max_directional_leverage + 1e-9:
            return False, f"net_{normalized_side}_leverage={projected_leverage:.2f}x"
        return True, None

    def recover_positions_once(self) -> str:
        if self.logical_position_provider is None:
            return "recovery:disabled"

        logical_positions = self.logical_position_provider() or {}
        actions: list[str] = []
        for symbol in self.symbols:
            local = logical_positions.get(symbol)
            actual = self._fetch_exchange_position(symbol)
            if self._positions_match(local, actual):
                continue

            if local is None and actual.is_flat:
                continue

            if local is None and not actual.is_flat:
                if self._grid_position_protected(symbol, actual):
                    actions.append(f"{symbol}:protected_grid_position")
                    continue
                self.shutdown_client.cancel_all_orders(symbol)
                self.shutdown_client.close_all_positions(symbol)
                actions.append(f"{symbol}:closed_rogue_position")
                continue

            if local is not None and actual.is_flat:
                if self.local_position_reset_callback is not None:
                    self.local_position_reset_callback(symbol, "exchange_flat")
                actions.append(f"{symbol}:reset_local_state")
                continue

            self.shutdown_client.cancel_all_orders(symbol)
            self.shutdown_client.close_all_positions(symbol)
            if self.local_position_reset_callback is not None:
                self.local_position_reset_callback(symbol, "position_mismatch")
            actions.append(f"{symbol}:flattened_mismatch")

        return "recovery:ok" if not actions else "recovery:" + "+".join(actions)

    def coordinate_strategy_cleanup(self, strategy_name: str, reason: str) -> str | None:
        if strategy_name != "grid" or self.grid_cleanup_callback is None:
            return None
        if not str(reason).startswith("status_switch"):
            return None

        result = self.grid_cleanup_callback(reason)
        self.update_grid_risk(None)
        return result

    def trigger_circuit_breaker(self, snapshot: AccountRiskSnapshot | None = None) -> None:
        if self.circuit_breaker_triggered:
            return

        if snapshot is None:
            snapshot = self.monitor_once()

        self.circuit_breaker_triggered = True
        self._persist_opening_block(True, "circuit_breaker")
        self._persist_system_status("OFF")

        self.shutdown_client.cancel_all_orders_for_symbols(self.symbols)
        self.shutdown_client.close_all_positions_for_symbols(self.symbols)

        if self.notifier is not None:
            self.notifier.send(
                "硬风控已触发",
                (
                    "账户已触发熔断保护。\n\n"
                    "触发原因：日内回撤超限\n"
                    f"当前回撤：{snapshot.daily_drawdown * 100:.2f}%\n"
                    f"阈值：{self.config.daily_loss_cutoff_pct * 100:.2f}%\n"
                    f"当前权益：{snapshot.equity:.4f} USDT\n"
                    f"日初权益：{snapshot.daily_start_equity:.4f} USDT\n"
                    f"保证金比率：{snapshot.margin_ratio * 100:.2f}%\n\n"
                    "已执行动作：\n"
                    "- 停止新开仓\n"
                    "- 撤销所有挂单\n"
                    "- 执行清仓\n"
                    "- 系统状态切换为 OFF"
                ),
            )

        logger.error(
            "Circuit breaker triggered | drawdown=%.2f%% cutoff=%.2f%% equity=%.4f daily_start=%.4f",
            snapshot.daily_drawdown * 100,
            self.config.daily_loss_cutoff_pct * 100,
            snapshot.equity,
            snapshot.daily_start_equity,
        )
        if self.stop_callback is not None:
            self.stop_callback()

    def _apply_cta_risk_controls(self) -> None:
        profile = self.latest_cta_risk
        if profile is None:
            self._cta_exit_applied = False
            return

        triggered, reason = self._evaluate_cta_stop(profile)
        if not triggered:
            self._cta_exit_applied = False
            return
        if self._cta_exit_applied or self.flatten_cta_position_callback is None:
            return

        self.flatten_cta_position_callback(reason)
        self._cta_exit_applied = True

    def _evaluate_cta_stop(self, profile: CTARiskProfile) -> tuple[bool, str]:
        current_price = float(self.client.fetch_last_price(profile.symbol))
        stop_hit = current_price <= profile.stop_price if profile.side == "long" else current_price >= profile.stop_price
        reason = (
            "cta_atr_stop_hit"
            f"|price={current_price:.2f}"
            f"|stop={profile.stop_price:.2f}"
            f"|distance={profile.stop_distance:.2f}"
            f"|atr={profile.atr_value:.4f}"
        )
        return stop_hit, reason

    def _apply_grid_risk_controls(self, snapshot: AccountRiskSnapshot) -> None:
        metrics = self._build_live_grid_metrics()
        if metrics is None or not metrics.has_exposure:
            return

        reduction_reason = self._resolve_grid_reduction_reason(snapshot, metrics)
        if reduction_reason is None:
            return
        if self.reduce_grid_exposure_callback is None:
            return
        if not self._grid_reduction_due():
            return

        step_pct = min(1.0, max(0.01, float(self.config.grid_reduction_step_pct)))
        self.reduce_grid_exposure_callback(reduction_reason, step_pct)
        self._grid_last_reduction_at = datetime.now(timezone.utc)

    def _resolve_strategy_opening_block(
        self,
        snapshot: AccountRiskSnapshot,
        strategy_name: str | None,
    ) -> str | None:
        if strategy_name != "grid":
            return None

        metrics = self._build_live_grid_metrics()
        if metrics is None:
            self._clear_grid_observe_mode("grid_metrics_unavailable")
            return None

        observe_reason = self._resolve_grid_observe_block(metrics)
        if observe_reason is not None:
            return observe_reason
        if snapshot.margin_ratio >= self.config.grid_margin_ratio_warning:
            return f"grid_margin_warning={snapshot.margin_ratio:.2%}"
        return None

    def _resolve_grid_observe_block(self, metrics: GridLiveRiskMetrics) -> str | None:
        breach_reason: str | None = None
        if metrics.below_lower_bound:
            breach_reason = (
                "grid_observe_lower_break"
                f"|price={metrics.current_price:.2f}"
                f"|lower={metrics.lower_bound:.2f}"
                f"|deviation={metrics.total_deviation_ratio:.2%}"
            )
        elif metrics.above_upper_bound:
            breach_reason = (
                "grid_observe_upper_break"
                f"|price={metrics.current_price:.2f}"
                f"|upper={metrics.upper_bound:.2f}"
                f"|deviation={metrics.total_deviation_ratio:.2%}"
            )

        if breach_reason is not None:
            if self._grid_observe_state is None:
                self._grid_observe_state = GridObserveState(
                    symbol=metrics.symbol,
                    lower_bound=metrics.lower_bound,
                    upper_bound=metrics.upper_bound,
                    reason=breach_reason,
                    entered_at=datetime.now(timezone.utc),
                )
            return self._grid_observe_state.reason

        if self._grid_observe_state is None:
            return None
        if self._grid_observe_state.symbol != metrics.symbol:
            self._clear_grid_observe_mode("grid_symbol_changed")
            return None
        if self._grid_observe_state.contains(metrics.current_price):
            self._clear_grid_observe_mode("grid_price_reentered_band")
            return None

        return (
            self._grid_observe_state.reason
            + f"|observe=waiting_return|band={self._grid_observe_state.lower_bound:.2f}-{self._grid_observe_state.upper_bound:.2f}"
        )

    def _resolve_grid_reduction_reason(
        self,
        snapshot: AccountRiskSnapshot,
        metrics: GridLiveRiskMetrics,
    ) -> str | None:
        if metrics.nearest_liquidation_distance_ratio is not None and (
            metrics.nearest_liquidation_distance_ratio <= self.config.grid_liquidation_warning_ratio
        ):
            assert metrics.nearest_liquidation_price is not None
            return (
                "grid_liquidation_warning"
                f"|price={metrics.current_price:.2f}"
                f"|liq={metrics.nearest_liquidation_price:.2f}"
                f"|distance={metrics.nearest_liquidation_distance_ratio:.2%}"
            )
        if snapshot.margin_ratio >= self.config.max_margin_ratio:
            return f"grid_margin_ratio_critical={snapshot.margin_ratio:.2%}"
        if metrics.total_deviation_ratio >= self.config.grid_deviation_reduce_ratio:
            return (
                "grid_deviation_critical"
                f"|price={metrics.current_price:.2f}"
                f"|bounds={metrics.lower_bound:.2f}-{metrics.upper_bound:.2f}"
                f"|deviation={metrics.total_deviation_ratio:.2%}"
            )
        return None

    def _grid_reduction_due(self) -> bool:
        if self._grid_last_reduction_at is None:
            return True
        elapsed = (datetime.now(timezone.utc) - self._grid_last_reduction_at).total_seconds()
        return elapsed >= max(1, int(self.config.grid_reduction_cooldown_seconds))

    def _build_live_grid_metrics(self) -> GridLiveRiskMetrics | None:
        profile = self.latest_grid_risk
        if profile is None:
            return None

        current_price = float(self.client.fetch_last_price(profile.symbol))
        positions = self.client.fetch_positions([profile.symbol]) or []
        net_position_size = 0.0
        nearest_liquidation_price = None
        nearest_liquidation_distance_ratio = None

        for position in positions:
            size = self._extract_position_size(position)
            if size <= 0:
                continue
            side = self._extract_position_side(position)
            if side == "short":
                net_position_size -= size
            else:
                net_position_size += size

            liquidation_price = self._extract_liquidation_price(position)
            if liquidation_price is None or liquidation_price <= 0:
                continue
            distance_ratio = abs(current_price - liquidation_price) / liquidation_price
            if nearest_liquidation_distance_ratio is None or distance_ratio < nearest_liquidation_distance_ratio:
                nearest_liquidation_distance_ratio = distance_ratio
                nearest_liquidation_price = liquidation_price

        band_width = max(profile.upper_bound - profile.lower_bound, 1e-12)
        below = max(0.0, profile.lower_bound - current_price)
        above = max(0.0, current_price - profile.upper_bound)
        total_deviation_ratio = (below + above) / band_width

        return GridLiveRiskMetrics(
            symbol=profile.symbol,
            current_price=current_price,
            lower_bound=profile.lower_bound,
            upper_bound=profile.upper_bound,
            total_deviation_ratio=total_deviation_ratio,
            net_position_size=net_position_size,
            nearest_liquidation_price=nearest_liquidation_price,
            nearest_liquidation_distance_ratio=nearest_liquidation_distance_ratio,
        )

    def _build_strategy_exposure_snapshot(self, account_equity: float) -> StrategyExposureSnapshot:
        gross_long_notional = 0.0
        gross_short_notional = 0.0

        for position in self.client.fetch_positions(self.symbols) or []:
            size = self._extract_position_size(position)
            if size <= 0:
                continue
            symbol = str(position.get("symbol") or position.get("info", {}).get("instId") or self.symbols[0])
            side = self._extract_position_side(position)
            notional = self.client.position_notional(symbol, position)
            if side == "short":
                gross_short_notional += notional
            else:
                gross_long_notional += notional

        net_long_notional = max(0.0, gross_long_notional - gross_short_notional)
        net_short_notional = max(0.0, gross_short_notional - gross_long_notional)
        return StrategyExposureSnapshot(
            equity=account_equity,
            gross_long_notional=gross_long_notional,
            gross_short_notional=gross_short_notional,
            net_long_notional=net_long_notional,
            net_short_notional=net_short_notional,
        )

    def _sync_daily_baseline(self, *, current_equity: float, now: datetime | None = None) -> None:
        if now is None:
            now = datetime.now(timezone.utc)
        local_now = now.astimezone(self.timezone)
        current_date = local_now.date().isoformat()

        stored_date = self.database.get_system_state("risk_daily_start_date")
        stored_equity = self.database.get_system_state("risk_daily_start_equity")
        if stored_date is not None and stored_equity is not None and stored_date.state_value == current_date:
            self.daily_start_date = stored_date.state_value
            self.daily_start_equity = float(stored_equity.state_value)
            return

        self.daily_start_date = current_date
        self.daily_start_equity = float(current_equity)
        timestamp = datetime.now(timezone.utc).isoformat()
        self.database.upsert_system_state(
            SystemStateRecord("risk_daily_start_date", self.daily_start_date, timestamp)
        )
        self.database.upsert_system_state(
            SystemStateRecord("risk_daily_start_equity", f"{self.daily_start_equity:.12f}", timestamp)
        )

    def _persist_system_status(self, status: str) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        records = [
            SystemStateRecord("system_status", status, timestamp),
            SystemStateRecord("market_oracle_status", status, timestamp),
            SystemStateRecord("cta_status", status, timestamp),
            SystemStateRecord("grid_status", status, timestamp),
        ]
        for record in records:
            self.database.upsert_system_state(record)

    def _persist_opening_block(self, blocked: bool, reason: str | None) -> None:
        self.new_openings_blocked = blocked
        self.block_reason = reason
        timestamp = datetime.now(timezone.utc).isoformat()
        self.database.upsert_system_state(
            SystemStateRecord("risk_new_openings", "OFF" if blocked else "ON", timestamp)
        )
        self.database.upsert_system_state(
            SystemStateRecord("risk_block_reason", reason or "", timestamp)
        )

    def _clear_grid_observe_mode(self, reason: str) -> None:
        if self._grid_observe_state is None:
            return
        logger.info("Grid observe mode cleared | symbol=%s reason=%s", self._grid_observe_state.symbol, reason)
        self._grid_observe_state = None

    def _cap_amount_by_equity_multiple(self, symbol: str, amount: float, price: float, account_equity: float) -> float:
        equity_multiple = float(self.config.cta_single_trade_equity_multiple)
        if equity_multiple <= 0 or amount <= 0 or account_equity <= 0:
            return max(0.0, float(amount))

        max_notional = account_equity * equity_multiple
        unit_notional = self.client.estimate_notional(symbol, 1.0, price)
        if max_notional <= 0 or unit_notional <= 0:
            return 0.0
        return min(float(amount), max_notional / unit_notional)

    def _cap_amount_by_symbol_limit(self, symbol: str, amount: float, price: float) -> float:
        symbol_limit = self.config.resolve_symbol_notional_limit(symbol)
        if symbol_limit <= 0 or amount <= 0:
            return max(0.0, float(amount))

        current_position_notional = self.client.fetch_symbol_position_notional(symbol)
        current_order_notional = self.client.fetch_symbol_open_order_notional(symbol)
        remaining_notional = symbol_limit - current_position_notional - current_order_notional
        if remaining_notional <= 0:
            return 0.0

        unit_notional = self.client.estimate_notional(symbol, 1.0, price)
        if unit_notional <= 0:
            return 0.0

        return min(float(amount), remaining_notional / unit_notional)

    def _grid_position_protected(self, symbol: str, actual: ExchangePositionSnapshot) -> bool:
        profile = self.latest_grid_risk
        if profile is None or profile.symbol != symbol:
            return False
        return not actual.is_flat

    def _fetch_exchange_position(self, symbol: str) -> ExchangePositionSnapshot:
        long_size = 0.0
        short_size = 0.0
        total_notional = 0.0

        for position in self.client.fetch_positions([symbol]):
            contracts = self._extract_position_size(position)
            if contracts <= 0:
                continue
            side = self._extract_position_side(position)
            total_notional += self.client.position_notional(symbol, position)
            if side == "short":
                short_size += contracts
            else:
                long_size += contracts

        if long_size > 0 and short_size <= 0:
            return ExchangePositionSnapshot(symbol=symbol, side="long", size=long_size, notional=total_notional)
        if short_size > 0 and long_size <= 0:
            return ExchangePositionSnapshot(symbol=symbol, side="short", size=short_size, notional=total_notional)
        if short_size > 0 and long_size > 0:
            return ExchangePositionSnapshot(symbol=symbol, side="mixed", size=long_size + short_size, notional=total_notional)
        return ExchangePositionSnapshot(symbol=symbol, side="flat", size=0.0, notional=0.0)

    def _positions_match(
        self,
        local: LogicalPositionSnapshot | None,
        actual: ExchangePositionSnapshot,
    ) -> bool:
        if local is None:
            return actual.is_flat
        if actual.is_flat:
            return False
        if local.side != actual.side:
            return False
        return abs(local.size - actual.size) <= self.config.position_sync_tolerance

    def _extract_liquidation_price(self, position: dict[str, Any]) -> float | None:
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

    @staticmethod
    def _normalize_opening_side(opening_side: str | None) -> str | None:
        if opening_side is None:
            return None
        normalized = str(opening_side).strip().lower()
        if normalized in {"buy", "long"}:
            return "long"
        if normalized in {"sell", "short"}:
            return "short"
        return None

    @staticmethod
    def _extract_position_size(position: dict[str, Any]) -> float:
        raw = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos") or 0.0
        return abs(float(raw or 0.0))

    @staticmethod
    def _extract_position_side(position: dict[str, Any]) -> str:
        side = str(position.get("side") or position.get("info", {}).get("posSide") or "").lower()
        if side in {"long", "short"}:
            return side
        raw = position.get("contracts") or position.get("positionAmt") or position.get("info", {}).get("pos") or 0.0
        return "long" if float(raw or 0.0) >= 0 else "short"
