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
    total_notional: float
    new_openings_blocked: bool = False
    block_reason: str | None = None


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
    reduce_grid_exposure_callback: Callable[[str], None] | None = None
    logical_position_provider: Callable[[], dict[str, LogicalPositionSnapshot | None]] | None = None
    local_position_reset_callback: Callable[[str, str], None] | None = None
    daily_start_equity: float | None = None
    daily_start_date: str | None = None
    latest_total_pnl: float = 0.0
    new_openings_blocked: bool = False
    block_reason: str | None = None
    circuit_breaker_triggered: bool = False
    _grid_reduction_applied: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.symbols = sorted(set(self.symbols))
        self.timezone = ZoneInfo(self.runtime_config.timezone)

    def initialize(self) -> None:
        current_equity = self.client.fetch_total_equity()
        self._sync_daily_baseline(current_equity=current_equity)
        self._persist_system_status("ON")
        self._persist_opening_block(False, None)

    def monitor_once(self) -> AccountRiskSnapshot:
        current_equity = self.client.fetch_total_equity()
        self.latest_total_pnl = self.client.fetch_total_unrealized_pnl(self.symbols)
        self._sync_daily_baseline(current_equity=current_equity)

        risk_payload = self.client.fetch_account_risk_snapshot(self.symbols)
        margin_ratio = max(0.0, float(risk_payload.get("margin_ratio", 0.0)))
        maintenance_margin = max(0.0, float(risk_payload.get("maintenance_margin", 0.0)))
        total_notional = max(0.0, float(risk_payload.get("total_notional", 0.0)))

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

        if margin_blocked and self.reduce_grid_exposure_callback is not None and not self._grid_reduction_applied:
            self.reduce_grid_exposure_callback(block_reason or "margin_ratio_limit")
            self._grid_reduction_applied = True
        if not margin_blocked:
            self._grid_reduction_applied = False

        snapshot = AccountRiskSnapshot(
            equity=current_equity,
            daily_start_equity=self.daily_start_equity,
            daily_drawdown=daily_drawdown,
            total_unrealized_pnl=self.latest_total_pnl,
            margin_ratio=margin_ratio,
            maintenance_margin=maintenance_margin,
            total_notional=total_notional,
            new_openings_blocked=margin_blocked,
            block_reason=block_reason,
        )

        logger.info(
            "Risk heartbeat | equity=%.4f daily_start=%.4f drawdown=%.2f%% unrealized_pnl=%.4f margin_ratio=%.2f%% total_notional=%.4f blocked=%s",
            snapshot.equity,
            snapshot.daily_start_equity,
            snapshot.daily_drawdown * 100,
            snapshot.total_unrealized_pnl,
            snapshot.margin_ratio * 100,
            snapshot.total_notional,
            snapshot.new_openings_blocked,
        )

        if daily_drawdown >= self.config.daily_loss_cutoff_pct:
            self.trigger_circuit_breaker(snapshot)

        return snapshot

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
        amount = self._cap_amount_by_symbol_limit(symbol, amount, price)
        minimum_amount = self.client.get_min_order_amount(symbol)
        if amount < minimum_amount:
            return 0.0
        return self.client.amount_to_precision(symbol, amount)

    def can_open_new_position(self, symbol: str, requested_notional: float, strategy_name: str | None = None) -> tuple[bool, str | None]:
        del strategy_name
        if self.circuit_breaker_triggered:
            return False, "circuit_breaker"

        snapshot = self.monitor_once()
        if snapshot.new_openings_blocked:
            return False, snapshot.block_reason

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
                "Risk Triggered",
                (
                    f"daily_drawdown={snapshot.daily_drawdown * 100:.2f}% | "
                    f"equity={snapshot.equity:.4f} | "
                    f"daily_start_equity={snapshot.daily_start_equity:.4f} | "
                    f"margin_ratio={snapshot.margin_ratio * 100:.2f}%"
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
