from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class OKXConfig:
    api_key: str
    api_secret: str
    passphrase: str
    sandbox: bool = True
    simulated_id: str = "1"
    simulated_trading: bool = True
    default_type: str = "swap"
    timeout_ms: int = 10_000

    @property
    def headers(self) -> dict[str, str]:
        headers = {"x-simulated-id": str(self.simulated_id)}
        if self.simulated_trading:
            headers["x-simulated-trading"] = "1"
        return headers


@dataclass
class DatabaseConfig:
    path: Path


@dataclass
class DiscordNotificationConfig:
    enabled: bool = False
    channel_id: str = ""
    webhook_url: str = ""
    bot_token: str = ""
    username: str = "Market-Adaptive"


@dataclass
class NotificationConfig:
    discord: DiscordNotificationConfig


@dataclass
class RuntimeConfig:
    timezone: str = "Asia/Shanghai"
    default_timeframe: str = "1h"
    default_ohlcv_limit: int = 200
    account_check_interval_seconds: int = 60
    risk_check_interval_seconds: int = 60
    fast_risk_check_interval_seconds: int = 1
    shutdown_cancel_open_orders: bool = True


@dataclass
class RiskControlConfig:
    daily_loss_cutoff_pct: float = 0.05
    max_margin_ratio: float = 0.60
    recovery_check_interval_seconds: int = 60
    position_sync_tolerance: float = 1e-6
    default_symbol_max_notional: float = 0.0
    symbol_notional_limits: dict[str, float] = field(default_factory=dict)
    cta_single_trade_equity_multiple: float = 0.0
    max_directional_leverage: float = 8.0
    grid_margin_ratio_warning: float = 0.45
    grid_deviation_reduce_ratio: float = 0.25
    grid_liquidation_warning_ratio: float = 0.10
    grid_reduction_step_pct: float = 0.25
    grid_reduction_cooldown_seconds: int = 300

    def resolve_symbol_notional_limit(self, symbol: str) -> float:
        if symbol in self.symbol_notional_limits:
            return float(self.symbol_notional_limits[symbol])
        return float(self.default_symbol_max_notional)


@dataclass
class SentimentConfig:
    enabled: bool = True
    symbol: str = ""
    timeframe: str = "5m"
    lookback_limit: int = 1
    extreme_bullish_ratio: float = 2.5
    cta_buy_action: str = "block"

    def resolve_symbol(self, fallback_symbol: str) -> str:
        return self.symbol or fallback_symbol

    @property
    def normalized_cta_buy_action(self) -> str:
        action = self.cta_buy_action.strip().lower()
        return action if action in {"block", "halve"} else "block"


@dataclass
class MarketOracleConfig:
    symbol: str = "BTC/USDT"
    polling_interval_seconds: int = 300
    higher_timeframe: str = "1h"
    lower_timeframe: str = "15m"
    lookback_limit: int = 200
    adx_length: int = 14
    bb_length: int = 20
    bb_std: float = 2.0
    trend_adx_threshold: float = 25.0
    sideways_adx_threshold: float = 20.0


@dataclass
class ExecutionConfig:
    td_mode: str = "isolated"
    cta_order_size: float = 0.01
    grid_order_size: float = 0.01


@dataclass
class CTAConfig:
    symbol: str = "BTC/USDT"
    timeframe: str = "15m"  # legacy alias for execution_timeframe
    lower_timeframe: str = "15m"  # legacy alias for execution_timeframe
    higher_timeframe: str = "1h"  # legacy alias for swing_timeframe
    major_timeframe: str = "4h"
    swing_timeframe: str = "1h"
    execution_timeframe: str = "15m"
    lookback_limit: int = 200
    supertrend_period: int = 10
    supertrend_multiplier: float = 3.0
    swing_rsi_period: int = 14
    swing_rsi_ready_threshold: float = 50.0
    kdj_length: int = 9
    kdj_k_smoothing: int = 3
    kdj_d_smoothing: int = 3
    execution_breakout_lookback: int = 3
    obv_signal_period: int = 8
    obv_slope_window: int = 8
    obv_slope_threshold_degrees: float = 30.0
    atr_period: int = 14
    atr_trailing_multiplier: float = 2.5
    stop_loss_atr: float = 2.0
    risk_percent_per_trade: float = 0.02
    boosted_risk_percent_per_trade: float = 0.03
    first_take_profit_pct: float = 0.02
    first_take_profit_size: float = 0.50
    second_take_profit_pct: float = 0.05
    second_take_profit_size: float = 0.25
    volume_profile_lookback_hours: int = 24
    volume_profile_bin_count: int = 24
    volume_profile_value_area_pct: float = 0.70
    fast_ema: int = 7  # legacy compatibility
    slow_ema: int = 21  # legacy compatibility
    polling_interval_seconds: int = 60

    def __post_init__(self) -> None:
        default_execution = "15m"
        default_swing = "1h"
        default_timeframe = "15m"

        if self.execution_timeframe == default_execution and self.lower_timeframe != default_execution:
            self.execution_timeframe = self.lower_timeframe
        if self.execution_timeframe == default_execution and self.timeframe != default_timeframe:
            self.execution_timeframe = self.timeframe
        if self.lower_timeframe == default_execution and self.execution_timeframe != default_execution:
            self.lower_timeframe = self.execution_timeframe
        if self.timeframe == default_timeframe and self.execution_timeframe != default_execution:
            self.timeframe = self.execution_timeframe

        if self.swing_timeframe == default_swing and self.higher_timeframe != default_swing:
            self.swing_timeframe = self.higher_timeframe
        if self.higher_timeframe == default_swing and self.swing_timeframe != default_swing:
            self.higher_timeframe = self.swing_timeframe

        self.major_timeframe = str(self.major_timeframe or "4h")
        self.swing_timeframe = str(self.swing_timeframe or self.higher_timeframe or default_swing)
        self.execution_timeframe = str(self.execution_timeframe or self.lower_timeframe or self.timeframe or default_execution)
        self.lower_timeframe = self.execution_timeframe
        self.higher_timeframe = self.swing_timeframe
        self.timeframe = self.execution_timeframe
        if self.boosted_risk_percent_per_trade <= 0:
            self.boosted_risk_percent_per_trade = self.risk_percent_per_trade


@dataclass
class GridConfig:
    symbol: str = "BTC/USDT"
    timeframe: str = "1h"  # legacy alias for Bollinger timeframe
    lookback_limit: int = 120
    bollinger_period: int = 20
    bollinger_std: float = 2.0
    levels: int = 10
    leverage: int = 3
    martingale_factor: float = 1.25
    trigger_window_seconds: int = 300
    trigger_limit_per_layer: int = 3
    layer_cooldown_seconds: int = 300
    rebalance_exposure_threshold: float = 2.0
    max_rebalance_orders: int = 2
    rebalance_threshold_ratio: float = 0.65  # legacy compatibility
    polling_interval_seconds: int = 60
    range_percent: float = 0.03  # neutral grid orders stay within current price ±3%
    liquidation_protection_ratio: float = 0.05


@dataclass
class AppConfig:
    okx: OKXConfig
    database: DatabaseConfig
    notification: NotificationConfig
    runtime: RuntimeConfig
    risk_control: RiskControlConfig
    sentiment: SentimentConfig
    market_oracle: MarketOracleConfig
    execution: ExecutionConfig
    cta: CTAConfig
    grid: GridConfig
    config_path: Path


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError("Root config payload must be a mapping")

    return payload


def load_config(config_path: str | Path) -> AppConfig:
    path = Path(config_path).expanduser().resolve()
    payload = _read_yaml(path)

    okx_payload = payload.get("okx") or {}
    db_payload = payload.get("database") or {}
    notification_payload = payload.get("notification") or {}
    discord_payload = notification_payload.get("discord") or {}
    runtime_payload = payload.get("runtime") or {}
    risk_payload = payload.get("risk_control") or {}
    sentiment_payload = payload.get("sentiment") or {}
    market_oracle_payload = payload.get("market_oracle") or {}
    execution_payload = payload.get("execution") or {}
    cta_payload = payload.get("cta") or {}
    grid_payload = payload.get("grid") or {}

    okx = OKXConfig(
        api_key=str(okx_payload.get("api_key", "")),
        api_secret=str(okx_payload.get("api_secret", "")),
        passphrase=str(okx_payload.get("passphrase", "")),
        sandbox=bool(okx_payload.get("sandbox", True)),
        simulated_id=str(okx_payload.get("simulated_id", "1")),
        simulated_trading=bool(okx_payload.get("simulated_trading", True)),
        default_type=str(okx_payload.get("default_type", "swap")),
        timeout_ms=int(okx_payload.get("timeout_ms", 10_000)),
    )
    database = DatabaseConfig(
        path=(path.parent.parent / str(db_payload.get("path", "data/market_adaptive.sqlite3"))).resolve()
    )
    notification = NotificationConfig(
        discord=DiscordNotificationConfig(
            enabled=bool(discord_payload.get("enabled", False)),
            channel_id=str(discord_payload.get("channel_id", "")),
            webhook_url=str(discord_payload.get("webhook_url", "")),
            bot_token=str(discord_payload.get("bot_token", "")),
            username=str(discord_payload.get("username", "Market-Adaptive")),
        )
    )
    runtime = RuntimeConfig(
        timezone=str(runtime_payload.get("timezone", "Asia/Shanghai")),
        default_timeframe=str(runtime_payload.get("default_timeframe", "1h")),
        default_ohlcv_limit=int(runtime_payload.get("default_ohlcv_limit", 200)),
        account_check_interval_seconds=int(runtime_payload.get("account_check_interval_seconds", 60)),
        risk_check_interval_seconds=int(runtime_payload.get("risk_check_interval_seconds", 60)),
        fast_risk_check_interval_seconds=int(runtime_payload.get("fast_risk_check_interval_seconds", 1)),
        shutdown_cancel_open_orders=bool(runtime_payload.get("shutdown_cancel_open_orders", True)),
    )
    risk_control = RiskControlConfig(
        daily_loss_cutoff_pct=float(risk_payload.get("daily_loss_cutoff_pct", 0.05)),
        max_margin_ratio=float(risk_payload.get("max_margin_ratio", 0.60)),
        recovery_check_interval_seconds=int(risk_payload.get("recovery_check_interval_seconds", 60)),
        position_sync_tolerance=float(risk_payload.get("position_sync_tolerance", 1e-6)),
        default_symbol_max_notional=float(risk_payload.get("default_symbol_max_notional", 0.0)),
        symbol_notional_limits={
            str(symbol): float(limit)
            for symbol, limit in (risk_payload.get("symbol_notional_limits") or {}).items()
        },
        cta_single_trade_equity_multiple=float(risk_payload.get("cta_single_trade_equity_multiple", 0.0)),
        max_directional_leverage=float(risk_payload.get("max_directional_leverage", 8.0)),
        grid_margin_ratio_warning=float(risk_payload.get("grid_margin_ratio_warning", 0.45)),
        grid_deviation_reduce_ratio=float(risk_payload.get("grid_deviation_reduce_ratio", 0.25)),
        grid_liquidation_warning_ratio=float(risk_payload.get("grid_liquidation_warning_ratio", 0.10)),
        grid_reduction_step_pct=float(risk_payload.get("grid_reduction_step_pct", 0.25)),
        grid_reduction_cooldown_seconds=int(risk_payload.get("grid_reduction_cooldown_seconds", 300)),
    )
    sentiment = SentimentConfig(
        enabled=bool(sentiment_payload.get("enabled", True)),
        symbol=str(sentiment_payload.get("symbol", "")),
        timeframe=str(sentiment_payload.get("timeframe", "5m")),
        lookback_limit=max(1, int(sentiment_payload.get("lookback_limit", 1))),
        extreme_bullish_ratio=float(sentiment_payload.get("extreme_bullish_ratio", 2.5)),
        cta_buy_action=str(sentiment_payload.get("cta_buy_action", "block")),
    )
    market_oracle = MarketOracleConfig(
        symbol=str(market_oracle_payload.get("symbol", "BTC/USDT")),
        polling_interval_seconds=int(market_oracle_payload.get("polling_interval_seconds", 300)),
        higher_timeframe=str(market_oracle_payload.get("higher_timeframe", "1h")),
        lower_timeframe=str(market_oracle_payload.get("lower_timeframe", "15m")),
        lookback_limit=int(market_oracle_payload.get("lookback_limit", 200)),
        adx_length=int(market_oracle_payload.get("adx_length", 14)),
        bb_length=int(market_oracle_payload.get("bb_length", 20)),
        bb_std=float(market_oracle_payload.get("bb_std", 2.0)),
        trend_adx_threshold=float(market_oracle_payload.get("trend_adx_threshold", 25)),
        sideways_adx_threshold=float(market_oracle_payload.get("sideways_adx_threshold", 20)),
    )
    execution = ExecutionConfig(
        td_mode=str(execution_payload.get("td_mode", "isolated")),
        cta_order_size=float(execution_payload.get("cta_order_size", 0.01)),
        grid_order_size=float(execution_payload.get("grid_order_size", 0.01)),
    )
    cta_timeframe = str(cta_payload.get("timeframe", "15m"))
    cta_execution_timeframe = str(
        cta_payload.get("execution_timeframe", cta_payload.get("lower_timeframe", cta_timeframe))
    )
    cta_swing_timeframe = str(
        cta_payload.get("swing_timeframe", cta_payload.get("higher_timeframe", "1h"))
    )
    cta_base_risk_percent = float(cta_payload.get("risk_percent_per_trade", 0.02))
    cta_boosted_risk_percent = float(
        cta_payload.get("boosted_risk_percent_per_trade", max(cta_base_risk_percent, 0.03))
    )
    cta = CTAConfig(
        symbol=str(cta_payload.get("symbol", "BTC/USDT")),
        timeframe=cta_timeframe,
        lower_timeframe=cta_execution_timeframe,
        higher_timeframe=cta_swing_timeframe,
        major_timeframe=str(cta_payload.get("major_timeframe", "4h")),
        swing_timeframe=cta_swing_timeframe,
        execution_timeframe=cta_execution_timeframe,
        lookback_limit=int(cta_payload.get("lookback_limit", 200)),
        supertrend_period=int(cta_payload.get("supertrend_period", 10)),
        supertrend_multiplier=float(cta_payload.get("supertrend_multiplier", 3.0)),
        swing_rsi_period=int(cta_payload.get("swing_rsi_period", 14)),
        swing_rsi_ready_threshold=float(cta_payload.get("swing_rsi_ready_threshold", 50.0)),
        kdj_length=int(cta_payload.get("kdj_length", 9)),
        kdj_k_smoothing=int(cta_payload.get("kdj_k_smoothing", 3)),
        kdj_d_smoothing=int(cta_payload.get("kdj_d_smoothing", 3)),
        execution_breakout_lookback=int(cta_payload.get("execution_breakout_lookback", 3)),
        obv_signal_period=int(cta_payload.get("obv_signal_period", 8)),
        obv_slope_window=int(cta_payload.get("obv_slope_window", 8)),
        obv_slope_threshold_degrees=float(cta_payload.get("obv_slope_threshold_degrees", 30.0)),
        atr_period=int(cta_payload.get("atr_period", 14)),
        atr_trailing_multiplier=float(cta_payload.get("atr_trailing_multiplier", 2.5)),
        stop_loss_atr=float(cta_payload.get("stop_loss_atr", 2.0)),
        risk_percent_per_trade=cta_base_risk_percent,
        boosted_risk_percent_per_trade=cta_boosted_risk_percent,
        first_take_profit_pct=float(cta_payload.get("first_take_profit_pct", 0.02)),
        first_take_profit_size=float(cta_payload.get("first_take_profit_size", 0.50)),
        second_take_profit_pct=float(cta_payload.get("second_take_profit_pct", 0.05)),
        second_take_profit_size=float(cta_payload.get("second_take_profit_size", 0.25)),
        volume_profile_lookback_hours=int(cta_payload.get("volume_profile_lookback_hours", 24)),
        volume_profile_bin_count=int(cta_payload.get("volume_profile_bin_count", 24)),
        volume_profile_value_area_pct=float(cta_payload.get("volume_profile_value_area_pct", 0.70)),
        fast_ema=int(cta_payload.get("fast_ema", 7)),
        slow_ema=int(cta_payload.get("slow_ema", 21)),
        polling_interval_seconds=int(cta_payload.get("polling_interval_seconds", 60)),
    )
    grid = GridConfig(
        symbol=str(grid_payload.get("symbol", "BTC/USDT")),
        timeframe=str(grid_payload.get("bollinger_timeframe", grid_payload.get("timeframe", "1h"))),
        lookback_limit=int(grid_payload.get("lookback_limit", 120)),
        bollinger_period=int(grid_payload.get("bollinger_length", grid_payload.get("bollinger_period", 20))),
        bollinger_std=float(grid_payload.get("bollinger_std", 2.0)),
        levels=int(grid_payload.get("levels", 10)),
        leverage=int(grid_payload.get("leverage", 3)),
        martingale_factor=float(grid_payload.get("martingale_factor", 1.25)),
        trigger_window_seconds=int(grid_payload.get("layer_trigger_window_seconds", grid_payload.get("trigger_window_seconds", 300))),
        trigger_limit_per_layer=int(grid_payload.get("layer_trigger_limit", grid_payload.get("trigger_limit_per_layer", 3))),
        layer_cooldown_seconds=int(grid_payload.get("layer_cooldown_seconds", 300)),
        rebalance_exposure_threshold=float(grid_payload.get("rebalance_exposure_threshold", 2.0)),
        max_rebalance_orders=int(grid_payload.get("max_rebalance_orders", 2)),
        rebalance_threshold_ratio=float(grid_payload.get("rebalance_threshold_ratio", 0.65)),
        polling_interval_seconds=int(grid_payload.get("polling_interval_seconds", 60)),
        range_percent=float(grid_payload.get("price_band_ratio", grid_payload.get("range_percent", 0.03))),
        liquidation_protection_ratio=float(grid_payload.get("liquidation_protection_ratio", 0.05)),
    )
    return AppConfig(
        okx=okx,
        database=database,
        notification=notification,
        runtime=runtime,
        risk_control=risk_control,
        sentiment=sentiment,
        market_oracle=market_oracle,
        execution=execution,
        cta=cta,
        grid=grid,
        config_path=path,
    )
