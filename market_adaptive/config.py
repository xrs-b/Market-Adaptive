from __future__ import annotations

from dataclasses import dataclass
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
    shutdown_cancel_open_orders: bool = True


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
    timeframe: str = "15m"
    lookback_limit: int = 200
    fast_ema: int = 7
    slow_ema: int = 21
    polling_interval_seconds: int = 60


@dataclass
class GridConfig:
    symbol: str = "BTC/USDT"
    range_percent: float = 0.02
    levels: int = 10
    polling_interval_seconds: int = 60


@dataclass
class AppConfig:
    okx: OKXConfig
    database: DatabaseConfig
    notification: NotificationConfig
    runtime: RuntimeConfig
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
        shutdown_cancel_open_orders=bool(runtime_payload.get("shutdown_cancel_open_orders", True)),
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
    cta = CTAConfig(
        symbol=str(cta_payload.get("symbol", "BTC/USDT")),
        timeframe=str(cta_payload.get("timeframe", "15m")),
        lookback_limit=int(cta_payload.get("lookback_limit", 200)),
        fast_ema=int(cta_payload.get("fast_ema", 7)),
        slow_ema=int(cta_payload.get("slow_ema", 21)),
        polling_interval_seconds=int(cta_payload.get("polling_interval_seconds", 60)),
    )
    grid = GridConfig(
        symbol=str(grid_payload.get("symbol", "BTC/USDT")),
        range_percent=float(grid_payload.get("range_percent", 0.02)),
        levels=int(grid_payload.get("levels", 10)),
        polling_interval_seconds=int(grid_payload.get("polling_interval_seconds", 60)),
    )
    return AppConfig(
        okx=okx,
        database=database,
        notification=notification,
        runtime=runtime,
        market_oracle=market_oracle,
        execution=execution,
        cta=cta,
        grid=grid,
        config_path=path,
    )
