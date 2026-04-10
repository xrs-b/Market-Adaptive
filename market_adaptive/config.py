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
class RuntimeConfig:
    timezone: str = "Asia/Shanghai"
    default_timeframe: str = "1h"
    default_ohlcv_limit: int = 200


@dataclass
class AppConfig:
    okx: OKXConfig
    database: DatabaseConfig
    runtime: RuntimeConfig
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
    runtime_payload = payload.get("runtime") or {}

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
    runtime = RuntimeConfig(
        timezone=str(runtime_payload.get("timezone", "Asia/Shanghai")),
        default_timeframe=str(runtime_payload.get("default_timeframe", "1h")),
        default_ohlcv_limit=int(runtime_payload.get("default_ohlcv_limit", 200)),
    )
    return AppConfig(okx=okx, database=database, runtime=runtime, config_path=path)
