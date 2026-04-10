from __future__ import annotations

from pathlib import Path

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import AppConfig, load_config
from market_adaptive.db import DatabaseInitializer
from market_adaptive.sentiment import SentimentAnalyst


class MarketAdaptiveBootstrap:
    """Central bootstrapper for config, database and shared clients."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.database = DatabaseInitializer(config.database.path)
        self.okx_client = OKXClient(config.okx, config.execution)
        self.sentiment_analyst = SentimentAnalyst(self.okx_client, config.sentiment)

    @classmethod
    def from_config_file(cls, config_path: str | Path) -> "MarketAdaptiveBootstrap":
        config = load_config(config_path)
        return cls(config)

    def initialize(self) -> None:
        self.database.initialize()
