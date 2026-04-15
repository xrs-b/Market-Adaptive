from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from market_adaptive.config import SentimentConfig


@dataclass(frozen=True)
class SentimentSnapshot:
    symbol: str
    ratio: float
    threshold: float
    timeframe: str
    timestamp: int | None = None
    source: str = "okx_long_short_accounts_ratio"

    @property
    def is_extreme_bullish(self) -> bool:
        return self.ratio > self.threshold


@dataclass(frozen=True)
class CTASentimentDecision:
    blocked: bool = False
    size_multiplier: float = 1.0
    reason: str | None = None
    snapshot: SentimentSnapshot | None = None

    @property
    def is_adjusted(self) -> bool:
        return self.blocked or abs(self.size_multiplier - 1.0) > 1e-12


class SentimentAnalyst:
    def __init__(self, client: Any, config: SentimentConfig) -> None:
        self.client = client
        self.config = config

    def fetch_latest_snapshot(self, symbol: str) -> SentimentSnapshot | None:
        if not self.config.enabled:
            return None

        payload = self.client.fetch_latest_long_short_account_ratio(
            self.config.resolve_symbol(symbol),
            timeframe=self.config.timeframe,
            limit=self.config.lookback_limit,
        )
        if not payload:
            return None

        ratio = payload.get("longShortRatio")
        if ratio in (None, ""):
            return None

        timestamp_raw = payload.get("timestamp")
        timestamp = int(timestamp_raw) if timestamp_raw not in (None, "") else None
        return SentimentSnapshot(
            symbol=self.config.resolve_symbol(symbol),
            ratio=float(ratio),
            threshold=float(self.config.extreme_bullish_ratio),
            timeframe=self.config.timeframe,
            timestamp=timestamp,
        )

    def evaluate_cta_buy(self, symbol: str) -> CTASentimentDecision:
        snapshot = self.fetch_latest_snapshot(symbol)
        if snapshot is None or not snapshot.is_extreme_bullish:
            return CTASentimentDecision(snapshot=snapshot)

        reason = f"sentiment_ratio={snapshot.ratio:.2f}>threshold={snapshot.threshold:.2f}"
        action = self.config.normalized_cta_buy_action
        if action == "gradient":
            heavy_threshold = float(getattr(self.config, "gradient_heavy_reduce_ratio_threshold", 3.5))
            heavy_multiplier = float(getattr(self.config, "gradient_heavy_reduce_ratio_multiplier", 0.4))
            reduce_threshold = float(getattr(self.config, "gradient_reduce_ratio_threshold", 3.0))
            reduce_multiplier = float(getattr(self.config, "gradient_reduce_ratio_multiplier", 0.7))
            if snapshot.ratio >= heavy_threshold:
                return CTASentimentDecision(
                    blocked=True,
                    size_multiplier=0.0,
                    reason=f"{reason}; gradient=block",
                    snapshot=snapshot,
                )
            if snapshot.ratio >= reduce_threshold:
                return CTASentimentDecision(
                    blocked=False,
                    size_multiplier=heavy_multiplier,
                    reason=f"{reason}; gradient=heavy_reduce",
                    snapshot=snapshot,
                )
            return CTASentimentDecision(
                blocked=False,
                size_multiplier=reduce_multiplier,
                reason=f"{reason}; gradient=reduce",
                snapshot=snapshot,
            )
        if action == "halve":
            return CTASentimentDecision(
                blocked=False,
                size_multiplier=0.5,
                reason=reason,
                snapshot=snapshot,
            )

        return CTASentimentDecision(
            blocked=True,
            size_multiplier=0.0,
            reason=reason,
            snapshot=snapshot,
        )
