from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import ccxt

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import MarketOracleConfig
from market_adaptive.db import DatabaseInitializer, MarketStatusRecord
from market_adaptive.indicators import IndicatorSnapshot, compute_atr, compute_indicator_snapshot, ohlcv_to_dataframe

logger = logging.getLogger(__name__)


@dataclass
class MultiTimeframeMarketSnapshot:
    symbol: str
    higher_timeframe: str
    lower_timeframe: str
    higher: IndicatorSnapshot
    lower: IndicatorSnapshot

    @property
    def strongest_adx(self) -> float:
        return max(self.higher.adx_value, self.lower.adx_value)

    @property
    def strongest_volatility(self) -> float:
        return max(self.higher.volatility, self.lower.volatility)


class MarketOracle:
    """Market sensing bot that classifies BTC/USDT as trend or sideways."""

    def _indicator_confirms_trend(self, indicator: IndicatorSnapshot) -> bool:
        return bool(
            indicator.adx_value > float(self.config.trend_adx_threshold)
            and indicator.adx_rising
            and indicator.di_gap >= float(self.config.trend_di_gap_threshold)
            and indicator.bb_width_expanding
        )

    def _indicator_summary(self, label: str, indicator: IndicatorSnapshot) -> str:
        return (
            f"{label}: adx={indicator.adx_value:.2f} adx_trend={indicator.adx_trend_label} "
            f"di_gap={indicator.di_gap:.2f} (+di={indicator.plus_di_value:.2f} -di={indicator.minus_di_value:.2f}) "
            f"bb_expand={indicator.bb_width_expanding}"
        )

    def __init__(
        self,
        client: OKXClient,
        database: DatabaseInitializer,
        config: MarketOracleConfig,
        notifier: Any | None = None,
    ) -> None:
        self.client = client
        self.database = database
        self.config = config
        self.notifier = notifier
        self._last_snapshot: MultiTimeframeMarketSnapshot | None = None

    def collect_market_snapshot(self) -> MultiTimeframeMarketSnapshot:
        higher_ohlcv = self.client.fetch_ohlcv(
            symbol=self.config.symbol,
            timeframe=self.config.higher_timeframe,
            limit=self.config.lookback_limit,
        )
        lower_ohlcv = self.client.fetch_ohlcv(
            symbol=self.config.symbol,
            timeframe=self.config.lower_timeframe,
            limit=self.config.lookback_limit,
        )

        higher_snapshot = compute_indicator_snapshot(
            higher_ohlcv,
            adx_length=self.config.adx_length,
            bb_length=self.config.bb_length,
            bb_std=self.config.bb_std,
        )
        lower_snapshot = compute_indicator_snapshot(
            lower_ohlcv,
            adx_length=self.config.adx_length,
            bb_length=self.config.bb_length,
            bb_std=self.config.bb_std,
        )

        return MultiTimeframeMarketSnapshot(
            symbol=self.config.symbol,
            higher_timeframe=self.config.higher_timeframe,
            lower_timeframe=self.config.lower_timeframe,
            higher=higher_snapshot,
            lower=lower_snapshot,
        )

    def determine_status(self, snapshot: MultiTimeframeMarketSnapshot) -> str:
        trend_detected = any(
            self._indicator_confirms_trend(indicator)
            for indicator in (snapshot.higher, snapshot.lower)
        )
        sideways_detected = all(
            (
                indicator.adx_value < float(self.config.sideways_adx_threshold)
                or not indicator.adx_rising
                or indicator.di_gap < float(self.config.trend_di_gap_threshold)
            )
            for indicator in (snapshot.higher, snapshot.lower)
        )

        if trend_detected:
            return "trend"
        if sideways_detected:
            return "sideways"

        previous = self.database.fetch_latest_market_status(snapshot.symbol)
        if previous is not None:
            return previous.status
        return "sideways"

    def run_once(self) -> MarketStatusRecord:
        previous = self.database.fetch_latest_market_status(self.config.symbol)
        snapshot = self.collect_market_snapshot()
        self._last_snapshot = snapshot
        status = self.determine_status(snapshot)
        record = MarketStatusRecord(
            timestamp=datetime.now(timezone.utc).isoformat(),
            symbol=snapshot.symbol,
            status=status,
            adx_value=snapshot.strongest_adx,
            volatility=snapshot.strongest_volatility,
        )
        self.database.insert_market_status(record)
        logger.info(
            "Market regime diagnostics | %s | %s",
            self._indicator_summary(snapshot.higher_timeframe, snapshot.higher),
            self._indicator_summary(snapshot.lower_timeframe, snapshot.lower),
        )
        logger.info(
            "Market status updated: symbol=%s status=%s adx=%.4f volatility=%.6f",
            record.symbol,
            record.status,
            record.adx_value,
            record.volatility,
        )
        if previous is None or previous.status != record.status:
            self._notify_status_change(previous.status if previous else None, record)
        return record

    def get_hourly_atr(self, symbol: str | None = None) -> float:
        target_symbol = symbol or self.config.symbol
        ohlcv = self.client.fetch_ohlcv(
            symbol=target_symbol,
            timeframe=self.config.higher_timeframe,
            limit=max(self.config.adx_length * 4, 80),
        )
        frame = ohlcv_to_dataframe(ohlcv)
        atr_series = compute_atr(frame, length=self.config.adx_length)
        return float(atr_series.iloc[-1])

    def current_higher_adx_trend(self) -> str:
        if self._last_snapshot is None:
            snapshot = self.collect_market_snapshot()
            self._last_snapshot = snapshot
        assert self._last_snapshot is not None
        return self._last_snapshot.higher.adx_trend_label

    def run_forever(self) -> None:
        logger.info(
            "Market-Oracle started: symbol=%s interval=%ss",
            self.config.symbol,
            self.config.polling_interval_seconds,
        )
        while True:
            try:
                self.run_once()
            except (ccxt.NetworkError, ccxt.ExchangeError, TimeoutError, ValueError) as exc:
                logger.warning("Market-Oracle transient failure: %s", exc, exc_info=True)
            except Exception as exc:  # pragma: no cover - hard runtime guard
                logger.exception("Market-Oracle unexpected failure: %s", exc)
            time.sleep(self.config.polling_interval_seconds)

    def _notify_status_change(self, previous_status: str | None, record: MarketStatusRecord) -> None:
        if self.notifier is None:
            return
        old_status = previous_status or "none"
        snapshot = self._last_snapshot
        if snapshot is not None and hasattr(self.notifier, "notify_market_shift"):
            reason = (
                f"symbol={record.symbol}; adx={record.adx_value:.4f}; atr/volatility={record.volatility:.6f}; "
                f"{self._indicator_summary(snapshot.higher_timeframe, snapshot.higher)}; "
                f"{self._indicator_summary(snapshot.lower_timeframe, snapshot.lower)}"
            )
            self.notifier.notify_market_shift(old_status, record.status, reason)
            return
        self.notifier.send(
            "Market Status Switched",
            (
                f"symbol={record.symbol} | {old_status} -> {record.status} | "
                f"adx={record.adx_value:.4f} | volatility={record.volatility:.6f}"
            ),
        )
