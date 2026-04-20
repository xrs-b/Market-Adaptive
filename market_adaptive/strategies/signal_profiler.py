from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from market_adaptive.strategies.mtf_engine import MTFSignal

logger = logging.getLogger(__name__)


@dataclass
class FunnelCounters:
    total_cycles: int = 0
    passed_regime: int = 0
    passed_swing: int = 0
    passed_trigger: int = 0


@dataclass
class CycleAuditRecord:
    cycle: int
    server_time_iso: str
    local_time_iso: str
    server_local_skew_ms: int | None
    major_supertrend_direction: int
    trigger_family: str
    trigger_group: str
    swing_rsi: float
    execution_obv_zscore: float
    execution_obv_threshold: float
    execution_price: float | None
    grid_center_price: float | None
    grid_center_gap: float | None
    atr_value: float
    atr_price_ratio_pct: float
    major_timestamp_ms: int
    swing_timestamp_ms: int
    execution_timestamp_ms: int
    data_alignment_valid: bool
    data_mismatch_ms: int
    blocker_reason: str
    passed_regime: bool
    passed_swing: bool
    passed_trigger: bool


@dataclass
class FunnelWindowSummary:
    window_cycles: int
    total_cycles: int
    passed_regime: int
    passed_swing: int
    passed_trigger: int
    regime_pass_rate_pct: float
    swing_pass_rate_pct: float
    trigger_pass_rate_pct: float
    top_blockers: list[tuple[str, int]]
    dominant_blocking_layer: str
    dominant_blocking_label: str
    dominant_blocking_count: int
    blocking_layer_counts: dict[str, int]
    latest_blocker_reason: str
    latest_execution_obv_zscore: float
    latest_execution_obv_threshold: float
    latest_execution_price: float | None
    latest_grid_center_gap: float | None

    def as_notification_payload(self) -> dict[str, Any]:
        return {
            "window_cycles": self.window_cycles,
            "total_cycles": self.total_cycles,
            "passed_regime": self.passed_regime,
            "passed_swing": self.passed_swing,
            "passed_trigger": self.passed_trigger,
            "regime_pass_rate_pct": self.regime_pass_rate_pct,
            "swing_pass_rate_pct": self.swing_pass_rate_pct,
            "trigger_pass_rate_pct": self.trigger_pass_rate_pct,
            "top_blockers": list(self.top_blockers),
            "dominant_blocking_layer": self.dominant_blocking_layer,
            "dominant_blocking_label": self.dominant_blocking_label,
            "dominant_blocking_count": self.dominant_blocking_count,
            "blocking_layer_counts": dict(self.blocking_layer_counts),
            "latest_blocker_reason": self.latest_blocker_reason,
            "latest_execution_obv_zscore": self.latest_execution_obv_zscore,
            "latest_execution_obv_threshold": self.latest_execution_obv_threshold,
            "latest_execution_price": self.latest_execution_price,
            "latest_grid_center_gap": self.latest_grid_center_gap,
        }


@dataclass
class SignalProfiler:
    summary_interval: int = 10
    min_blocking_count: int = 1
    notifier: Any | None = None
    symbol: str = "BTC/USDT"
    counters: FunnelCounters = field(default_factory=FunnelCounters)
    _window_records: list[CycleAuditRecord] = field(default_factory=list)

    _BLOCKING_LAYER_PRIORITY: ClassVar[dict[str, int]] = {
        "REGIME": 0,
        "SWING": 1,
        "TRIGGER": 2,
        "OBV": 3,
        "DATA": 4,
        "PASSED": 5,
        "OTHER": 6,
    }

    def _classify_blocking_layer(self, blocker_reason: str) -> tuple[str, str]:
        reason = str(blocker_reason or "PASSED")
        normalized = reason.upper()
        if normalized == "PASSED":
            return "PASSED", "已通过"
        if normalized == "DATA_MISMATCH_WARNING":
            return "DATA", "数据同步"
        if normalized == "BLOCKED_BY_SUPERTREND_REGIME":
            return "REGIME", "Regime（趋势层）"
        if normalized in {"BLOCKED_BY_RSI_THRESHOLD", "BLOCKED_BY_BULLISH_SCORE"}:
            return "SWING", "Swing（摆动层）"
        if normalized.startswith("BLOCKED_BY_TRIGGER:"):
            return "TRIGGER", "Trigger（触发层）"
        if normalized.startswith("BLOCKED_BY_"):
            post_trigger_suffix = normalized[len("BLOCKED_BY_") :]
            if post_trigger_suffix.startswith("OBV_") or post_trigger_suffix in {
                "BELOW_POC",
                "INSIDE_VALUE_AREA",
                "BELOW_VALUE_AREA_HIGH",
                "MISSING_VOLUME_PROFILE",
            }:
                return "OBV", "OBV（执行过滤层）"
        return "OTHER", "其他"

    def _resolve_dominant_blocking_layer(self, records: list[CycleAuditRecord]) -> tuple[str, str, int, dict[str, int]]:
        layer_counts: dict[str, int] = {}
        layer_labels: dict[str, str] = {}
        latest_layer = "PASSED"
        for record in records:
            layer, label = self._classify_blocking_layer(record.blocker_reason)
            layer_labels[layer] = label
            if record is records[-1]:
                latest_layer = layer
            if layer == "PASSED":
                continue
            layer_counts[layer] = layer_counts.get(layer, 0) + 1
        if not layer_counts:
            return "PASSED", layer_labels.get("PASSED", "已通过"), 0, {}
        dominant_layer, dominant_count = sorted(
            layer_counts.items(),
            key=lambda item: (
                -item[1],
                0 if item[0] == latest_layer else 1,
                self._BLOCKING_LAYER_PRIORITY.get(item[0], 999),
                item[0],
            ),
        )[0]
        return dominant_layer, layer_labels.get(dominant_layer, dominant_layer), dominant_count, layer_counts

    def _normalize_execution_price(self, price: Any) -> float | None:
        try:
            numeric_price = float(price)
        except (TypeError, ValueError):
            return None
        return numeric_price if numeric_price > 0 else None

    def _latest_non_null(self, records: list[CycleAuditRecord], field_name: str) -> Any | None:
        for record in reversed(records):
            value = getattr(record, field_name)
            if value is not None:
                return value
        return None

    def record(self, signal: "MTFSignal", *, grid_center_price: float | None = None, blocker_reason: str = "", execution_obv_threshold: float | None = None) -> CycleAuditRecord:
        self.counters.total_cycles += 1
        passed_regime = bool(abs(int(signal.major_direction)) > 0 or getattr(signal, "weak_bull_bias", False) or getattr(signal, "early_bullish", False) or getattr(signal, "weak_bear_bias", False) or getattr(signal, "early_bearish", False))
        passed_swing = bool(signal.bullish_ready or getattr(signal, "bearish_ready", False))
        passed_trigger = bool(signal.fully_aligned)
        if passed_regime:
            self.counters.passed_regime += 1
        if passed_swing:
            self.counters.passed_swing += 1
        if passed_trigger:
            self.counters.passed_trigger += 1

        gap = None
        if grid_center_price is not None:
            gap = float(signal.current_price) - float(grid_center_price)

        execution_price = self._normalize_execution_price(getattr(signal, "current_price", None))

        execution_trigger = getattr(signal, "execution_trigger", None)

        record = CycleAuditRecord(
            cycle=self.counters.total_cycles,
            server_time_iso=signal.server_time_iso,
            local_time_iso=signal.local_time_iso,
            server_local_skew_ms=signal.server_local_skew_ms,
            major_supertrend_direction=signal.major_direction,
            trigger_family=str(getattr(execution_trigger, "family", "waiting")),
            trigger_group=str(getattr(execution_trigger, "group", "waiting")),
            swing_rsi=float(signal.swing_rsi),
            execution_obv_zscore=float(signal.execution_obv_zscore),
            execution_obv_threshold=float(signal.execution_obv_threshold if execution_obv_threshold is None else execution_obv_threshold),
            execution_price=execution_price,
            grid_center_price=grid_center_price,
            grid_center_gap=gap,
            atr_value=float(signal.execution_atr),
            atr_price_ratio_pct=float(signal.atr_price_ratio_pct),
            major_timestamp_ms=int(signal.major_timestamp_ms),
            swing_timestamp_ms=int(signal.swing_timestamp_ms),
            execution_timestamp_ms=int(signal.execution_timestamp_ms),
            data_alignment_valid=bool(signal.data_alignment_valid),
            data_mismatch_ms=int(signal.data_mismatch_ms),
            blocker_reason=str(blocker_reason or signal.blocker_reason or "PASSED"),
            passed_regime=passed_regime,
            passed_swing=passed_swing,
            passed_trigger=passed_trigger,
        )
        self._window_records.append(record)
        logger.info(
            "Strategy audit snapshot | cycle=%s server_time=%s local_time=%s skew_ms=%s 4h_supertrend=%s 1h_rsi=%.2f 15m_obv_z=%.2f/%.2f price=%.4f grid_center=%s grid_gap=%s atr=%.6f atr_price_pct=%.4f data_ok=%s mismatch_ms=%s blocker=%s",
            record.cycle,
            record.server_time_iso,
            record.local_time_iso,
            record.server_local_skew_ms,
            record.major_supertrend_direction,
            record.swing_rsi,
            record.execution_obv_zscore,
            record.execution_obv_threshold,
            record.execution_price if record.execution_price is not None else float("nan"),
            f"{record.grid_center_price:.4f}" if record.grid_center_price is not None else "n/a",
            f"{record.grid_center_gap:.4f}" if record.grid_center_gap is not None else "n/a",
            record.atr_value,
            record.atr_price_ratio_pct,
            record.data_alignment_valid,
            record.data_mismatch_ms,
            record.blocker_reason,
        )
        if record.cycle % max(1, int(self.summary_interval)) == 0:
            summary = self._build_window_summary()
            logger.info(
                "Strategy funnel summary | Total Cycles=%s Passed Regime=%s Passed Swing=%s Passed Trigger=%s",
                self.counters.total_cycles,
                self.counters.passed_regime,
                self.counters.passed_swing,
                self.counters.passed_trigger,
            )
            self._notify_summary(summary)
            self._window_records.clear()
        return record

    def _build_window_summary(self) -> FunnelWindowSummary:
        records = list(self._window_records)
        latest = records[-1]
        blocker_counts: dict[str, int] = {}
        for record in records:
            blocker = str(record.blocker_reason or "PASSED")
            blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
        top_blockers = sorted(blocker_counts.items(), key=lambda item: (-item[1], item[0]))[:3]
        dominant_layer, dominant_label, dominant_count, layer_counts = self._resolve_dominant_blocking_layer(records)
        window_cycles = max(1, len(records))
        return FunnelWindowSummary(
            window_cycles=window_cycles,
            total_cycles=self.counters.total_cycles,
            passed_regime=sum(1 for record in records if record.passed_regime),
            passed_swing=sum(1 for record in records if record.passed_swing),
            passed_trigger=sum(1 for record in records if record.passed_trigger),
            regime_pass_rate_pct=sum(1 for record in records if record.passed_regime) / window_cycles * 100,
            swing_pass_rate_pct=sum(1 for record in records if record.passed_swing) / window_cycles * 100,
            trigger_pass_rate_pct=sum(1 for record in records if record.passed_trigger) / window_cycles * 100,
            top_blockers=top_blockers,
            dominant_blocking_layer=dominant_layer,
            dominant_blocking_label=dominant_label,
            dominant_blocking_count=dominant_count,
            blocking_layer_counts=layer_counts,
            latest_blocker_reason=latest.blocker_reason,
            latest_execution_obv_zscore=latest.execution_obv_zscore,
            latest_execution_obv_threshold=latest.execution_obv_threshold,
            latest_execution_price=self._latest_non_null(records, "execution_price"),
            latest_grid_center_gap=self._latest_non_null(records, "grid_center_gap"),
        )

    def _notify_summary(self, summary: FunnelWindowSummary) -> None:
        if self.notifier is None or not hasattr(self.notifier, "notify_signal_profiler_summary"):
            return
        if str(summary.dominant_blocking_layer or "").upper() == "PASSED":
            return
        if int(summary.dominant_blocking_count or 0) < max(1, int(self.min_blocking_count)):
            return
        try:
            self.notifier.notify_signal_profiler_summary(
                symbol=self.symbol,
                summary_interval=max(1, int(self.summary_interval)),
                summary=summary.as_notification_payload(),
            )
        except Exception:  # pragma: no cover
            logger.exception("Signal profiler summary notification failed")
