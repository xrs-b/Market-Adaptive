from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from statistics import pstdev
from typing import Any

from market_adaptive.config import CTAConfig


@dataclass
class OrderFlowAssessment:
    symbol: str
    side: str
    depth_levels: int
    bid_sum: float
    ask_sum: float
    imbalance_ratio: float
    best_bid: float | None
    best_ask: float | None
    confirmation_passed: bool
    high_conviction: bool
    recommended_limit_price: float | None
    expected_average_price: float | None
    depth_boundary_price: float | None
    reason: str

    @property
    def entry_allowed(self) -> bool:
        return self.confirmation_passed

    @property
    def use_limit_order(self) -> bool:
        return self.high_conviction and self.recommended_limit_price is not None

    @property
    def reference_price(self) -> float | None:
        return self.best_ask if self.side == "buy" else self.best_bid


class OrderFlowSentinel:
    """Order-book based CTA entry guard and slippage-aware limit price helper."""

    def __init__(self, client: Any, config: CTAConfig) -> None:
        self.client = client
        self.config = config
        self._imbalance_history: deque[float] = deque(maxlen=max(1, int(getattr(config, "order_flow_history_window", 20))))

    def assess_entry(self, symbol: str, side: str, amount: float) -> OrderFlowAssessment:
        depth_levels = max(1, int(self.config.order_flow_depth_levels))
        normalized_side = str(side).strip().lower()
        if normalized_side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported order-book side: {side}")

        order_book = self.client.fetch_order_book(symbol, limit=depth_levels)
        bids = self._normalize_levels(order_book.get("bids"), depth_levels)
        asks = self._normalize_levels(order_book.get("asks"), depth_levels)
        bid_sum = sum(size for _, size in bids)
        ask_sum = sum(size for _, size in asks)
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None

        if not bids or not asks:
            return OrderFlowAssessment(
                symbol=symbol,
                side=normalized_side,
                depth_levels=depth_levels,
                bid_sum=bid_sum,
                ask_sum=ask_sum,
                imbalance_ratio=0.0,
                best_bid=best_bid,
                best_ask=best_ask,
                confirmation_passed=False,
                high_conviction=False,
                recommended_limit_price=None,
                expected_average_price=None,
                depth_boundary_price=None,
                reason="empty_order_book",
            )

        imbalance_ratio = self._compute_imbalance_ratio(
            bid_sum=bid_sum,
            ask_sum=ask_sum,
            side=normalized_side,
        )
        history_mean = self._history_mean()
        history_sigma = self._history_sigma()
        health_floor = history_mean + (history_sigma * max(0.0, float(getattr(self.config, "order_flow_health_sigma_multiplier", 1.0))))
        decay_detected = self._is_decay_detected(imbalance_ratio)
        confirmation_threshold = max(0.0, float(self.config.order_flow_confirmation_ratio), health_floor)
        confirmation_passed = imbalance_ratio >= confirmation_threshold and not decay_detected
        high_conviction_threshold = max(
            confirmation_threshold,
            float(self.config.order_flow_high_conviction_ratio),
        )
        high_conviction = confirmation_passed and imbalance_ratio >= high_conviction_threshold

        expected_average_price = None
        depth_boundary_price = None
        recommended_limit_price = None
        if high_conviction:
            expected_average_price, depth_boundary_price = self._estimate_depth_price(
                levels=asks if normalized_side == "buy" else bids,
                amount=max(0.0, float(amount)),
                side=normalized_side,
            )
            recommended_limit_price = self._build_limit_price(
                best_price=best_ask if normalized_side == "buy" else best_bid,
                boundary_price=depth_boundary_price,
                side=normalized_side,
                symbol=symbol,
            )

        reason = "confirmed"
        if decay_detected:
            reason = "imbalance_decay_detected"
        elif not confirmation_passed:
            reason = f"imbalance_below_{confirmation_threshold:.2f}"
        elif high_conviction and recommended_limit_price is None:
            reason = "missing_depth_price"
        elif high_conviction:
            reason = "confirmed_high_conviction"

        self._record_imbalance(imbalance_ratio)

        return OrderFlowAssessment(
            symbol=symbol,
            side=normalized_side,
            depth_levels=depth_levels,
            bid_sum=bid_sum,
            ask_sum=ask_sum,
            imbalance_ratio=imbalance_ratio,
            best_bid=best_bid,
            best_ask=best_ask,
            confirmation_passed=confirmation_passed,
            high_conviction=high_conviction,
            recommended_limit_price=recommended_limit_price,
            expected_average_price=expected_average_price,
            depth_boundary_price=depth_boundary_price,
            reason=reason,
        )

    @staticmethod
    def _normalize_levels(levels: Any, depth_levels: int) -> list[tuple[float, float]]:
        normalized: list[tuple[float, float]] = []
        for level in list(levels or [])[:depth_levels]:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price = float(level[0])
            size = abs(float(level[1]))
            if price <= 0 or size <= 0:
                continue
            normalized.append((price, size))
        return normalized

    @staticmethod
    def _compute_imbalance_ratio(*, bid_sum: float, ask_sum: float, side: str) -> float:
        epsilon = 1e-12
        if side == "buy":
            if ask_sum <= epsilon:
                return float("inf") if bid_sum > epsilon else 0.0
            return bid_sum / ask_sum
        if bid_sum <= epsilon:
            return float("inf") if ask_sum > epsilon else 0.0
        return ask_sum / bid_sum

    def _record_imbalance(self, imbalance_ratio: float) -> None:
        if imbalance_ratio >= 0:
            self._imbalance_history.append(float(imbalance_ratio))

    def _history_mean(self) -> float:
        if not self._imbalance_history:
            return 0.0
        return sum(self._imbalance_history) / len(self._imbalance_history)

    def _history_sigma(self) -> float:
        if len(self._imbalance_history) < 2:
            return 0.0
        return float(pstdev(self._imbalance_history))

    def _is_decay_detected(self, current_ratio: float) -> bool:
        lookback = max(1, int(getattr(self.config, "order_flow_decay_lookback", 3)))
        if len(self._imbalance_history) < lookback:
            return False
        recent = list(self._imbalance_history)[-lookback:]
        if not recent:
            return False
        trend = recent + [float(current_ratio)]
        return all(left > right for left, right in zip(trend, trend[1:]))

    @staticmethod
    def _estimate_depth_price(
        *,
        levels: list[tuple[float, float]],
        amount: float,
        side: str,
    ) -> tuple[float | None, float | None]:
        if amount <= 0 or not levels:
            return None, None

        remaining = amount
        weighted_total = 0.0
        executed = 0.0
        boundary_price = None
        for price, size in levels:
            taken = min(size, remaining)
            if taken <= 0:
                continue
            weighted_total += price * taken
            executed += taken
            remaining -= taken
            boundary_price = price
            if remaining <= 1e-12:
                break

        if executed <= 0 or boundary_price is None:
            return None, None

        average_price = weighted_total / executed
        if side == "sell":
            average_price = min(average_price, levels[0][0])
        else:
            average_price = max(average_price, levels[0][0])
        return average_price, boundary_price

    def _build_limit_price(
        self,
        *,
        best_price: float | None,
        boundary_price: float | None,
        side: str,
        symbol: str,
    ) -> float | None:
        if best_price is None or boundary_price is None:
            return None

        buffer_multiplier = 1.0 + max(0.0, float(self.config.order_flow_limit_buffer_bps)) / 10_000.0
        max_slippage_multiplier = 1.0 + max(0.0, float(self.config.order_flow_max_slippage_bps)) / 10_000.0

        if side == "buy":
            raw_price = max(best_price, boundary_price * buffer_multiplier)
            capped_price = min(raw_price, best_price * max_slippage_multiplier)
        else:
            raw_price = min(best_price, boundary_price / buffer_multiplier)
            capped_price = max(raw_price, best_price / max_slippage_multiplier)

        if hasattr(self.client, "price_to_precision"):
            return float(self.client.price_to_precision(symbol, capped_price))
        return float(capped_price)
