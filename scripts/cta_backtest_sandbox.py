#!/usr/bin/env python3
from __future__ import annotations

import csv
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from market_adaptive.config import CTAConfig, ExecutionConfig
from market_adaptive.db import MarketStatusRecord, StrategyRuntimeState
from market_adaptive.strategies.cta_robot import CTARobot

TIMEFRAME_TO_PANDAS = {
    '1m': '1min',
    '3m': '3min',
    '5m': '5min',
    '15m': '15min',
    '30m': '30min',
    '1h': '1h',
    '4h': '4h',
    '1d': '1d',
}

TIMEFRAME_TO_MS = {
    '1m': 60_000,
    '3m': 180_000,
    '5m': 300_000,
    '15m': 900_000,
    '30m': 1_800_000,
    '1h': 3_600_000,
    '4h': 14_400_000,
    '1d': 86_400_000,
}


@dataclass
class BacktestTrade:
    timestamp_ms: int
    side: str
    reduce_only: bool
    amount: float
    fill_price: float
    fee: float
    realized_pnl: float = 0.0
    note: str = ''


@dataclass
class BacktestReport:
    total_return_pct: float
    max_drawdown_abs: float
    max_drawdown_pct: float
    win_rate: float
    profit_factor: float
    total_trades: int
    starting_balance: float
    ending_equity: float
    fees_paid: float
    realized_pnl: float
    diagnostics: dict[str, Any] = field(default_factory=dict)


class BacktestDatabaseStub:
    def __init__(self, default_status: str = 'trend') -> None:
        self.default_status = default_status
        self.runtime_states: dict[tuple[str, str], StrategyRuntimeState] = {}
        self.current_timestamp_iso = '1970-01-01T00:00:00+00:00'

    def set_runtime_context(self, *, timestamp_iso: str, status: str | None = None) -> None:
        self.current_timestamp_iso = timestamp_iso
        if status is not None:
            self.default_status = status

    def fetch_latest_market_status(self, symbol: str):
        return MarketStatusRecord(
            timestamp=self.current_timestamp_iso,
            symbol=symbol,
            status=self.default_status,
            adx_value=25.0,
            volatility=0.01,
        )

    def get_strategy_runtime_state(self, strategy_name: str, symbol: str):
        return self.runtime_states.get((strategy_name, symbol))

    def upsert_strategy_runtime_state(self, state):
        self.runtime_states[(state.strategy_name, state.symbol)] = state
        return None

    def insert_trade_journal(self, record):
        del record
        return None


class MockExchangeClient:
    def __init__(
        self,
        *,
        starting_balance: float = 10_000.0,
        taker_fee_rate: float = 0.0004,
        slippage_rate: float = 0.0005,
        fill_mode: str = 'next_open',
        contract_value: float = 1.0,
        min_order_amount: float = 0.0,
    ) -> None:
        self.starting_balance = float(starting_balance)
        self.realized_pnl = 0.0
        self.fees_paid = 0.0
        self.taker_fee_rate = float(taker_fee_rate)
        self.slippage_rate = float(slippage_rate)
        self.fill_mode = str(fill_mode)
        self.contract_value = float(contract_value)
        self.min_order_amount = float(min_order_amount)
        self.market_orders: list[dict[str, Any]] = []
        self.limit_orders: list[dict[str, Any]] = []
        self.trades: list[BacktestTrade] = []
        self.current_symbol = 'BTC/USDT'
        self.current_time_ms = 0
        self.current_close = 0.0
        self.next_open = 0.0
        self.ohlcv_by_timeframe: dict[str, list[list[float]]] = {}
        self.net_position = 0.0
        self.avg_entry_price = 0.0

    def set_market_context(self, *, symbol: str, current_time_ms: int, current_close: float, next_open: float | None, ohlcv_by_timeframe: dict[str, list[list[float]]]) -> None:
        self.current_symbol = symbol
        self.current_time_ms = int(current_time_ms)
        self.current_close = float(current_close)
        self.next_open = float(next_open) if next_open is not None else float(current_close)
        self.ohlcv_by_timeframe = ohlcv_by_timeframe

    def fetch_ohlcv(self, symbol: str, timeframe: str = '15m', limit: int = 200, since=None):
        del symbol, since
        return list(self.ohlcv_by_timeframe.get(timeframe, []))[-limit:]

    def fetch_server_time(self) -> int | None:
        return int(self.current_time_ms)

    def fetch_last_price(self, symbol: str) -> float:
        del symbol
        return float(self.current_close)

    def fetch_order_book(self, symbol: str, limit: int | None = None):
        del symbol
        mid = float(self.current_close)
        spread = max(mid * 0.0001, 0.1)
        depth = max(3, limit or 3)
        bids = [[mid - spread - i * spread, 1.0] for i in range(depth)]
        asks = [[mid + spread + i * spread, 1.0] for i in range(depth)]
        if limit is None:
            return {'bids': bids, 'asks': asks}
        return {'bids': bids[:limit], 'asks': asks[:limit]}

    def cancel_all_orders(self, symbol: str):
        del symbol
        self.limit_orders = []
        return []

    def close_all_positions(self, symbol: str):
        del symbol
        return []

    def fetch_total_equity(self, quote_currency: str = 'USDT') -> float:
        del quote_currency
        return self.starting_balance + self.realized_pnl + self._unrealized_pnl(self.current_close)

    def get_min_order_amount(self, symbol: str) -> float:
        del symbol
        return self.min_order_amount

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        del symbol
        return round(float(amount), 8)

    def price_to_precision(self, symbol: str, price: float) -> float:
        del symbol
        return round(float(price), 8)

    def estimate_notional(self, symbol: str, amount: float, price: float) -> float:
        del symbol
        return abs(float(amount)) * abs(float(price)) * self.contract_value

    def place_market_order(self, symbol: str, side: str, amount: float, **kwargs):
        del symbol
        fill_price = self._resolve_fill_price(side)
        filled = float(amount)
        fee = abs(filled * fill_price * self.contract_value) * self.taker_fee_rate
        trade = self._apply_fill(side=side, amount=filled, fill_price=fill_price, reduce_only=bool(kwargs.get('reduce_only', False)), note='market')
        trade.fee = fee
        self.fees_paid += fee
        self.trades.append(trade)
        payload = {
            'id': f'mkt-{len(self.market_orders)+1}',
            'side': side,
            'amount': filled,
            'filled': filled,
            'average': fill_price,
            'price': fill_price,
            'status': 'closed',
            'reduceOnly': bool(kwargs.get('reduce_only', False)),
            'info': {'fee': fee, **kwargs},
        }
        self.market_orders.append(payload)
        return payload

    def place_limit_order(self, symbol: str, side: str, amount: float, price: float, **kwargs):
        del symbol
        fill_price = self._resolve_fill_price(side, preferred_price=float(price))
        filled = float(amount)
        fee = abs(filled * fill_price * self.contract_value) * self.taker_fee_rate
        trade = self._apply_fill(side=side, amount=filled, fill_price=fill_price, reduce_only=bool(kwargs.get('reduce_only', False)), note='limit')
        trade.fee = fee
        self.fees_paid += fee
        self.trades.append(trade)
        payload = {
            'id': f'lmt-{len(self.limit_orders)+1}',
            'side': side,
            'amount': filled,
            'filled': filled,
            'average': fill_price,
            'price': float(price),
            'status': 'closed',
            'reduceOnly': bool(kwargs.get('reduce_only', False)),
            'info': {'fee': fee, **kwargs},
        }
        self.limit_orders.append(payload)
        return payload

    def _resolve_fill_price(self, side: str, preferred_price: float | None = None) -> float:
        base = self.next_open if self.fill_mode == 'next_open' else self.current_close
        if preferred_price is not None and self.fill_mode == 'current_close':
            base = preferred_price
        if side == 'buy':
            return float(base) * (1.0 + self.slippage_rate)
        return float(base) * (1.0 - self.slippage_rate)

    def _apply_fill(self, *, side: str, amount: float, fill_price: float, reduce_only: bool, note: str) -> BacktestTrade:
        amount = float(amount)
        fill_price = float(fill_price)
        realized = 0.0
        if side == 'buy' and not reduce_only:
            if self.net_position >= 0:
                total_cost = (self.avg_entry_price * self.net_position) + (fill_price * amount)
                self.net_position += amount
                self.avg_entry_price = total_cost / self.net_position if self.net_position else 0.0
            else:
                close_qty = min(amount, abs(self.net_position))
                realized += (self.avg_entry_price - fill_price) * close_qty * self.contract_value
                self.net_position += close_qty
                remaining_open = amount - close_qty
                if abs(self.net_position) < 1e-12:
                    self.net_position = 0.0
                    self.avg_entry_price = 0.0
                if remaining_open > 0:
                    self.net_position += remaining_open
                    self.avg_entry_price = fill_price
        elif side == 'sell' and not reduce_only:
            if self.net_position <= 0:
                total_cost = (self.avg_entry_price * abs(self.net_position)) + (fill_price * amount)
                self.net_position -= amount
                self.avg_entry_price = total_cost / abs(self.net_position) if self.net_position else 0.0
            else:
                close_qty = min(amount, self.net_position)
                realized += (fill_price - self.avg_entry_price) * close_qty * self.contract_value
                self.net_position -= close_qty
                remaining_open = amount - close_qty
                if abs(self.net_position) < 1e-12:
                    self.net_position = 0.0
                    self.avg_entry_price = 0.0
                if remaining_open > 0:
                    self.net_position -= remaining_open
                    self.avg_entry_price = fill_price
        elif side == 'sell' and reduce_only:
            close_qty = min(amount, max(self.net_position, 0.0))
            realized += (fill_price - self.avg_entry_price) * close_qty * self.contract_value
            self.net_position -= close_qty
            if abs(self.net_position) < 1e-12:
                self.net_position = 0.0
                self.avg_entry_price = 0.0
        elif side == 'buy' and reduce_only:
            close_qty = min(amount, abs(min(self.net_position, 0.0)))
            realized += (self.avg_entry_price - fill_price) * close_qty * self.contract_value
            self.net_position += close_qty
            if abs(self.net_position) < 1e-12:
                self.net_position = 0.0
                self.avg_entry_price = 0.0

        self.realized_pnl += realized
        return BacktestTrade(timestamp_ms=self.current_time_ms, side=side, reduce_only=reduce_only, amount=amount, fill_price=fill_price, fee=0.0, realized_pnl=realized, note=note)

    def _unrealized_pnl(self, mark_price: float) -> float:
        if abs(self.net_position) < 1e-12:
            return 0.0
        if self.net_position > 0:
            return (float(mark_price) - self.avg_entry_price) * self.net_position * self.contract_value
        return (self.avg_entry_price - float(mark_price)) * abs(self.net_position) * self.contract_value


class CTABacktester:
    def __init__(self, df: pd.DataFrame, *, cta_config: CTAConfig, execution_config: ExecutionConfig, symbol: str = 'BTC/USDT', starting_balance: float = 10_000.0, taker_fee_rate: float = 0.0004, slippage_rate: float = 0.0005, fill_mode: str = 'next_open', warmup_bars: int = 400) -> None:
        self.df = self._normalize_dataframe(df)
        self.cta_config = cta_config
        self.execution_config = execution_config
        self.symbol = symbol
        self.client = MockExchangeClient(starting_balance=starting_balance, taker_fee_rate=taker_fee_rate, slippage_rate=slippage_rate, fill_mode=fill_mode)
        self.database = BacktestDatabaseStub(default_status='trend')
        self.robot = CTARobot(client=self.client, database=self.database, config=self.cta_config, execution_config=self.execution_config, notifier=None, risk_manager=None, sentiment_analyst=None, runtime_context=None, signal_profiler=None, grid_center_provider=None)
        self.warmup_bars = self._resolve_required_warmup_bars(int(warmup_bars))
        self.equity_curve: list[tuple[int, float]] = []
        self.actions: list[tuple[int, str]] = []
        self.diagnostics: dict[str, Any] = {
            'heartbeats': 0,
            'waiting_drift': 0,
            'waiting_near_breakout': 0,
            'waiting_recovery_probe': 0,
            'blocked_obv': 0,
            'blocked_obv_below_sma': 0,
            'blocked_rsi_threshold': 0,
            'blocked_order_flow': 0,
            'raw_directional': 0,
            'directional_ready': 0,
            'opened_actions': 0,
            'opened_obv_scalp_actions': 0,
            'closed_actions': 0,
            'quality_tier_counts': {},
            'entry_pathway_counts': {},
            'opened_by_pathway': {},
            'opened_by_quality_tier': {},
            'trigger_family_funnel': {},
            'signal_samples': [],
        }

    def _bump_trigger_family_metric(self, family: str, metric: str) -> None:
        normalized_family = str(family or '').strip() or 'waiting'
        if normalized_family == 'waiting':
            return
        bucket = self.diagnostics['trigger_family_funnel'].setdefault(normalized_family, {})
        bucket[metric] = int(bucket.get(metric, 0) or 0) + 1

    def _resolve_required_warmup_bars(self, requested_warmup_bars: int) -> int:
        minimum_bars = int(getattr(self.robot.mtf_engine, 'minimum_bars', 100))
        major_ms = TIMEFRAME_TO_MS[str(self.cta_config.major_timeframe).lower()]
        base_ms = self._infer_base_interval_ms()
        ratio = max(1, major_ms // base_ms)
        derived = minimum_bars * ratio
        return max(100, int(requested_warmup_bars), int(derived))

    def _infer_base_interval_ms(self) -> int:
        if len(self.df) < 2:
            return TIMEFRAME_TO_MS[str(self.cta_config.execution_timeframe).lower()]
        diffs = self.df['timestamp'].diff().dropna()
        diffs = diffs[diffs > 0]
        if diffs.empty:
            return TIMEFRAME_TO_MS[str(self.cta_config.execution_timeframe).lower()]
        return int(diffs.mode().iloc[0])

    def run(self) -> BacktestReport:
        for current_index in range(self.warmup_bars, len(self.df)):
            current_slice = self.df.iloc[: current_index + 1].copy()
            current_row = self.df.iloc[current_index]
            next_row = self.df.iloc[current_index + 1] if current_index + 1 < len(self.df) else None
            ohlcv_payload = self._build_ohlcv_payloads(current_slice)
            self.client.set_market_context(symbol=self.symbol, current_time_ms=int(current_row['timestamp']), current_close=float(current_row['close']), next_open=float(next_row['open']) if next_row is not None else float(current_row['close']), ohlcv_by_timeframe=ohlcv_payload)
            signal = None
            try:
                signal = self.robot._build_trend_signal()
            except Exception:
                signal = None
            self._collect_diagnostics(signal)
            previous_position = self.robot.position
            action = self.robot.execute_active_cycle()
            self._collect_action_diagnostics(action, signal=signal, previous_position=previous_position)
            self.actions.append((int(current_row['timestamp']), action))
            equity = self.client.fetch_total_equity('USDT')
            self.equity_curve.append((int(current_row['timestamp']), equity))
        return self._build_report()

    def _collect_diagnostics(self, signal) -> None:
        if signal is None:
            return
        self.diagnostics['heartbeats'] += 1
        reason = str(getattr(signal, 'execution_trigger_reason', '') or '')
        blocker = str(getattr(signal, 'blocker_reason', '') or '')
        raw_direction = int(getattr(signal, 'raw_direction', 0) or 0)
        direction = int(getattr(signal, 'direction', 0) or 0)
        quality_tier = str(getattr(getattr(signal, 'signal_quality_tier', None), 'name', getattr(signal, 'signal_quality_tier', 'TIER_LOW')) or 'TIER_LOW')
        entry_pathway = str(getattr(getattr(signal, 'entry_pathway', None), 'name', getattr(signal, 'entry_pathway', 'STRICT')) or 'STRICT')
        trigger_family = str(getattr(signal, 'execution_trigger_family', '') or '')
        if raw_direction != 0:
            self._bump_trigger_family_metric(trigger_family, 'appeared')
        if reason == 'waiting_execution_trigger_drift':
            self.diagnostics['waiting_drift'] += 1
        elif reason == 'waiting_execution_trigger_near_breakout':
            self.diagnostics['waiting_near_breakout'] += 1
        elif reason == 'waiting_execution_trigger_recovery_probe':
            self.diagnostics['waiting_recovery_probe'] += 1
        if blocker == 'Blocked_By_OBV_STRENGTH_NOT_CONFIRMED':
            self.diagnostics['blocked_obv'] += 1
        if blocker == 'Blocked_By_OBV_BELOW_SMA':
            self.diagnostics['blocked_obv_below_sma'] += 1
        if blocker == 'Blocked_By_RSI_Threshold':
            self.diagnostics['blocked_rsi_threshold'] += 1
        if 'order_flow_blocked' in blocker:
            self.diagnostics['blocked_order_flow'] += 1
        if raw_direction != 0:
            self.diagnostics['raw_directional'] += 1
        if direction != 0:
            self.diagnostics['directional_ready'] += 1
        if blocker and blocker != 'PASSED':
            self._bump_trigger_family_metric(trigger_family, 'blocked')
        quality_counts = self.diagnostics['quality_tier_counts']
        quality_counts[quality_tier] = quality_counts.get(quality_tier, 0) + 1
        pathway_counts = self.diagnostics['entry_pathway_counts']
        pathway_counts[entry_pathway] = pathway_counts.get(entry_pathway, 0) + 1
        if raw_direction != 0 and len(self.diagnostics['signal_samples']) < 12:
            self.diagnostics['signal_samples'].append({
                'ts': int(self.client.current_time_ms),
                'price': float(getattr(signal, 'price', 0.0) or 0.0),
                'raw_direction': raw_direction,
                'direction': direction,
                'reason': reason,
                'blocker': blocker,
                'quick_trade_mode': bool(getattr(signal, 'quick_trade_mode', False)),
                'entry_mode': str(getattr(signal, 'execution_entry_mode', '')),
                'quality_tier': quality_tier,
                'entry_pathway': entry_pathway,
                'signal_confidence': float(getattr(signal, 'signal_confidence', 0.0) or 0.0),
                'family': str(getattr(signal, 'execution_trigger_family', '')),
                'bullish_score': float(getattr(signal, 'bullish_score', 0.0) or 0.0),
                'bearish_score': float(getattr(signal, 'bearish_score', 0.0) or 0.0),
            })

    def _collect_action_diagnostics(self, action: str, signal=None, previous_position=None) -> None:
        if action.startswith('cta:open_'):
            self.diagnostics['opened_actions'] += 1
            if 'obv_scalp' in action:
                self.diagnostics['opened_obv_scalp_actions'] += 1
            if signal is not None:
                entry_pathway = str(getattr(getattr(signal, 'entry_pathway', None), 'name', getattr(signal, 'entry_pathway', 'STRICT')) or 'STRICT')
                quality_tier = str(getattr(getattr(signal, 'signal_quality_tier', None), 'name', getattr(signal, 'signal_quality_tier', 'TIER_LOW')) or 'TIER_LOW')
                opened_by_pathway = self.diagnostics['opened_by_pathway']
                opened_by_pathway[entry_pathway] = opened_by_pathway.get(entry_pathway, 0) + 1
                opened_by_quality = self.diagnostics['opened_by_quality_tier']
                opened_by_quality[quality_tier] = opened_by_quality.get(quality_tier, 0) + 1
                trigger_family = str(getattr(signal, 'execution_trigger_family', '') or '')
                self._bump_trigger_family_metric(trigger_family, 'opened')
        if 'all_out' in action or action.startswith('cta:signal_flip_exit'):
            self.diagnostics['closed_actions'] += 1
            trigger_family = str(getattr(previous_position, 'origin_trigger_family', '') or '')
            self._bump_trigger_family_metric(trigger_family, 'closed')
        if action.startswith('cta:order_flow_blocked'):
            self.diagnostics['blocked_order_flow'] += 1

    def _build_report(self) -> BacktestReport:
        if not self.equity_curve:
            return BacktestReport(0.0, 0.0, 0.0, 0.0, 0.0, 0, self.client.starting_balance, self.client.starting_balance, 0.0, 0.0, diagnostics=self.diagnostics)
        start_equity = float(self.client.starting_balance)
        end_equity = float(self.equity_curve[-1][1])
        total_return_pct = ((end_equity / start_equity) - 1.0) * 100.0 if start_equity > 0 else 0.0
        peak = self.equity_curve[0][1]
        max_drawdown_abs = 0.0
        max_drawdown_pct = 0.0
        for _, equity in self.equity_curve:
            if equity > peak:
                peak = equity
            dd_abs = peak - equity
            dd_pct = (dd_abs / peak * 100.0) if peak > 0 else 0.0
            max_drawdown_abs = max(max_drawdown_abs, dd_abs)
            max_drawdown_pct = max(max_drawdown_pct, dd_pct)
        closed_trades = [trade for trade in self.client.trades if trade.reduce_only]
        total_trades = len(closed_trades)
        wins = [trade for trade in closed_trades if trade.realized_pnl > 0]
        losses = [trade for trade in closed_trades if trade.realized_pnl < 0]
        win_rate = (len(wins) / total_trades * 100.0) if total_trades > 0 else 0.0
        gross_profit = sum(trade.realized_pnl for trade in wins)
        gross_loss = abs(sum(trade.realized_pnl for trade in losses))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float('inf') if gross_profit > 0 else 0.0)
        return BacktestReport(total_return_pct, max_drawdown_abs, max_drawdown_pct, win_rate, profit_factor, total_trades, start_equity, end_equity, self.client.fees_paid, self.client.realized_pnl, diagnostics=self.diagnostics)

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        required = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f'Input DataFrame missing required columns: {sorted(missing)}')
        out = df.copy().sort_values('timestamp').drop_duplicates(subset=['timestamp']).reset_index(drop=True)
        out['datetime'] = pd.to_datetime(out['timestamp'], unit='ms', utc=True)
        return out

    def _build_ohlcv_payloads(self, current_slice: pd.DataFrame) -> dict[str, list[list[float]]]:
        indexed = current_slice.set_index('datetime')
        payloads: dict[str, list[list[float]]] = {}
        for tf in {self.cta_config.major_timeframe, self.cta_config.swing_timeframe, self.cta_config.execution_timeframe}:
            freq = TIMEFRAME_TO_PANDAS[str(tf).lower()]
            resampled = indexed.resample(freq, label='right', closed='right').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna().reset_index()
            payloads[tf] = [[int(row['datetime'].timestamp()*1000), float(row['open']), float(row['high']), float(row['low']), float(row['close']), float(row['volume'])] for _, row in resampled.iterrows()]
        return payloads


def load_csv(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    with p.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        rows = [{'timestamp': int(row['timestamp']), 'open': float(row['open']), 'high': float(row['high']), 'low': float(row['low']), 'close': float(row['close']), 'volume': float(row['volume'])} for row in reader]
    return pd.DataFrame(rows)


if __name__ == '__main__':
    csv_path = Path('data/okx/BTC-USDT-SWAP/1m.csv')
    df = load_csv(csv_path)
    # Limit to last 20000 bars for faster backtest
    df = df.tail(20000).reset_index(drop=True)
    cta_config = CTAConfig(symbol='BTC/USDT')
    execution_config = ExecutionConfig()
    backtester = CTABacktester(df, cta_config=cta_config, execution_config=execution_config, symbol='BTC/USDT', starting_balance=10_000.0, taker_fee_rate=0.0004, slippage_rate=0.0005, fill_mode='next_open', warmup_bars=200)
    print(f'Derived warmup_bars: {backtester.warmup_bars}')
    report = backtester.run()
    print('=== CTA Backtest Report ===')
    print(f'Total Return: {report.total_return_pct:.2f}%')
    print(f'Max Drawdown: {report.max_drawdown_abs:.2f} ({report.max_drawdown_pct:.2f}%)')
    print(f'Win Rate: {report.win_rate:.2f}%')
    print(f'Profit Factor: {report.profit_factor:.2f}')
    print(f'Total Trades: {report.total_trades}')
    print(f'Starting Balance: {report.starting_balance:.2f}')
    print(f'Ending Equity: {report.ending_equity:.2f}')
    print(f'Fees Paid: {report.fees_paid:.2f}')
    print(f'Realized PnL: {report.realized_pnl:.2f}')
    print('=== CTA Funnel Diagnostics ===')
    for key in ['heartbeats','waiting_drift','waiting_near_breakout','waiting_recovery_probe','blocked_obv','blocked_obv_below_sma','blocked_rsi_threshold','blocked_order_flow','raw_directional','directional_ready','opened_actions','opened_obv_scalp_actions','closed_actions']:
        print(f'{key}: {report.diagnostics.get(key)}')
    print('signal_samples:', report.diagnostics.get('signal_samples'))
