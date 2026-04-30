#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_adaptive.config import CTAConfig, ExecutionConfig
from scripts.cta_backtest_sandbox import CTABacktester, load_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run CTA backtest and emit trade-quality bucket report.')
    parser.add_argument('--csv', required=True, help='OHLCV csv path with timestamp/open/high/low/close/volume columns')
    parser.add_argument('--symbol', default='BTC/USDT')
    parser.add_argument('--tail', type=int, default=20000)
    parser.add_argument('--warmup-bars', type=int, default=200)
    parser.add_argument('--starting-balance', type=float, default=10000.0)
    parser.add_argument('--taker-fee-rate', type=float, default=0.0004)
    parser.add_argument('--slippage-rate', type=float, default=0.0005)
    parser.add_argument('--fill-mode', default='next_open', choices=['next_open', 'current_close'])
    parser.add_argument('--output', help='Optional JSON output path')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    df = load_csv(args.csv)
    if args.tail and args.tail > 0:
        df = df.tail(args.tail).reset_index(drop=True)

    cta_config = CTAConfig(symbol=args.symbol)
    execution_config = ExecutionConfig()
    backtester = CTABacktester(
        df,
        cta_config=cta_config,
        execution_config=execution_config,
        symbol=args.symbol,
        starting_balance=args.starting_balance,
        taker_fee_rate=args.taker_fee_rate,
        slippage_rate=args.slippage_rate,
        fill_mode=args.fill_mode,
        warmup_bars=args.warmup_bars,
    )
    report = backtester.run()
    payload = {
        'summary': {
            'total_return_pct': report.total_return_pct,
            'max_drawdown_abs': report.max_drawdown_abs,
            'max_drawdown_pct': report.max_drawdown_pct,
            'win_rate': report.win_rate,
            'profit_factor': report.profit_factor,
            'total_trades': report.total_trades,
            'starting_balance': report.starting_balance,
            'ending_equity': report.ending_equity,
            'fees_paid': report.fees_paid,
            'realized_pnl': report.realized_pnl,
        },
        'diagnostics': report.diagnostics,
    }

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(text, encoding='utf-8')
    print(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
