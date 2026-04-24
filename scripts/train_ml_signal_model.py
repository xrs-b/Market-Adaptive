#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_adaptive.ml_signal_engine import MarketAdaptiveModelTrainer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and persist the Market-Adaptive ML signal model from OHLCV CSV data.")
    parser.add_argument("--symbol", default="BTC/USDT", help="Trading symbol used for the saved model name")
    parser.add_argument("--csv", required=True, help="Input CSV with open/high/low/close/volume columns")
    parser.add_argument("--model-path", default="Market-Adaptive/data/ml_models", help="Directory for persisted model bundle + metrics")
    parser.add_argument("--label-horizon", type=int, default=3, help="Bars ahead used for labeling")
    parser.add_argument("--min-return-threshold", type=float, default=0.002, help="Minimum forward return required to label class=1")
    parser.add_argument("--tail", type=int, default=0, help="Optional: only use the last N rows from the CSV")
    return parser.parse_args()


def load_csv(path: Path, *, tail: int = 0) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if tail and tail > 0:
        frame = frame.tail(int(tail)).reset_index(drop=True)
    return frame


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    frame = load_csv(csv_path, tail=args.tail)
    trainer = MarketAdaptiveModelTrainer(model_path=args.model_path)
    metrics = trainer.train(
        symbol=args.symbol,
        historical_data=frame,
        label_horizon=args.label_horizon,
        min_return_threshold=args.min_return_threshold,
    )
    print(json.dumps({
        'csv': str(csv_path),
        'model_path': str(Path(args.model_path).expanduser().resolve()),
        'metrics': metrics,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
