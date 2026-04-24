from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from market_adaptive.ml_signal_engine import MarketAdaptiveMLEngine, MarketAdaptiveModelTrainer


class MLSignalEngineTests(unittest.TestCase):
    def _build_frame(self, rows: int = 240) -> pd.DataFrame:
        candles: list[dict[str, float]] = []
        price = 100.0
        for i in range(rows):
            drift = 0.6 if (i % 12) < 7 else -0.45
            price += drift
            candles.append(
                {
                    'open': price - 0.2,
                    'high': price + 0.6,
                    'low': price - 0.7,
                    'close': price,
                    'volume': 1000.0 + ((i % 9) * 35.0),
                }
            )
        return pd.DataFrame(candles)

    def test_trainer_persists_bundle_and_engine_evaluates(self) -> None:
        frame = self._build_frame()
        with tempfile.TemporaryDirectory() as tmpdir:
            trainer = MarketAdaptiveModelTrainer(model_path=tmpdir)
            metrics = trainer.train(symbol='BTC/USDT', historical_data=frame, label_horizon=3, min_return_threshold=0.002)

            self.assertGreater(metrics['dataset_samples'], 120)
            self.assertEqual(metrics['dataset_samples'], metrics['positive_labels'] + metrics['negative_labels'])
            self.assertIn('trained_at', metrics)

            metrics_path = Path(tmpdir) / 'BTC_USDT_metrics.json'
            model_path = Path(tmpdir) / 'BTC_USDT_model.pkl'
            self.assertTrue(metrics_path.exists())
            self.assertTrue(model_path.exists())

            on_disk_metrics = json.loads(metrics_path.read_text(encoding='utf-8'))
            self.assertEqual(on_disk_metrics['bundle_version'], 1)
            self.assertEqual(on_disk_metrics['dataset_samples'], metrics['dataset_samples'])

            engine = MarketAdaptiveMLEngine(enabled=True, model_path=tmpdir)
            decision = engine.evaluate(symbol='BTC/USDT', execution_frame=frame.tail(120), direction=1, min_confidence=0.0)
            self.assertTrue(decision.used_model)
            self.assertIn(decision.prediction, {0, 1})
            self.assertGreaterEqual(decision.probability_up, 0.0)
            self.assertLessEqual(decision.probability_up, 1.0)

    def test_trainer_drops_unlabeled_tail_rows_before_metrics(self) -> None:
        frame = self._build_frame(rows=220)
        horizon = 5
        with tempfile.TemporaryDirectory() as tmpdir:
            trainer = MarketAdaptiveModelTrainer(model_path=tmpdir)
            metrics = trainer.train(symbol='ETH/USDT', historical_data=frame, label_horizon=horizon, min_return_threshold=0.002)

            cleaned = trainer._validate_training_frame(frame)
            feats = MarketAdaptiveMLEngine(enabled=True, model_path=tmpdir)._calc_feature_frame(cleaned)
            future_return = cleaned['close'].shift(-horizon) / cleaned['close'] - 1.0
            expected_dataset = pd.concat([
                feats[[
                    'RSI', 'MACD', 'MACD_signal', 'MACD_hist',
                    'BB_upper', 'BB_lower', 'BB_position',
                    'MA5_ratio', 'trend_gap', 'volume_ratio', 'returns', 'returns_3d', 'returns_5d',
                    'volatility', 'momentum', 'atr_ratio',
                ]],
                future_return.rename('future_return'),
            ], axis=1).iloc[:-horizon].dropna().reset_index(drop=True)

            self.assertEqual(metrics['dataset_samples'], len(expected_dataset))


if __name__ == '__main__':
    unittest.main()
