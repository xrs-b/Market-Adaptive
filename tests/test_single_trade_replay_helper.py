from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "replay_single_trade_timestamp.py"
spec = importlib.util.spec_from_file_location("replay_single_trade_timestamp", SCRIPT_PATH)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


class PartialExecutionAggregationTests(unittest.TestCase):
    def test_partial_execution_excludes_incomplete_target_minute(self) -> None:
        ts = pd.date_range("2026-04-13T22:00:00Z", periods=7, freq="1min")
        frame = pd.DataFrame(
            {
                "timestamp": ts,
                "open": [100, 101, 102, 103, 104, 105, 106],
                "high": [101, 102, 103, 104, 105, 106, 107],
                "low": [99, 100, 101, 102, 103, 104, 105],
                "close": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5],
                "volume": [1, 2, 3, 4, 5, 6, 7],
            }
        )

        result = module.build_partial_execution_frame(
            frame,
            execution_timeframe="15m",
            target_ts=pd.Timestamp("2026-04-13T22:05:51Z"),
            lookback_limit=200,
        )

        self.assertEqual(len(result), 1)
        row = result.iloc[0]
        self.assertEqual(row["timestamp"], pd.Timestamp("2026-04-13T22:00:00Z"))
        self.assertEqual(row["open"], 100)
        self.assertEqual(row["high"], 105)
        self.assertEqual(row["low"], 99)
        self.assertEqual(row["close"], 104.5)
        self.assertEqual(row["volume"], 15)


if __name__ == "__main__":
    unittest.main()
