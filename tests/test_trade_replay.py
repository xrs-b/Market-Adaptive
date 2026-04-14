from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "analyze_trade_opportunities.py"
spec = importlib.util.spec_from_file_location("analyze_trade_opportunities", SCRIPT_PATH)
assert spec is not None and spec.loader is not None
analyze_trade_opportunities = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = analyze_trade_opportunities
spec.loader.exec_module(analyze_trade_opportunities)


class StopReplay(Exception):
    pass


class ReplayExecutionCandlePreferenceTests(unittest.TestCase):
    def test_replay_cta_respects_execution_candle_preference(self) -> None:
        cfg = SimpleNamespace(
            okx=SimpleNamespace(),
            execution=SimpleNamespace(),
            cta=SimpleNamespace(
                symbol="BTC/USDT",
                major_timeframe="4h",
                swing_timeframe="1h",
                execution_timeframe="15m",
                prefer_closed_major_timeframe_candles=True,
                prefer_closed_swing_timeframe_candles=True,
                prefer_closed_execution_timeframe_candles=False,
            ),
            market_oracle=SimpleNamespace(
                higher_timeframe="1h",
                lower_timeframe="15m",
                prefer_closed_higher_timeframe_candles=True,
                prefer_closed_lower_timeframe_candles=False,
            ),
            sentiment=SimpleNamespace(timeframe="5m"),
        )
        recorded_calls: list[tuple[str, bool]] = []

        def fake_fetch_ohlcv_df(client, symbol, timeframe, *, limit_per_call=200, prefer_closed=True):
            del client, symbol, limit_per_call
            recorded_calls.append((timeframe, prefer_closed))
            if len(recorded_calls) >= 5:
                raise StopReplay
            return pd.DataFrame({"timestamp": []})

        with (
            patch.object(analyze_trade_opportunities, "load_config", return_value=cfg),
            patch.object(analyze_trade_opportunities, "OKXClient"),
            patch.object(analyze_trade_opportunities, "fetch_ohlcv_df", side_effect=fake_fetch_ohlcv_df),
        ):
            with self.assertRaises(StopReplay):
                analyze_trade_opportunities.replay_cta(Path("config/config.yaml"), hours=24)

        self.assertEqual(
            recorded_calls,
            [
                ("4h", True),
                ("1h", True),
                ("15m", False),
                ("1h", True),
                ("15m", False),
            ],
        )


if __name__ == "__main__":
    unittest.main()
