#!/usr/bin/env python3
"""
permit + replay + ML gate 半集成闭环测试

测试路径:
  1. 从 OKX 拉取真实 BTC/USDT 历史数据 (4h/1h/15m/1m)
  2. 对齐时间轴，构建 IntrabarReplayFrames
  3. 对多个目标时间点做 intrabar scan (逐根1m bar重建15m信号)
  4. 对每个 TrendSignal 执行 EntryDeciderLite (permit层)
  5. 对每个 TrendSignal 跑 ML gate (MarketAdaptiveMLEngine)
  6. 对每个 TrendSignal 打印完整决策链摘要

用法:
  python scripts/test_permit_replay_ml_gate.py [--limit 200] [--bars 20]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

# 必须在导入 market_adaptive 之前设置 sys.path（和 run_main_controller.py 一样）
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("permit_replay_ml_gate_test")


# ── 1. 数据拉取 ──────────────────────────────────────────────────────────────

def fetch_with_retry(client, method, *args, max_attempts=3, **kwargs):
    """带重试的 ccxt 数据拉取"""
    last_err = None
    for attempt in range(max_attempts):
        try:
            return method(*args, **kwargs)
        except Exception as e:
            last_err = e
            if attempt < max_attempts - 1:
                time.sleep(2 ** attempt)
    raise last_err


def fetch_btc_ohlcv(client, timeframe: str, limit: int, since: int | None = None) -> pd.DataFrame:
    raw = fetch_with_retry(client, client.fetch_ohlcv, "BTC/USDT", timeframe, limit, since)
    df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna().reset_index(drop=True)
    return df


def fetch_all_frames(limit: int = 300) -> dict[str, pd.DataFrame]:
    """拉取 4h / 1h / 15m / 1m 四个时间轴数据"""
    from market_adaptive.clients.okx_client import OKXClient
    from market_adaptive.config import load_config

    config = load_config(Path("config/config.yaml"))
    client = OKXClient(config.okx, config.execution)

    now_ts = pd.Timestamp.utcnow()
    since_4h = int((now_ts - pd.Timedelta(hours=limit * 4)).value // 1_000_000)
    since_1h = int((now_ts - pd.Timedelta(hours=limit)).value // 1_000_000)
    since_15m = int((now_ts - pd.Timedelta(minutes=limit * 15)).value // 1_000_000)
    since_1m = int((now_ts - pd.Timedelta(minutes=limit)).value // 1_000_000)

    frames = {
        "major": fetch_btc_ohlcv(client, "4h", limit, since_4h),
        "swing": fetch_btc_ohlcv(client, "1h", limit, since_1h),
        "execution": fetch_btc_ohlcv(client, "15m", limit, since_15m),
        "intrabar": fetch_btc_ohlcv(client, "1m", limit, since_1m),
    }
    logger.info("Data fetched | major=%d swing=%d execution=%d intrabar=%d rows",
                *(len(f) for f in frames.values()))
    return frames


# ── 2. Permits / EntryDeciderLite ───────────────────────────────────────────

def build_entry_decider(config: Any):
    from market_adaptive.strategies.entry_decider_lite import EntryDeciderLite
    return EntryDeciderLite(config)


def run_permit(signal, entry_decider) -> dict[str, Any]:
    result = entry_decider.evaluate(signal)
    return {
        "decision": result.decision,
        "score": result.score,
        "reasons": list(result.reasons),
        "breakdown": result.breakdown,
    }


# ── 3. ML Gate ───────────────────────────────────────────────────────────────

def build_ml_engine(model_path: str = "data/ml_models"):
    from market_adaptive.ml_signal_engine import MarketAdaptiveMLEngine
    engine = MarketAdaptiveMLEngine(enabled=True, model_path=model_path)
    return engine


def run_ml_gate(engine, signal, execution_frame) -> dict[str, Any]:
    direction = int(getattr(signal, "direction", 0) or 0)
    result = engine.evaluate(
        symbol=getattr(signal, "symbol", "BTC/USDT") or "BTC/USDT",
        execution_frame=execution_frame,
        direction=direction,
    )
    return {
        "used_model": result.used_model,
        "prediction": result.prediction,
        "probability_up": result.probability_up,
        "aligned_confidence": result.aligned_confidence,
        "gate_passed": result.gate_passed,
        "reason": result.reason,
    }


# ── 4. Intraday Replay ────────────────────────────────────────────────────────

def select_target_bars(execution_frame: pd.DataFrame, intrabar_frame: pd.DataFrame, n: int = 20) -> list[pd.Timestamp]:
    """从 execution_frame 尾部选 n 个已完成 15m bar，作为 replay 目标"""
    if execution_frame.empty:
        return []
    bars = execution_frame.tail(n).copy()
    # 取每个 bar 的开始时间戳（index）
    targets = []
    for _, row in bars.iterrows():
        ts = pd.Timestamp(row["timestamp"])
        # 找对应的 intrabar bucket 结束时间
        targets.append(ts)
    return targets


def run_intrabar_replay(config, frames, target_bar_ts: pd.Timestamp):
    from market_adaptive.strategies.intrabar_replay import (
        IntrabarReplayFrames,
        replay_trend_signal_at_timestamp,
        replay_trend_signal_with_intrabar_scan,
    )

    replay_frames = IntrabarReplayFrames(
        major=frames["major"],
        swing=frames["swing"],
        execution=frames["execution"],
        intrabar=frames["intrabar"],
    )

    # 先做 intrabar scan（逐根1m扫整个15m bucket）
    scan_results = replay_trend_signal_with_intrabar_scan(
        config=config,
        frames=replay_frames,
        target_bar_ts=target_bar_ts,
        execution_config=None,
    )
    return scan_results


def trend_signal_to_dict(signal) -> dict[str, Any]:
    """把 TrendSignal 关键字段提取成 dict（避免dataclass循环引用打印问题）"""
    from market_adaptive.strategies.cta_robot import EntryPathway

    def safe(val, default=None):
        if val is None:
            return default
        try:
            return float(val)
        except Exception:
            return default

    return {
        "direction": int(getattr(signal, "direction", 0) or 0),
        "raw_direction": int(getattr(signal, "raw_direction", 0) or 0),
        "bullish_score": safe(getattr(signal, "bullish_score", 0)),
        "bearish_score": safe(getattr(signal, "bearish_score", 0)),
        "signal_confidence": safe(getattr(signal, "signal_confidence", 0)),
        "signal_strength_bonus": safe(getattr(signal, "signal_strength_bonus", 0)),
        "signal_quality_tier": str(getattr(signal, "signal_quality_tier", "TIER_LOW") or "TIER_LOW"),
        "execution_trigger_family": str(getattr(signal, "execution_trigger_family", "waiting") or "waiting"),
        "execution_trigger_reason": str(getattr(signal, "execution_trigger_reason", "") or ""),
        "execution_entry_mode": str(getattr(signal, "execution_entry_mode", "") or ""),
        "entry_pathway": str(getattr(signal, "entry_pathway", EntryPathway.STRICT).name if hasattr(getattr(signal, "entry_pathway", None), "name") else str(getattr(signal, "entry_pathway", "STRICT"))),
        "bullish_ready": bool(getattr(signal, "bullish_ready", False)),
        "bearish_ready": bool(getattr(signal, "bearish_ready", False)),
        "long_setup_blocked": bool(getattr(signal, "long_setup_blocked", False)),
        "long_setup_reason": str(getattr(signal, "long_setup_reason", "") or ""),
        "obv_confirmation_passed": bool(getattr(signal, "obv_confirmation_passed", False)),
        "volume_filter_passed": bool(getattr(signal, "volume_filter_passed", False)),
        "obv_zscore": safe(getattr(signal, "obv_confirmation", None) and getattr(signal.obv_confirmation, "zscore", 0) or 0),
        "relaxed_entry": bool(getattr(signal, "relaxed_entry", False)),
        "relaxed_reasons": list(getattr(signal, "relaxed_reasons", []) or []),
        "price": safe(getattr(signal, "price", 0)),
        "atr": safe(getattr(signal, "atr", 0)),
        "entry_size_multiplier": safe(getattr(signal, "entry_size_multiplier", 1.0)),
        "entry_decider_decision": str(getattr(signal, "entry_decider_decision", "unevaluated") or "unevaluated"),
        "entry_decider_score": safe(getattr(signal, "entry_decider_score", 0.0)),
        "entry_decider_reasons": list(getattr(signal, "entry_decider_reasons", []) or []),
        "blocker_reason": str(getattr(signal, "blocker_reason", "") or ""),
    }


# ── 5. 主测试循环 ────────────────────────────────────────────────────────────

def print_divider(title: str, width: int = 120):
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}")


def run_full_test(limit: int = 300, n_target_bars: int = 10):
    from market_adaptive.config import load_config

    config = load_config(Path("config/config.yaml"))
    entry_decider = build_entry_decider(config.cta)
    ml_engine = build_ml_engine()

    print_divider(f"Step 1: Fetching real BTC/USDT data (limit={limit})")
    frames = fetch_all_frames(limit=limit)

    # 找最后一个完整的 15m bar 作为当前评估点
    latest_15m_bar = frames["execution"].iloc[-1]["timestamp"]
    latest_15m_ts = pd.Timestamp(latest_15m_bar)
    logger.info("Latest 15m bar timestamp: %s", latest_15m_ts)

    print_divider("Step 2: Building MTFSignal at latest bar (permit basis)")

    from market_adaptive.strategies.intrabar_replay import (
        IntrabarReplayFrames,
        replay_trend_signal_at_timestamp,
    )

    replay_frames = IntrabarReplayFrames(
        major=frames["major"],
        swing=frames["swing"],
        execution=frames["execution"],
        intrabar=frames["intrabar"],
    )

    # 在 latest bar 闭包时间点做一次信号评估
    latest_signal = replay_trend_signal_at_timestamp(
        config=config.cta,
        frames=replay_frames,
        evaluation_ts=latest_15m_ts + pd.Timedelta(minutes=1),  # 闭包后
    )

    if latest_signal is None:
        logger.warning("Latest bar returned None signal - check data alignment")
        return

    sig_dict = trend_signal_to_dict(latest_signal)
    print(f"\n[MTFSignal @ latest bar]")
    for k, v in sig_dict.items():
        print(f"  {k:40s} = {v}")

    print_divider("Step 3: EntryDeciderLite Permit Gate")
    permit_result = run_permit(latest_signal, entry_decider)
    print(f"\n  Permit decision : {permit_result['decision']}")
    print(f"  Score           : {permit_result['score']:.2f}")
    print(f"  Reasons         : {permit_result['reasons']}")
    print(f"  Breakdown       : ")
    for k, v in permit_result["breakdown"].items():
        print(f"    {k:30s} = {v}")

    print_divider("Step 4: ML Gate")
    execution_frame = frames["execution"].copy()
    ml_result = run_ml_gate(ml_engine, latest_signal, execution_frame)
    print(f"\n  used_model       : {ml_result['used_model']}")
    print(f"  prediction       : {ml_result['prediction']}")
    print(f"  probability_up   : {ml_result['probability_up']:.4f}")
    print(f"  aligned_confidence: {ml_result['aligned_confidence']:.4f}")
    print(f"  gate_passed      : {ml_result['gate_passed']}")
    print(f"  reason           : {ml_result['reason']}")

    print_divider("Step 5: Intraday Replay Scan (last N bars)")

    target_bars = select_target_bars(frames["execution"], frames["intrabar"], n=n_target_bars)

    scan_summary: list[dict] = []
    for bar_ts in target_bars:
        scan_results = run_intrabar_replay(config.cta, frames, bar_ts)
        if not scan_results:
            continue

        # 取信号最强的那个（按 direction != 0 过滤）
        valid_signals = [s for s in scan_results if getattr(s, "direction", 0) != 0]
        if not valid_signals:
            continue

        # 取 bullish_score 最高的
        best = max(valid_signals, key=lambda s: float(getattr(s, "bullish_score", 0) or 0))
        sdict = trend_signal_to_dict(best)

        permit = run_permit(best, entry_decider)
        ml = run_ml_gate(ml_engine, best, frames["execution"])

        bar_result = {
            "bar_ts": str(bar_ts),
            "signal": sdict,
            "permit": permit,
            "ml": ml,
        }
        scan_summary.append(bar_result)

    # 打印扫描摘要表
    print(f"\n  {'Bar TS':<22} {'Dir':>4} {'BullScore':>8} {'Tier':<12} {'Pathway':<14} "
          f"{'Permit':<7} {'PermScore':>8} {'MLGate':<7} {'MLReason':<20}")
    print(f"  {'-'*22} {'-'*4} {'-'*8} {'-'*12} {'-'*14} {'-'*7} {'-'*8} {'-'*7} {'-'*20}")

    for item in scan_summary:
        s = item["signal"]
        p = item["permit"]
        m = item["ml"]
        print(f"  {item['bar_ts']:<22} "
              f"{s['direction']:>4} "
              f"{s['bullish_score']:>8.1f} "
              f"{s['signal_quality_tier']:<12} "
              f"{s['entry_pathway']:<14} "
              f"{p['decision']:<7} "
              f"{p['score']:>8.1f} "
              f"{str(m['gate_passed']):<7} "
              f"{m['reason']:<20}")

    print_divider("Step 6: Full Chain Integration Summary")

    # 统计
    total = len(scan_summary)
    permit_blocks = sum(1 for x in scan_summary if x["permit"]["decision"] == "block")
    permit_allows = sum(1 for x in scan_summary if x["permit"]["decision"] == "allow")
    permit_watches = sum(1 for x in scan_summary if x["permit"]["decision"] == "watch")
    ml_gated = sum(1 for x in scan_summary if x["ml"]["used_model"] and not x["ml"]["gate_passed"])
    no_direction = sum(1 for x in scan_summary if x["signal"]["direction"] == 0)

    print(f"\n  Total scanned bars  : {total}")
    print(f"  Direction=0 (skip)   : {no_direction}")
    print(f"  Permit allow         : {permit_allows}")
    print(f"  Permit watch         : {permit_watches}")
    print(f"  Permit block         : {permit_blocks}")
    print(f"  ML gate blocked      : {ml_gated}")
    print(f"\n  Latest bar:")
    print(f"    Permit decision    : {permit_result['decision']} (score={permit_result['score']:.1f})")
    print(f"    ML gate            : {'PASSED' if ml_result['gate_passed'] else 'BLOCKED'} [{ml_result['reason']}]")
    print(f"    Signal direction   : {sig_dict['direction']} | pathway={sig_dict['entry_pathway']}")
    print(f"    Trigger family     : {sig_dict['execution_trigger_family']}")
    print(f"    Block reason       : {sig_dict['long_setup_reason'] or sig_dict['blocker_reason'] or 'n/a'}")

    print("\n  ⚡ Chain is LIVE — results above reflect real market data.\n")

    return {
        "latest_signal": sig_dict,
        "latest_permit": permit_result,
        "latest_ml": ml_result,
        "scan_summary": scan_summary,
        "stats": {
            "total": total,
            "no_direction": no_direction,
            "permit_allow": permit_allows,
            "permit_watch": permit_watches,
            "permit_block": permit_blocks,
            "ml_gated": ml_gated,
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="permit + replay + ML gate 半集成闭环测试")
    parser.add_argument("--limit", type=int, default=300, help="每时间轴拉取 K 线数量")
    parser.add_argument("--bars", type=int, default=10, help="replay 扫描的 bar 数量")
    args = parser.parse_args()

    result = run_full_test(limit=args.limit, n_target_bars=args.bars)

    # 把结果 json 落盘供后续分析
    out_path = PROJECT_ROOT / "data" / "permit_replay_ml_test_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # 序列化时去除不可 JSON 序列化的字段
    serializable = {
        "latest_signal": result["latest_signal"],
        "latest_permit": result["latest_permit"],
        "scan_summary": result["scan_summary"],
        "stats": result["stats"],
    }
    out_path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Results written to %s", out_path)