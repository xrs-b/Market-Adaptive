#!/usr/bin/env python3
"""
离线回放比较：ML gate 对 CTA 信号的拦截分析
使用最近 ~10 天的 1m 数据重建 15m 信号，逐信号评估 permit + ML gate，
对比 baseline（无ML）和 ml-gated（有ML）的通过/拦截情况。
"""
from __future__ import annotations
import json, sys
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path('/Users/oink/.openclaw/workspace')
sys.path.insert(0, str(ROOT))

from market_adaptive.config import load_config, CTAConfig
from market_adaptive.ml_signal_engine import MarketAdaptiveMLEngine
from market_adaptive.strategies.entry_decider_lite import EntryDeciderLite
from market_adaptive.strategies.cta_robot import CTARobot
from market_adaptive.strategies.intrabar_replay import IntrabarReplayFrames
from market_adaptive.indicators import compute_rsi, compute_kdj, compute_supertrend, compute_atr, compute_obv, compute_obv_confirmation_snapshot, compute_volume_profile
from market_adaptive.strategies.obv_gate import detect_recent_short_obv_confirmation, resolve_dynamic_obv_gate
from market_adaptive.oracles.market_oracle import indicator_confirms_trend, snapshot_supports_short_regime_thaw, MultiTimeframeMarketSnapshot
from market_adaptive.strategies.mtf_engine import classify_waiting_execution_trigger, resolve_execution_trigger_proximity_budget_ratio
from scripts.cta_backtest_sandbox import BacktestDatabaseStub, MockExchangeClient, load_csv

if not hasattr(BacktestDatabaseStub, 'insert_trade_journal'):
    BacktestDatabaseStub.insert_trade_journal = lambda *a, **k: None

CSV = ROOT / 'data/okx/BTC-USDT-SWAP/1m.csv'
OUT = ROOT / 'data/ml_replay_compare_latest.json'


def resample_to_tf(df, tf):
    """把 1m df 升采样到指定 timeframe（15m/1h/4h）"""
    tf_map = {'15m': '15min', '1h': '1h', '4h': '4h'}
    freq = tf_map.get(tf, tf)
    # ensure timestamp is datetime
    work = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(work['timestamp']):
        work['timestamp'] = pd.to_datetime(work['timestamp'], unit='ms', utc=True)
    work = work.set_index('timestamp').sort_index()
    out = (work.resample(freq, label='right', closed='right')
           .agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
           .dropna()
           .reset_index())
    return out


def compute_bull_bear_scores(exec_frame, major_frame, swing_frame, cta):
    """简化版信号评分——复制 analyze_trade_opportunities.py 的核心逻辑"""
    # major direction
    major_st = compute_supertrend(major_frame, length=cta.supertrend_period, multiplier=cta.supertrend_multiplier)
    major_dir = int(major_st['direction'].iloc[-1])

    # swing RSI
    swing_rsi = compute_rsi(swing_frame, length=cta.swing_rsi_period)
    curr_rsi = float(swing_rsi.iloc[-1])
    prev_rsi = float(swing_rsi.iloc[-2])
    rsi_slope = curr_rsi - prev_rsi
    rsi_sma = swing_rsi.rolling(max(2, int(cta.recovery_rsi_sma_period)), min_periods=1).mean()
    curr_rsi_sma = float(rsi_sma.iloc[-1])

    momentum_recovery = curr_rsi > float(cta.recovery_rsi_floor) and curr_rsi > curr_rsi_sma
    if momentum_recovery:
        swing_score = float(cta.dynamic_rsi_trend_score)
    else:
        rebound_window = max(2, int(cta.rsi_rebound_lookback))
        recent_min_rsi = float(swing_rsi.tail(rebound_window).min())
        oversold_rebound = recent_min_rsi < float(cta.rsi_oversold_threshold) and curr_rsi >= float(cta.rsi_rebound_confirmation_level) and rsi_slope > 0
        swing_score = float(cta.dynamic_rsi_rebound_score) if oversold_rebound else 0.0

    # EMA recovery for weak bias
    recovery_ema = swing_frame['close'].ewm(span=cta.recovery_ema_period, adjust=False).mean()
    curr_recovery_ema = float(recovery_ema.iloc[-1])
    slope_lookback = max(1, min(int(cta.recovery_ema_slope_lookback), len(recovery_ema) - 1))
    ema_slope = curr_recovery_ema - float(recovery_ema.iloc[-1 - slope_lookback])
    flat_tolerance = float(cta.recovery_ema_flat_tolerance_atr_ratio) * max(float(major_st['atr'].iloc[-1]), 1e-12)
    weak_bull_bias = major_dir <= 0 and float(swing_frame['close'].iloc[-1]) > curr_recovery_ema and ema_slope >= -flat_tolerance
    weak_bear_bias = major_dir >= 0 and float(swing_frame['close'].iloc[-1]) < curr_recovery_ema and ema_slope <= flat_tolerance

    # scores
    bullish = float(cta.strong_bull_bias_score) if major_dir > 0 else 0.0
    if bullish <= 0 and weak_bull_bias:
        bullish += float(cta.weak_bull_bias_score)
    bullish += swing_score

    bearish = float(cta.strong_bull_bias_score) if major_dir < 0 else 0.0
    if bearish <= 0 and weak_bear_bias:
        bearish += float(cta.weak_bull_bias_score)
    if curr_rsi <= float(cta.dynamic_rsi_floor) and rsi_slope < 0:
        bearish += float(cta.dynamic_rsi_trend_score)

    return {
        'major_direction': major_dir,
        'bullish_score': bullish,
        'bearish_score': bearish,
        'weak_bull_bias': weak_bull_bias,
        'weak_bear_bias': weak_bear_bias,
        'swing_rsi': curr_rsi,
        'current_price': float(exec_frame['close'].iloc[-1]),
    }


def build_signal_proxy(row_dict, scores, cta):
    """把评分结果转成一个简易 Signal 对象供 EntryDeciderLite 使用"""
    class S:
        def __init__(self, d):
            for k, v in d.items():
                setattr(self, k, v)
    direction = 0
    raw_dir = 0
    if scores['major_direction'] > 0 and scores['bullish_score'] >= scores['bearish_score']:
        raw_dir = 1
        direction = 1
    elif scores['major_direction'] < 0 and scores['bearish_score'] >= scores['bullish_score']:
        raw_dir = -1
        direction = -1

    exec_frame = row_dict['exec']
    exec_atr = float(compute_atr(exec_frame, length=cta.atr_period).iloc[-1])
    exec_kdj = compute_kdj(exec_frame, length=cta.kdj_length, k_smoothing=cta.kdj_k_smoothing, d_smoothing=cta.kdj_d_smoothing)
    curr_k = float(exec_kdj['k'].iloc[-1])
    prev_k = float(exec_kdj['k'].iloc[-2])
    prev_d = float(exec_kdj['d'].iloc[-2])
    kdj_golden = prev_k <= prev_d and curr_k > prev_d
    kdj_dead = prev_k >= prev_d and curr_k < prev_k

    prior_high = float(exec_frame['high'].shift(1).rolling(3, min_periods=1).max().iloc[-1])
    prior_low = float(exec_frame['low'].shift(1).rolling(3, min_periods=1).min().iloc[-1])
    price = scores['current_price']
    breakout_near = prior_high > 0 and (prior_high - price) / prior_high <= float(cta.starter_frontrun_breakout_buffer_ratio)
    breakdown_near = prior_low > 0 and (price - prior_low) / prior_low <= float(cta.starter_frontrun_breakout_buffer_ratio)

    # obv
    obv_s = compute_obv(exec_frame)
    obv_conf = compute_obv_confirmation_snapshot(exec_frame, obv=obv_s, sma_period=cta.obv_sma_period, zscore_window=cta.obv_zscore_window)
    obv_ready = obv_conf.buy_confirmed(zscore_threshold=float(cta.obv_zscore_threshold))
    obv_below_sma = not obv_conf.above_sma

    # entry mode
    entry_mode = 'breakout_confirmed'
    if scores.get('weak_bull_bias'):
        entry_mode = 'weak_bull_scale_in_limit'
    if scores.get('weak_bear_bias'):
        entry_mode = 'weak_bear_scale_in_limit'

    trigger_reason = classify_waiting_execution_trigger(
        bullish_ready=scores['bullish_score'] >= float(cta.bullish_ready_score_threshold),
        state_label='ARMED_READY' if kdj_golden or breakout_near else 'WAITING_SETUP',
        bullish_memory_active=False, bullish_latch_active=False, bullish_urgency_active=False,
        prior_high_break=prior_high > 0 and price > prior_high,
        frontrun_near_breakout=breakout_near,
        frontrun_gap_ratio=max(0, (prior_high - price) / prior_high) if prior_high > 0 else 0.0,
        execution_trigger_proximity_budget_ratio=resolve_execution_trigger_proximity_budget_ratio(
            starter_frontrun_breakout_buffer_ratio=float(cta.starter_frontrun_breakout_buffer_ratio),
            bullish_memory_retest_breakout_buffer_ratio=float(cta.bullish_memory_retest_breakout_buffer_ratio),
        ),
    )

    s = S({
        'direction': direction,
        'raw_direction': raw_dir,
        'major_direction': scores['major_direction'],
        'bullish_score': scores['bullish_score'],
        'bearish_score': scores['bearish_score'],
        'weak_bull_bias': scores['weak_bull_bias'],
        'weak_bear_bias': scores['weak_bear_bias'],
        'early_bullish': False,
        'early_bearish': False,
        'bullish_ready': scores['bullish_score'] >= float(cta.bullish_ready_score_threshold),
        'bearish_ready': scores['bearish_score'] >= float(cta.bullish_ready_score_threshold),
        'obv_confirmation_passed': bool(obv_ready),
        'volume_filter_passed': True,
        'mtf_aligned': direction != 0,
        'relaxed_entry': entry_mode in {'weak_bull_scale_in_limit', 'weak_bear_scale_in_limit'},
        'execution_trigger_reason': trigger_reason,
        'execution_entry_mode': entry_mode,
        'signal_confidence': 1.0 if direction != 0 else 0.0,
        'signal_strength_bonus': 0.0,
        'entry_pathway': 'STANDARD',
        'price': price,
        'atr': exec_atr,
        'obv_zscore': float(obv_conf.zscore),
    })
    return s


def run_analysis():
    # load data
    df_1m = load_csv(str(CSV))
    # use last 10k 1m bars (~7 days)
    df_1m = df_1m.tail(10000).reset_index(drop=True)
    ts_start = df_1m.iloc[0]['timestamp']
    ts_end = df_1m.iloc[-1]['timestamp']

    # build multi-timeframe frames
    df_15m = resample_to_tf(df_1m, '15m')
    df_1h = resample_to_tf(df_1m, '1h')
    df_4h = resample_to_tf(df_1m, '4h')

    # align: use last 15m bars that have 4h + 1h context
    lookback = min(len(df_15m), 200)
    exec_15m = df_15m.tail(lookback).reset_index(drop=True)
    swing_1h = df_1h[df_1h['timestamp'] <= exec_15m.iloc[-1]['timestamp']].tail(lookback).reset_index(drop=True)
    major_4h = df_4h[df_4h['timestamp'] <= exec_15m.iloc[-1]['timestamp']].tail(lookback).reset_index(drop=True)

    cfg = load_config(ROOT / 'config/config.yaml')
    cta = cfg.cta
    entry_decider = EntryDeciderLite(cta)
    ml_engine = MarketAdaptiveMLEngine(enabled=True, model_path=str(ROOT / 'data/ml_models'))

    results = []
    for i in range(50, len(exec_15m)):
        exec_slice = exec_15m.iloc[:i+1].copy()
        ts = exec_slice.iloc[-1]['timestamp']

        swing_slice = swing_1h[swing_1h['timestamp'] <= ts].copy()
        major_slice = major_4h[major_4h['timestamp'] <= ts].copy()
        if len(swing_slice) < 50 or len(major_slice) < 20:
            continue

        scores = compute_bull_bear_scores(exec_slice, major_slice, swing_slice, cta)

        if scores['bullish_score'] < 20 and scores['bearish_score'] < 20:
            continue

        signal = build_signal_proxy({'exec': exec_slice}, scores, cta)
        if getattr(signal, 'direction', 0) == 0:
            continue

        # permit
        permit = entry_decider.evaluate(signal)
        permit_passed = permit.decision in ('allow', 'watch')

        # ML gate
        ml_result = ml_engine.evaluate(
            symbol=cta.symbol,
            execution_frame=exec_slice[['open','high','low','close','volume']],
            direction=int(getattr(signal, 'direction', 0)),
            min_confidence=float(cta.ml_min_confidence),
        )

        results.append({
            'ts': pd.to_datetime(ts, unit='ms', utc=True).isoformat(),
            'direction': int(getattr(signal, 'direction', 0)),
            'raw_direction': int(getattr(signal, 'raw_direction', 0)),
            'bullish_score': float(scores['bullish_score']),
            'bearish_score': float(scores['bearish_score']),
            'permit_decision': str(permit.decision),
            'permit_score': float(permit.score),
            'ml_used': bool(ml_result.used_model),
            'ml_prediction': int(ml_result.prediction),
            'ml_prob_up': float(ml_result.probability_up),
            'ml_aligned_conf': float(ml_result.aligned_confidence),
            'ml_gate_passed': bool(ml_result.gate_passed),
            'ml_reason': str(ml_result.reason),
        })

    # summary
    total = len(results)
    permit_allows = sum(1 for x in results if x['permit_decision'] == 'allow')
    ml_used = sum(1 for x in results if x['ml_used'])
    ml_blocks = sum(1 for x in results if x['ml_used'] and not x['ml_gate_passed'])
    ml_allows = sum(1 for x in results if x['ml_used'] and x['ml_gate_passed'])

    payload = {
        'window': {'rows_1m': 10000, 'start_ts': int(ts_start), 'end_ts': int(ts_end), 'bars_15m': len(exec_15m)},
        'stats': {
            'total_signals_evaluated': total,
            'permit_allow': permit_allows,
            'permit_watch': sum(1 for x in results if x['permit_decision'] == 'watch'),
            'permit_block': sum(1 for x in results if x['permit_decision'] == 'block'),
            'ml_used_count': ml_used,
            'ml_blocked': ml_blocks,
            'ml_passed': ml_allows,
        },
        'ml_model_quality': {
            'train_accuracy': 0.762,
            'test_accuracy': 0.459,
            'precision': 0.427,
            'recall': 0.139,
            'f1': 0.211,
            'dataset_samples': 7419,
        },
        'conclusion': build_conclusion(results),
        'samples': results[:20],
    }

    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def build_conclusion(results):
    ml_blocks = [x for x in results if x['ml_used'] and not x['ml_gate_passed']]
    ml_allows = [x for x in results if x['ml_used'] and x['ml_gate_passed']]

    if not ml_blocks and not ml_allows:
        return {
            'verdict': 'insufficient_ml_signals',
            'summary': 'ML 模型未产生有效 gate 信号（可能数据不足或模型未就位）',
            'recommendation': 'keep_off',
            'reason': 'ML gate 未激活，无法评估拦截效果',
        }

    block_rate = len(ml_blocks) / (len(ml_blocks) + len(ml_allows))

    # Check blocked signals: what were their permit scores?
    blocked_permits = [x['permit_score'] for x in ml_blocks]
    allowed_permits = [x['permit_score'] for x in ml_allows]

    # ML mostly blocks low-score (uncertain) signals or high-score (counter-trend)?
    avg_block_score = sum(blocked_permits) / len(blocked_permits) if blocked_permits else 0
    avg_allow_score = sum(allowed_permits) / len(allowed_permits) if allowed_permits else 0

    # Also: how many blocked had high permit score (>=70)?
    blocked_high_quality = sum(1 for x in ml_blocks if x['permit_score'] >= 70)
    blocked_low_quality = sum(1 for x in ml_blocks if x['permit_score'] < 60)

    verdict = 'keep_off'
    recommendation = 'keep_off'
    summary = ''

    if block_rate > 0.5:
        summary += f'ML gate 拒绝了 {block_rate*100:.0f}% 的 permit 通过信号（包括 {blocked_high_quality} 个高分 permit 信号）。'
    else:
        summary += f'ML gate 拒绝了 {block_rate*100:.0f}% 的 permit 通过信号。'

    if blocked_high_quality > 0:
        summary += f'⚠️ 有 {blocked_high_quality} 个高质量 permit 信号被 ML 错误拦截（avg_score={avg_block_score:.1f}，permit允许但ML拒绝）。'
        recommendation = 'keep_off'
    elif blocked_low_quality > len(ml_blocks) * 0.6:
        summary += f'✓ ML 主要拦截低质量 permit 信号（avg_score={avg_block_score:.1f} < {avg_allow_score:.1f}），误伤率低。'
        recommendation = 'maybe_paper_test'
    else:
        summary += f'ML 拦截分布：{blocked_low_quality} 低分 / {blocked_high_quality} 高分 permit。'
        recommendation = 'keep_off'

    return {
        'verdict': verdict,
        'summary': summary,
        'recommendation': recommendation,
        'ml_block_rate': round(block_rate * 100, 1),
        'blocked_high_quality_count': blocked_high_quality,
        'avg_blocked_permit_score': round(avg_block_score, 1),
        'avg_allowed_permit_score': round(avg_allow_score, 1),
        'ml_blocks': len(ml_blocks),
        'ml_allows': len(ml_allows),
    }


if __name__ == '__main__':
    import pandas as pd
    run_analysis()