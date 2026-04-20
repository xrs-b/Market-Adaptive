#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import asdict
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.cta_backtest_sandbox import load_csv, CTABacktester
from market_adaptive.config import CTAConfig, ExecutionConfig


def _build_aggregate(results: list[dict]) -> dict:
    total_trades = sum(r['report']['total_trades'] for r in results)
    total_fees = sum(r['report']['fees_paid'] for r in results)
    total_realized = sum(r['report']['realized_pnl'] for r in results)
    avg_return = sum(r['report']['total_return_pct'] for r in results) / len(results) if results else 0.0
    max_drawdown_pct = max((r['report']['max_drawdown_pct'] for r in results), default=0.0)
    opened_obv_scalp_actions = sum(r['report']['diagnostics'].get('opened_obv_scalp_actions', 0) for r in results)
    blocked_obv = sum(r['report']['diagnostics'].get('blocked_obv', 0) for r in results)
    directional_ready = sum(r['report']['diagnostics'].get('directional_ready', 0) for r in results)
    opened_actions = sum(r['report']['diagnostics'].get('opened_actions', 0) for r in results)
    quality_tier_counts = Counter()
    entry_pathway_counts = Counter()
    opened_by_pathway = Counter()
    opened_by_quality_tier = Counter()
    for result in results:
        diagnostics = result['report']['diagnostics']
        quality_tier_counts.update(diagnostics.get('quality_tier_counts', {}))
        entry_pathway_counts.update(diagnostics.get('entry_pathway_counts', {}))
        opened_by_pathway.update(diagnostics.get('opened_by_pathway', {}))
        opened_by_quality_tier.update(diagnostics.get('opened_by_quality_tier', {}))
    return {
        'avg_segment_return_pct': avg_return,
        'max_segment_drawdown_pct': max_drawdown_pct,
        'total_trades': total_trades,
        'total_fees_paid': total_fees,
        'total_realized_pnl': total_realized,
        'opened_actions': opened_actions,
        'opened_obv_scalp_actions': opened_obv_scalp_actions,
        'blocked_obv': blocked_obv,
        'directional_ready': directional_ready,
        'quality_tier_counts': dict(quality_tier_counts),
        'entry_pathway_counts': dict(entry_pathway_counts),
        'opened_by_pathway': dict(opened_by_pathway),
        'opened_by_quality_tier': dict(opened_by_quality_tier),
    }


def main() -> int:
    csv_path = ROOT / 'data' / 'okx' / 'BTC-USDT-SWAP' / '1m.csv'
    out_dir_name = os.environ.get('CTA_BACKTEST_OUTDIR', 'cta_backtest_segments')
    out_dir = ROOT / 'tmp' / out_dir_name
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / 'summary.json'

    df = load_csv(csv_path).reset_index(drop=True)
    total_rows = len(df)

    base_cfg = CTAConfig(symbol='BTC/USDT')
    dry_bt = CTABacktester(df.tail(min(total_rows, 20000)).reset_index(drop=True), cta_config=base_cfg, execution_config=ExecutionConfig(), symbol='BTC/USDT', warmup_bars=400)
    warmup = int(dry_bt.warmup_bars)

    segment_total_rows = int(os.environ.get('CTA_SEGMENT_TOTAL_ROWS', '20000'))
    usable_rows_per_segment = max(500, segment_total_rows - warmup)
    if usable_rows_per_segment <= 0:
        raise SystemExit(f'Invalid segmentation config: warmup={warmup}, segment_total_rows={segment_total_rows}')

    if total_rows <= warmup:
        raise SystemExit(f'Not enough rows for backtest: rows={total_rows}, warmup={warmup}')

    remaining_usable = total_rows - warmup
    num_segments = math.ceil(remaining_usable / usable_rows_per_segment)
    segment_start = max(1, int(os.environ.get('CTA_SEGMENT_START', '1')))
    segment_end = min(num_segments, int(os.environ.get('CTA_SEGMENT_END', str(num_segments))))
    if segment_end < segment_start:
        raise SystemExit(f'Invalid segment range: start={segment_start}, end={segment_end}, total={num_segments}')
    results = []

    for seg_idx in range(segment_start - 1, segment_end):
        usable_start = warmup + seg_idx * usable_rows_per_segment
        usable_end_exclusive = min(total_rows, warmup + (seg_idx + 1) * usable_rows_per_segment)
        window_start = max(0, usable_start - warmup)
        window_end = usable_end_exclusive
        segment_df = df.iloc[window_start:window_end].reset_index(drop=True)

        bt = CTABacktester(segment_df, cta_config=CTAConfig(symbol='BTC/USDT'), execution_config=ExecutionConfig(), symbol='BTC/USDT', warmup_bars=400)
        report = bt.run()

        first_ts = int(segment_df.iloc[0]['timestamp'])
        last_ts = int(segment_df.iloc[-1]['timestamp'])
        segment_payload = {
            'segment_index': seg_idx + 1,
            'num_segments': num_segments,
            'window_start_row': int(window_start),
            'window_end_row_exclusive': int(window_end),
            'usable_start_row': int(usable_start),
            'usable_end_row_exclusive': int(usable_end_exclusive),
            'first_timestamp': first_ts,
            'last_timestamp': last_ts,
            'derived_warmup_bars': bt.warmup_bars,
            'report': asdict(report),
        }
        segment_path = out_dir / f'segment_{seg_idx+1:03d}.json'
        segment_path.write_text(json.dumps(segment_payload, ensure_ascii=False, indent=2))
        print(json.dumps({
            'segment': seg_idx + 1,
            'segments': num_segments,
            'ret': report.total_return_pct,
            'mdd_pct': report.max_drawdown_pct,
            'pf': report.profit_factor,
            'trades': report.total_trades,
            'fees': report.fees_paid,
            'opened_obv_scalp_actions': report.diagnostics.get('opened_obv_scalp_actions'),
            'blocked_obv': report.diagnostics.get('blocked_obv'),
            'pathways': report.diagnostics.get('entry_pathway_counts', {}),
            'opened_by_pathway': report.diagnostics.get('opened_by_pathway', {}),
        }, ensure_ascii=False), flush=True)
        results.append(segment_payload)
        partial_summary = {
            'csv_path': str(csv_path),
            'total_rows': total_rows,
            'derived_warmup_bars': warmup,
            'segment_total_rows': segment_total_rows,
            'usable_rows_per_segment': usable_rows_per_segment,
            'segments': num_segments,
            'segment_start': segment_start,
            'segment_end': segment_end,
            'segments_ran': len(results),
            'last_completed_segment': seg_idx + 1,
            'aggregate': _build_aggregate(results),
        }
        summary_path.write_text(json.dumps(partial_summary, ensure_ascii=False, indent=2))

    if not results:
        raise SystemExit('No segments were run; check CTA_SEGMENT_START/CTA_SEGMENT_END')

    summary = {
        'csv_path': str(csv_path),
        'total_rows': total_rows,
        'derived_warmup_bars': warmup,
        'segment_total_rows': segment_total_rows,
        'usable_rows_per_segment': usable_rows_per_segment,
        'segments': num_segments,
        'segment_start': segment_start,
        'segment_end': segment_end,
        'segments_ran': len(results),
        'last_completed_segment': results[-1]['segment_index'],
        'aggregate': _build_aggregate(results),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print('SUMMARY', json.dumps(summary['aggregate'], ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
