#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from statistics import mean, median


def load_rows(db_path: Path, since: str | None = None):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    where = "where strategy_name='cta'"
    params = []
    if since:
        where += " and timestamp >= ?"
        params.append(since)
    return list(con.execute(f"select * from trade_journal {where} order by id", params))


def parse_meta(row):
    raw = row['metadata_json'] if 'metadata_json' in row.keys() else None
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def pnl_rows(rows):
    return [r for r in rows if r['pnl'] is not None]


def summarize_group(rows, key_fn):
    groups = defaultdict(list)
    for r in rows:
        groups[key_fn(r)].append(float(r['pnl']))
    out = []
    for k, vals in groups.items():
        wins = [v for v in vals if v > 0]
        losses = [v for v in vals if v < 0]
        out.append({
            'key': k or '(empty)',
            'trades': len(vals),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': len(wins) / len(vals) if vals else 0,
            'pnl': sum(vals),
            'avg': mean(vals),
            'median': median(vals),
            'avg_win': mean(wins) if wins else 0,
            'avg_loss': mean(losses) if losses else 0,
            'profit_factor': (sum(wins) / abs(sum(losses))) if losses and sum(losses) else float('inf'),
        })
    return sorted(out, key=lambda x: x['pnl'])


def summarize_count(rows, key_fn):
    counts = defaultdict(int)
    for r in rows:
        counts[key_fn(r) or '(empty)'] += 1
    return sorted(counts.items(), key=lambda kv: (-kv[1], str(kv[0])))


def print_count_table(title, items, limit=50):
    print(f"\n## {title}")
    print("key | count")
    print("-" * 72)
    for key, count in items[:limit]:
        print(f"{key} | {count}")


def print_table(title, items, limit=50):
    print(f"\n## {title}")
    print("key | trades | win% | pnl | avg | avg_win | avg_loss | pf")
    print("-" * 96)
    for x in items[:limit]:
        pf = 'inf' if x['profit_factor'] == float('inf') else f"{x['profit_factor']:.2f}"
        print(f"{x['key']} | {x['trades']} | {x['win_rate']*100:5.1f}% | {x['pnl']:9.4f} | {x['avg']:8.4f} | {x['avg_win']:8.4f} | {x['avg_loss']:9.4f} | {pf}")


def bucket(v, cuts):
    if v is None:
        return 'missing'
    try:
        v = float(v)
    except Exception:
        return 'bad'
    last = '-inf'
    for c in cuts:
        if v < c:
            return f"{last}..{c}"
        last = str(c)
    return f">={last}"


def enrich(rows):
    enriched = []
    for r in rows:
        m = parse_meta(r)
        rr = dict(r)
        rr['_meta'] = m
        enriched.append(rr)
    return enriched


def pro_key(row, key: str):
    return row.get('_meta', {}).get(key)


def print_pro_signal_section(realized, blocked, limit):
    enriched_realized = enrich(realized)
    enriched_blocked = enrich(blocked)
    pro_realized = [r for r in enriched_realized if pro_key(r, 'pro_signal_setup_family')]
    pro_blocked = [r for r in enriched_blocked if pro_key(r, 'pro_signal_reason') or str(r.get('action') or '').startswith('cta:pro_signal_')]

    print(f"\n## Pro-signal coverage")
    print(f"realized_with_pro_metadata={len(pro_realized)} blocked_with_pro_metadata={len(pro_blocked)}")

    print_count_table('Pro-signal blocked actions', summarize_count(pro_blocked, lambda r: r.get('action')), limit)
    print_count_table('Pro-signal blocked reasons', summarize_count(pro_blocked, lambda r: pro_key(r, 'pro_signal_reason')), limit)
    print_count_table('Pro-signal blocked setup_family', summarize_count(pro_blocked, lambda r: pro_key(r, 'pro_signal_setup_family')), limit)
    print_count_table('Pro-signal blocked HTF bias', summarize_count(pro_blocked, lambda r: pro_key(r, 'pro_signal_htf_bias')), limit)

    if pro_realized:
        print_table('PnL by pro_signal_setup_family', summarize_group(pro_realized, lambda r: pro_key(r, 'pro_signal_setup_family')), limit)
        print_table('PnL by pro_signal_htf_bias', summarize_group(pro_realized, lambda r: pro_key(r, 'pro_signal_htf_bias')), limit)
        print_table('PnL by pro_signal_rr bucket', summarize_group(pro_realized, lambda r: bucket(pro_key(r, 'pro_signal_rr'), [1.0, 1.5, 1.8, 2.5, 4.0])), limit)
        print_table('PnL by pro_signal_location_score bucket', summarize_group(pro_realized, lambda r: bucket(pro_key(r, 'pro_signal_location_score'), [0.25, 0.35, 0.5, 0.75])), limit)
    else:
        print('\n## PnL by pro_signal_setup_family')
        print('No realized closes with pro-signal metadata yet.')

    print('\n## Recent pro-signal blocks')
    for r in pro_blocked[-20:]:
        m = r['_meta']
        print(
            f"{r['id']} {r['timestamp']} {r['side']} action={r['action']} "
            f"family={r['trigger_family']} pro_setup={m.get('pro_signal_setup_family')} "
            f"reason={m.get('pro_signal_reason')} bias={m.get('pro_signal_htf_bias')} "
            f"loc={m.get('pro_signal_location_score')} rr={m.get('pro_signal_rr')}"
        )


def main():
    ap = argparse.ArgumentParser(description='Analyze CTA realized PnL and pro-signal decisions from market_adaptive sqlite journal.')
    ap.add_argument('--db', default='data/market_adaptive.sqlite3')
    ap.add_argument('--since')
    ap.add_argument('--limit', type=int, default=30)
    args = ap.parse_args()

    rows = load_rows(Path(args.db), args.since)
    realized = pnl_rows(rows)
    opens = [r for r in rows if r['event_type'] == 'trade_open']
    blocked = [r for r in rows if r['event_type'] == 'blocked_signal']

    print(f"DB: {args.db}")
    print(f"CTA rows={len(rows)} opens={len(opens)} blocked={len(blocked)} realized_closes={len(realized)}")
    if realized:
        total = sum(float(r['pnl']) for r in realized)
        wins = [float(r['pnl']) for r in realized if float(r['pnl']) > 0]
        losses = [float(r['pnl']) for r in realized if float(r['pnl']) < 0]
        print(f"Total realized PnL={total:.4f}; win_rate={len(wins)/len(realized)*100:.1f}%; avg_win={mean(wins) if wins else 0:.4f}; avg_loss={mean(losses) if losses else 0:.4f}; PF={(sum(wins)/abs(sum(losses))) if losses else float('inf'):.2f}")

    print_table('PnL by trigger_family', summarize_group(realized, lambda r: r['trigger_family']), args.limit)
    print_table('PnL by side', summarize_group(realized, lambda r: r['side']), args.limit)
    print_table('PnL by action', summarize_group(realized, lambda r: r['action']), args.limit)
    print_table('PnL by day', summarize_group(realized, lambda r: r['timestamp'][:10]), args.limit)

    enriched = enrich(realized)
    print_table('PnL by signal_quality_tier', summarize_group(enriched, lambda r: r['_meta'].get('signal_quality_tier')), args.limit)
    print_table('PnL by market_regime', summarize_group(enriched, lambda r: r['_meta'].get('market_regime')), args.limit)
    print_table('PnL by entry_decider_score bucket', summarize_group(enriched, lambda r: bucket(r['_meta'].get('entry_decider_score'), [0.5,0.7,0.85,0.95])), args.limit)
    print_table('PnL by confidence bucket', summarize_group(enriched, lambda r: bucket(r['_meta'].get('signal_confidence'), [0.5,0.65,0.8,0.95])), args.limit)

    print_pro_signal_section(realized, blocked, args.limit)

    print('\n## Recent realized closes')
    for r in realized[-20:]:
        print(f"{r['id']} {r['timestamp']} {r['side']} pnl={float(r['pnl']):+.4f} family={r['trigger_family']} reason={r['trigger_reason']}")

if __name__ == '__main__':
    main()
