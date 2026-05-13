#!/usr/bin/env python3
"""Replay recent CTA signals against 1m candles and summarize future MFE/MAE.

Read-only analysis tool. It joins trade_journal rows to data/okx/BTC-USDT-SWAP/1m.csv
and measures forward 15/30/60/120 minute excursions from the signal/open price.
"""
from __future__ import annotations

import argparse, csv, datetime as dt, json, sqlite3, statistics
from collections import defaultdict
from pathlib import Path

UTC = dt.timezone.utc
WINDOWS = (15, 30, 60, 120)


def parse_ts(s: str) -> dt.datetime:
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    x = dt.datetime.fromisoformat(s)
    if x.tzinfo is None:
        x = x.replace(tzinfo=UTC)
    return x.astimezone(UTC)


def load_candles(path: Path):
    rows = []
    with path.open() as f:
        for r in csv.DictReader(f):
            ts = dt.datetime.fromtimestamp(int(r['timestamp']) / 1000, tz=UTC)
            rows.append((ts, float(r['open']), float(r['high']), float(r['low']), float(r['close'])))
    rows.sort(key=lambda x: x[0])
    return rows


def lower_bound(candles, ts):
    lo, hi = 0, len(candles)
    while lo < hi:
        mid = (lo + hi) // 2
        if candles[mid][0] < ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


def pct(x):
    return None if x is None else x * 100


def fmt(x, n=3):
    if x is None:
        return ''
    return f"{x:.{n}f}"


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.mean(xs) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='data/market_adaptive.sqlite3')
    ap.add_argument('--candles', default='data/okx/BTC-USDT-SWAP/1m.csv')
    ap.add_argument('--days', type=int, default=7)
    ap.add_argument('--out', default='tmp/cta_signal_replay_report.json')
    args = ap.parse_args()

    root = Path.cwd()
    db = root / args.db
    candles_path = root / args.candles
    candles = load_candles(candles_path)
    cut = dt.datetime.now(tz=UTC) - dt.timedelta(days=args.days)

    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT * FROM trade_journal
        WHERE timestamp >= ? AND strategy_name='cta'
          AND event_type IN ('blocked_signal','trade_open')
          AND price IS NOT NULL
        ORDER BY timestamp
        """,
        (cut.isoformat(),),
    ).fetchall()

    enriched = []
    for r in rows:
        ts = parse_ts(r['timestamp'])
        side = (r['side'] or '').lower()
        entry = float(r['price'])
        i = lower_bound(candles, ts)
        item = dict(r)
        item['timestamp_utc'] = ts.isoformat()
        try:
            md = json.loads(r['metadata_json'] or '{}')
        except Exception:
            md = {}
        item['entry_decider_score'] = md.get('entry_decider_score')
        item['entry_decider_decision'] = md.get('entry_decider_decision')
        item['blocker_reason'] = md.get('blocker_reason')
        item['entry_location_score'] = md.get('entry_location_score')
        item['market_regime'] = md.get('market_regime')
        for w in WINDOWS:
            end = ts + dt.timedelta(minutes=w)
            j = i
            hi = None; lo = None; close = None
            while j < len(candles) and candles[j][0] <= end:
                _, _, h, l, c = candles[j]
                hi = h if hi is None else max(hi, h)
                lo = l if lo is None else min(lo, l)
                close = c
                j += 1
            if hi is None:
                item[f'mfe_{w}m_pct'] = None
                item[f'mae_{w}m_pct'] = None
                item[f'ret_{w}m_pct'] = None
                continue
            if side == 'short':
                mfe = (entry - lo) / entry
                mae = (hi - entry) / entry
                ret = (entry - close) / entry
            else:
                mfe = (hi - entry) / entry
                mae = (entry - lo) / entry
                ret = (close - entry) / entry
            item[f'mfe_{w}m_pct'] = pct(mfe)
            item[f'mae_{w}m_pct'] = pct(mae)
            item[f'ret_{w}m_pct'] = pct(ret)
        enriched.append(item)

    def summarize(group_rows):
        d = {'n': len(group_rows)}
        for w in WINDOWS:
            mfes = [x.get(f'mfe_{w}m_pct') for x in group_rows]
            maes = [x.get(f'mae_{w}m_pct') for x in group_rows]
            rets = [x.get(f'ret_{w}m_pct') for x in group_rows]
            valid = [x for x in rets if x is not None]
            d[f'median_mfe_{w}m_pct'] = median(mfes)
            d[f'median_mae_{w}m_pct'] = median(maes)
            d[f'median_ret_{w}m_pct'] = median(rets)
            d[f'win_rate_{w}m'] = (sum(1 for x in valid if x > 0) / len(valid)) if valid else None
        return d

    groups = {}
    for key in ['event_type', 'action', 'trigger_family', 'pathway']:
        bucket = defaultdict(list)
        for x in enriched:
            bucket[x.get(key) or ''] .append(x)
        groups[key] = {k: summarize(v) for k, v in sorted(bucket.items(), key=lambda kv: len(kv[1]), reverse=True)}

    combo = defaultdict(list)
    for x in enriched:
        combo[(x.get('action') or '', x.get('trigger_family') or '', x.get('pathway') or '')].append(x)
    groups['action_family_pathway'] = {' | '.join(k): summarize(v) for k, v in sorted(combo.items(), key=lambda kv: len(kv[1]), reverse=True)}

    report = {
        'generated_at': dt.datetime.now(tz=UTC).isoformat(),
        'cutoff_utc': cut.isoformat(),
        'rows': len(enriched),
        'windows_min': WINDOWS,
        'groups': groups,
        'signals': enriched,
    }
    out = root / args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    print(f"wrote {out} rows={len(enriched)}")
    print("\nTop action groups:")
    for k, s in list(groups['action'].items())[:12]:
        print(k, 'n=', s['n'], 'ret60=', fmt(s.get('median_ret_60m_pct')), 'mfe60=', fmt(s.get('median_mfe_60m_pct')), 'mae60=', fmt(s.get('median_mae_60m_pct')), 'wr60=', fmt(s.get('win_rate_60m'),2))
    print("\nTrigger families:")
    for k, s in groups['trigger_family'].items():
        print(k, 'n=', s['n'], 'ret60=', fmt(s.get('median_ret_60m_pct')), 'mfe60=', fmt(s.get('median_mfe_60m_pct')), 'mae60=', fmt(s.get('median_mae_60m_pct')), 'wr60=', fmt(s.get('win_rate_60m'),2))

if __name__ == '__main__':
    main()
