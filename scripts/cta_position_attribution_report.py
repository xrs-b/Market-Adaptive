#!/usr/bin/env python3
"""Summarize CTA trade_journal open/close attribution by position_id.

Backward compatible: older rows without position_id are grouped by row id.
"""
from __future__ import annotations
import argparse, json, sqlite3
from collections import defaultdict
from pathlib import Path


def load_meta(row):
    try:
        return json.loads(row['metadata_json'] or '{}')
    except Exception:
        return {}


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--db',default='data/market_adaptive.sqlite3')
    ap.add_argument('--days',type=int,default=14)
    ap.add_argument('--out',default='tmp/cta_position_attribution_report.json')
    args=ap.parse_args()
    con=sqlite3.connect(args.db); con.row_factory=sqlite3.Row
    rows=con.execute("""
      SELECT * FROM trade_journal
      WHERE strategy_name='cta'
        AND event_type IN ('trade_open','trade_close')
        AND timestamp >= datetime('now', ?)
      ORDER BY timestamp,id
    """, (f'-{args.days} days',)).fetchall()
    groups=defaultdict(lambda:{'opens':[],'closes':[]})
    unattributed=[]
    for r in rows:
        m=load_meta(r)
        pid=m.get('position_id')
        if not pid:
            # legacy fallback, keeps the report explicit about dirty attribution
            pid=f"legacy-row-{r['id']}" if r['event_type']=='trade_open' else None
        rec={k:r[k] for k in r.keys()}
        rec['metadata']=m
        if pid:
            groups[str(pid)]['opens' if r['event_type']=='trade_open' else 'closes'].append(rec)
        else:
            unattributed.append(rec)
    positions=[]
    for pid,g in groups.items():
        opens=g['opens']; closes=g['closes']
        pnl=sum(float(x['pnl'] or 0) for x in closes)
        open_size=sum(float(x['size'] or 0) for x in opens)
        close_size=sum(float(x['size'] or 0) for x in closes)
        first_open=opens[0] if opens else None
        positions.append({
            'position_id':pid,
            'open_count':len(opens),
            'close_count':len(closes),
            'open_time': first_open['timestamp'] if first_open else None,
            'side': first_open['side'] if first_open else (closes[0]['side'] if closes else None),
            'trigger_family': first_open['trigger_family'] if first_open else (closes[0]['trigger_family'] if closes else None),
            'pathway': first_open['pathway'] if first_open else (closes[0]['pathway'] if closes else None),
            'open_size':open_size,
            'close_size':close_size,
            'remaining_by_journal':open_size-close_size,
            'pnl':pnl,
            'entry_order_ids':[x['metadata'].get('entry_order_id') for x in opens],
            'exit_order_ids':[x['metadata'].get('exit_order_id') for x in closes],
            'mfe_pct_max':max([float(x['metadata'].get('mfe_pct') or 0) for x in closes] or [0]),
            'status':'closed' if opens and abs(open_size-close_size) < 1e-9 else ('open_or_partial' if opens else 'close_without_open'),
        })
    positions.sort(key=lambda x: x.get('open_time') or '')
    summary={
        'rows':len(rows),
        'positions':len(positions),
        'closed':sum(1 for x in positions if x['status']=='closed'),
        'open_or_partial':sum(1 for x in positions if x['status']=='open_or_partial'),
        'close_without_open':sum(1 for x in positions if x['status']=='close_without_open'),
        'unattributed_closes':len(unattributed),
        'total_pnl':sum(x['pnl'] for x in positions),
    }
    report={'summary':summary,'positions':positions,'unattributed':unattributed}
    out=Path(args.out); out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(summary,ensure_ascii=False,indent=2))
    print('wrote',out)
    for p in positions[-20:]:
        print(p['position_id'],p['status'],p['side'],p['trigger_family'],p['pathway'],'open',p['open_size'],'close',p['close_size'],'pnl',round(p['pnl'],4))

if __name__=='__main__': main()
