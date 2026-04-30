#!/usr/bin/env python3
"""Analyze major_bull_retest appear/allowed/traded from CTA log files."""
import re, glob, os

LOG_FILES = sorted(glob.glob('logs/archive/main_controller-2026*.log')) + ['logs/main_controller.log']

signal_pat = re.compile(r'trigger=major_bull_retest_ready')
trade_pat = re.compile(r'\[TRADE_OPEN\].*?Reason:.*?major_bull_retest')
block_quality_pat = re.compile(r'(?:targeted long quality blocked|order flow blocked).*?family=major_bull_retest')
cooldown_pat = re.compile(r'CTA fast-track reuse cooldown armed.*?major_bull_retest')
block_family_pat = re.compile(r'trigger_family=bearish_retest')  # not relevant to bull side

stats = {'appear':0,'allowed_signal':0,'traded':0,'blocked_quality':0}
per_file = {}

for fpath in LOG_FILES:
    if not os.path.exists(fpath): continue
    s = {'appear':0,'allowed_signal':0,'traded':0,'blocked_quality':0}
    with open(fpath,'r',errors='ignore') as f:
        for line in f:
            if signal_pat.search(line): s['appear']+=1
            if cooldown_pat.search(line): s['allowed_signal']+=1
            if trade_pat.search(line): s['traded']+=1
            if block_quality_pat.search(line): s['blocked_quality']+=1
    for k in stats: stats[k]+=s[k]
    if any(v>0 for v in s.values()):
        per_file[fpath.split('/')[-1]] = s

for fname, s in per_file.items():
    print(f'{fname}: appear={s["appear"]} allowed={s["allowed_signal"]} traded={s["traded"]} blocked={s["blocked_quality"]}')

print(f'\nTotal: appear={stats["appear"]} allowed={stats["allowed_signal"]} traded={stats["traded"]} blocked={stats["blocked_quality"]}')
if stats["appear"] > 0:
    print(f'  allow_rate={stats["allowed_signal"]/stats["appear"]*100:.1f}%')
    print(f'  trade_rate={stats["traded"]/stats["appear"]*100:.1f}%')