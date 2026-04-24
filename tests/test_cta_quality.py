from market_adaptive.cta_quality import summarize_cta_trade_quality


def test_cta_quality_summary_builds_bucket_rows():
    trades = [
        {
            'symbol': 'BTC/USDT',
            'side': 'long',
            'trigger_family': 'starter_frontrun',
            'entry_pathway': 'FAST_TRACK',
            'quality_tier': 'TIER_HIGH',
            'realized_pnl': 12.5,
            'fees': 1.2,
            'holding_minutes': 18,
            'quick_trade_mode': False,
            'relaxed_entry': False,
        },
        {
            'symbol': 'BTC/USDT',
            'side': 'long',
            'trigger_family': 'starter_frontrun',
            'entry_pathway': 'FAST_TRACK',
            'quality_tier': 'TIER_HIGH',
            'realized_pnl': -4.0,
            'fees': 1.0,
            'holding_minutes': 11,
            'quick_trade_mode': False,
            'relaxed_entry': False,
        },
        {
            'symbol': 'BTC/USDT',
            'side': 'short',
            'trigger_family': 'obv_scalp',
            'entry_pathway': 'STANDARD',
            'quality_tier': 'TIER_MEDIUM',
            'realized_pnl': 6.0,
            'fees': 0.8,
            'holding_minutes': 6,
            'quick_trade_mode': True,
            'relaxed_entry': True,
        },
    ]

    report = summarize_cta_trade_quality(trades)

    assert report['summary']['trade_count'] == 3
    assert report['summary']['wins'] == 2
    assert report['summary']['quick_trade_count'] == 1
    assert report['summary']['relaxed_entry_count'] == 1

    by_trigger = {row['trigger_family']: row for row in report['by_trigger_family']}
    assert by_trigger['starter_frontrun']['trade_count'] == 2
    assert by_trigger['starter_frontrun']['win_rate_pct'] == 50.0
    assert by_trigger['obv_scalp']['quick_trade_count'] == 1

    by_pathway_quality = {
        (row['entry_pathway'], row['quality_tier']): row for row in report['by_pathway_quality']
    }
    assert by_pathway_quality[('FAST_TRACK', 'TIER_HIGH')]['trade_count'] == 2
    assert by_pathway_quality[('STANDARD', 'TIER_MEDIUM')]['short_count'] == 1
