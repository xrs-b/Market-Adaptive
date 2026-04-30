from scripts.cta_multiwindow_validator import collect_bucket_stability, iter_windows, summarize_overall


def test_iter_windows_builds_sliding_ranges() -> None:
    windows = iter_windows(total_rows=3000, window_size=1200, step_size=600, max_windows=0)
    assert windows == [(0, 1200), (600, 1800), (1200, 2400), (1800, 3000)]


def test_collect_bucket_stability_flags_stable_positive_and_negative_buckets() -> None:
    window_results = [
        {
            'summary': {'realized_pnl': 10.0},
            'by_trigger_family': [
                {'trigger_family': 'bullish_memory_breakout', 'trade_count': 2, 'wins': 2, 'losses': 0, 'total_realized_pnl': 12.0},
                {'trigger_family': 'early_bearish', 'trade_count': 2, 'wins': 0, 'losses': 2, 'total_realized_pnl': -5.0},
            ],
            'by_side': [],
            'by_pathway_quality': [],
            'by_trigger_pathway': [],
        },
        {
            'summary': {'realized_pnl': 8.0},
            'by_trigger_family': [
                {'trigger_family': 'bullish_memory_breakout', 'trade_count': 3, 'wins': 2, 'losses': 1, 'total_realized_pnl': 7.0},
                {'trigger_family': 'early_bearish', 'trade_count': 2, 'wins': 0, 'losses': 2, 'total_realized_pnl': -4.0},
            ],
            'by_side': [],
            'by_pathway_quality': [],
            'by_trigger_pathway': [],
        },
    ]

    stability = collect_bucket_stability(window_results, min_trades=2)
    rows = {row['bucket']: row for row in stability['trigger_family']}

    assert rows['bullish_memory_breakout']['stable_positive'] is True
    assert rows['bullish_memory_breakout']['negative_windows'] == 0
    assert rows['early_bearish']['stable_negative'] is True
    assert rows['early_bearish']['positive_windows'] == 0


def test_summarize_overall_counts_window_directions() -> None:
    summary = summarize_overall([
        {'summary': {'realized_pnl': 10.0}},
        {'summary': {'realized_pnl': -2.0}},
        {'summary': {'realized_pnl': 0.0}},
    ])

    assert summary['windows'] == 3
    assert summary['positive_windows'] == 1
    assert summary['negative_windows'] == 1
    assert summary['flat_windows'] == 1
    assert summary['total_realized_pnl'] == 8.0
