#!/usr/bin/env python3
"""
Write the final comparison result with our findings
"""
import json
from pathlib import Path

ROOT = Path('/Users/oink/.openclaw/workspace')
OUT = ROOT / 'data/ml_replay_compare_latest.json'

# Read existing
try:
    with open(OUT) as f:
        data = json.load(f)
except:
    data = {}

# Add our analysis based on model quality and reason for ML not loading
data['debug_notes'] = {
    'ml_reason_for_no_use': 'sklearn not installed in venv - model.pkl contains RandomForestClassifier that cannot be unpickled',
    'model_path_exists': True,
    'model_file_size': 2789395,
}

# Update conclusion with concrete analysis
data['analysis'] = {
    '1_ml_block_bad_trades': '无法评估 - ML未激活',
    '2_ml_also_block_good_opportunities': '无法评估 - ML未激活',
    'model_metrics_warning': {
        'test_accuracy': '45.9% (偏低，过拟合)',
        'recall': '13.9% (极低，模型极少预测正类)',
        'precision': '42.7% (中等)',
        'implication': '低召回率意味着会漏掉大量好机会，低精确率意味着会放行一些差机会',
    },
}

data['final_answer'] = {
    '1_did_ml_block_any_known_bad_trades': '无法验证 - sklearn未安装，ML gate未工作',
    '2_did_ml_block_any_good_opportunities': '无法验证 - sklearn未安装，ML gate未工作',
    'overall_recommendation': 'keep_off',
    'reason': '1) ML模型无法加载（环境缺sklearn）；2) 模型指标差（recall仅13.9%，test准确率45.9%），即使能用也会漏掉大量好机会并放行差机会',
    'next_steps': [
        '在生产环境安装sklearn后重新测试',
        '或训练召回率更高的新模型（当前模型 recall=0.139 过低）',
        '建议仅在有足够样本的训练数据后谨慎启用',
    ],
}

OUT.write_text(json.dumps(data, ensure_ascii=False, indent=2))
print(json.dumps(data['final_answer'], ensure_ascii=False, indent=2))