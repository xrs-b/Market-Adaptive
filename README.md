# Market-Adaptive

Market-Adaptive 是一个面向 OKX 模拟盘的模块化量化交易系统骨架。

## 当前已完成
- YAML 全局配置加载
- OKX 模拟盘连接配置（含 `x-simulated-id` 与 `x-simulated-trading` 请求头）
- SQLite 初始化模块
- `market_status` 表与索引初始化
- 可复用的 `OKXClient`
- 统一 bootstrap 入口

## 目录结构

```text
Market-Adaptive/
├── config/
│   └── config.yaml.example
├── market_adaptive/
│   ├── clients/
│   │   └── okx_client.py
│   ├── bootstrap.py
│   ├── config.py
│   └── db.py
├── scripts/
│   └── init_app.py
└── requirements.txt
```

## 快速开始

```bash
cd Market-Adaptive
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config/config.yaml.example config/config.yaml
python3 scripts/init_app.py --config config/config.yaml
```

后续机器人可直接复用：
- `market_adaptive.config.load_config`
- `market_adaptive.db.DatabaseInitializer`
- `market_adaptive.clients.OKXClient`
- `market_adaptive.bootstrap.MarketAdaptiveBootstrap`
