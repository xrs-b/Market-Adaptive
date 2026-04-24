# DEPLOYMENT.md

## Goal

This repository should be deployable as a **single self-contained project directory**.

Expected layout after clone:

```text
Market-Adaptive/
├── market_adaptive/
├── scripts/
├── admin-api/
├── admin-web/
├── config/
│   ├── config.yaml.example
│   └── config.yaml          # local only
├── .venv/                   # local only
├── logs/                    # local only
├── data/                    # local only
└── README.md
```

## Local-only paths

These should exist locally but should not be committed:

- `.venv/`
- `config/config.yaml`
- `logs/`
- `data/`
- `tmp/`
- `admin-web/node_modules/`
- `admin-web/dist/`

## Bootstrap

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config/config.yaml.example config/config.yaml
python scripts/init_app.py --config config/config.yaml
```

## Run

### Main controller

```bash
.venv/bin/python scripts/run_main_controller.py --config config/config.yaml
```

### Market oracle only

```bash
.venv/bin/python scripts/run_market_oracle.py --config config/config.yaml --once
```

### CTA/Grid coordinator

```bash
.venv/bin/python scripts/run_the_hands.py --config config/config.yaml
```

## launchd / service scripts

Service wrappers should resolve paths relative to the repository root and use:

- Python: `.venv/bin/python`
- Config: `config/config.yaml`
- Logs: `logs/`
- Data: `data/`

They should not require a sibling or external `Market-Adaptive/` runtime-assets directory.

## Migration note

Historically this machine used a split layout:

- source code in `market_adaptive/`
- runtime assets in a separate `Market-Adaptive/` directory

That layout is being retired in favor of the single-directory repo layout above.
