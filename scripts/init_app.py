from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_adaptive.bootstrap import MarketAdaptiveBootstrap


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize Market-Adaptive base environment")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parents[1] / "config" / "config.yaml"),
        help="Path to YAML config file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bootstrap = MarketAdaptiveBootstrap.from_config_file(args.config)
    bootstrap.initialize()
    print(f"Database initialized: {bootstrap.config.database.path}")
    print(f"OKX sandbox enabled: {bootstrap.config.okx.sandbox}")
    print(f"OKX headers: {bootstrap.config.okx.headers}")


if __name__ == "__main__":
    main()
