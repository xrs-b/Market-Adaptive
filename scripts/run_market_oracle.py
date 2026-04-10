from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_adaptive.bootstrap import MarketAdaptiveBootstrap
from market_adaptive.oracles import MarketOracle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Market-Oracle sensor bot")
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "config" / "config.yaml"),
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single sensing cycle and exit",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level, e.g. INFO/DEBUG",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    bootstrap = MarketAdaptiveBootstrap.from_config_file(args.config)
    bootstrap.initialize()
    oracle = MarketOracle(
        client=bootstrap.okx_client,
        database=bootstrap.database,
        config=bootstrap.config.market_oracle,
    )

    if args.once:
        record = oracle.run_once()
        print(record)
        return

    oracle.run_forever()


if __name__ == "__main__":
    main()
