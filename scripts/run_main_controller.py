from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_adaptive.bootstrap import MarketAdaptiveBootstrap
from market_adaptive.controller import MainController
from market_adaptive.logging_utils import configure_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Market-Adaptive Main Controller")
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "config" / "config.yaml"),
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level, e.g. INFO/DEBUG",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    bootstrap = MarketAdaptiveBootstrap.from_config_file(args.config)
    bootstrap.initialize()
    controller = MainController(bootstrap.config, bootstrap.database)
    controller.start()


if __name__ == "__main__":
    main()
