from __future__ import annotations

import logging

RESET = "\033[0m"
COLORS = {
    "market_oracle": "\033[94m",
    "cta": "\033[92m",
    "grid": "\033[96m",
    "risk": "\033[91m",
    "main": "\033[95m",
}


class ColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "robot"):
            record.robot = record.name.split(".")[-1]
        base = super().format(record)
        robot_name = getattr(record, "robot", record.name.split(".")[-1])
        color = COLORS.get(robot_name, "")
        return f"{color}{base}{RESET}" if color else base


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(
        ColorFormatter("%(asctime)s [%(levelname)s] [%(robot)s] %(message)s")
    )
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
