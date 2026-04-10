from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import ccxt

try:
    import psutil
except ImportError:  # pragma: no cover - optional runtime dependency fallback
    psutil = None

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import AppConfig
from market_adaptive.db import DatabaseInitializer, SystemStateRecord
from market_adaptive.notifiers import DiscordNotifier, NullNotifier
from market_adaptive.oracles import MarketOracle
from market_adaptive.strategies import CTARobot, GridRobot


@dataclass
class WorkerSpec:
    name: str
    interval_seconds: int
    target: Callable[[], object]


class MainController:
    def __init__(self, config: AppConfig, database: DatabaseInitializer) -> None:
        self.config = config
        self.database = database
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []
        self.logger = logging.LoggerAdapter(logging.getLogger(__name__), {"robot": "main"})
        self.symbols = sorted({config.market_oracle.symbol, config.cta.symbol, config.grid.symbol})
        self.notifier = DiscordNotifier(config.notification.discord) if config.notification.discord.enabled else NullNotifier()

        self.oracle_client = OKXClient(config.okx, config.execution)
        self.cta_client = OKXClient(config.okx, config.execution)
        self.grid_client = OKXClient(config.okx, config.execution)
        self.risk_client = OKXClient(config.okx, config.execution)
        self.shutdown_client = OKXClient(config.okx, config.execution)

        self.market_oracle = MarketOracle(self.oracle_client, database, config.market_oracle, notifier=self.notifier)
        self.cta_robot = CTARobot(self.cta_client, database, config.cta, config.execution, notifier=self.notifier)
        self.grid_robot = GridRobot(self.grid_client, database, config.grid, config.execution, notifier=self.notifier)

        self.starting_equity: float | None = None
        self.latest_total_pnl: float = 0.0

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def start(self) -> None:
        self.database.initialize()
        self.install_signal_handlers()
        self.starting_equity = self.risk_client.fetch_total_equity()
        self.logger.info("Main Controller started | starting_equity=%.4f", self.starting_equity)
        self.notifier.send("System Started", f"starting_equity={self.starting_equity:.4f} | symbols={','.join(self.symbols)}")

        worker_specs = [
            WorkerSpec("market_oracle", self.config.market_oracle.polling_interval_seconds, self.market_oracle.run_once),
            WorkerSpec("cta", self.config.cta.polling_interval_seconds, self.cta_robot.run),
            WorkerSpec("grid", self.config.grid.polling_interval_seconds, self.grid_robot.run),
            WorkerSpec("risk", self.config.runtime.risk_check_interval_seconds, self.monitor_risk_once),
            WorkerSpec("main", self.config.runtime.account_check_interval_seconds, self.log_system_health_once),
        ]

        for spec in worker_specs:
            thread = threading.Thread(target=self._worker_loop, args=(spec,), daemon=True, name=spec.name)
            self.threads.append(thread)
            thread.start()

        while not self.stop_event.is_set():
            time.sleep(0.5)

        self._shutdown()

    def stop(self) -> None:
        self.stop_event.set()

    def monitor_risk_once(self) -> None:
        logger = logging.LoggerAdapter(logging.getLogger(__name__), {"robot": "risk"})
        current_equity = self.risk_client.fetch_total_equity()
        total_pnl = self.risk_client.fetch_total_unrealized_pnl(self.symbols)
        self.latest_total_pnl = total_pnl

        if self.starting_equity is None or self.starting_equity <= 0:
            self.starting_equity = current_equity

        drawdown = max(0.0, (self.starting_equity - current_equity) / self.starting_equity)
        logger.info(
            "Risk heartbeat | equity=%.4f unrealized_pnl=%.4f drawdown=%.2f%%",
            current_equity,
            total_pnl,
            drawdown * 100,
        )
        if drawdown > 0.05:
            logger.error("Max drawdown breached: %.2f%% > 5.00%% | flattening now", drawdown * 100)
            self.notifier.send(
                "Risk Triggered",
                f"drawdown={drawdown * 100:.2f}% | equity={current_equity:.4f} | unrealized_pnl={total_pnl:.4f}",
            )
            self.shutdown_client.cancel_all_orders_for_symbols(self.symbols)
            self.shutdown_client.close_all_positions_for_symbols(self.symbols)
            self.stop_event.set()

    def log_system_health_once(self) -> None:
        cpu_percent = psutil.cpu_percent(interval=None) if psutil is not None else 0.0
        self.logger.info(
            "System heartbeat | cpu=%.2f%% unrealized_pnl=%.4f tracked_symbols=%s",
            cpu_percent,
            self.latest_total_pnl,
            ",".join(self.symbols),
        )

    def _worker_loop(self, spec: WorkerSpec) -> None:
        logger = logging.LoggerAdapter(logging.getLogger(__name__), {"robot": spec.name})
        logger.info("Worker started | interval=%ss", spec.interval_seconds)
        while not self.stop_event.is_set():
            started_at = time.time()
            try:
                result = spec.target()
                logger.info("Cycle completed | result=%s", result)
            except (ccxt.NetworkError, ccxt.ExchangeError, TimeoutError, ValueError) as exc:
                logger.warning("Transient worker error: %s", exc, exc_info=True)
            except Exception as exc:  # pragma: no cover
                logger.exception("Unexpected worker error: %s", exc)
            elapsed = time.time() - started_at
            sleep_seconds = max(0.0, spec.interval_seconds - elapsed)
            if self.stop_event.wait(sleep_seconds):
                break
        logger.info("Worker exiting")

    def _shutdown(self) -> None:
        if self.config.runtime.shutdown_cancel_open_orders:
            self.logger.info("Graceful shutdown: cancelling open orders and closing positions")
            try:
                self.shutdown_client.cancel_all_orders_for_symbols(self.symbols)
                self.shutdown_client.close_all_positions_for_symbols(self.symbols)
            except Exception as exc:  # pragma: no cover
                self.logger.exception("Shutdown cleanup failed: %s", exc)

        shutdown_at = datetime.now(timezone.utc).isoformat()
        self.database.upsert_system_state(
            SystemStateRecord(
                state_key="last_shutdown_at",
                state_value=shutdown_at,
                updated_at=shutdown_at,
            )
        )
        self.notifier.send(
            "System Stopped",
            f"shutdown_at={shutdown_at} | unrealized_pnl={self.latest_total_pnl:.4f}",
        )
        self.logger.info("Database checkpoint saved. Main Controller stopped.")

    def _handle_signal(self, signum: int, _frame: object) -> None:
        self.logger.info("Signal received: %s | requesting graceful shutdown", signum)
        self.stop_event.set()
