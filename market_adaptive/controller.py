from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
import traceback

import ccxt

try:
    import psutil
except ImportError:  # pragma: no cover - optional runtime dependency fallback
    psutil = None

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import AppConfig
from market_adaptive.coordination import StrategyRuntimeContext
from market_adaptive.db import DatabaseInitializer, SystemStateRecord
from market_adaptive.notifiers import DiscordNotifier, NullNotifier
from market_adaptive.oracles import MarketOracle
from market_adaptive.risk import LogicalPositionSnapshot, RiskControlManager
from market_adaptive.sentiment import SentimentAnalyst
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
        self.runtime_context = StrategyRuntimeContext()

        self.oracle_client = OKXClient(config.okx, config.execution)
        self.cta_client = OKXClient(config.okx, config.execution)
        self.grid_client = OKXClient(config.okx, config.execution)
        self.risk_client = OKXClient(config.okx, config.execution)
        self.shutdown_client = OKXClient(config.okx, config.execution)

        self.sentiment_analyst = SentimentAnalyst(self.cta_client, config.sentiment)
        self.market_oracle = MarketOracle(self.oracle_client, database, config.market_oracle, notifier=self.notifier)
        self.cta_robot = CTARobot(
            self.cta_client,
            database,
            config.cta,
            config.execution,
            notifier=self.notifier,
            sentiment_analyst=self.sentiment_analyst,
            runtime_context=self.runtime_context,
        )
        self.grid_robot = GridRobot(
            self.grid_client,
            database,
            config.grid,
            config.execution,
            notifier=self.notifier,
            market_oracle=self.market_oracle,
            runtime_context=self.runtime_context,
        )
        self.risk_control = RiskControlManager(
            config=config.risk_control,
            runtime_config=config.runtime,
            database=database,
            client=self.risk_client,
            shutdown_client=self.shutdown_client,
            symbols=self.symbols,
            notifier=self.notifier,
            stop_callback=self.stop,
            reduce_grid_exposure_callback=self._reduce_grid_exposure,
            flatten_cta_position_callback=self._flatten_cta_position,
            logical_position_provider=self._collect_logical_positions,
            local_position_reset_callback=self._reset_local_position,
            grid_cleanup_callback=self._cleanup_grid_positions,
        )
        self.cta_robot.risk_manager = self.risk_control
        self.grid_robot.risk_manager = self.risk_control

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    @property
    def starting_equity(self) -> float | None:
        return self.risk_control.daily_start_equity

    @starting_equity.setter
    def starting_equity(self, value: float | None) -> None:
        self.risk_control.daily_start_equity = value

    @property
    def latest_total_pnl(self) -> float:
        return self.risk_control.latest_total_pnl

    def start(self) -> None:
        self.database.initialize()
        self.install_signal_handlers()
        self.risk_control.initialize()
        self.logger.info("Main Controller started | daily_start_equity=%.4f", self.risk_control.daily_start_equity or 0.0)
        self.notifier.send(
            "系统已启动",
            (
                "交易系统已完成启动。\n"
                f"今日起始权益：{(self.risk_control.daily_start_equity or 0.0):.4f} USDT\n"
                f"监控交易对：{', '.join(self.symbols)}"
            ),
        )

        worker_specs = [
            WorkerSpec("market_oracle", self.config.market_oracle.polling_interval_seconds, self.market_oracle.run_once),
            WorkerSpec("cta", self.config.cta.polling_interval_seconds, self.cta_robot.run),
            WorkerSpec("grid", self.config.grid.polling_interval_seconds, self.grid_robot.run),
            WorkerSpec("risk", self.config.runtime.risk_check_interval_seconds, self.monitor_risk_once),
            WorkerSpec(
                "cta_fast_risk",
                self.config.runtime.fast_risk_check_interval_seconds,
                self.monitor_cta_fast_once,
            ),
            WorkerSpec("recovery", self.config.risk_control.recovery_check_interval_seconds, self.recover_orders_once),
            WorkerSpec("main", self.config.runtime.account_check_interval_seconds, self.log_system_health_once),
        ]

        for spec in worker_specs:
            if spec.interval_seconds <= 0:
                continue
            thread = threading.Thread(target=self._worker_loop, args=(spec,), daemon=True, name=spec.name)
            self.threads.append(thread)
            thread.start()

        while not self.stop_event.is_set():
            time.sleep(0.5)

        self._shutdown()

    def stop(self) -> None:
        self.stop_event.set()

    def monitor_risk_once(self):
        return self.risk_control.monitor_once()

    def monitor_cta_fast_once(self) -> str:
        return self.risk_control.monitor_cta_fast_once()

    def recover_orders_once(self) -> str:
        return self.risk_control.recover_positions_once()

    def log_system_health_once(self) -> None:
        cpu_percent = psutil.cpu_percent(interval=None) if psutil is not None else 0.0
        self.logger.info(
            "System heartbeat | cpu=%.2f%% unrealized_pnl=%.4f blocked=%s tracked_symbols=%s",
            cpu_percent,
            self.latest_total_pnl,
            self.risk_control.new_openings_blocked,
            ",".join(self.symbols),
        )

    def _collect_logical_positions(self) -> dict[str, LogicalPositionSnapshot | None]:
        positions: dict[str, LogicalPositionSnapshot | None] = {}
        cta_position = self.cta_robot.get_logical_position()
        positions[self.cta_robot.symbol] = cta_position
        if self.grid_robot.symbol not in positions:
            positions[self.grid_robot.symbol] = None
        return positions

    def _reset_local_position(self, symbol: str, reason: str) -> None:
        if symbol == self.cta_robot.symbol:
            self.cta_robot.reset_local_position(reason)

    def _flatten_cta_position(self, reason: str) -> None:
        result = self.cta_robot.force_risk_exit(reason)
        if self.notifier is not None:
            self.notifier.send(
                "风控执行动作",
                (
                    "CTA 仓位已执行风控离场。\n"
                    f"策略：cta\n"
                    f"交易对：{self.cta_robot.symbol}\n"
                    "动作：全部平仓\n"
                    f"原因：{reason}\n"
                    f"结果：{result}"
                ),
            )

    def _reduce_grid_exposure(self, reason: str, reduction_step_pct: float) -> None:
        result = self.grid_robot.reduce_exposure_step(reason, reduction_step_pct)
        if self.notifier is not None:
            self.notifier.send(
                "风控执行动作",
                (
                    "网格仓位已执行风险收缩。\n"
                    f"策略：grid\n"
                    f"交易对：{self.grid_robot.symbol}\n"
                    "动作：逐步减仓\n"
                    f"减仓步长：{reduction_step_pct:.0%}\n"
                    f"原因：{reason}\n"
                    f"结果：{result}"
                ),
            )

    def _cleanup_grid_positions(self, reason: str) -> str:
        return self.grid_robot.cleanup_for_regime_switch(reason)

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
                if self.notifier is not None and hasattr(self.notifier, "notify_error"):
                    self.notifier.notify_error(
                        str(exc),
                        traceback=traceback.format_exc(),
                        module_name=f"market_adaptive.controller.{spec.name}",
                    )
            except Exception as exc:  # pragma: no cover
                logger.exception("Unexpected worker error: %s", exc)
                if self.notifier is not None and hasattr(self.notifier, "notify_error"):
                    self.notifier.notify_error(
                        str(exc),
                        traceback=traceback.format_exc(),
                        module_name=f"market_adaptive.controller.{spec.name}",
                    )
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
            "系统已停止",
            (
                "交易系统已完成停机。\n"
                f"停止时间：{datetime.fromisoformat(shutdown_at).astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"当前未实现盈亏：{self.latest_total_pnl:.4f} USDT"
            ),
        )
        self.logger.info("Database checkpoint saved. Main Controller stopped.")

    def _handle_signal(self, signum: int, _frame: object) -> None:
        self.logger.info("Signal received: %s | requesting graceful shutdown", signum)
        self.stop_event.set()
