from __future__ import annotations

import os
import re
import secrets
import signal
import subprocess
from collections import deque
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import psutil
import yaml
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from market_adaptive.clients.okx_client import OKXClient
from market_adaptive.config import AppConfig, load_config
from market_adaptive.db import AccountDailySnapshotRecord, DatabaseInitializer, SystemStateRecord

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = ROOT / "Market-Adaptive" / "config" / "config.yaml"
DEFAULT_LOG_PATH = Path("/Users/oink/.openclaw/logs/main_controller_manual.log")
RESTART_SCRIPT = ROOT / "scripts" / "restart_main_controller.sh"
RUN_SCRIPT = ROOT / "scripts" / "run_main_controller.py"
PROCESS_MATCH = str(RUN_SCRIPT)
ANSI_ESCAPE = "\u001b["
TIME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
WORKER_RE = re.compile(r"\[(main|cta|grid|risk|market_oracle|recovery|cta_fast_risk)\]")
LEVEL_RE = re.compile(r"\[(INFO|WARNING|ERROR)\]")
RISK_HEARTBEAT_RE = re.compile(
    r"equity=(?P<equity>-?\d+(?:\.\d+)?)\s+"
    r"daily_start=(?P<daily_start>-?\d+(?:\.\d+)?)\s+"
    r"drawdown=(?P<drawdown>-?\d+(?:\.\d+)?)%\s+"
    r"unrealized_pnl=(?P<unrealized_pnl>-?\d+(?:\.\d+)?)\s+"
    r"margin_ratio=(?P<margin_ratio>-?\d+(?:\.\d+)?)%\s+"
    r"position_notional=(?P<position_notional>-?\d+(?:\.\d+)?)\s+"
    r"open_order_notional=(?P<open_order_notional>-?\d+(?:\.\d+)?)\s+"
    r"total_notional=(?P<total_notional>-?\d+(?:\.\d+)?)\s+"
    r"blocked=(?P<blocked>True|False)"
)

app = FastAPI(title="Market Adaptive Admin API", version="0.4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SESSIONS: dict[str, dict[str, Any]] = {}


class LoginPayload(BaseModel):
    username: str
    password: str


class ConfirmPayload(BaseModel):
    confirm: bool = False


class AdminSettings(BaseModel):
    username: str
    password: str


class AppSettings(BaseModel):
    config_path: Path
    log_path: Path
    admin: AdminSettings


class ConfigSavePayload(BaseModel):
    values: dict[str, Any]


class InitialEquityPayload(BaseModel):
    initialEquity: float


class LogSnapshot(BaseModel):
    main: str | None = None
    cta: str | None = None
    grid: str | None = None
    risk: str | None = None
    oracle: str | None = None


WORKER_NAME_MAP = {
    "main": "主控",
    "cta": "CTA",
    "grid": "网格",
    "risk": "风控",
    "market_oracle": "市场判定",
    "recovery": "恢复器",
    "cta_fast_risk": "CTA 快速风控",
}

CONFIG_SCHEMA: list[dict[str, Any]] = [
    {
        "section": "runtime",
        "label": "运行参数",
        "description": "影响轮询频率、资金基线等全局运行行为。",
        "fields": [
            {"path": "runtime.account_check_interval_seconds", "label": "账户检查间隔(秒)", "group": "轮询频率", "type": "number", "description": "主控轮询账户状态的间隔。", "mutable": True, "restartRequired": True, "step": 1},
            {"path": "runtime.risk_check_interval_seconds", "label": "风控检查间隔(秒)", "group": "轮询频率", "type": "number", "description": "风控快照刷新间隔。", "mutable": True, "restartRequired": True, "step": 1},
            {"path": "runtime.account_initial_equity", "label": "初始资金基线", "group": "资金展示", "type": "number", "description": "用于总盈亏展示的初始权益。", "mutable": True, "restartRequired": False, "step": 0.01},
        ],
    },
    {
        "section": "execution",
        "label": "执行参数",
        "description": "控制下单模式和默认下单数量。",
        "fields": [
            {"path": "execution.td_mode", "label": "持仓模式", "group": "交易执行", "type": "select", "options": ["isolated", "cross"], "description": "交易所下单使用的 td_mode。", "mutable": True, "restartRequired": True},
            {"path": "execution.cta_order_size", "label": "CTA 默认下单量", "group": "交易执行", "type": "number", "description": "CTA 下单基准数量。", "mutable": True, "restartRequired": True, "step": 0.0001},
            {"path": "execution.grid_order_size", "label": "网格默认下单量", "group": "交易执行", "type": "number", "description": "网格下单基准数量。", "mutable": True, "restartRequired": True, "step": 0.0001},
        ],
    },
    {
        "section": "cta",
        "label": "CTA 参数",
        "description": "CTA 的风险、止盈止损与信号过滤参数。",
        "fields": [
            {"path": "cta.minimum_expected_rr", "label": "最小预期 RR", "group": "RR / 入口过滤", "type": "number", "description": "基础 RR 下限，低于该值不放行。", "mutable": True, "restartRequired": True, "step": 0.01},
            {"path": "cta.relaxed_entry_minimum_expected_rr", "label": "relaxed 入口最小 RR", "group": "RR / 入口过滤", "type": "number", "description": "宽松入口额外 RR 门槛。", "mutable": True, "restartRequired": True, "step": 0.01},
            {"path": "cta.starter_entry_minimum_expected_rr", "label": "starter 入口最小 RR", "group": "RR / 入口过滤", "type": "number", "description": "starter / early 类入口 RR 门槛。", "mutable": True, "restartRequired": True, "step": 0.01},
            {"path": "cta.breakout_rr_target_atr_multiplier", "label": "突破 RR ATR 倍数", "group": "RR / 入口过滤", "type": "number", "description": "突破形态目标价的 ATR 扩展倍数。", "mutable": True, "restartRequired": True, "step": 0.1},
            {"path": "cta.risk_percent_per_trade", "label": "单笔风险比例", "group": "风险仓位", "highImpact": True, "type": "number", "description": "普通信号每笔风险占比。", "mutable": True, "restartRequired": True, "step": 0.001},
            {"path": "cta.boosted_risk_percent_per_trade", "label": "高质量单笔风险比例", "group": "风险仓位", "highImpact": True, "type": "number", "description": "高质量信号的提升风险占比。", "mutable": True, "restartRequired": True, "step": 0.001},
            {"path": "cta.first_take_profit_pct", "label": "第一止盈比例", "group": "止盈设置", "type": "number", "description": "第一档止盈百分比。", "mutable": True, "restartRequired": True, "step": 0.001},
            {"path": "cta.first_take_profit_size", "label": "第一止盈减仓比例", "group": "止盈设置", "type": "number", "description": "第一档止盈减仓仓位比例。", "mutable": True, "restartRequired": True, "step": 0.01},
            {"path": "cta.second_take_profit_pct", "label": "第二止盈比例", "group": "止盈设置", "type": "number", "description": "第二档止盈百分比。", "mutable": True, "restartRequired": True, "step": 0.001},
            {"path": "cta.second_take_profit_size", "label": "第二止盈减仓比例", "group": "止盈设置", "type": "number", "description": "第二档止盈减仓仓位比例。", "mutable": True, "restartRequired": True, "step": 0.01},
            {"path": "cta.early_entry_direction_confirmation_bars", "label": "early 方向确认K数", "group": "RR / 入口过滤", "type": "number", "description": "early/starter 类入口要求方向稳定的连续 K 数。", "mutable": True, "restartRequired": True, "step": 1},
            {"path": "cta.heartbeat_interval_seconds", "label": "CTA 心跳间隔(秒)", "group": "诊断 / 报告", "type": "number", "description": "CTA 诊断心跳输出频率。", "mutable": True, "restartRequired": True, "step": 1},
            {"path": "cta.near_miss_report_interval_seconds", "label": "near-miss 报告间隔(秒)", "group": "诊断 / 报告", "type": "number", "description": "错失信号分析报告的最短间隔。", "mutable": True, "restartRequired": True, "step": 1},
        ],
    },
    {
        "section": "grid",
        "label": "网格参数",
        "description": "网格资金分配、层数、重建和异常防护参数。",
        "fields": [
            {"path": "grid.equity_allocation_ratio", "label": "网格资金占比", "group": "资金与杠杆", "highImpact": True, "type": "number", "description": "网格使用账户权益的比例。", "mutable": True, "restartRequired": True, "step": 0.01},
            {"path": "grid.leverage", "label": "网格杠杆", "group": "资金与杠杆", "highImpact": True, "type": "number", "description": "网格订单使用的杠杆。", "mutable": True, "restartRequired": True, "step": 1},
            {"path": "grid.levels", "label": "网格层数", "group": "网格结构", "highImpact": True, "type": "number", "description": "网格总层数。", "mutable": True, "restartRequired": True, "step": 1},
            {"path": "grid.min_spacing_ratio", "label": "最小间距比例", "group": "网格结构", "type": "number", "description": "网格最小层间距比例。", "mutable": True, "restartRequired": True, "step": 0.0001},
            {"path": "grid.atr_multiplier", "label": "ATR 范围倍数", "group": "网格结构", "type": "number", "description": "动态区间范围的 ATR 倍数。", "mutable": True, "restartRequired": True, "step": 0.1},
            {"path": "grid.regrid_trigger_atr_ratio", "label": "重建触发 ATR 比例", "group": "重建控制", "type": "number", "description": "价格偏离中心到多大程度触发重建。", "mutable": True, "restartRequired": True, "step": 0.01},
            {"path": "grid.hard_reanchor_atr_ratio", "label": "硬重锚 ATR 比例", "group": "重建控制", "highImpact": True, "type": "number", "description": "达到该比例时直接强制重锚。", "mutable": True, "restartRequired": True, "step": 0.01},
            {"path": "grid.flash_crash_enabled", "label": "启用闪崩防护", "group": "异常防护", "type": "boolean", "description": "是否启用 flash crash 保护。", "mutable": True, "restartRequired": True},
            {"path": "grid.flash_crash_atr_multiplier", "label": "闪崩 ATR 倍数", "group": "异常防护", "type": "number", "description": "闪崩阈值使用的 ATR 倍数。", "mutable": True, "restartRequired": True, "step": 0.1},
            {"path": "grid.flash_crash_cooldown_seconds", "label": "闪崩冷却(秒)", "group": "异常防护", "type": "number", "description": "触发闪崩保护后的冷却时间。", "mutable": True, "restartRequired": True, "step": 1},
        ],
    },
    {
        "section": "market_oracle",
        "label": "市场判定参数",
        "description": "控制市场状态判定频率和阈值。",
        "fields": [
            {"path": "market_oracle.polling_interval_seconds", "label": "判定轮询间隔(秒)", "group": "轮询频率", "type": "number", "description": "市场判定刷新间隔。", "mutable": True, "restartRequired": True, "step": 1},
            {"path": "market_oracle.trend_adx_threshold", "label": "趋势 ADX 阈值", "group": "判定阈值", "type": "number", "description": "大于该值更倾向判定趋势。", "mutable": True, "restartRequired": True, "step": 0.1},
            {"path": "market_oracle.sideways_adx_threshold", "label": "震荡 ADX 阈值", "group": "判定阈值", "type": "number", "description": "低于该值更倾向判定震荡。", "mutable": True, "restartRequired": True, "step": 0.1},
            {"path": "market_oracle.impulse_consecutive_bars", "label": "冲击连续K数", "group": "判定阈值", "type": "number", "description": "识别 impulse 需要连续满足的 bars 数。", "mutable": True, "restartRequired": True, "step": 1},
        ],
    },
]


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_settings() -> AppSettings:
    return AppSettings(
        config_path=Path(os.getenv("ADMIN_CONFIG_PATH", str(DEFAULT_CONFIG_PATH))).expanduser().resolve(),
        log_path=Path(os.getenv("ADMIN_LOG_PATH", str(DEFAULT_LOG_PATH))).expanduser().resolve(),
        admin=AdminSettings(
            username=os.getenv("ADMIN_USERNAME", "admin"),
            password=os.getenv("ADMIN_PASSWORD", "admin123"),
        ),
    )


def safe_okx(default: Any, fn, *, context: str) -> Any:
    try:
        return fn()
    except Exception as exc:
        print(f"[admin-api] degraded {context}: {exc}")
        return default


def strip_ansi(value: str) -> str:
    while ANSI_ESCAPE in value:
        start = value.find(ANSI_ESCAPE)
        end = value.find("m", start)
        if end == -1:
            break
        value = value[:start] + value[end + 1 :]
    return value


def tail_lines(path: Path, limit: int = 200) -> list[str]:
    if not path.exists():
        return []
    bucket: deque[str] = deque(maxlen=limit)
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw in handle:
            bucket.append(strip_ansi(raw.rstrip()))
    return list(bucket)


def load_runtime_config(settings: AppSettings) -> AppConfig:
    return load_config(settings.config_path)


def load_database(settings: AppSettings) -> DatabaseInitializer:
    cfg = load_runtime_config(settings)
    database = DatabaseInitializer(cfg.database.path)
    database.initialize()
    return database


def resolve_initial_equity(settings: AppSettings, payload: dict[str, Any] | None = None) -> tuple[float, str, str]:
    database = load_database(settings)
    stored = database.get_system_state("account_initial_equity")
    if stored is not None:
        try:
            value = float(stored.state_value)
            if value > 0:
                return value, "database", str(stored.updated_at)
        except (TypeError, ValueError):
            pass
    runtime_payload = payload or read_config_payload(settings)
    configured = max(0.0, float(get_by_path(runtime_payload, "runtime.account_initial_equity") or 0.0))
    if configured > 0:
        return configured, "config", now_text()
    return 0.0, "unset", now_text()


def set_initial_equity(settings: AppSettings, value: float) -> tuple[float, bool]:
    baseline = float(value)
    if baseline <= 0:
        raise HTTPException(status_code=400, detail="初始资金必须大于 0")
    database = load_database(settings)
    stored = database.get_system_state("account_initial_equity")
    if stored is not None:
        try:
            if abs(float(stored.state_value) - baseline) <= 1e-9:
                return baseline, False
        except (TypeError, ValueError):
            pass
    database.upsert_system_state(SystemStateRecord("account_initial_equity", f"{baseline:.12f}", datetime.now().isoformat()))
    return baseline, True


def backfill_daily_snapshots_from_logs(settings: AppSettings) -> dict[str, Any]:
    database = load_database(settings)
    initial_equity, _, _ = resolve_initial_equity(settings)
    log_paths = [settings.log_path]
    archive_dir = settings.log_path.parent / 'archive'
    if archive_dir.exists():
        log_paths.extend(sorted(archive_dir.glob('main_controller-*.log')))

    daily_rows: dict[str, dict[str, Any]] = {}
    for path in log_paths:
        if not path.exists():
            continue
        with path.open('r', encoding='utf-8', errors='ignore') as handle:
            for raw in handle:
                parsed = parse_risk_heartbeat(strip_ansi(raw.rstrip()))
                if not parsed:
                    continue
                snapshot_date = str(parsed['time'])[:10]
                existing = daily_rows.get(snapshot_date)
                if existing is None or str(parsed['time']) > str(existing['time']):
                    daily_rows[snapshot_date] = parsed

    written = 0
    months: set[str] = set()
    for snapshot_date, item in sorted(daily_rows.items()):
        equity = float(item['equity'])
        daily_start_equity = float(item['daily_start'])
        daily_pnl = float(item['daily_pnl'])
        total_pnl = equity - float(initial_equity)
        database.upsert_account_daily_snapshot(
            AccountDailySnapshotRecord(
                snapshot_date=snapshot_date,
                settled_at=str(item['time']),
                equity=equity,
                daily_start_equity=daily_start_equity,
                daily_pnl=daily_pnl,
                initial_equity=float(initial_equity),
                total_pnl=total_pnl,
            )
        )
        written += 1
        months.add(snapshot_date[:7])

    return {
        'ok': True,
        'writtenDays': written,
        'months': sorted(months),
        'message': f'已从日志回填 {written} 天资金快照',
        '刷新时间': now_text(),
    }


def build_calendar_payload(settings: AppSettings, month: str | None = None) -> dict[str, Any]:
    cfg = load_runtime_config(settings)
    database = load_database(settings)
    now = datetime.now()
    current_month = now.strftime("%Y-%m")
    selected_month = month or current_month
    if not re.match(r"^\d{4}-\d{2}$", selected_month):
        raise HTTPException(status_code=400, detail="month 参数格式必须为 YYYY-MM")

    initial_equity, source, updated_at = resolve_initial_equity(settings)
    snapshots = database.fetch_account_daily_snapshots(selected_month)
    items = [
        {
            "date": item.snapshot_date,
            "day": int(item.snapshot_date[-2:]),
            "equity": item.equity,
            "dailyPnl": item.daily_pnl,
            "dailyStartEquity": item.daily_start_equity,
            "initialEquity": item.initial_equity,
            "totalPnl": item.total_pnl,
            "settledAt": item.settled_at,
            "kind": "settled",
        }
        for item in snapshots
    ]

    if selected_month == current_month:
        client = OKXClient(cfg.okx, cfg.execution)
        current_equity = float(safe_okx(initial_equity, lambda: client.fetch_total_equity("USDT"), context="account_daily_calendar:current_equity"))
        daily_state = database.get_system_state("risk_daily_start_equity")
        daily_start_equity = float(daily_state.state_value) if daily_state is not None else current_equity
        total_pnl = current_equity - initial_equity
        today = now.strftime("%Y-%m-%d")
        live_item = {
            "date": today,
            "day": int(today[-2:]),
            "equity": current_equity,
            "dailyPnl": current_equity - daily_start_equity,
            "dailyStartEquity": daily_start_equity,
            "initialEquity": initial_equity,
            "totalPnl": total_pnl,
            "settledAt": now_text(),
            "kind": "live",
        }
        items = [item for item in items if item["date"] != today]
        items.append(live_item)

    items = sorted(items, key=lambda item: item["date"])
    month_total_pnl = sum(float(item["dailyPnl"]) for item in items)
    return {
        "month": selected_month,
        "initialEquity": initial_equity,
        "initialEquitySource": source,
        "initialEquityUpdatedAt": updated_at,
        "monthTotalPnl": month_total_pnl,
        "dayCount": len(items),
        "items": items,
        "刷新时间": now_text(),
    }


def main_controller_pids() -> list[int]:
    pids: list[int] = []
    for proc in psutil.process_iter(attrs=["pid", "cmdline"]):
        try:
            cmdline = " ".join(proc.info.get("cmdline") or [])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if PROCESS_MATCH in cmdline:
            pids.append(int(proc.info["pid"]))
    return sorted(set(pids))


def process_running() -> bool:
    return bool(main_controller_pids())


def extract_time(line: str | None) -> str:
    if not line:
        return "--"
    match = TIME_RE.search(line)
    return match.group(1) if match else "--"


def extract_worker(line: str | None) -> str:
    if not line:
        return "未知"
    match = WORKER_RE.search(line)
    key = match.group(1) if match else ""
    return WORKER_NAME_MAP.get(key, key or "未知")


def extract_level(line: str | None) -> str:
    if not line:
        return "信息"
    match = LEVEL_RE.search(line)
    level = match.group(1) if match else "INFO"
    return {"INFO": "信息", "WARNING": "警告", "ERROR": "错误"}.get(level, level)


def latest_worker_actions(lines: list[str]) -> LogSnapshot:
    snapshot = LogSnapshot()
    for line in reversed(lines):
        if snapshot.cta is None and "[cta] Cycle completed" in line:
            snapshot.cta = line
        if snapshot.grid is None and "[grid] Cycle completed" in line:
            snapshot.grid = line
        if snapshot.risk is None and "[risk] Risk heartbeat" in line:
            snapshot.risk = line
        if snapshot.main is None and "[main] System heartbeat" in line:
            snapshot.main = line
        if snapshot.oracle is None and "[market_oracle]" in line:
            snapshot.oracle = line
        if all(getattr(snapshot, key) is not None for key in ["cta", "grid", "risk", "main", "oracle"]):
            break
    return snapshot


def extract_cta_status(cta_line: str | None) -> str:
    if not cta_line:
        return "未知"
    if "skip:inactive" in cta_line:
        return "未激活"
    if "cta:open_long" in cta_line or "cta:open_short" in cta_line:
        return "已开仓"
    if "cta:bullish_ready" in cta_line:
        return "多头待命"
    if "cta:order_flow_blocked" in cta_line:
        return "订单流拦截"
    if "cta:reward_risk_blocked" in cta_line:
        return "盈亏比拦截"
    return "运行中"


def extract_grid_status(grid_line: str | None) -> str:
    if not grid_line:
        return "未知"
    if "skip:inactive" in grid_line:
        return "未激活"
    if "placed_" in grid_line:
        return "运行中"
    return "运行中"


def summarize_worker_line(name: str, line: str | None) -> dict[str, str]:
    return {
        "名称": name,
        "时间": extract_time(line),
        "级别": extract_level(line),
        "内容": line or "--",
    }


def summarize_log_line(line: str) -> dict[str, str]:
    category = "普通"
    if "[TRADE_OPEN]" in line or "cta:open_" in line:
        category = "开仓"
    elif "reward/risk blocked" in line or "cta:reward_risk_blocked" in line:
        category = "盈亏比拦截"
    elif "order flow" in line.lower() or "cta:order_flow_blocked" in line:
        category = "订单流"
    elif "Risk heartbeat" in line or "风控" in line:
        category = "风控"
    elif "signal heartbeat" in line or "Strategy audit snapshot" in line:
        category = "信号"
    return {
        "时间": extract_time(line),
        "模块": extract_worker(line),
        "级别": extract_level(line),
        "分类": category,
        "内容": line,
    }


def extract_cta_event_label(line: str) -> str | None:
    for label in [
        "cta:open_long",
        "cta:open_short",
        "cta:bullish_ready",
        "cta:bearish_ready",
        "cta:order_flow_blocked",
        "cta:reward_risk_blocked",
        "skip:inactive",
    ]:
        if label in line:
            return label
    return None


def parse_risk_heartbeat(line: str) -> dict[str, Any] | None:
    if "[risk] Risk heartbeat" not in line:
        return None
    match = RISK_HEARTBEAT_RE.search(line)
    if not match:
        return None
    data = match.groupdict()
    equity = float(data["equity"])
    daily_start = float(data["daily_start"])
    return {
        "time": extract_time(line),
        "equity": equity,
        "daily_start": daily_start,
        "daily_pnl": equity - daily_start,
        "drawdown_pct": float(data["drawdown"]),
        "unrealized_pnl": float(data["unrealized_pnl"]),
        "margin_ratio_pct": float(data["margin_ratio"]),
        "position_notional": float(data["position_notional"]),
        "open_order_notional": float(data["open_order_notional"]),
        "total_notional": float(data["total_notional"]),
        "blocked": data["blocked"] == "True",
        "blocked_value": 1.0 if data["blocked"] == "True" else 0.0,
        "raw": line,
    }


def build_timeline(lines: list[str]) -> dict[str, Any]:
    activity_map: dict[str, int] = {}
    cta_events: list[dict[str, Any]] = []
    risk_events: list[dict[str, Any]] = []
    equity_points: list[dict[str, Any]] = []

    for line in lines:
        time_text = extract_time(line)
        if time_text != "--":
            minute_key = time_text[:16] + ":00"
            activity_map[minute_key] = activity_map.get(minute_key, 0) + 1

        if "[cta] Cycle completed" in line:
            label = extract_cta_event_label(line)
            if label:
                cta_events.append({"time": time_text, "label": label, "raw": line})

        risk_point = parse_risk_heartbeat(line)
        if risk_point:
            equity_points.append(risk_point)
            risk_events.append({"time": risk_point["time"], "blocked": risk_point["blocked"], "raw": line})

    activity = [{"time": key, "count": activity_map[key]} for key in sorted(activity_map.keys())][-24:]
    deduped_cta: list[dict[str, Any]] = []
    last_cta_key: tuple[str, str] | None = None
    for item in cta_events:
        current_key = (item["time"], item["label"])
        if current_key == last_cta_key:
            continue
        deduped_cta.append(item)
        last_cta_key = current_key

    deduped_risk: list[dict[str, Any]] = []
    last_risk_key: tuple[str, bool] | None = None
    for item in risk_events:
        current_key = (item["time"], item["blocked"])
        if current_key == last_risk_key:
            continue
        deduped_risk.append(item)
        last_risk_key = current_key

    chart_points = equity_points[-48:]
    return {
        "activity": activity,
        "equityPoints": chart_points,
        "ctaEvents": deduped_cta[-12:],
        "riskEvents": deduped_risk[-12:],
        "刷新时间": now_text(),
    }


def require_auth(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    session = SESSIONS.get(token)
    if session is None:
        raise HTTPException(status_code=401, detail="Invalid session")
    return session


def read_config_payload(settings: AppSettings) -> dict[str, Any]:
    with settings.config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise HTTPException(status_code=500, detail="配置文件根节点不是映射结构")
    return payload


def get_by_path(payload: dict[str, Any], path: str) -> Any:
    current: Any = payload
    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def set_by_path(payload: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    current = payload
    for key in parts[:-1]:
        next_value = current.get(key)
        if not isinstance(next_value, dict):
            next_value = {}
            current[key] = next_value
        current = next_value
    current[parts[-1]] = value


def apply_config_values(payload: dict[str, Any], values: dict[str, Any], allowed: dict[str, dict[str, Any]]) -> list[str]:
    changed_paths: list[str] = []
    for path, raw_value in values.items():
        meta = allowed.get(path)
        if meta is None:
            raise HTTPException(status_code=400, detail=f"不允许修改的参数: {path}")
        current_value = get_by_path(payload, path)
        if raw_value is None:
            # 对于当前配置文件里缺省、依赖运行时默认值的参数，前端回传 None 时直接跳过，
            # 避免保存整页配置时把缺省字段错误写成 null 或触发数字转换报错。
            continue
        value = cast_config_value(raw_value, meta.get("type", "text"))
        set_by_path(payload, path, value)
        changed_paths.append(path)
    return changed_paths


def cast_config_value(raw: Any, field_type: str) -> Any:
    if field_type == "boolean":
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
        return bool(raw)
    if field_type == "number":
        if isinstance(raw, (int, float)):
            return raw
        if isinstance(raw, str) and raw.strip() != "":
            text = raw.strip()
            if "." in text:
                return float(text)
            return int(text)
        raise HTTPException(status_code=400, detail=f"无效数字值: {raw}")
    return raw


def build_config_sections(payload: dict[str, Any], settings: AppSettings | None = None) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    initial_equity_value = None
    runtime_cfg: AppConfig | None = None
    if settings is not None:
        initial_equity_value, _, _ = resolve_initial_equity(settings, payload)
        runtime_cfg = load_runtime_config(settings)

    def get_runtime_value(path: str) -> Any:
        if runtime_cfg is None:
            return None
        current: Any = runtime_cfg
        for part in path.split('.'):
            if current is None or not hasattr(current, part):
                return None
            current = getattr(current, part)
        return current

    for section in CONFIG_SCHEMA:
        block = {
            "section": section["section"],
            "label": section["label"],
            "description": section["description"],
            "fields": [],
        }
        for field in section["fields"]:
            item = deepcopy(field)
            if field["path"] == "runtime.account_initial_equity" and initial_equity_value is not None:
                item["value"] = initial_equity_value
            else:
                value = get_by_path(payload, field["path"])
                if value is None:
                    value = get_runtime_value(field["path"])
                item["value"] = value
            block["fields"].append(item)
        sections.append(block)
    return sections


def stop_main_controller() -> tuple[bool, str]:
    pids = main_controller_pids()
    if not pids:
        return True, "主控当前未运行"
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
    return True, f"已发送停止信号：{' '.join(str(pid) for pid in pids)}"


def start_main_controller() -> tuple[bool, str]:
    try:
        completed = subprocess.run(
            ["bash", str(RESTART_SCRIPT)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except Exception as exc:  # pragma: no cover
        return False, f"启动失败：{exc}"
    output = (completed.stdout or completed.stderr or "").strip()
    if completed.returncode != 0:
        return False, output or "启动失败"
    return True, output or "已执行启动脚本"


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "service": "admin-api", "运行中": process_running(), "时间": now_text()}


@app.post("/api/auth/login")
def login(payload: LoginPayload, settings: AppSettings = Depends(get_settings)) -> dict[str, Any]:
    if payload.username != settings.admin.username or payload.password != settings.admin.password:
        raise HTTPException(status_code=401, detail="账号或密码错误")
    token = secrets.token_urlsafe(32)
    created_at = now_text()
    SESSIONS[token] = {"username": payload.username, "createdAt": created_at}
    return {"token": token, "username": payload.username, "createdAt": created_at}


@app.get("/api/auth/me")
def me(session: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    return {"username": session["username"], "createdAt": session["createdAt"]}


@app.get("/api/dashboard/overview")
def dashboard_overview(
    session: dict[str, Any] = Depends(require_auth), settings: AppSettings = Depends(get_settings)
) -> dict[str, Any]:
    del session
    cfg = load_runtime_config(settings)
    client = OKXClient(cfg.okx, cfg.execution)
    risk_snapshot = safe_okx({
        "equity": None,
        "margin_ratio": None,
        "position_notional": None,
        "open_order_notional": None,
        "total_notional": None,
    }, lambda: client.fetch_account_risk_snapshot([cfg.cta.symbol]), context="dashboard_overview:risk_snapshot")
    unrealized_pnl = safe_okx(None, lambda: client.fetch_total_unrealized_pnl([cfg.cta.symbol]), context="dashboard_overview:unrealized_pnl")
    lines = tail_lines(settings.log_path, limit=300)
    latest = latest_worker_actions(lines)
    initial_equity, _, _ = resolve_initial_equity(settings)
    return {
        "账户权益": risk_snapshot.get("equity"),
        "初始资金": initial_equity,
        "未实现盈亏": unrealized_pnl,
        "保证金率": risk_snapshot.get("margin_ratio"),
        "持仓名义价值": risk_snapshot.get("position_notional"),
        "挂单名义价值": risk_snapshot.get("open_order_notional"),
        "总名义价值": risk_snapshot.get("total_notional"),
        "CTA状态": extract_cta_status(latest.cta),
        "网格状态": extract_grid_status(latest.grid),
        "风控阻断": False if latest.risk is None else ("blocked=True" in latest.risk),
        "主进程运行": process_running(),
        "主进程PID": main_controller_pids(),
        "交易对": cfg.cta.symbol,
        "刷新时间": now_text(),
    }


@app.get("/api/bots/status")
def bot_status(session: dict[str, Any] = Depends(require_auth), settings: AppSettings = Depends(get_settings)) -> dict[str, Any]:
    del session
    lines = tail_lines(settings.log_path, limit=400)
    latest = latest_worker_actions(lines)
    return {
        "主进程运行": process_running(),
        "主进程PID": main_controller_pids(),
        "主控": summarize_worker_line("主控", latest.main),
        "CTA": summarize_worker_line("CTA", latest.cta),
        "网格": summarize_worker_line("网格", latest.grid),
        "风控": summarize_worker_line("风控", latest.risk),
        "市场判定": summarize_worker_line("市场判定", latest.oracle),
    }


@app.get("/api/dashboard/timeline")
def dashboard_timeline(
    limit: int = 240,
    session: dict[str, Any] = Depends(require_auth),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, Any]:
    del session
    lines = tail_lines(settings.log_path, limit=max(60, min(limit, 800)))
    return build_timeline(lines)


@app.get("/api/account/positions")
def account_positions(
    session: dict[str, Any] = Depends(require_auth), settings: AppSettings = Depends(get_settings)
) -> dict[str, Any]:
    del session
    cfg = load_runtime_config(settings)
    client = OKXClient(cfg.okx, cfg.execution)
    positions = safe_okx([], lambda: client.fetch_positions([cfg.cta.symbol]), context="account_positions")
    items = []
    for item in positions:
        contracts = float(item.get("contracts") or item.get("info", {}).get("pos") or 0.0)
        if abs(contracts) <= 1e-12:
            continue
        items.append(
            {
                "交易对": item.get("symbol"),
                "方向": item.get("side"),
                "合约数量": contracts,
                "开仓均价": item.get("entryPrice") or item.get("info", {}).get("avgPx"),
                "标记价格": item.get("markPrice") or item.get("info", {}).get("markPx"),
                "未实现盈亏": item.get("unrealizedPnl") or item.get("info", {}).get("upl"),
                "名义价值": client.position_notional(cfg.cta.symbol, item),
            }
        )
    return {"items": items, "刷新时间": now_text()}


@app.get("/api/account/orders")
def account_orders(
    session: dict[str, Any] = Depends(require_auth), settings: AppSettings = Depends(get_settings)
) -> dict[str, Any]:
    del session
    cfg = load_runtime_config(settings)
    client = OKXClient(cfg.okx, cfg.execution)
    orders = safe_okx([], lambda: client.fetch_open_orders(cfg.cta.symbol), context="account_orders")
    items = []
    for item in orders:
        items.append(
            {
                "订单ID": item.get("id"),
                "交易对": item.get("symbol"),
                "方向": item.get("side"),
                "类型": item.get("type"),
                "价格": item.get("price"),
                "数量": item.get("amount"),
                "剩余": item.get("remaining"),
                "仅减仓": bool(item.get("reduceOnly") or item.get("info", {}).get("reduceOnly")),
                "状态": item.get("status"),
            }
        )
    return {"items": items, "刷新时间": now_text()}


@app.get("/api/logs/recent")
def recent_logs(
    limit: int = 120,
    session: dict[str, Any] = Depends(require_auth),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, Any]:
    del session
    lines = tail_lines(settings.log_path, limit=max(20, min(limit, 500)))
    return {"items": [summarize_log_line(line) for line in lines], "刷新时间": now_text()}


@app.get("/api/account/initial-equity")
def account_initial_equity(
    session: dict[str, Any] = Depends(require_auth),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, Any]:
    del session
    value, source, updated_at = resolve_initial_equity(settings)
    return {
        "initialEquity": value,
        "source": source,
        "updatedAt": updated_at,
        "刷新时间": now_text(),
    }


@app.post("/api/account/initial-equity")
def set_account_initial_equity(
    payload: InitialEquityPayload,
    session: dict[str, Any] = Depends(require_auth),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, Any]:
    del session
    value, changed = set_initial_equity(settings, payload.initialEquity)
    runtime_payload = read_config_payload(settings)
    return {
        "ok": True,
        "initialEquity": value,
        "message": f"初始资金已更新为 {value:.2f} USDT" if changed else "没有检测到初始资金变更，无需保存",
        "sections": build_config_sections(runtime_payload, settings),
        "刷新时间": now_text(),
    }


@app.get("/api/account/daily-calendar")
def account_daily_calendar(
    month: str | None = None,
    session: dict[str, Any] = Depends(require_auth),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, Any]:
    del session
    return build_calendar_payload(settings, month)


@app.post("/api/account/daily-calendar/backfill")
def account_daily_calendar_backfill(
    session: dict[str, Any] = Depends(require_auth),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, Any]:
    del session
    return backfill_daily_snapshots_from_logs(settings)


@app.get("/api/config/schema")
def config_schema(
    session: dict[str, Any] = Depends(require_auth),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, Any]:
    del session
    payload = read_config_payload(settings)
    return {"sections": build_config_sections(payload, settings), "刷新时间": now_text()}


@app.post("/api/config/save")
def config_save(
    payload: ConfigSavePayload,
    session: dict[str, Any] = Depends(require_auth),
    settings: AppSettings = Depends(get_settings),
) -> dict[str, Any]:
    del session
    allowed = {field["path"]: field for section in CONFIG_SCHEMA for field in section["fields"]}
    submitted_values = dict(payload.values)
    changed_paths: list[str] = []

    if "runtime.account_initial_equity" in submitted_values:
        _, baseline_changed = set_initial_equity(settings, float(submitted_values.pop("runtime.account_initial_equity")))
        if baseline_changed:
            changed_paths.append("runtime.account_initial_equity")

    config_payload = read_config_payload(settings)
    yaml_changed_paths = apply_config_values(config_payload, submitted_values, allowed)
    changed_paths.extend(yaml_changed_paths)

    if yaml_changed_paths:
        with settings.config_path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(config_payload, handle, allow_unicode=True, sort_keys=False)

    if not changed_paths:
        return {
            "ok": True,
            "message": "没有检测到配置变更，无需保存",
            "changedPaths": [],
            "sections": build_config_sections(config_payload, settings),
            "刷新时间": now_text(),
        }
    return {
        "ok": True,
        "message": f"已保存 {len(changed_paths)} 个参数",
        "changedPaths": changed_paths,
        "sections": build_config_sections(config_payload, settings),
        "刷新时间": now_text(),
    }


@app.post("/api/system/start")
def system_start(payload: ConfirmPayload, session: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    del session
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="必须显式确认 confirm=true")
    ok, message = start_main_controller()
    if not ok:
        raise HTTPException(status_code=500, detail=message)
    return {"ok": True, "message": message, "时间": now_text()}


@app.post("/api/system/stop")
def system_stop(payload: ConfirmPayload, session: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    del session
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="必须显式确认 confirm=true")
    ok, message = stop_main_controller()
    if not ok:
        raise HTTPException(status_code=500, detail=message)
    return {"ok": True, "message": message, "时间": now_text()}


@app.post("/api/system/restart")
def restart_system(payload: ConfirmPayload, session: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    del session
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="必须显式确认 confirm=true")
    ok, message = start_main_controller()
    if not ok:
        raise HTTPException(status_code=500, detail=message)
    return {"ok": True, "message": message, "时间": now_text()}
