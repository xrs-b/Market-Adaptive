from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from market_adaptive.db import TradeJournalRow, TriggerFamilyPerformance


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _norm(value: Any, fallback: str = "unknown") -> str:
    text = str(value or "").strip()
    return text or fallback


def _meta(row: TradeJournalRow, key: str, default: Any = None) -> Any:
    if not isinstance(row.metadata, dict):
        return default
    return row.metadata.get(key, default)


def build_family_leaderboard(records: list[TriggerFamilyPerformance], *, side: str | None = None) -> list[dict[str, Any]]:
    rows = []
    for rec in records:
        if side is not None and str(rec.side or "") != str(side):
            continue
        rows.append(
            {
                "trigger_family": str(rec.trigger_family),
                "side": str(rec.side or "any"),
                "close_count": int(rec.close_count),
                "win_rate": round(float(rec.win_rate), 4),
                "avg_pnl": round(float(rec.avg_pnl), 6),
                "total_pnl": round(float(rec.total_pnl), 6),
                "score": round((float(rec.win_rate) - 0.45) + float(rec.avg_pnl) * 0.05, 6),
            }
        )
    rows.sort(key=lambda item: (-float(item["score"]), -float(item["total_pnl"]), -int(item["close_count"]), item["trigger_family"]))
    return rows


def build_regime_matrix(journal_rows: list[TradeJournalRow]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str, str], list[TradeJournalRow]] = defaultdict(list)
    for row in journal_rows:
        if row.event_type != "trade_close":
            continue
        regime = _norm(_meta(row, "market_regime", "unknown"))
        family = _norm(row.trigger_family, "unknown")
        side = _norm(row.side, "any")
        buckets[(regime, family, side)].append(row)
    results = []
    for (regime, family, side), rows in buckets.items():
        pnl_values = [_safe_float(row.pnl) for row in rows]
        wins = sum(1 for pnl in pnl_values if pnl > 0)
        results.append(
            {
                "market_regime": regime,
                "trigger_family": family,
                "side": side,
                "trade_count": len(rows),
                "win_rate": round(wins / len(rows), 4) if rows else 0.0,
                "avg_pnl": round(sum(pnl_values) / len(rows), 6) if rows else 0.0,
                "total_pnl": round(sum(pnl_values), 6),
            }
        )
    results.sort(key=lambda item: (item["market_regime"], -float(item["total_pnl"]), -int(item["trade_count"]), item["trigger_family"], item["side"]))
    return results


def build_family_catalog(journal_rows: list[TradeJournalRow], family_records: list[TriggerFamilyPerformance]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    for rec in family_records:
        family = _norm(getattr(rec, "trigger_family", None), "")
        if family and family not in seen:
            seen.add(family)
            ordered.append(family)

    close_rows = [row for row in journal_rows if row.event_type == "trade_close"]
    close_rows.sort(key=lambda row: str(row.timestamp))
    for row in close_rows:
        family = _norm(row.trigger_family, "")
        if family and family not in seen:
            seen.add(family)
            ordered.append(family)
    return ordered


def build_family_score_timeseries(journal_rows: list[TradeJournalRow], families: list[str]) -> list[dict[str, Any]]:
    if not families:
        return []
    close_rows = [row for row in journal_rows if row.event_type == "trade_close"]
    close_rows.sort(key=lambda row: str(row.timestamp))
    running_pnl: dict[str, float] = {family: 0.0 for family in families}
    running_trade_count: dict[str, int] = {family: 0 for family in families}
    running_win_count: dict[str, int] = {family: 0 for family in families}
    series: list[dict[str, Any]] = []
    for row in close_rows:
        family = _norm(row.trigger_family)
        if family not in running_pnl:
            continue
        pnl = _safe_float(row.pnl)
        running_pnl[family] += pnl
        running_trade_count[family] += 1
        if pnl > 0:
            running_win_count[family] += 1
        trade_count = running_trade_count[family]
        point = {
            "timestamp": row.timestamp,
            "family": family,
            "market_regime": _norm(_meta(row, "market_regime", "unknown")),
            "cum_pnl": round(running_pnl[family], 6),
            "trade_pnl": round(pnl, 6),
            "rolling_wr": round(running_win_count[family] / trade_count, 6) if trade_count else 0.0,
            "trade_count": trade_count,
        }
        series.append(point)
    return series[-360:]


def build_regime_transition_comparison(journal_rows: list[TradeJournalRow]) -> list[dict[str, Any]]:
    close_rows = [row for row in journal_rows if row.event_type == "trade_close"]
    close_rows.sort(key=lambda row: str(row.timestamp))
    transitions: list[dict[str, Any]] = []
    last_regime: str | None = None
    current_bucket: list[TradeJournalRow] = []

    def summarize_bucket(regime: str, rows: list[TradeJournalRow]) -> dict[str, Any]:
        pnl_values = [_safe_float(item.pnl) for item in rows]
        wins = sum(1 for value in pnl_values if value > 0)
        return {
            "market_regime": regime,
            "trade_count": len(rows),
            "win_rate": round(wins / len(rows), 4) if rows else 0.0,
            "total_pnl": round(sum(pnl_values), 6),
            "avg_pnl": round(sum(pnl_values) / len(rows), 6) if rows else 0.0,
        }

    for row in close_rows:
        regime = _norm(_meta(row, "market_regime", "unknown"))
        if last_regime is None:
            last_regime = regime
        if regime != last_regime:
            transitions.append(summarize_bucket(last_regime, current_bucket))
            current_bucket = []
            last_regime = regime
        current_bucket.append(row)
    if current_bucket and last_regime is not None:
        transitions.append(summarize_bucket(last_regime, current_bucket))
    return transitions[-12:]


def build_decision_audit(journal_rows: list[TradeJournalRow]) -> dict[str, Any]:
    blocked_rows = [row for row in journal_rows if row.event_type == "blocked_signal"]
    close_rows = [row for row in journal_rows if row.event_type == "trade_close"]

    missed_opportunities = []
    for row in blocked_rows:
        decider = _norm(_meta(row, "entry_decider_decision", "unknown"))
        location_score = _safe_float(_meta(row, "entry_location_score", 0.0))
        confidence = _safe_float(_meta(row, "signal_confidence", 0.0))
        if decider in {"watch", "probe"} or (confidence >= 0.65 and location_score >= 0.0):
            missed_opportunities.append(
                {
                    "timestamp": row.timestamp,
                    "trigger_family": _norm(row.trigger_family),
                    "side": _norm(row.side),
                    "action": _norm(row.action),
                    "decider": decider,
                    "confidence": round(confidence, 4),
                    "location_score": round(location_score, 4),
                    "market_regime": _norm(_meta(row, "market_regime", "unknown")),
                }
            )

    bad_releases = []
    for row in close_rows:
        pnl = _safe_float(row.pnl)
        decider = _norm(_meta(row, "entry_decider_decision", "unknown"))
        confidence = _safe_float(_meta(row, "signal_confidence", 0.0))
        if pnl < 0 and (decider == "open" or confidence >= 0.70):
            bad_releases.append(
                {
                    "timestamp": row.timestamp,
                    "trigger_family": _norm(row.trigger_family),
                    "side": _norm(row.side),
                    "pnl": round(pnl, 6),
                    "decider": decider,
                    "confidence": round(confidence, 4),
                    "market_regime": _norm(_meta(row, "market_regime", "unknown")),
                }
            )

    return {
        "missed_opportunities": missed_opportunities[:10],
        "bad_releases": bad_releases[:10],
    }


def build_family_regime_actions(regime_matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: {
        "long_pnl": 0.0,
        "short_pnl": 0.0,
        "long_trades": 0.0,
        "short_trades": 0.0,
    }))
    for row in regime_matrix:
        family = _norm(row.get("trigger_family"), "unknown")
        regime = _norm(row.get("market_regime"), "unknown")
        side = _norm(row.get("side"), "any").lower()
        trade_count = _safe_float(row.get("trade_count"), 0.0)
        total_pnl = _safe_float(row.get("total_pnl"), 0.0)
        bucket = grouped[family][regime]
        if side == "long":
            bucket["long_pnl"] += total_pnl
            bucket["long_trades"] += trade_count
        elif side == "short":
            bucket["short_pnl"] += total_pnl
            bucket["short_trades"] += trade_count

    actions: list[dict[str, Any]] = []
    for family, regime_map in grouped.items():
        for regime, stats in regime_map.items():
            long_pnl = float(stats["long_pnl"])
            short_pnl = float(stats["short_pnl"])
            long_trades = int(stats["long_trades"])
            short_trades = int(stats["short_trades"])
            total_trades = long_trades + short_trades
            net_pnl = long_pnl + short_pnl
            bias_pnl = long_pnl - short_pnl
            abs_bias = abs(bias_pnl)
            label = "观察"
            action = "observe"
            detail = "样本不足或优劣不明显"
            if regime in {"trend", "trend_impulse"} and net_pnl > 0 and total_trades >= 2:
                label = "建议提升"
                action = "promote"
                detail = f"{regime} 下净表现为正，适合继续观察放大"
            if regime == "sideways" and net_pnl < 0 and total_trades >= 2:
                label = "建议压制"
                action = "suppress"
                detail = "sideways 下持续失真，可考虑压低优先级"
            if abs_bias >= 0.5 and total_trades >= 2:
                if bias_pnl > 0:
                    label = "偏多有效"
                    action = "favor_long"
                    detail = "long 明显强于 short，存在方向性偏移"
                else:
                    label = "偏空有效"
                    action = "favor_short"
                    detail = "short 明显强于 long，存在方向性偏移"
            preset_patch: dict[str, Any] = {}
            preset_label: str | None = None
            if action == "promote":
                preset_patch = {
                    "cta.long_standard_min_confidence": 0.55,
                    "cta.short_standard_min_confidence": 0.53,
                    "cta.family_adaptation_boost_cap": 0.22,
                }
                preset_label = "放宽放行 + 提升 family boost"
            elif action == "suppress":
                preset_patch = {
                    "cta.long_standard_min_confidence": 0.62,
                    "cta.short_standard_min_confidence": 0.60,
                    "cta.family_adaptation_boost_cap": 0.14,
                }
                preset_label = "收紧放行 + 降低 family boost"
            elif action == "favor_long":
                preset_patch = {
                    "cta.long_standard_min_confidence": 0.55,
                    "cta.short_standard_min_confidence": 0.60,
                }
                preset_label = "偏向 long / 压 short"
            elif action == "favor_short":
                preset_patch = {
                    "cta.long_standard_min_confidence": 0.62,
                    "cta.short_standard_min_confidence": 0.53,
                }
                preset_label = "偏向 short / 压 long"

            actions.append(
                {
                    "trigger_family": family,
                    "market_regime": regime,
                    "label": label,
                    "action": action,
                    "detail": detail,
                    "net_pnl": round(net_pnl, 6),
                    "bias_pnl": round(bias_pnl, 6),
                    "long_pnl": round(long_pnl, 6),
                    "short_pnl": round(short_pnl, 6),
                    "trade_count": total_trades,
                    "preset_patch": preset_patch,
                    "preset_label": preset_label,
                }
            )
    actions.sort(key=lambda item: (-abs(_safe_float(item.get("bias_pnl"))), -abs(_safe_float(item.get("net_pnl"))), item.get("trigger_family", ""), item.get("market_regime", "")))
    return actions[:24]


def filter_journal_rows_by_hours(journal_rows: list[TradeJournalRow], hours: int | None) -> list[TradeJournalRow]:
    if hours is None or hours <= 0:
        return list(journal_rows or [])
    cutoff = datetime.now() - timedelta(hours=int(hours))
    filtered: list[TradeJournalRow] = []
    for row in journal_rows or []:
        try:
            ts = datetime.fromisoformat(str(row.timestamp).replace("Z", "+00:00"))
        except Exception:
            filtered.append(row)
            continue
        if ts.replace(tzinfo=None) >= cutoff:
            filtered.append(row)
    return filtered



def _trigger_group(family: str) -> str:
    value = _norm(family, "waiting")
    if value.startswith("waiting_") or value == "waiting":
        return "waiting"
    if value.startswith("early_"):
        return "early"
    if value in {"bullish_memory_breakout", "bearish_memory_breakdown"}:
        return "memory_breakout"
    if value in {"bearish_retest", "major_bull_retest"}:
        return "major_retest"
    if value in {"near_breakout_release", "trend_continuation_near_breakout", "price_led_override", "soft_latch_breakout", "major_bull_impulse_reclaim", "rail_momentum"}:
        return "breakout_continuation"
    if value in {"spring_reclaim", "upthrust_reclaim", "bullish_retest_entry", "bearish_retest_entry", "pullback_support_entry", "pullback_resistance_entry"}:
        return "reversal_reclaim"
    if value in {"starter_frontrun", "starter_short_frontrun"}:
        return "starter"
    if value in {"weak_bull_scale_in", "weak_bear_scale_in"}:
        return "scale_in"
    return "other"

def _loss_decay_meta(consecutive_losses: int, config_meta: dict[str, Any] | None) -> dict[str, Any]:
    cfg = config_meta or {}
    enabled = bool(cfg.get("loss_acceleration_decay_enabled", True))
    threshold = _safe_int(cfg.get("loss_acceleration_consecutive_losses", 3), 3)
    score = _safe_float(cfg.get("loss_acceleration_decay_score", 0.12), 0.12)
    ttl = _safe_int(cfg.get("loss_acceleration_decay_ttl_seconds", 0), 0)
    return {"enabled": enabled, "threshold": threshold, "decay_score": round(score, 6), "ttl_seconds": ttl, "consecutive_losses": int(consecutive_losses), "applied": bool(enabled and threshold > 0 and consecutive_losses >= threshold)}

def build_pre_signal_observations(journal_rows: list[TradeJournalRow], *, limit: int = 40) -> list[dict[str, Any]]:
    rows = [row for row in journal_rows if row.event_type == "pre_signal_observation"]
    rows.sort(key=lambda row: str(row.timestamp), reverse=True)
    return [{"timestamp": row.timestamp, "side": _norm(row.side, "none"), "action": _norm(row.action, "unknown"), "trigger_family": _norm(row.trigger_family), "trigger_group": _trigger_group(_norm(row.trigger_family)), "trigger_reason": _norm(row.trigger_reason, ""), "pathway": _norm(row.pathway, "unknown"), "price": round(_safe_float(row.price), 6), "candidate_state": _norm(_meta(row, "candidate_state", row.action)), "candidate_reason": _norm(_meta(row, "candidate_reason", ""), ""), "raw_direction": _safe_int(_meta(row, "raw_direction", 0)), "final_direction": _safe_int(_meta(row, "final_direction", 0)), "confidence": round(_safe_float(_meta(row, "signal_confidence", 0.0)), 4), "entry_decider_decision": _norm(_meta(row, "entry_decider_decision", "unknown")), "entry_decider_score": round(_safe_float(_meta(row, "entry_decider_score", 0.0)), 4), "blocker_reason": _norm(_meta(row, "blocker_reason", ""), ""), "market_regime": _norm(_meta(row, "market_regime", "unknown"))} for row in rows[: max(1, int(limit))]]

def build_family_health(records: list[TriggerFamilyPerformance], journal_rows: list[TradeJournalRow], *, config_meta: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    closes = [row for row in journal_rows if row.event_type == "trade_close"]
    by_key = {(str(rec.trigger_family), str(rec.side or "any")): rec for rec in records}
    keys = set(by_key) | {(_norm(row.trigger_family), _norm(row.side, "any")) for row in closes if _norm(row.trigger_family, "")}
    health = []
    for family, side in keys:
        rows = [row for row in closes if _norm(row.trigger_family) == family and _norm(row.side, "any") == side]
        rows.sort(key=lambda row: str(row.timestamp), reverse=True)
        consecutive_losses = 0
        for row in rows:
            if _safe_float(row.pnl) < 0: consecutive_losses += 1
            else: break
        rec = by_key.get((family, side))
        close_count = int(rec.close_count) if rec else len(rows)
        win_count = int(rec.win_count) if rec else sum(1 for row in rows if _safe_float(row.pnl) > 0)
        loss_count = int(rec.loss_count) if rec else sum(1 for row in rows if _safe_float(row.pnl) < 0)
        total_pnl = float(rec.total_pnl) if rec else sum(_safe_float(row.pnl) for row in rows)
        avg_pnl = float(rec.avg_pnl) if rec else (total_pnl / close_count if close_count else 0.0)
        win_rate = float(rec.win_rate) if rec else (win_count / close_count if close_count else 0.0)
        health.append({"trigger_family": family, "trigger_group": _trigger_group(family), "side": side, "sample_count": int(rec.sample_count) if rec else close_count, "close_count": close_count, "win_count": win_count, "loss_count": loss_count, "win_rate": round(win_rate, 4), "avg_pnl": round(avg_pnl, 6), "total_pnl": round(total_pnl, 6), "score": round((win_rate - 0.45) + avg_pnl * 0.05, 6), "consecutive_losses": consecutive_losses, "loss_decay": _loss_decay_meta(consecutive_losses, config_meta)})
    health.sort(key=lambda item: (-int(item["loss_decay"]["applied"]), -int(item["consecutive_losses"]), float(item["score"])))
    return health

def build_long_short_distribution(journal_rows: list[TradeJournalRow]) -> dict[str, Any]:
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0.0, "closed": 0.0, "pnl": 0.0})
    for row in [r for r in journal_rows if r.event_type in {"trade_open", "trade_close", "blocked_signal", "pre_signal_observation"}]:
        side = _norm(row.side, "none").lower(); buckets[side]["count"] += 1
        if row.event_type == "trade_close": buckets[side]["closed"] += 1; buckets[side]["pnl"] += _safe_float(row.pnl)
    long_count, short_count = buckets["long"]["count"], buckets["short"]["count"]; total = long_count + short_count
    return {"by_side": {side: {"count": int(v["count"]), "closed": int(v["closed"]), "total_pnl": round(v["pnl"], 6)} for side, v in sorted(buckets.items())}, "long_count": int(long_count), "short_count": int(short_count), "long_ratio": round(long_count / total, 4) if total else 0.0, "short_ratio": round(short_count / total, 4) if total else 0.0, "imbalance": round(((long_count - short_count) / total) if total else 0.0, 4)}

def build_short_diagnostics(journal_rows: list[TradeJournalRow], distribution: dict[str, Any]) -> dict[str, Any]:
    short_rows = [row for row in journal_rows if _norm(row.side, "").lower() == "short"]
    blocked = [row for row in short_rows if row.event_type == "blocked_signal"]
    observations = [row for row in short_rows if row.event_type == "pre_signal_observation"]
    closes = [row for row in short_rows if row.event_type == "trade_close"]
    pnl = [_safe_float(row.pnl) for row in closes]
    counts: dict[str, int] = defaultdict(int)
    for row in blocked: counts[_norm(_meta(row, "blocker_reason", row.trigger_reason), "unknown")] += 1
    return {"short_rows": len(short_rows), "blocked_count": len(blocked), "pre_signal_count": len(observations), "close_count": len(closes), "win_rate": round(sum(1 for value in pnl if value > 0) / len(pnl), 4) if pnl else 0.0, "total_pnl": round(sum(pnl), 6), "long_short_imbalance": distribution.get("imbalance", 0.0), "top_blockers": [{"reason": k, "count": v} for k, v in sorted(counts.items(), key=lambda item: -item[1])[:8]]}

def build_cta_dashboard_snapshot(*, family_records: list[TriggerFamilyPerformance], journal_rows: list[TradeJournalRow], hours: int | None = None, config_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    scoped_rows = filter_journal_rows_by_hours(journal_rows, hours)
    long_board = build_family_leaderboard(family_records, side="long")
    short_board = build_family_leaderboard(family_records, side="short")
    all_board = build_family_leaderboard(family_records, side=None)
    regime_matrix = build_regime_matrix(scoped_rows)
    decision_audit = build_decision_audit(scoped_rows)

    family_catalog = build_family_catalog(scoped_rows, family_records)
    family_score_timeseries = build_family_score_timeseries(scoped_rows, family_catalog)
    regime_transitions = build_regime_transition_comparison(scoped_rows)
    family_regime_actions = build_family_regime_actions(regime_matrix)
    pre_signal_observations = build_pre_signal_observations(scoped_rows)
    family_health = build_family_health(family_records, scoped_rows, config_meta=config_meta)
    long_short_distribution = build_long_short_distribution(scoped_rows)
    short_diagnostics = build_short_diagnostics(scoped_rows, long_short_distribution)

    return {
        "overview": {
            "family_count": len(all_board),
            "long_family_count": len(long_board),
            "short_family_count": len(short_board),
            "recent_close_count": sum(1 for row in scoped_rows if row.event_type == "trade_close"),
            "recent_blocked_count": sum(1 for row in scoped_rows if row.event_type == "blocked_signal"),
            "recent_pre_signal_count": sum(1 for row in scoped_rows if row.event_type == "pre_signal_observation"),
        },
        "leaderboards": {
            "all": all_board[:12],
            "long": long_board[:8],
            "short": short_board[:8],
        },
        "family_catalog": family_catalog,
        "regime_matrix": regime_matrix,
        "regime_transitions": regime_transitions,
        "family_regime_actions": family_regime_actions,
        "family_score_timeseries": family_score_timeseries,
        "pre_signal_observations": pre_signal_observations,
        "family_health": family_health,
        "long_short_distribution": long_short_distribution,
        "short_diagnostics": short_diagnostics,
        "decision_audit": decision_audit,
    }
