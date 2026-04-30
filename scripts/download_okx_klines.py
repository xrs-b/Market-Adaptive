#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import signal
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

OKX_HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
DEFAULT_LIMIT = 100
DEFAULT_CHECKPOINT_EVERY = 20
BAR_TO_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1H": 3_600_000,
    "2H": 7_200_000,
    "4H": 14_400_000,
    "1D": 86_400_000,
}
STOP_REQUESTED = False


@dataclass(frozen=True)
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def handle_stop(signum, frame) -> None:  # pragma: no cover
    del signum, frame
    global STOP_REQUESTED
    STOP_REQUESTED = True
    print("stop requested: will checkpoint and exit cleanly", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download OKX historical candles into canonical CSV format.")
    parser.add_argument("--inst-id", default="BTC-USDT-SWAP", help="OKX instrument id, e.g. BTC-USDT-SWAP")
    parser.add_argument("--bar", default="1m", help="OKX bar, e.g. 1m/5m/15m/1H")
    parser.add_argument("--months", type=int, default=6, help="How many months of history to fetch")
    parser.add_argument(
        "--out",
        default="data/okx/BTC-USDT-SWAP/1m.csv",
        help="Output CSV path (default: data/okx/BTC-USDT-SWAP/1m.csv)",
    )
    parser.add_argument("--sleep", type=float, default=0.25, help="Sleep seconds between OKX requests")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Candles per OKX request (max 100)")
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=DEFAULT_CHECKPOINT_EVERY,
        help="Persist CSV every N pages so long downloads can resume safely",
    )
    return parser.parse_args()


def ensure_bar_supported(bar: str) -> int:
    if bar not in BAR_TO_MS:
        raise SystemExit(f"Unsupported bar: {bar}. Supported: {', '.join(sorted(BAR_TO_MS))}")
    return BAR_TO_MS[bar]


def fetch_json(url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "openclaw-okx-kline-downloader/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_history_page(inst_id: str, bar: str, after: int | None, limit: int) -> list[list[str]]:
    params = {
        "instId": inst_id,
        "bar": bar,
        "limit": str(min(max(1, limit), 100)),
    }
    if after is not None:
        params["after"] = str(after)
    url = f"{OKX_HISTORY_URL}?{urllib.parse.urlencode(params)}"
    payload = fetch_json(url)
    if payload.get("code") != "0":
        raise RuntimeError(f"OKX returned error: {payload}")
    return payload.get("data") or []


def row_to_candle(row: list[str]) -> Candle:
    return Candle(
        timestamp=int(row[0]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
    )


def load_existing(path: Path) -> dict[int, Candle]:
    if not path.exists():
        return {}
    result: dict[int, Candle] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                candle = Candle(
                    timestamp=int(row["timestamp"]),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                )
            except Exception:
                continue
            result[candle.timestamp] = candle
    return result


def write_csv(path: Path, candles: Iterable[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for c in candles:
            writer.writerow([
                c.timestamp,
                format(c.open, ".10f").rstrip("0").rstrip("."),
                format(c.high, ".10f").rstrip("0").rstrip("."),
                format(c.low, ".10f").rstrip("0").rstrip("."),
                format(c.close, ".10f").rstrip("0").rstrip("."),
                format(c.volume, ".10f").rstrip("0").rstrip("."),
            ])
    tmp_path.replace(path)


def summarize_gaps(candles: list[Candle], interval_ms: int) -> tuple[int, list[tuple[int, int]]]:
    if len(candles) < 2:
        return 0, []
    gaps: list[tuple[int, int]] = []
    missing = 0
    for prev, curr in zip(candles, candles[1:]):
        delta = curr.timestamp - prev.timestamp
        if delta <= interval_ms:
            continue
        missing_count = max(0, int(delta // interval_ms) - 1)
        if missing_count > 0:
            missing += missing_count
            gaps.append((prev.timestamp, curr.timestamp))
    return missing, gaps[:10]


def format_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def checkpoint(path: Path, fetched: dict[int, Candle], start_ms: int, interval_ms: int, *, label: str) -> list[Candle]:
    ordered = sorted((c for c in fetched.values() if c.timestamp >= start_ms), key=lambda c: c.timestamp)
    write_csv(path, ordered)
    missing_count, gap_examples = summarize_gaps(ordered, interval_ms)
    oldest = format_ts(ordered[0].timestamp) if ordered else "n/a"
    newest = format_ts(ordered[-1].timestamp) if ordered else "n/a"
    print(
        f"checkpoint[{label}] rows_written={len(ordered)} oldest={oldest} newest={newest} missing={missing_count}",
        flush=True,
    )
    if gap_examples:
        left, right = gap_examples[0]
        print(f"checkpoint[{label}] first_gap={format_ts(left)} -> {format_ts(right)}", flush=True)
    return ordered


def main() -> int:
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    args = parse_args()
    interval_ms = ensure_bar_supported(args.bar)
    out_path = Path(args.out).expanduser().resolve()

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=max(1, args.months) * 30)
    start_ms = int(start_time.timestamp() * 1000)

    existing = load_existing(out_path)
    fetched: dict[int, Candle] = dict(existing)
    ordered_existing = sorted((c for c in fetched.values() if c.timestamp >= start_ms), key=lambda c: c.timestamp)
    if ordered_existing:
        oldest_existing = ordered_existing[0].timestamp
        newest_existing = ordered_existing[-1].timestamp
        print(
            f"resume existing rows={len(ordered_existing)} range=[{format_ts(oldest_existing)} .. {format_ts(newest_existing)}]",
            flush=True,
        )
        if oldest_existing <= start_ms:
            print("existing file already covers requested start range; only gap summary will be reported", flush=True)
            ordered = checkpoint(out_path, fetched, start_ms, interval_ms, label="resume-complete")
            missing_count, gap_examples = summarize_gaps(ordered, interval_ms)
            print("---")
            print(f"instrument: {args.inst_id}")
            print(f"bar: {args.bar}")
            print(f"start_utc: {start_time.isoformat()}")
            print(f"end_utc:   {end_time.isoformat()}")
            print("pages: 0")
            print("rows_fetched: 0")
            print(f"rows_written: {len(ordered)}")
            print(f"output: {out_path}")
            print(f"missing_candles_detected: {missing_count}")
            if gap_examples:
                print("gap_examples:")
                for left, right in gap_examples:
                    print(f"  {format_ts(left)} -> {format_ts(right)}")
            return 0
        after = oldest_existing - 1
    else:
        after = None

    page_count = 0
    fetched_rows = 0
    seen_oldest: set[int] = set()

    while True:
        if STOP_REQUESTED:
            checkpoint(out_path, fetched, start_ms, interval_ms, label="stop")
            return 130

        rows = fetch_history_page(args.inst_id, args.bar, after=after, limit=args.limit)
        page_count += 1
        if not rows:
            break

        candles = [row_to_candle(row) for row in rows]
        fetched_rows += len(candles)
        for candle in candles:
            fetched[candle.timestamp] = candle

        oldest_ts = min(c.timestamp for c in candles)
        newest_ts = max(c.timestamp for c in candles)
        print(
            f"page={page_count} rows={len(candles)} range=[{format_ts(oldest_ts)} .. {format_ts(newest_ts)}] total_unique={len(fetched)}",
            flush=True,
        )

        if oldest_ts in seen_oldest:
            print("duplicate oldest timestamp detected; checkpointing and stopping to avoid loop", flush=True)
            break
        seen_oldest.add(oldest_ts)

        if page_count % max(1, args.checkpoint_every) == 0:
            checkpoint(out_path, fetched, start_ms, interval_ms, label=f"page-{page_count}")

        if oldest_ts <= start_ms:
            break

        after = oldest_ts - 1
        time.sleep(max(0.0, args.sleep))

    ordered = checkpoint(out_path, fetched, start_ms, interval_ms, label="final")
    missing_count, gap_examples = summarize_gaps(ordered, interval_ms)
    print("---")
    print(f"instrument: {args.inst_id}")
    print(f"bar: {args.bar}")
    print(f"start_utc: {start_time.isoformat()}")
    print(f"end_utc:   {end_time.isoformat()}")
    print(f"pages: {page_count}")
    print(f"rows_fetched: {fetched_rows}")
    print(f"rows_written: {len(ordered)}")
    print(f"output: {out_path}")
    print(f"missing_candles_detected: {missing_count}")
    if gap_examples:
        print("gap_examples:")
        for left, right in gap_examples:
            print(f"  {format_ts(left)} -> {format_ts(right)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
