#!/usr/bin/env python3
"""
immich_jobs_eta.py
Poll Immich /api/jobs and estimate queue drain rate + running ETA (total + per-queue).

Usage:
  IMMICH_API_KEY="..." python3 immich_jobs_eta.py --interval 60 --samples 15
  IMMICH_API_KEY="..." python3 immich_jobs_eta.py --interval 60 --samples 0     # run forever
  IMMICH_API_KEY="..." python3 immich_jobs_eta.py --remaining 123526 --interval 60 --samples 0

Notes:
- Schema-tolerant: walks JSON to find job count dicts (waiting/active/delayed/paused/etc).
- Uses Redis/BullMQ-style counts and defines "pending" = waiting+active+delayed+paused.
- Rates:
    - inst: drained since last sample / actual elapsed seconds
    - ema: exponentially-smoothed inst rate
    - avg: total drained since start / total elapsed seconds
- ETA:
    - total ETA based on (remaining override if provided else total pending), using EMA rate
    - per-queue ETA based on current queue pending, using that queue's EMA rate
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from typing import Any, Dict, List, Optional, Tuple

COUNT_KEYS = {"waiting", "active", "delayed", "paused", "failed", "completed"}

def http_get_json(url: str, api_key: str) -> Any:
    req = Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("x-api-key", api_key)
    with urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)

def is_counts_dict(d: Any) -> bool:
    if not isinstance(d, dict):
        return False
    hits = COUNT_KEYS.intersection(d.keys())
    # require at least 2 keys to avoid false positives
    return len(hits) >= 2 and all(isinstance(d[k], (int, float)) for k in hits if k in d)

def extract_queues(obj: Any, path: str = "root") -> List[Tuple[str, Dict[str, Any]]]:
    """
    Walk arbitrary JSON, yielding (queue_name, counts_dict).
    queue_name is best-effort derived from nearby keys or JSON path.
    """
    found: List[Tuple[str, Dict[str, Any]]] = []

    if isinstance(obj, dict):
        if is_counts_dict(obj):
            name = obj.get("queue") or obj.get("name") or obj.get("job") or path
            found.append((str(name), obj))

        for k, v in obj.items():
            child_path = f"{path}.{k}"
            found.extend(extract_queues(v, child_path))

    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            found.extend(extract_queues(v, f"{path}[{i}]"))

    return found

def normalize_queue_name(name: str) -> str:
    """
    Make names readable: your output looked like "root.metadataExtraction.jobCounts"
    We try to extract the meaningful middle.
    """
    n = name

    # Common pattern: root.<queue>.jobCounts
    m = re.match(r"^root\.([^.]+)\.jobCounts$", n)
    if m:
        return m.group(1)

    # Strip common prefixes
    n = re.sub(r"^root\.", "", n)

    # Strip noisy suffixes
    n = re.sub(r"\.jobCounts$", "", n)

    return n

def summarize(queues: List[Tuple[str, Dict[str, Any]]]) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    queues: list of (name, counts_dict)
    Return:
      per_queue_pending: dict[name] = pending
      totals: dict
    """
    per_queue_pending: Dict[str, int] = {}
    totals = {"pending": 0, "failed": 0, "active": 0, "waiting": 0, "delayed": 0, "paused": 0}

    for raw_name, c in queues:
        name = normalize_queue_name(raw_name)

        waiting = int(c.get("waiting", 0) or 0)
        active  = int(c.get("active", 0) or 0)
        delayed = int(c.get("delayed", 0) or 0)
        paused  = int(c.get("paused", 0) or 0)
        failed  = int(c.get("failed", 0) or 0)

        pending = waiting + active + delayed + paused

        # Deduplicate: if we see the same queue name multiple times via different paths,
        # keep the maximum pending (most conservative).
        per_queue_pending[name] = max(per_queue_pending.get(name, 0), pending)

        totals["pending"] += pending
        totals["failed"] += failed
        totals["active"] += active
        totals["waiting"] += waiting
        totals["delayed"] += delayed
        totals["paused"] += paused

    return per_queue_pending, totals

def human_td(seconds: float) -> str:
    if seconds <= 0:
        return "0s"
    return str(timedelta(seconds=int(seconds)))

def fmt_rate_per_hr(rate_per_sec: float) -> str:
    return f"{rate_per_sec * 3600:,.0f}/hr"

def fmt_delta(delta: int) -> str:
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:,}"

def ema_update(prev: Optional[float], inst: float, alpha: float) -> float:
    if prev is None:
        return inst
    return alpha * inst + (1 - alpha) * prev

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:2283/api", help="Immich base URL ending in /api")
    ap.add_argument("--interval", type=int, default=60, help="Seconds between samples (default 60)")
    ap.add_argument("--samples", type=int, default=10, help="Number of samples (0 = run forever)")
    ap.add_argument("--remaining", type=int, default=None,
                    help="Optional remaining count you care about for ETA (uses TOTAL drain rate)")
    ap.add_argument("--top", type=int, default=8, help="Show top N queues by pending (default 8)")
    ap.add_argument("--ema-alpha", type=float, default=0.30,
                    help="EMA smoothing factor for rate (0.1=very smooth, 0.5=snappier). Default 0.30")
    ap.add_argument("--focus", default=None,
                    help="Optional queue name to highlight (e.g. metadataExtraction)")
    ap.add_argument("--show-critical-path", action="store_true",
                    help="Also show ETA for the slowest (max ETA) non-zero queue")
    args = ap.parse_args()

    api_key = os.environ.get("IMMICH_API_KEY", "").strip()
    if not api_key:
        print("Set IMMICH_API_KEY in your environment (Immich UI: User Settings -> API Keys).", file=sys.stderr)
        sys.exit(2)

    jobs_url = args.base_url.rstrip("/") + "/jobs"

    print(f"Polling:  {jobs_url}")
    print(f"Interval: {args.interval}s")
    print(f"Samples:  {'forever' if args.samples == 0 else args.samples}")
    if args.remaining is not None:
        print(f"Remaining override (total ETA numerator): {args.remaining:,}")
    print(f"EMA α:    {args.ema_alpha}")
    print()

    start_ts: Optional[float] = None
    start_total_pending: Optional[int] = None

    prev_ts: Optional[float] = None
    prev_total_pending: Optional[int] = None
    prev_perq: Dict[str, int] = {}

    # Rate trackers (per second)
    ema_total_rate: Optional[float] = None
    ema_queue_rate: Dict[str, float] = {}  # queue -> rate/sec

    drained_since_start_total = 0

    iteration = 0
    while True:
        iteration += 1
        t_query_start = time.time()

        # Fetch jobs JSON
        try:
            data = http_get_json(jobs_url, api_key)
        except HTTPError as e:
            print(f"HTTP error {e.code}: {e.reason}", file=sys.stderr)
            sys.exit(1)
        except URLError as e:
            print(f"Network error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error reading/parsing JSON: {e}", file=sys.stderr)
            sys.exit(1)

        queues = extract_queues(data)
        perq, totals = summarize(queues)

        now = time.time()
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")

        total_pending = totals["pending"]

        if start_ts is None:
            start_ts = now
            start_total_pending = total_pending

        # Compute deltas + rates
        inst_total_rate = 0.0
        inst_total_drained = 0
        elapsed = 0.0

        if prev_ts is not None and prev_total_pending is not None:
            elapsed = max(1e-6, now - prev_ts)
            inst_total_drained = prev_total_pending - total_pending  # positive means draining
            inst_total_rate = inst_total_drained / elapsed
            drained_since_start_total += inst_total_drained

            ema_total_rate = ema_update(ema_total_rate, inst_total_rate, args.ema_alpha)

        # Running average rate
        total_elapsed_since_start = max(1e-6, now - (start_ts or now))
        avg_total_rate = 0.0
        if start_total_pending is not None:
            avg_total_drained = (start_total_pending - total_pending)
            avg_total_rate = avg_total_drained / total_elapsed_since_start

        # Header line
        header = (
            f"[{stamp}] total pending={total_pending:,} "
            f"(waiting={totals['waiting']:,}, active={totals['active']:,}, delayed={totals['delayed']:,}, paused={totals['paused']:,}) "
            f"failed={totals['failed']:,}"
        )
        print(header)

        # Rate line (only after we have at least two samples)
        if prev_ts is not None:
            print(
                f"  Δ total={fmt_delta(-inst_total_drained)} pending  "
                f"drained={fmt_delta(inst_total_drained)} in {elapsed:.1f}s  "
                f"inst={fmt_rate_per_hr(inst_total_rate)}  "
                f"ema={fmt_rate_per_hr(ema_total_rate or 0.0)}  "
                f"avg={fmt_rate_per_hr(avg_total_rate)}"
            )
        else:
            print("  (collecting baseline sample for rate/ETA...)")

        # ETA (total)
        if prev_ts is not None and (ema_total_rate or 0) > 0:
            numerator = args.remaining if args.remaining is not None else total_pending
            eta_sec = numerator / (ema_total_rate or 1e-9)
            label = "override remaining" if args.remaining is not None else "total pending"
            print(f"  ETA ({label}, using EMA): ~{human_td(eta_sec)}")
        # Critical-path ETA: max per-queue ETA among non-zero queues (EMA-based)
        if args.show_critical_path and prev_ts is not None:
            worst = None  # (eta_seconds, qname, pending, rate)
            for qname, qpending in perq.items():
                if qpending <= 0:
                    continue
                q_ema = ema_queue_rate.get(qname)
                if q_ema is None or q_ema <= 0:
                    continue
                q_eta = qpending / q_ema
                if worst is None or q_eta > worst[0]:
                    worst = (q_eta, qname, qpending, q_ema)

            if worst:
                q_eta, qname, qpending, q_ema = worst
                print(f"  ETA (critical path: {qname}, using EMA): ~{human_td(q_eta)}  (pending={qpending:,}, rate={fmt_rate_per_hr(q_ema)})")

        elif prev_ts is not None:
            print("  ETA: n/a (rate <= 0)")

        # Per-queue breakdown: top queues by current pending
        #topq = sorted(perq.items(), key=lambda x: x[1], reverse=True)[:args.top]
        topq = sorted(perq.items(), key=lambda x: x[1], reverse=True)
        if args.focus:
            # Move focused queue to the top if present
            topq = sorted(topq, key=lambda x: (0 if x[0] == args.focus else 1, -x[1]))
        topq = topq[:args.top]

        if topq:
            print("  Queues (top by pending):")
            for qname, qpending in topq:
                # Compute queue-specific drain and rate
                q_inst_rate = None
                q_inst_drained = None
                if prev_ts is not None:
                    prev_q = prev_perq.get(qname, qpending)  # if new queue appears, assume no delta
                    q_inst_drained = prev_q - qpending
                    q_inst_rate = q_inst_drained / max(1e-6, elapsed)

                    # Update queue EMA
                    prev_ema = ema_queue_rate.get(qname)
                    ema_queue_rate[qname] = ema_update(prev_ema, q_inst_rate, args.ema_alpha)

                # Print queue line
                if prev_ts is None:
                    print(f"    - {qname}: pending={qpending:,}")
                else:
                    q_ema = ema_queue_rate.get(qname, 0.0)
                    # ETA per queue (using EMA)
                    if q_ema > 0:
                        q_eta = qpending / q_ema
                        eta_str = f"ETA ~{human_td(q_eta)}"
                    else:
                        eta_str = "ETA n/a"
                    print(
                        f"    - {qname}: pending={qpending:,}  "
                        f"drained={fmt_delta(q_inst_drained or 0)}  "
                        f"ema={fmt_rate_per_hr(q_ema)}  "
                        f"{eta_str}"
                    )

        print()

        # Update previous snapshot
        prev_ts = now
        prev_total_pending = total_pending
        prev_perq = perq

        # Stop condition
        if args.samples != 0 and iteration >= args.samples:
            break

        # Sleep until next interval (accounting for query time)
        query_elapsed = time.time() - t_query_start
        time.sleep(max(0.0, args.interval - query_elapsed))

if __name__ == "__main__":
    main()

