# Immich Jobs ETA (Queue Drain + Running ETA)
Vibe coded in ChatGPT 5.2.

Usage:
```
export IMMICH_API_KEY="PASTE_YOUR_KEY"
python3 immich_eta.py --interval 60 --samples 0
python3 immich_eta.py --interval 120 --samples 0 --ema-alpha 0.2 --show-critical-path --focus metadataExtraction
```

A small Python CLI tool that polls Immich’s `/api/jobs` endpoint and turns queue numbers into something human-friendly:

- Total pending jobs
- Per-queue pending jobs (metadataExtraction, smartSearch, etc.)
- Drain rates (instant, EMA-smoothed, running average)
- Running ETA (total + per-queue)
- Optional “critical path” ETA (the slowest queue wins)

If you’ve ever stared at Immich’s job counts thinking “is this going to finish tonight or next week?”, this is your flashlight 🔦

---

## Why this exists

In newer Immich releases, background job processing is handled primarily by `immich_server` (the old `immich_microservices` container is gone). The `immich_machine_learning` container is mainly an inference service and often doesn’t emit reliable “job completed” log lines you can count.

This tool avoids log parsing entirely and reads job counts directly from Immich’s API.

---

## Requirements

- Python 3.10+ (3.9+ may work, but 3.10+ recommended)
- An Immich instance reachable over HTTP
- An Immich API key

No third-party Python packages are required.

---

## Setup

### 1) Create an Immich API key

In the Immich web UI:

`User Settings → API Keys → Create`

### 2) Export the API key

```bash
export IMMICH_API_KEY="paste_your_key_here"
```

## Example Output

```
[2025-12-27 19:56:43] total pending=138,939 (waiting=138,932, active=7, delayed=0, paused=0) failed=0
  Δ total=-967 pending  drained=+967 in 120.1s  inst=28,987/hr  ema=29,462/hr  avg=29,644/hr
  ETA (total pending, using EMA): ~4:42:57
  ETA (critical path: metadataExtraction, using EMA): ~5:56:05  (pending=108,264, rate=18,242/hr)
  Queues (top by pending):
    - metadataExtraction: pending=108,264  drained=+581  ema=18,077/hr  ETA ~5:59:20
    - smartSearch: pending=30,675  drained=+386  ema=11,384/hr  ETA ~2:41:40
    - thumbnailGeneration: pending=0  drained=+0  ema=0/hr  ETA n/a
    - videoConversion: pending=0  drained=+0  ema=0/hr  ETA n/a
    - storageTemplateMigration: pending=0  drained=+0  ema=1/hr  ETA ~0s
    - migration: pending=0  drained=+0  ema=0/hr  ETA n/a
    - backgroundTask: pending=0  drained=+0  ema=0/hr  ETA n/a
    - search: pending=0  drained=+0  ema=0/hr  ETA n/a
```
