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
