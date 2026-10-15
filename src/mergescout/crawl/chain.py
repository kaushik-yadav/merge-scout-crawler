import json
from datetime import datetime
from pathlib import Path

from mergescout.crawl.state import CrawlState

LAST_RUN = "last-run.json"
# Runs that used their whole window had points to spare; others stopped on GitHub's limits and should pause first.
FULL_RUN = {"time", "limit"}


def write_last_run(data_dir: Path, out: dict) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    keep = {k: out.get(k) for k in ("repos", "points", "failed", "reason")}
    (data_dir / LAST_RUN).write_text(json.dumps(keep), encoding="utf-8")


def next_step(state: CrawlState, data_dir: Path, now: datetime, after_discover: bool = False) -> str:
    # The relay: GitHub's hourly schedule is best effort, so each finished run decides what starts next.
    marker = data_dir / "last-discover"
    discovered = marker.read_text(encoding="utf-8").strip() if marker.exists() else ""
    if discovered != now.strftime("%Y-%m-%d"):
        return "discover"
    if state.due_count() == 0:
        return "none"
    if after_discover:
        return "crawl"
    try:
        last = json.loads((data_dir / LAST_RUN).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return "none"
    if not last.get("repos"):
        return "none"
    return "crawl" if last.get("reason") in FULL_RUN else "crawl-later"
