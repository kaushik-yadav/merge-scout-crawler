import math
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds

from mergescout.crawl.state import CrawlState
from mergescout.discover.sweep import SCHEMA


def active_repos_from_sweep(data_dir: Path, min_stars: int, active_days: int, now: datetime) -> list[dict]:
    base = data_dir / "discover" / "basics"
    if not base.exists() or not any(base.glob("*.parquet")):
        return []
    # The fixed schema lets older sweep files without newer columns load as nulls.
    table = ds.dataset(str(base), format="parquet", schema=SCHEMA).to_table(
        filter=(pc.field("stars") >= min_stars) & (pc.field("pushed_at") >= now - timedelta(days=active_days)))
    best: dict[int, dict] = {}
    for r in table.to_pylist():
        # A repo seen in two slices (stars moved mid-sweep) keeps the higher count.
        if r["repo_id"] is not None and (r["repo_id"] not in best or r["stars"] > best[r["repo_id"]]["stars"]):
            best[r["repo_id"]] = r
    return sorted(best.values(), key=lambda r: -r["stars"])


def expected_cost(repo: dict) -> int:
    # Points a first crawl will likely cost: settings, PR pages (busier with stars), 2 points per 50 open issues.
    stars = repo.get("stars") or 0
    pr_pages = 6 if stars >= 10000 else 3 if stars >= 1000 else 1
    issue_pages = max(1, math.ceil(min(repo.get("open_issues") or 0, 200) / 50))
    return 1 + pr_pages + 2 * issue_pages


def priority(repo: dict) -> int:
    # Beginner issues per expected point, so the daily budget covers the most candidate issues first.
    beginner = min(repo.get("opportunity_issues") or 0, 20)
    if beginner:
        value = beginner
    elif repo.get("has_issues") and (repo.get("open_issues") or 0) > 0:
        value = 0.5
    else:
        value = 0.1
    return round(1000 * value / expected_cost(repo))


# Watched repos (a list of owner/name lines) are crawled before everything else, so their history stays current.
WATCHED = 1_000_000


def read_watch(path: Path | None) -> set[str]:
    if not path or not Path(path).exists():
        return set()
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return {x.strip().lower() for x in lines if x.strip() and not x.strip().startswith("#")}


def apply_watch(rows: list[dict], watch: set[str]) -> int:
    hits = 0
    for r in rows:
        if r["name"].lower() in watch:
            r["priority"], hits = WATCHED, hits + 1
    return hits


def seed_queue(state: CrawlState, data_dir: Path, now: datetime, min_stars: int = 100, active_days: int = 90,
               log=print, watch: set[str] | None = None) -> dict:
    active = active_repos_from_sweep(data_dir, min_stars, active_days, now)
    rows = [{"repo_id": r["repo_id"], "name": r["name"], "stars": r["stars"],
             "pushed_at": r["pushed_at"].isoformat() if r["pushed_at"] else None, "priority": priority(r)}
            for r in active]
    watched = apply_watch(rows, watch or set())
    added = state.upsert_repos(rows)
    out = {"active": len(active), "opportunity_repos": sum(1 for r in active if r.get("opportunity_issues")),
           "added": added, "watched": watched}
    log(f"seed: {out}")
    return out
