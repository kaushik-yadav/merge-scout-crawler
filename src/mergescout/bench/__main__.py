import argparse
import gzip
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

from mergescout.bench.ci import TokenPool, aggregate, fetch_all, todays_share
from mergescout.config import load_config
from mergescout.github.releases import Releases
from mergescout.store.sync import STATE_TAG, safe_extract

LATEST_TAG = "bench-latest"
DAILY = re.compile(r"^ci-(\d{4}-\d{2}-\d{2})\.ndjson\.gz$")


def week_tag(day: datetime) -> str:
    # One release per ISO week keeps every release small.
    return f"bench-{day:%G-W%V}"


def load_repos(basics: Path, now: datetime, active_days: int = 30, min_stars: int = 100) -> list[dict]:
    """Repos from the discovery sweep that pushed recently: the only ones with push runs to read."""
    since, best = now - timedelta(days=active_days), {}
    for path in sorted(basics.rglob("*.parquet")):
        for r in pq.read_table(path, columns=["name", "stars", "language", "pushed_at"]).to_pylist():
            if not r["name"] or (r["stars"] or 0) < min_stars or not r["pushed_at"] or r["pushed_at"] < since:
                continue
            key = r["name"].lower()
            if key not in best or (r["stars"] or 0) > best[key]["stars"]:
                best[key] = {"name": r["name"], "language": r["language"], "stars": r["stars"]}
    return sorted(best.values(), key=lambda r: r["name"].lower())


def _read(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def publish(data_dir: Path, releases, records: list[dict], now: datetime, days: int = 35, log=print) -> dict:
    """Store today's sample privately, then rebuild the benchmarks from the last five weeks of samples."""
    folder = data_dir / "bench"
    folder.mkdir(parents=True, exist_ok=True)
    name = f"ci-{now:%Y-%m-%d}.ndjson.gz"
    today = folder / name
    # A second run on the same day adds to that day's file instead of replacing it.
    release = releases.get(week_tag(now))
    earlier = next((a for a in releases.assets(release) if a["name"] == name), None) if release else None
    merged = {}
    if earlier:
        releases.download_asset(earlier, today)
        merged = {r["repo"].lower(): r for r in _read(today)}
    merged.update({r["repo"].lower(): r for r in records})
    records = list(merged.values())
    with gzip.open(today, "wt", encoding="utf-8") as f:
        f.writelines(json.dumps(r) + "\n" for r in records)
    releases.upload(week_tag(now), today)
    first = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    every = list(records)
    for tag in sorted({week_tag(now - timedelta(days=d)) for d in range(days + 1)}):
        release = releases.get(tag)
        for asset in releases.assets(release) if release else []:
            m = DAILY.match(asset["name"])
            if not m or asset["name"] == name or m[1] < first:
                continue
            local = folder / asset["name"]
            if not local.exists():
                releases.download_asset(asset, local)
            every += _read(local)
    agg = aggregate(every, now)
    out = folder / "ci-benchmarks.json"
    out.write_text(json.dumps(agg, indent=1, sort_keys=True), encoding="utf-8")
    releases.upload(LATEST_TAG, out)
    log(f"benchmarks: {agg['repos']} repos, {len(agg['groups'])} groups with enough repos")
    return agg


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m mergescout.bench",
                                     description="Sample CI statistics of crawled repos into open benchmarks.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    ci = sub.add_parser("ci", help="Read today's seventh of the active repos' push runs and rebuild the benchmarks")
    ci.add_argument("--workers", type=int, default=10)
    ci.add_argument("--max-wait", type=int, default=3600, help="seconds to wait in total for hourly limits to reset")
    ci.add_argument("--limit", type=int, default=0, help="read at most this many repos (0: all of today's share)")
    parser.add_argument("--repo", default=os.environ.get("MS_STORE_REPO") or os.environ.get("GITHUB_REPOSITORY"),
                        help="owner/name of the repo whose releases hold the data")
    args = parser.parse_args(argv)
    store_token = os.environ.get("MS_RELEASE_TOKEN")
    tokens = [os.environ.get(k) for k in ("GITHUB_TOKEN", "MS_APP_TOKEN", "MS_ACTIONS_TOKEN")]
    if not store_token or not args.repo or not any(tokens):
        print("Set MS_RELEASE_TOKEN, MS_STORE_REPO (or GITHUB_REPOSITORY) and GITHUB_TOKEN.", file=sys.stderr)
        return 1
    log = lambda msg: print(msg, flush=True)  # noqa: E731
    now, data_dir = datetime.now(timezone.utc), load_config().data_dir
    releases = Releases(store_token, args.repo)
    basics = data_dir / "discover" / "basics"
    if not basics.exists():
        # Only the sweep results are needed, never the crawl state.
        bundle = data_dir / "outbox" / "discover.tar"
        if not releases.download(STATE_TAG, "discover.tar", bundle):
            print("No sweep results in the releases yet.", file=sys.stderr)
            return 1
        safe_extract(bundle, data_dir)
        bundle.unlink()
    repos = load_repos(basics, now)
    share = todays_share([r["name"] for r in repos], now)
    picked = set(share[:args.limit] if args.limit else share)
    todo = [r for r in repos if r["name"] in picked]
    log(f"{len(repos)} active repos; today's share: {len(todo)}")
    pool = TokenPool(tokens, max_wait=args.max_wait)
    got = fetch_all(pool, todo, now, workers=args.workers, log=log)
    log(f"{len(got['records'])} repos with enough push runs, {got['requests']} requests, "
        f"{'complete' if got['complete'] else 'stopped at the hourly limits'}")
    publish(data_dir, releases, got["records"], now, log=log)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
