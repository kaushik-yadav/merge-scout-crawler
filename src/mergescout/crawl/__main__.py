import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone

from mergescout.config import load_config
from mergescout.crawl.runner import run_crawl
from mergescout.crawl.seed import seed_queue
from mergescout.crawl.state import CrawlState
from mergescout.github.graphql import GitHubGraphQL
from mergescout.github.identities import identities_from_env


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m mergescout.crawl", description="Crawl repos at full depth.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="Crawl until the time or points budget runs out (safe to stop and re-run)")
    run.add_argument("--minutes", type=float, default=50)
    run.add_argument("--limit", type=int, default=None)
    run.add_argument("--in-flight", type=int, default=12)
    run.add_argument("--reserve", type=int, default=300, help="Stop when fewer points than this are left")
    run.add_argument("--hot-days", type=int, default=1, help="Refresh repos with beginner issues after this many days")
    run.add_argument("--cold-days", type=int, default=7, help="Refresh other repos after this many days")
    run.add_argument("--max-wait", type=float, default=120, help="Stop instead of waiting longer for the rate limit")
    run.add_argument("--full-every-days", type=int, default=30, help="Redo a full crawl this often; refreshes in between")
    sub.add_parser("status", help="Show the queue and the last runs")
    seed = sub.add_parser("seed", help="Queue active repos from the sweep, beginner-friendly repos first")
    seed.add_argument("--min-stars", type=int, default=100)
    seed.add_argument("--active-days", type=int, default=90)
    sub.add_parser("requeue-failed", help="Give failed repos another try (up to 3 attempts)")
    bk = sub.add_parser("backup", help="Copy the state file safely, even while it is in use")
    bk.add_argument("dest")
    args = parser.parse_args(argv)

    cfg = load_config()
    state = CrawlState(cfg.data_dir / "crawl.sqlite")
    now = datetime.now(timezone.utc)
    if args.cmd == "status":
        print(f"repos={state.counts()}")
        for r in state.db.execute("SELECT run_id, started_at, finished_at, repos, points, reason FROM runs "
                                  "ORDER BY started_at DESC LIMIT 5"):
            print("run " + " ".join(str(v) for v in tuple(r)))
        tracked = state.db.execute("SELECT COUNT(*) FROM issue_state").fetchone()[0]
        print(f"tracked issues={tracked:,}")
        return 0
    if args.cmd == "backup":
        dest = sqlite3.connect(args.dest)
        with dest:
            state.db.backup(dest)
        dest.close()
        return 0
    if args.cmd == "requeue-failed":
        print(f"requeued {state.requeue_failed(3)}")
        return 0
    if args.cmd == "seed":
        seed_queue(state, cfg.data_dir, now, min_stars=args.min_stars, active_days=args.active_days)
        return 0
    try:
        token = cfg.require_token()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    run_id = os.environ.get("MS_RUN_ID") or now.strftime("%Y%m%dT%H%M%SZ")
    print(f"requeued stale={state.requeue_stale(120)} failed={state.requeue_failed(3)}", flush=True)
    # Personal token plus, when set, the GitHub App and workflow tokens: each with its own limits and wait.
    identities = identities_from_env(token, args.max_wait, client=GitHubGraphQL)
    out = run_crawl(state, identities, cfg.data_dir, run_id,
                    minutes=args.minutes, in_flight=args.in_flight, reserve=args.reserve, limit=args.limit,
                    hot_days=args.hot_days, cold_days=args.cold_days, full_every_days=args.full_every_days,
                    log=lambda msg: print(msg, flush=True))
    for i in identities:
        print(f"github {i.name}: {i.gate.summary()}", flush=True)
    return 1 if out["repos"] == 0 and out["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
