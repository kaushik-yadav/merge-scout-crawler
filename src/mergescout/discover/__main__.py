import argparse
import sys
from datetime import datetime, timedelta, timezone

from mergescout.config import load_config
from mergescout.discover.sweep import SweepState, initial_slices, run_sweep
from mergescout.github.graphql import GitHubGraphQL
from mergescout.github.identities import identities_from_env, slots


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m mergescout.discover", description="List public repos at scale.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sw = sub.add_parser("sweep", help="List every repo above a star count (safe to stop and re-run)")
    sw.add_argument("--min-stars", type=int, default=10)
    sw.add_argument("--workers", type=int, default=4)
    sw.add_argument("--active-days", type=int, default=None, help="Only repos pushed in this many days")
    sw.add_argument("--max-wait", type=float, default=None,
                    help="Pause instead of waiting longer than this many seconds for the rate limit")
    sub.add_parser("status", help="Show sweep progress")
    args = parser.parse_args(argv)

    cfg = load_config()
    db_path = cfg.data_dir / "discover" / "sweep.sqlite"
    if args.cmd == "status":
        state = SweepState(db_path)
        rows, cost = state.totals()
        print(f"repos={rows:,} points={cost:,} slices={state.counts()}")
        return 0
    token = cfg.require_token()
    state = SweepState(db_path, resume=True)
    saved = state.setting("min_stars")
    if saved is not None and int(saved) != args.min_stars:
        print(f"This sweep was started with --min-stars {saved}; delete {db_path} to start over.", file=sys.stderr)
        return 2
    state.setting("min_stars", str(args.min_stars))
    now = datetime.now(timezone.utc).replace(microsecond=0)
    # The activity cutoff is fixed when a sweep starts, so a resumed sweep keeps the same slices.
    if args.active_days and state.setting("pushed_after") is None and state.active() == 0 and not state.counts():
        state.setting("pushed_after", (now - timedelta(days=args.active_days)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    pushed = state.setting("pushed_after")
    state.seed(initial_slices(args.min_stars, datetime(2007, 1, 1, tzinfo=timezone.utc), now,
                              datetime.fromisoformat(pushed.replace("Z", "+00:00")) if pushed else None))
    # Every identity (personal token, App, workflow token) gets workers in proportion to its hourly points.
    identities = identities_from_env(token, args.max_wait, client=GitHubGraphQL)
    factories = [i.make_gql for i in identities for _ in range(slots(args.workers, i.share))]
    paused = run_sweep(factories, state, cfg.data_dir / "discover", log=lambda msg: print(msg, flush=True))
    rows, cost = state.totals()
    counts = state.counts()
    for i in identities:
        print(f"github {i.name}: {i.gate.summary()}", flush=True)
    word = "paused (rate limit; run again to resume)" if paused else "finished"
    print(f"{word}: repos={rows:,} points={cost:,} slices={counts}", file=sys.stderr)
    return 1 if counts.get("failed") else 0


if __name__ == "__main__":
    raise SystemExit(main())
