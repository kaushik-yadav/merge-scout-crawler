import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mergescout.crawl.fetch import cutoff, fetch_repo
from mergescout.crawl.outcomes import apply_outcomes, plan_outcomes
from mergescout.crawl.state import CrawlState
from mergescout.crawl.writer import BatchWriter
from mergescout.github.graphql import Gate, RateLimited, TokenRejected
from mergescout.github.identities import Identity, slots
from mergescout.labels import has_opportunity
from mergescout.timeutil import parse_time


def is_hot(issues: list[dict]) -> bool:
    return any(has_opportunity(i.get("labels") or []) for i in issues if i.get("state") == "OPEN")


def changed_since(repo: dict, now: datetime, full_every_days: int) -> datetime | None:
    # Refreshes ask only for changes since the last crawl (two hours of overlap); a full crawl comes back monthly.
    last, full = parse_time(repo.get("last_done_at")), parse_time(repo.get("last_full_at"))
    if last is None or full is None or now - full >= timedelta(days=full_every_days):
        return None
    return last - timedelta(hours=2)


class _Lane:
    # One identity's share of the run: its own parallel slots, points left and reason for stepping aside.
    def __init__(self, identity: Identity, in_flight: int):
        self.name, self.make_gql, self.gate = identity.name, identity.make_gql, identity.gate
        self.max_slots = slots(in_flight, identity.share)
        self.slots = self.lowest = self.max_slots
        self.busy, self.repos, self.points = 0, 0, 0
        self.remaining: int | None = None
        self.out: str | None = None
        self.seen_waits, self.calm = 0, 0

    def pace(self, log) -> None:
        # Halve parallel requests when GitHub asked this identity to slow down; add one back after 20 calm repos.
        waits = self.gate.stats["rate_waits"]
        if waits > self.seen_waits:
            self.seen_waits, self.calm = waits, 0
            if self.slots > 1:
                self.slots = max(1, self.slots // 2)
                self.lowest = min(self.lowest, self.slots)
                log(f"{self.name}: GitHub asked to slow down; parallel requests now {self.slots}")
        else:
            self.calm += 1
            if self.calm >= 20 and self.slots < self.max_slots:
                self.slots, self.calm = self.slots + 1, 0


def run_crawl(state: CrawlState, make_gql, data_dir: Path, run_id: str, *, minutes: float, in_flight: int = 12,
              reserve: int = 300, limit: int | None = None, flush_every: int = 250, hot_days: int = 1,
              cold_days: int = 7, issue_days: int = 90, full_every_days: int = 30,
              now=lambda: datetime.now(timezone.utc), log=print,
              clock=time.monotonic) -> dict:
    # make_gql is one client factory, or a list of identities that share the queue, each with its own limits.
    identities = make_gql if isinstance(make_gql, list) else [Identity("default", make_gql, Gate())]
    lanes = [_Lane(i, in_flight) for i in identities]
    state.start_run(run_id, "crawl")
    writer = BatchWriter(data_dir, run_id)
    local = threading.local()
    started = clock()
    stats = {"repos": 0, "points": 0, "failed": 0, "gone": 0}
    pending: list[tuple] = []
    stop = None

    def work(lane: _Lane, repo):
        # Runs in a pool thread; each thread keeps one client per identity. Never raises.
        clients = local.__dict__.setdefault("clients", {})
        if lane.name not in clients:
            clients[lane.name] = lane.make_gql()
        gql = clients[lane.name]
        try:
            since = changed_since(repo, now(), full_every_days)
            track = state.open_issue_numbers(repo["repo_id"]) if since is None else []
            bundle = fetch_repo(gql, repo["repo_id"], repo["name"], now(), issue_days=issue_days, track=track,
                                changed_since=since)
            return repo, bundle, None, getattr(gql, "last_remaining", None)
        except Exception as exc:
            return repo, None, exc, getattr(gql, "last_remaining", None)

    def flush():
        # Repos count as done only once their rows are safely on disk.
        writer.flush()
        for repo_id, points, prs, issues, days, keep, forget, full in pending:
            apply_outcomes(state, repo_id, keep, forget)
            state.done(repo_id, run_id, points, prs, issues, refresh_days=days, full=full)
        pending.clear()

    with ThreadPoolExecutor(max_workers=sum(lane.max_slots for lane in lanes)) as pool:
        futures: dict = {}
        while True:
            if stop is None:
                if (clock() - started) / 60 >= minutes:
                    stop = "time"
                elif limit is not None and stats["repos"] + len(futures) >= limit:
                    stop = "limit"
                elif all(lane.out for lane in lanes):
                    stop = "budget" if all(lane.out == "budget" for lane in lanes) else "rate_limit"
            if stop is None:
                claimed = False
                for lane in lanes:
                    room = lane.slots - lane.busy
                    if limit is not None:
                        room = min(room, limit - stats["repos"] - len(futures))
                    if lane.out or room <= 0:
                        continue
                    for repo in state.claim(room, run_id):
                        futures[pool.submit(work, lane, repo)] = lane
                        lane.busy += 1
                        claimed = True
                if not claimed and not futures:
                    stop = "empty"
            if not futures:
                break
            finished, _ = wait(list(futures), return_when=FIRST_COMPLETED)
            for f in finished:
                lane = futures.pop(f)
                lane.busy -= 1
                repo, bundle, exc, left = f.result()
                lane.pace(log)
                if left is not None:
                    lane.remaining = left
                    if left < reserve and lane.out is None:
                        lane.out = "budget"
                        log(f"{lane.name}: points nearly used up ({left} left); it stops taking repos")
                if isinstance(exc, (RateLimited, TokenRejected)):
                    state.release(repo["repo_id"])
                    if lane.out not in ("rate_limit", "rejected"):
                        lane.out = "rejected" if isinstance(exc, TokenRejected) else "rate_limit"
                        why = "its token was rejected" if lane.out == "rejected" else "GitHub's rate limit"
                        log(f"{lane.name}: stops on {why} ({exc})")
                elif exc is not None:
                    state.fail(repo["repo_id"], run_id, f"{type(exc).__name__}: {exc}")
                    stats["failed"] += 1
                    log(f"failed {repo['name']}: {exc}")
                elif bundle.status == "gone":
                    state.gone(repo["repo_id"], run_id, bundle.error or "gone")
                    stats["gone"] += 1
                else:
                    if bundle.meta:
                        writer.add("repo_meta", [bundle.meta])
                    writer.add("prs", bundle.prs)
                    writer.add("issues", bundle.issues)
                    writer.add("comments", bundle.comments)
                    today = now()
                    events, keep, forget = plan_outcomes(state, repo["repo_id"], bundle.issues, today.date().isoformat(),
                                                         cutoff(today, issue_days), bundle.deleted_issues)
                    writer.add("issue_events", events)
                    hot = is_hot(bundle.issues) or state.has_hot_tracked(repo["repo_id"])
                    days = hot_days if hot else cold_days
                    pending.append((repo["repo_id"], bundle.points, len(bundle.prs), len(bundle.issues), days, keep,
                                    forget, bundle.full))
                    stats["repos"] += 1
                    stats["points"] += bundle.points
                    lane.repos += 1
                    lane.points += bundle.points
                    if len(pending) >= flush_every:
                        flush()
    flush()
    reason = stop or "empty"
    state.finish_run(run_id, stats["points"], stats["repos"], reason)
    per = {lane.name: {"repos": lane.repos, "points": lane.points, "left": lane.remaining, "out": lane.out,
                       "slots": lane.slots, "lowest_slots": lane.lowest} for lane in lanes}
    log(f"run {run_id}: repos={stats['repos']} points={stats['points']} failed={stats['failed']} "
        f"gone={stats['gone']} reason={reason} queue={state.counts()}")
    for name, s in per.items():
        log(f"  {name}: repos={s['repos']} points={s['points']} left={s['left']} slots={s['slots']} "
            f"(lowest {s['lowest_slots']}) stopped={s['out'] or 'no'}")
    return {**stats, "reason": reason, "identities": per}
