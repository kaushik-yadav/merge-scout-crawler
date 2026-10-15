import hashlib
import os
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mergescout.github.graphql import GraphQLError, RateLimited, TokenRejected, too_heavy
from mergescout.labels import OPPORTUNITY_LABELS
from mergescout.timeutil import parse_time

# GitHub search never returns more than this many results for one query.
MAX_RESULTS = 1000
FMT = "%Y-%m-%dT%H:%M:%SZ"
STAR_BOUNDS = [10, 20, 50, 100, 1000, 10000]

COUNT_QUERY = """
query($q: String!) {
  rateLimit { cost remaining resetAt }
  search(query: $q, type: REPOSITORY, first: 1) { repositoryCount }
}
"""

REPO_QUERY = """
query($q: String!, $cursor: String, $n: Int!, $labels: [String!]) {
  rateLimit { cost remaining resetAt }
  search(query: $q, type: REPOSITORY, first: $n, after: $cursor) {
    repositoryCount
    pageInfo { hasNextPage endCursor }
    nodes { ... on Repository {
      databaseId nameWithOwner stargazerCount forkCount createdAt pushedAt
      primaryLanguage { name } hasIssuesEnabled licenseInfo { spdxId }
      issues(states: OPEN) { totalCount }
      opp: issues(states: OPEN, labels: $labels) { totalCount }
    } }
  }
}
"""

SCHEMA = pa.schema([
    ("repo_id", pa.int64()),
    ("name", pa.string()),
    ("stars", pa.int32()),
    ("forks", pa.int32()),
    ("created_at", pa.timestamp("us", tz="UTC")),
    ("pushed_at", pa.timestamp("us", tz="UTC")),
    ("language", pa.string()),
    ("has_issues", pa.bool_()),
    ("license", pa.string()),
    ("open_issues", pa.int32()),
    ("open_prs", pa.int32()),
    ("opportunity_issues", pa.int32()),
])


@dataclass(frozen=True)
class Slice:
    stars_lo: int
    stars_hi: int | None  # None means no upper limit
    created_lo: datetime
    created_hi: datetime
    pushed_lo: datetime | None = None  # only repos pushed since then (active repos)

    def query(self) -> str:
        stars = f">={self.stars_lo}" if self.stars_hi is None else f"{self.stars_lo}..{self.stars_hi}"
        span = f"{self.created_lo.strftime(FMT)}..{self.created_hi.strftime(FMT)}"
        pushed = f" pushed:>={self.pushed_lo:%Y-%m-%d}" if self.pushed_lo else ""
        return f"stars:{stars}{pushed} created:{span} fork:false archived:false"

    def split(self) -> list["Slice"]:
        # Split by stars while the range has more than one value, then by creation time.
        if self.stars_hi is not None and self.stars_hi > self.stars_lo:
            mid = (self.stars_lo + self.stars_hi) // 2
            return [Slice(self.stars_lo, mid, self.created_lo, self.created_hi, self.pushed_lo),
                    Slice(mid + 1, self.stars_hi, self.created_lo, self.created_hi, self.pushed_lo)]
        if self.created_hi - self.created_lo <= timedelta(minutes=1):
            return []
        mid = (self.created_lo + (self.created_hi - self.created_lo) / 2).replace(microsecond=0)
        return [Slice(self.stars_lo, self.stars_hi, self.created_lo, mid, self.pushed_lo),
                Slice(self.stars_lo, self.stars_hi, mid + timedelta(seconds=1), self.created_hi, self.pushed_lo)]


def initial_slices(min_stars: int, start: datetime, end: datetime, pushed_lo: datetime | None = None) -> list[Slice]:
    bounds = [b for b in STAR_BOUNDS if b > min_stars]
    lows = [min_stars] + bounds
    highs = [b - 1 for b in bounds] + [None]
    return [Slice(lo, hi, start, end, pushed_lo) for lo, hi in zip(lows, highs)]


def _row(n: dict) -> dict:
    return {
        "repo_id": n.get("databaseId"),
        "name": n["nameWithOwner"],
        "stars": n.get("stargazerCount"),
        "forks": n.get("forkCount"),
        "created_at": parse_time(n.get("createdAt")),
        "pushed_at": parse_time(n.get("pushedAt")),
        "language": (n.get("primaryLanguage") or {}).get("name"),
        "has_issues": n.get("hasIssuesEnabled"),
        "license": (n.get("licenseInfo") or {}).get("spdxId"),
        "open_issues": (n.get("issues") or {}).get("totalCount"),
        "open_prs": (n.get("pullRequests") or {}).get("totalCount"),
        "opportunity_issues": (n.get("opp") or {}).get("totalCount"),
    }


def fetch_slice(gql, sl: Slice, page: int = 50) -> tuple[list[dict] | None, int, int]:
    # Returns (rows, total count, points spent); rows is None when the slice must be split first.
    probe = gql.query(COUNT_QUERY, {"q": sl.query()})
    cost = probe.cost
    count = probe.data["search"]["repositoryCount"]
    if count > MAX_RESULTS and sl.split():
        return None, count, cost
    rows: dict[int | str, dict] = {}
    cursor = None
    while count:
        try:
            result = gql.query(REPO_QUERY, {"q": sl.query(), "cursor": cursor, "n": page,
                                            "labels": list(OPPORTUNITY_LABELS)})
        except GraphQLError as exc:
            # Big repos make pages heavy and slow; retry the same page with fewer repos.
            if too_heavy(exc) and page > 10:
                page = max(10, page // 2)
                continue
            raise
        cost += result.cost
        found = result.data["search"]
        for n in found["nodes"]:
            if n and n.get("nameWithOwner"):
                row = _row(n)
                rows[row["repo_id"] or row["name"]] = row
        if not found["nodes"] or not found["pageInfo"]["hasNextPage"] or not found["pageInfo"]["endCursor"]:
            break
        if len(rows) >= MAX_RESULTS:
            break
        cursor = found["pageInfo"]["endCursor"]
    return list(rows.values()), count, cost


class SweepState:
    def __init__(self, path: Path, resume: bool = False):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        with self.db:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS slices (key TEXT PRIMARY KEY, stars_lo INTEGER, stars_hi INTEGER, "
                "created_lo TEXT, created_hi TEXT, status TEXT, rows INTEGER, cost INTEGER, error TEXT)"
            )
            self.db.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
            if resume:
                # Anything interrupted or failed last time gets another go.
                self.db.execute("UPDATE slices SET status = 'queued' WHERE status IN ('running', 'failed')")

    def setting(self, key: str, value: str | None = None) -> str | None:
        with self.lock, self.db:
            if value is not None:
                self.db.execute("INSERT OR REPLACE INTO settings VALUES (?, ?)", (key, value))
            row = self.db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def seed(self, slices: list[Slice]) -> None:
        with self.lock, self.db:
            if self.db.execute("SELECT COUNT(*) FROM slices").fetchone()[0] == 0:
                self._insert(slices)

    def _insert(self, slices: list[Slice]) -> None:
        self.db.executemany(
            "INSERT OR IGNORE INTO slices VALUES (?, ?, ?, ?, ?, 'queued', NULL, NULL, NULL)",
            [(s.query(), s.stars_lo, s.stars_hi, s.created_lo.strftime(FMT), s.created_hi.strftime(FMT))
             for s in slices],
        )

    def claim(self) -> Slice | None:
        pushed = parse_time(self.setting("pushed_after"))
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT key, stars_lo, stars_hi, created_lo, created_hi FROM slices "
                "WHERE status = 'queued' ORDER BY stars_lo DESC LIMIT 1").fetchone()
            if row is None:
                return None
            self.db.execute("UPDATE slices SET status = 'running' WHERE key = ?", (row[0],))
        return Slice(row[1], row[2], parse_time(row[3]), parse_time(row[4]), pushed)

    def done(self, sl: Slice, rows: int, cost: int) -> None:
        with self.lock, self.db:
            self.db.execute("UPDATE slices SET status = 'done', rows = ?, cost = ? WHERE key = ?",
                            (rows, cost, sl.query()))

    def split(self, sl: Slice, children: list[Slice], cost: int) -> None:
        with self.lock, self.db:
            self._insert(children)
            self.db.execute("UPDATE slices SET status = 'split', cost = ? WHERE key = ?", (cost, sl.query()))

    def release(self, sl: Slice) -> None:
        with self.lock, self.db:
            self.db.execute("UPDATE slices SET status = 'queued' WHERE key = ?", (sl.query(),))

    def fail(self, sl: Slice, error: str) -> None:
        with self.lock, self.db:
            self.db.execute("UPDATE slices SET status = 'failed', error = ? WHERE key = ?", (error[:300], sl.query()))

    def active(self) -> int:
        with self.lock:
            return self.db.execute(
                "SELECT COUNT(*) FROM slices WHERE status IN ('queued', 'running')").fetchone()[0]

    def counts(self) -> dict:
        with self.lock:
            return dict(self.db.execute("SELECT status, COUNT(*) FROM slices GROUP BY status").fetchall())

    def totals(self) -> tuple[int, int]:
        with self.lock:
            rows, cost = self.db.execute("SELECT COALESCE(SUM(rows), 0), COALESCE(SUM(cost), 0) FROM slices").fetchone()
        return rows, cost


def _write(rows: list[dict], out: Path) -> None:
    tmp = out.with_name(out.name + ".part")
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp, compression="zstd")
    os.replace(tmp, out)


def run_sweep(make_gql, state: SweepState, data_dir: Path, workers: int = 4,
              log: Callable[..., None] = print) -> bool:
    # make_gql is one client factory (used by every worker) or a list with one factory per worker.
    # Returns True when slices are left because an identity hit its limit; the next run resumes them.
    out_dir = data_dir / "basics"
    out_dir.mkdir(parents=True, exist_ok=True)
    factories = make_gql if isinstance(make_gql, list) else [make_gql] * workers

    in_flight = [0]
    flight_lock = threading.Lock()
    limited = threading.Event()

    def worker(factory) -> None:
        gql = factory()
        while True:
            sl = state.claim()
            if sl is None:
                with flight_lock:
                    busy = in_flight[0]
                if busy == 0:
                    return
                time.sleep(0.5)
                continue
            with flight_lock:
                in_flight[0] += 1
            try:
                rows, count, cost = fetch_slice(gql, sl)
                if rows is None:
                    state.split(sl, sl.split(), cost)
                else:
                    if rows:
                        _write(rows, out_dir / f"{hashlib.sha1(sl.query().encode()).hexdigest()[:16]}.parquet")
                    state.done(sl, len(rows), cost)
                    if len(rows) < min(count, MAX_RESULTS):
                        log(f"warning: {sl.query()} returned {len(rows)} of {count} repos")
            except (RateLimited, TokenRejected) as exc:
                # This worker's identity is out for now; the slice goes back for the others or the next run.
                state.release(sl)
                limited.set()
                log(f"worker stops: {exc}")
                return  # the finally below still marks this slice as no longer in flight
            except Exception as exc:
                # Anything that breaks after the fetch must not leave the slice stuck as running.
                state.fail(sl, f"{type(exc).__name__}: {exc}")
                log(f"failed {sl.query()}: {exc}")
            finally:
                with flight_lock:
                    in_flight[0] -= 1
            total_rows, total_cost = state.totals()
            log(f"{datetime.now(timezone.utc):%H:%M:%S} repos={total_rows:,} points={total_cost:,} {state.counts()}")

    threads = [threading.Thread(target=worker, args=(f,), daemon=True) for f in factories]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return limited.is_set() and state.counts().get("queued", 0) > 0
