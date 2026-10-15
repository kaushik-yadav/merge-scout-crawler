import threading
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from mergescout.crawl import writer as writer_mod
from mergescout.crawl.runner import run_crawl
from mergescout.crawl.state import CrawlState
from mergescout.github.graphql import Gate, GraphQLError, GraphQLResult, NotFound, RateLimited, TokenRejected
from mergescout.github.identities import Identity

NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)
QUIET = {"now": lambda: NOW, "log": lambda *a: None}


class GQL:
    def __init__(self, budget, fail_for=(), gone_for=(), limited_for=(), labels=(), assignees=0,
                 updated="2026-10-10T00:00:00Z", rejected=False):
        self.budget, self.fail_for, self.gone_for = budget, set(fail_for), set(gone_for)
        self.limited_for, self.labels, self.assignees = set(limited_for), list(labels), assignees
        self.updated = updated
        self.rejected = rejected
        self.last_remaining = None

    def query(self, query, variables=None, **kw):
        name = f"{variables['owner']}/{variables['name']}"
        if self.rejected:
            raise TokenRejected("GitHub rejected the token (401)")
        if name in self.fail_for:
            raise GraphQLError("boom")
        if name in self.gone_for:
            raise NotFound("gone")
        if name in self.limited_for:
            raise RateLimited("wait")
        node = {"number": 1, "createdAt": "2026-10-01T00:00:00Z", "updatedAt": self.updated,
                "state": "OPEN", "author": None, "reviews": {"nodes": []}, "closingIssuesReferences": {"nodes": []},
                "closedByPullRequestsReferences": {"nodes": []},
                "labels": {"nodes": [{"name": n} for n in self.labels]},
                "assignees": {"totalCount": self.assignees}, "comments": {"totalCount": 0, "nodes": []}}
        if "pullRequests(first:" in query or "issues(first:" in query:
            conn = "pullRequests" if "pullRequests(first:" in query else "issues"
            data = {"repository": {conn: {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [node]}}}
        else:
            data = {"repository": {"databaseId": 1, "nameWithOwner": name, "stargazerCount": 200,
                                   "releases": {"nodes": []}}}
        with self.budget["lock"]:
            self.budget.setdefault("calls", []).append((query, dict(variables)))
            self.budget["left"] -= 1
            self.last_remaining = self.budget["left"]
        return GraphQLResult(data=data, cost=1, remaining=self.last_remaining, reset_at="2026-10-14T01:00:00Z")


def budget(left=4000):
    return {"left": left, "lock": threading.Lock()}


def seed(tmp_path, n, clock=lambda: NOW):
    s = CrawlState(tmp_path / "c.sqlite", now=clock)
    s.upsert_repos([{"repo_id": i, "name": f"o/r{i}", "stars": 100 + i, "pushed_at": "2026-10-10T00:00:00Z",
                     "priority": 0} for i in range(1, n + 1)])
    return s


def rows(tmp_path, table):
    return sum(pq.read_table(p).num_rows for p in (tmp_path / "crawl" / table).glob("*.parquet"))


def test_runs_all_and_writes_parquet(tmp_path):
    s = seed(tmp_path, 5)
    b = budget()
    out = run_crawl(s, lambda: GQL(b), tmp_path, "r1", minutes=5, in_flight=3, **QUIET)
    assert out["repos"] == 5 and out["reason"] == "empty" and out["points"] == 15
    assert s.counts() == {"done": 5}
    assert rows(tmp_path, "prs") == 5 and rows(tmp_path, "repo_meta") == 5 and rows(tmp_path, "issues") == 5
    assert tuple(s.db.execute("SELECT reason, repos FROM runs WHERE run_id = 'r1'").fetchone()) == ("empty", 5)


def test_stops_when_budget_low(tmp_path):
    s = seed(tmp_path, 20)
    b = budget(330)
    out = run_crawl(s, lambda: GQL(b), tmp_path, "r1", minutes=5, in_flight=2, reserve=300, **QUIET)
    assert out["reason"] == "budget"
    assert 0 < out["repos"] < 20
    assert s.counts().get("running", 0) == 0


def test_stops_at_time_limit(tmp_path):
    s = seed(tmp_path, 5)
    ticks = iter([0, 0, 0] + [999] * 50)
    out = run_crawl(s, lambda: GQL(budget()), tmp_path, "r1", minutes=1, in_flight=1,
                    clock=lambda: next(ticks) * 60, **QUIET)
    assert out["reason"] == "time" and 1 <= out["repos"] < 5


def test_limit_caps_repos(tmp_path):
    s = seed(tmp_path, 6)
    out = run_crawl(s, lambda: GQL(budget()), tmp_path, "r1", minutes=5, in_flight=4, limit=3, **QUIET)
    assert out["reason"] == "limit" and out["repos"] == 3 and s.counts() == {"done": 3, "queued": 3}


def test_failed_gone_and_rate_limited_are_recorded(tmp_path):
    s = seed(tmp_path, 4)
    gql = lambda: GQL(budget(), fail_for={"o/r3"}, gone_for={"o/r2"}, limited_for={"o/r1"})
    logs = []
    out = run_crawl(s, gql, tmp_path, "r1", minutes=5, in_flight=1, now=lambda: NOW, log=logs.append)
    # Highest stars first: r4 done, r3 failed, r2 gone, r1 hits the rate limit and goes back to the queue.
    assert s.counts() == {"done": 1, "failed": 1, "gone": 1, "queued": 1}
    assert out["repos"] == 1 and out["reason"] == "rate_limit"
    assert any("rate limit" in line and "wait" in line for line in logs)


def test_opportunity_repos_refresh_daily(tmp_path):
    s = seed(tmp_path, 2)
    run_crawl(s, lambda: GQL(budget(), labels=["Good First Issue"]), tmp_path, "r1", minutes=5, in_flight=1,
              limit=1, **QUIET)
    run_crawl(s, lambda: GQL(budget()), tmp_path, "r2", minutes=5, in_flight=1, **QUIET)
    due = dict(s.db.execute("SELECT repo_id, next_due FROM repos").fetchall())
    assert due[2] == "2026-10-15T00:00:00+00:00" and due[1] == "2026-10-21T00:00:00+00:00"


def test_kill_before_flush_loses_nothing_and_rerun_has_no_duplicates(tmp_path, monkeypatch):
    s = seed(tmp_path, 4)

    def killed(self):
        raise KeyboardInterrupt
    monkeypatch.setattr(writer_mod.BatchWriter, "flush", killed)
    with pytest.raises(KeyboardInterrupt):
        run_crawl(s, lambda: GQL(budget()), tmp_path, "r1", minutes=5, in_flight=2, **QUIET)
    monkeypatch.undo()
    assert s.counts().get("done", 0) == 0 and not (tmp_path / "crawl").exists()
    later = NOW.replace(hour=5)
    s2 = CrawlState(tmp_path / "c.sqlite", now=lambda: later)
    assert s2.requeue_stale(120) == 4
    run_crawl(s2, lambda: GQL(budget()), tmp_path, "r2", minutes=5, in_flight=2, now=lambda: later,
              log=lambda *a: None)
    assert s2.counts() == {"done": 4} and rows(tmp_path, "prs") == 4


def test_flushes_in_batches_and_marks_done_only_after_flush(tmp_path):
    s = seed(tmp_path, 5)
    run_crawl(s, lambda: GQL(budget()), tmp_path, "r1", minutes=5, in_flight=1, flush_every=2, **QUIET)
    files = sorted(p.name for p in (tmp_path / "crawl" / "prs").glob("*.parquet"))
    assert files == ["run=r1-000.parquet", "run=r1-001.parquet", "run=r1-002.parquet"]
    assert s.counts() == {"done": 5}


def test_second_crawl_writes_issue_events(tmp_path):
    clock = [NOW]
    s = seed(tmp_path, 1, clock=lambda: clock[0])
    run_crawl(s, lambda: GQL(budget()), tmp_path, "r1", minutes=5, in_flight=1, now=lambda: clock[0],
              log=lambda *a: None)
    clock[0] = NOW.replace(day=22)
    # Assigning an issue bumps its update time on GitHub, which is what a refresh looks for.
    run_crawl(s, lambda: GQL(budget(), assignees=1, updated="2026-10-21T00:00:00Z"), tmp_path, "r2", minutes=5,
              in_flight=1, now=lambda: clock[0],
              log=lambda *a: None)
    events = pq.read_table(tmp_path / "crawl" / "issue_events" / "run=r2-000.parquet").to_pylist()
    assert [(e["number"], e["event"], e["observed_at"]) for e in events] == [(1, "claimed", "2026-10-22")]
    assert s.open_issue_numbers(1) == [1]


def test_refreshes_ask_only_for_changes_until_a_full_crawl_is_due(tmp_path):
    clock = [NOW]
    s = seed(tmp_path, 1, clock=lambda: clock[0])
    b = budget()

    def crawl(run_id):
        run_crawl(s, lambda: GQL(b), tmp_path, run_id, minutes=5, in_flight=1, cold_days=7, full_every_days=30,
                  now=lambda: clock[0], log=lambda *a: None)
        orders = [v["order"] for q, v in b["calls"] if "pullRequests(first:" in q]
        b["calls"].clear()
        return orders
    assert crawl("r1") == ["CREATED_AT"]
    clock[0] = NOW + timedelta(days=8)
    assert crawl("r2") == ["UPDATED_AT"]
    clock[0] = NOW + timedelta(days=31)
    assert crawl("r3") == ["CREATED_AT"]


def test_hot_repo_stays_hot_when_its_beginner_issues_did_not_change(tmp_path):
    clock = [NOW]
    s = seed(tmp_path, 1, clock=lambda: clock[0])
    run_crawl(s, lambda: GQL(budget(), labels=["Good First Issue"]), tmp_path, "r1", minutes=5, in_flight=1,
              now=lambda: clock[0], log=lambda *a: None)
    clock[0] = NOW + timedelta(days=2)
    run_crawl(s, lambda: GQL(budget()), tmp_path, "r2", minutes=5, in_flight=1, now=lambda: clock[0],
              log=lambda *a: None)
    due = s.db.execute("SELECT next_due FROM repos").fetchone()[0]
    assert due == (NOW + timedelta(days=3)).isoformat(timespec="seconds")


def ident(name, make, share=1.0):
    return Identity(name, make, Gate(), share)


def test_identities_share_one_queue(tmp_path):
    s = seed(tmp_path, 6)
    out = run_crawl(s, [ident("personal", lambda: GQL(budget())), ident("app", lambda: GQL(budget()))], tmp_path,
                    "r1", minutes=5, in_flight=2, **QUIET)
    assert out["repos"] == 6 and out["reason"] == "empty" and s.counts() == {"done": 6}
    per = out["identities"]
    assert per["personal"]["repos"] > 0 and per["app"]["repos"] > 0
    assert per["personal"]["repos"] + per["app"]["repos"] == 6 and per["app"]["slots"] == 2


def test_an_identity_that_runs_low_steps_aside_while_the_others_finish(tmp_path):
    s = seed(tmp_path, 8)
    low = budget(310)
    out = run_crawl(s, [ident("personal", lambda: GQL(low)), ident("app", lambda: GQL(budget()))], tmp_path, "r1",
                    minutes=5, in_flight=1, reserve=300, **QUIET)
    assert out["reason"] == "empty" and s.counts() == {"done": 8}
    assert out["identities"]["personal"]["out"] == "budget" and out["identities"]["app"]["out"] is None


def test_an_expired_token_hands_its_repo_back(tmp_path):
    s = seed(tmp_path, 4)
    out = run_crawl(s, [ident("app", lambda: GQL(budget(), rejected=True)), ident("personal", lambda: GQL(budget()))],
                    tmp_path, "r1", minutes=5, in_flight=1, **QUIET)
    assert s.counts() == {"done": 4} and out["reason"] == "empty"
    assert out["identities"]["app"]["out"] == "rejected" and out["identities"]["app"]["repos"] == 0


def test_the_run_stops_when_every_identity_is_out(tmp_path):
    s = seed(tmp_path, 3)
    names = {"o/r1", "o/r2", "o/r3"}
    out = run_crawl(s, [ident("a", lambda: GQL(budget(), limited_for=names)),
                        ident("b", lambda: GQL(budget(), rejected=True))], tmp_path, "r1", minutes=5, in_flight=1,
                    **QUIET)
    assert out["reason"] == "rate_limit" and s.counts() == {"queued": 3}


class SlowDown(GQL):
    # Pretends GitHub asked this identity to slow down on its first few queries.
    def __init__(self, budget, gate, times):
        super().__init__(budget)
        self.gate, self.times = gate, times

    def query(self, query, variables=None, **kw):
        with self.budget["lock"]:
            if self.budget.setdefault("asked", 0) < self.times:
                self.budget["asked"] += 1
                self.gate.note("rate_waits")
        return super().query(query, variables, **kw)


def test_parallel_requests_drop_when_github_asks_to_slow_down_and_grow_back(tmp_path):
    s = seed(tmp_path, 60)
    gate, b = Gate(), budget()
    logs = []
    out = run_crawl(s, [Identity("personal", lambda: SlowDown(b, gate, 2), gate)], tmp_path, "r1", minutes=5,
                    in_flight=8, now=lambda: NOW, log=logs.append)
    per = out["identities"]["personal"]
    assert out["repos"] == 60 and per["lowest_slots"] < 8 and per["slots"] > per["lowest_slots"]
    assert any("slow down" in line for line in logs)
