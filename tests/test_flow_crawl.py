# The crawl end to end: seed to Parquet, safe restarts, refreshes and several identities sharing GitHub's limits.
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mergescout.crawl import __main__ as cli
from mergescout.crawl import writer as writer_mod
from mergescout.crawl.__main__ import main
from mergescout.crawl.fetch import fetch_repo
from mergescout.crawl.runner import run_crawl
from mergescout.crawl.state import CrawlState
from mergescout.discover.sweep import SCHEMA
from mergescout.github.graphql import Gate, GraphQLError, GraphQLResult, NotFound, RateLimited, TokenRejected
from mergescout.github.identities import Identity


QUIET = {"now": lambda: NOW, "log": lambda *a: None}
NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)
META = {"repository": {"databaseId": 1, "nameWithOwner": "o/r", "stargazerCount": 200, "releases": {"nodes": []}}}


def repo(repo_id, name, stars, pushed=NOW, open_issues=1, opp=0, has_issues=True):
    return {"repo_id": repo_id, "name": name, "stars": stars, "forks": 1, "created_at": NOW, "pushed_at": pushed,
            "language": "Python", "has_issues": has_issues, "license": None, "open_issues": open_issues,
            "open_prs": 0, "opportunity_issues": opp}


def write_basics(data_dir: Path, rows, name="a.parquet", schema=SCHEMA):
    out = data_dir / "discover" / "basics"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), out / name)


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


def test_failed_gone_and_rate_limited_are_recorded(tmp_path):
    s = seed(tmp_path, 4)
    gql = lambda: GQL(budget(), fail_for={"o/r3"}, gone_for={"o/r2"}, limited_for={"o/r1"})
    logs = []
    out = run_crawl(s, gql, tmp_path, "r1", minutes=5, in_flight=1, now=lambda: NOW, log=logs.append)
    # Highest stars first: r4 done, r3 failed, r2 gone, r1 hits the rate limit and goes back to the queue.
    assert s.counts() == {"done": 1, "failed": 1, "gone": 1, "queued": 1}
    assert out["repos"] == 1 and out["reason"] == "rate_limit"
    assert any("rate limit" in line and "wait" in line for line in logs)


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


def ident(name, make, share=1.0):
    return Identity(name, make, Gate(), share)


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


def test_a_repo_from_an_older_crawler_gets_a_full_crawl(tmp_path):
    from mergescout.crawl.state import CRAWL_VERSION
    clock = [NOW]
    s = seed(tmp_path, 1, clock=lambda: clock[0])
    b = budget()
    run_crawl(s, lambda: GQL(b), tmp_path, "r1", minutes=5, in_flight=1, now=lambda: clock[0], log=lambda *a: None)
    s.db.execute("UPDATE repos SET crawl_version = ?", (CRAWL_VERSION - 1,))
    b["calls"].clear()
    clock[0] = NOW + timedelta(hours=3)
    run_crawl(s, lambda: GQL(b), tmp_path, "r2", minutes=5, in_flight=1, now=lambda: clock[0], log=lambda *a: None)
    assert [v["order"] for q, v in b["calls"] if "pullRequests(first:" in q] == ["CREATED_AT"]
    assert s.db.execute("SELECT crawl_version FROM repos").fetchone()[0] == CRAWL_VERSION


def env(monkeypatch, tmp_path, token="t"):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path))
    if token:
        monkeypatch.setenv("GITHUB_TOKEN", token)
    else:
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)


def test_seed_then_run_end_to_end(monkeypatch, tmp_path, capsys):
    env(monkeypatch, tmp_path, token=None)
    write_basics(tmp_path, [repo(1, "o/a", 500, pushed=datetime.now(timezone.utc)),
                            repo(2, "o/b", 300, pushed=datetime.now(timezone.utc), opp=2)])
    assert main(["seed"]) == 0
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("MS_RUN_ID", "42-1")
    b = budget()
    made = []
    monkeypatch.setattr(cli, "GitHubGraphQL", lambda token, **kw: made.append(kw) or GQL(b))
    assert main(["run", "--minutes", "1", "--in-flight", "2"]) == 0
    assert made and callable(made[0]["max_wait"]) and 100 < made[0]["max_wait"]() <= 120
    assert all(kw["gate"] is made[0]["gate"] for kw in made) and made[0]["gate"] is not None
    assert pq.read_table(tmp_path / "crawl" / "prs" / "run=42-1-000.parquet").num_rows == 2
    assert main(["status"]) == 0
    out = capsys.readouterr().out
    assert "'done': 2" in out and "42-1" in out


def test_run_exits_nonzero_when_everything_failed(monkeypatch, tmp_path):
    env(monkeypatch, tmp_path)
    CrawlState(tmp_path / "crawl.sqlite").upsert_repos([{"repo_id": 1, "name": "o/r1", "stars": 100,
                                                          "pushed_at": None, "priority": 0}])
    monkeypatch.setattr(cli, "GitHubGraphQL", lambda token, **kw: GQL(budget(), fail_for={"o/r1"}))
    assert main(["run", "--minutes", "1"]) == 1


def test_run_uses_every_identity_it_is_given(monkeypatch, tmp_path, capsys):
    env(monkeypatch, tmp_path)
    monkeypatch.setenv("MS_APP_TOKEN", "app")
    monkeypatch.setenv("MS_ACTIONS_TOKEN", "act")
    CrawlState(tmp_path / "crawl.sqlite").upsert_repos([{"repo_id": i, "name": f"o/r{i}", "stars": 100,
                                                          "pushed_at": None, "priority": 0} for i in range(1, 30)])
    tokens = []
    monkeypatch.setattr(cli, "GitHubGraphQL", lambda token, **kw: tokens.append(token) or GQL(budget()))
    assert main(["run", "--minutes", "1", "--in-flight", "5"]) == 0
    out = capsys.readouterr().out
    assert "github personal:" in out and "github app:" in out and "github actions:" in out
    assert set(tokens) == {"t", "app", "act"}


def pr(n, created="2026-10-01T00:00:00Z"):
    return {"number": n, "createdAt": created, "state": "OPEN", "author": {"__typename": "User", "login": "a"},
            "reviews": {"nodes": []}, "closingIssuesReferences": {"nodes": []}}


def issue(n, updated="2026-10-01T00:00:00Z"):
    return {"number": n, "title": "t", "state": "OPEN", "updatedAt": updated, "labels": {"nodes": []},
            "assignees": {"totalCount": 0}, "closedByPullRequestsReferences": {"nodes": []},
            "comments": {"totalCount": 0, "nodes": []}}


def page(conn, nodes, has_next, cursor=None):
    return {"repository": {conn: {"pageInfo": {"hasNextPage": has_next, "endCursor": cursor}, "nodes": nodes}}}


def test_heavy_page_halves_then_succeeds(fake_gql):
    calls = []

    def responder(query, variables):
        if "pullRequests(first:" in query:
            calls.append(variables["n"])
            if variables["n"] > 12:
                raise GraphQLError("Resource limits for this query exceeded")
            return page("pullRequests", [pr(1)], False), 1
        if "issues(first:" in query:
            return page("issues", [], False), 1
        return META, 1
    b = fetch_repo(fake_gql(responder), 1, "o/r", NOW)
    assert calls == [50, 25, 12] and len(b.prs) == 1


def closed_page_responder(hot_labels):
    hot = {**issue(1, "2026-10-10T00:00:00Z"), "labels": {"nodes": [{"name": n} for n in hot_labels]}}
    closed = page("issues", [{"number": 5, "updatedAt": "2026-09-01T00:00:00Z", "closedAt": "2026-09-01T00:00:00Z"},
                             {"number": 6, "updatedAt": "2025-01-01T00:00:00Z"}], True, "z")

    def responder(query, variables):
        if "states: CLOSED" in query:
            return closed, 1
        if "issues(first:" in query:
            return page("issues", [hot], False), 1
        if "pullRequests(first:" in query:
            return page("pullRequests", [], False), 1
        return META, 1
    return responder


def test_closed_labelled_issues_only_for_hot_repos_and_stop_at_cutoff(fake_gql):
    gql = fake_gql(closed_page_responder(["Good First Issue"]))
    b = fetch_repo(gql, 1, "o/r", NOW)
    assert [c["number"] for c in b.closed_issues] == [5]
    assert sum("states: CLOSED" in q for q, _ in gql.calls) == 1
    cold = fake_gql(closed_page_responder(["bug"]))
    assert fetch_repo(cold, 1, "o/r", NOW).closed_issues == []
    assert not any("states: CLOSED" in q for q, _ in cold.calls)


def test_closed_history_asks_for_the_repo_s_own_label_spellings(fake_gql):
    # GitHub matches label names exactly; decorated labels got no closed history before (Phase 2 review).
    meta = {"repository": {**META["repository"],
                           "labelNames": {"nodes": [{"name": "good first issue :wave:"}, {"name": "bug"}]}}}
    responder = closed_page_responder(["good first issue :wave:"])
    gql = fake_gql(lambda q, v: (meta, 1) if "labelNames" in q else responder(q, v))
    fetch_repo(gql, 1, "o/r", NOW)
    closed = [v for q, v in gql.calls if "states: CLOSED" in q]
    assert closed and closed[0]["labels"] == ["good first issue :wave:"]

