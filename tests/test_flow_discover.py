# Repo discovery end to end: the search sweep resumes, survives limits and feeds a value-per-point queue.
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mergescout.crawl.seed import priority, seed_queue
from mergescout.crawl.state import CrawlState
from mergescout.discover.sweep import SCHEMA, Slice, SweepState, run_sweep


T0 = datetime(2007, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 12, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)


def node(i):
    return {"databaseId": i, "nameWithOwner": f"o/r{i}", "stargazerCount": 12, "forkCount": 0,
            "createdAt": "2020-01-01T00:00:00Z", "pushedAt": "2026-10-01T00:00:00Z",
            "primaryLanguage": {"name": "Python"}, "hasIssuesEnabled": True, "licenseInfo": None,
            "issues": {"totalCount": 3}, "pullRequests": {"totalCount": 1}, "opp": {"totalCount": 2}}


def search(count, nodes, has_next=False, cursor=None):
    return {"search": {"repositoryCount": count, "nodes": nodes,
                       "pageInfo": {"hasNextPage": has_next, "endCursor": cursor}}}


def test_run_sweep_splits_writes_and_resumes(tmp_path, fake_gql):
    def responder(q, v):
        if "stars:10..19 " in v["q"]:
            return search(1500, []), 1
        base = 0 if "stars:10..14 " in v["q"] else 10
        return search(3, [node(base + i) for i in range(3)] if "cursor" in v else []), 1

    gql = fake_gql(responder)
    state = SweepState(tmp_path / "sweep.sqlite")
    state.seed([Slice(10, 19, T0, T1)])
    run_sweep(lambda: gql, state, tmp_path, workers=2, log=lambda *a: None)
    counts = state.counts()
    assert counts == {"done": 2, "split": 1}
    names = sorted(r for f in (tmp_path / "basics").glob("*.parquet") for r in pq.read_table(f)["name"].to_pylist())
    assert len(names) == 6
    before = len(gql.calls)
    run_sweep(lambda: gql, state, tmp_path, workers=2, log=lambda *a: None)
    assert len(gql.calls) == before


def test_worker_crash_after_fetch_does_not_hang(tmp_path, monkeypatch, fake_gql):
    import threading
    from mergescout.discover import sweep as sweep_mod

    def responder(query, variables):
        if "repositoryCount" in query and "nodes" not in query:
            return {"search": {"repositoryCount": 1}}, 1
        return {"search": {"repositoryCount": 1, "pageInfo": {"hasNextPage": False, "endCursor": None},
                           "nodes": [{"databaseId": 1, "nameWithOwner": "o/r", "stargazerCount": 100}]}}, 1

    def broken_write(rows, out):
        raise OSError("disk full")

    monkeypatch.setattr(sweep_mod, "_write", broken_write)
    state = sweep_mod.SweepState(tmp_path / "s.sqlite")
    state.seed(sweep_mod.initial_slices(100, sweep_mod.datetime(2020, 1, 1, tzinfo=sweep_mod.timezone.utc),
                                        sweep_mod.datetime(2020, 1, 2, tzinfo=sweep_mod.timezone.utc)))
    done = threading.Event()

    def run():
        sweep_mod.run_sweep(lambda: fake_gql(responder), state, tmp_path, workers=2, log=lambda *a, **k: None)
        done.set()

    threading.Thread(target=run, daemon=True).start()
    assert done.wait(10), "sweep hung after a worker crashed mid-slice"
    counts = state.counts()
    assert counts.get("failed", 0) >= 1 and counts.get("running", 0) == 0


def test_changed_min_stars_is_refused(tmp_path, monkeypatch, capsys):
    from mergescout.discover.__main__ import main
    from mergescout.discover.sweep import SweepState
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    SweepState(tmp_path / "discover" / "sweep.sqlite").setting("min_stars", "100")
    assert main(["sweep", "--min-stars", "50"]) == 2
    assert "started with --min-stars 100" in capsys.readouterr().err


def test_rate_limit_pauses_the_sweep_and_requeues_the_slice(tmp_path, fake_gql):
    from mergescout.github.graphql import RateLimited

    def responder(q, v):
        raise RateLimited("wait 3000 s")

    state = SweepState(tmp_path / "sweep.sqlite")
    state.seed([Slice(10, 19, T0, T1), Slice(20, 49, T0, T1)])
    paused = run_sweep(lambda: fake_gql(responder), state, tmp_path, workers=2, log=lambda *a: None)
    assert paused is True and state.counts() == {"queued": 2}


def test_active_days_is_fixed_at_the_start_and_reused_on_resume(tmp_path, monkeypatch):
    from mergescout.discover import __main__ as cli
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setattr(cli, "run_sweep", lambda *a, **k: False)
    assert cli.main(["sweep", "--min-stars", "100", "--active-days", "90"]) == 0
    state = SweepState(tmp_path / "discover" / "sweep.sqlite")
    saved = state.setting("pushed_after")
    assert saved and "pushed:>=" + saved[:10] in state.claim().query()
    assert cli.main(["sweep", "--min-stars", "100", "--active-days", "30"]) == 0
    assert state.setting("pushed_after") == saved


def test_one_identity_running_out_does_not_stop_the_others(tmp_path, fake_gql):
    from mergescout.github.graphql import RateLimited

    def limited(q, v):
        raise RateLimited("wait 3000 s")

    def fine(q, v):
        return search(2, [node(1), node(2)] if "cursor" in v else []), 1

    state = SweepState(tmp_path / "sweep.sqlite")
    state.seed([Slice(10, 19, T0, T1), Slice(20, 49, T0, T1), Slice(50, 99, T0, T1)])
    paused = run_sweep([lambda: fake_gql(limited), lambda: fake_gql(fine)], state, tmp_path, log=lambda *a: None)
    assert paused is False and state.counts() == {"done": 3}


def repo(repo_id, name, stars, pushed=NOW, open_issues=1, opp=0, has_issues=True):
    return {"repo_id": repo_id, "name": name, "stars": stars, "forks": 1, "created_at": NOW, "pushed_at": pushed,
            "language": "Python", "has_issues": has_issues, "license": None, "open_issues": open_issues,
            "open_prs": 0, "opportunity_issues": opp}


def write_basics(data_dir: Path, rows, name="a.parquet", schema=SCHEMA):
    out = data_dir / "discover" / "basics"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), out / name)


def test_a_cheap_repo_beats_a_costly_one_with_the_same_beginner_issues():
    giant = {"stars": 190000, "has_issues": True, "open_issues": 18000, "opportunity_issues": 5}
    mid = {"stars": 3000, "has_issues": True, "open_issues": 40, "opportunity_issues": 5}
    assert priority(mid) > priority(giant)


def test_seed_queue_puts_opportunity_repos_first(tmp_path):
    write_basics(tmp_path, [repo(1, "o/plain", 5000), repo(2, "o/friendly", 120, opp=3)])
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    out = seed_queue(s, tmp_path, NOW, log=lambda *a: None)
    assert out == {"active": 2, "opportunity_repos": 1, "added": 2}
    assert [c["name"] for c in s.claim(2, "r1")] == ["o/friendly", "o/plain"]
    assert seed_queue(s, tmp_path, NOW, log=lambda *a: None)["added"] == 0
