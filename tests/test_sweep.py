from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq

import pytest

from mergescout.discover.sweep import Slice, SweepState, fetch_slice, initial_slices, run_sweep
from mergescout.github.graphql import GraphQLError

T0 = datetime(2007, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 12, tzinfo=timezone.utc)


def node(i):
    return {"databaseId": i, "nameWithOwner": f"o/r{i}", "stargazerCount": 12, "forkCount": 0,
            "createdAt": "2020-01-01T00:00:00Z", "pushedAt": "2026-10-01T00:00:00Z",
            "primaryLanguage": {"name": "Python"}, "hasIssuesEnabled": True, "licenseInfo": None,
            "issues": {"totalCount": 3}, "pullRequests": {"totalCount": 1}, "opp": {"totalCount": 2}}


def search(count, nodes, has_next=False, cursor=None):
    return {"search": {"repositoryCount": count, "nodes": nodes,
                       "pageInfo": {"hasNextPage": has_next, "endCursor": cursor}}}


def test_slice_query_format():
    q = Slice(10, 19, T0, T1).query()
    assert q == "stars:10..19 created:2007-01-01T00:00:00Z..2026-10-12T00:00:00Z fork:false archived:false"
    assert Slice(10000, None, T0, T1).query().startswith("stars:>=10000 ")


def test_split_stars_first_then_dates_without_overlap():
    a, b = Slice(10, 19, T0, T1).split()
    assert (a.stars_lo, a.stars_hi, b.stars_lo, b.stars_hi) == (10, 14, 15, 19)
    c, d = Slice(10, 10, T0, T1).split()
    assert c.created_lo == T0 and d.created_hi == T1
    assert c.created_hi + timedelta(seconds=1) == d.created_lo
    tiny = Slice(10, 10, T0, T0 + timedelta(seconds=30))
    assert tiny.split() == []


def test_initial_slices_cover_from_min_stars():
    sl = initial_slices(10, T0, T1)
    assert [(s.stars_lo, s.stars_hi) for s in sl] == [(10, 19), (20, 49), (50, 99), (100, 999), (1000, 9999), (10000, None)]


def test_fetch_slice_asks_for_split_when_over_1000(fake_gql):
    gql = fake_gql(lambda q, v: (search(5000, []), 1))
    rows, count, cost = fetch_slice(gql, Slice(10, 19, T0, T1))
    assert (rows, count, cost) == (None, 5000, 1)
    assert len(gql.calls) == 1


def test_fetch_slice_probes_then_pages(fake_gql):
    pages = {None: (search(150, [node(i) for i in range(100)], True, "c1"), 1),
             "c1": (search(150, [node(i) for i in range(100, 150)], False), 1)}

    def responder(q, v):
        if "cursor" not in v:
            return search(150, []), 1
        return pages[v["cursor"]]

    gql = fake_gql(responder)
    rows, count, cost = fetch_slice(gql, Slice(10, 10, T0, T1))
    assert (len(rows), count, cost) == (150, 150, 3)
    assert rows[0]["name"] == "o/r0" and rows[0]["language"] == "Python" and rows[0]["open_issues"] == 3
    assert rows[0]["opportunity_issues"] == 2 and "good first issue" in gql.calls[-1][1]["labels"]


def test_fetch_slice_shrinks_page_when_query_too_heavy(fake_gql):
    sizes = []

    def responder(q, v):
        if "cursor" not in v:
            return search(20, []), 1
        sizes.append(v["n"])
        if v["n"] > 25:
            raise GraphQLError("Resource limits for this query exceeded.")
        return search(20, [node(i) for i in range(20)]), 1

    rows, count, cost = fetch_slice(fake_gql(responder), Slice(100, 100, T0, T1))
    assert len(rows) == 20 and sizes == [50, 25]


def test_fetch_slice_gives_up_on_other_errors(fake_gql):
    def responder(q, v):
        if "cursor" not in v:
            return search(20, []), 1
        raise GraphQLError("boom")

    with pytest.raises(GraphQLError, match="boom"):
        fetch_slice(fake_gql(responder), Slice(100, 100, T0, T1))


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


def test_status_open_does_not_reset_running_slices(tmp_path):
    from mergescout.discover.sweep import SweepState, Slice, datetime, timezone
    s = SweepState(tmp_path / "s.sqlite", resume=True)
    sl = Slice(100, None, datetime(2020, 1, 1, tzinfo=timezone.utc), datetime(2020, 1, 2, tzinfo=timezone.utc))
    s.seed([sl])
    assert s.claim() == sl
    assert SweepState(tmp_path / "s.sqlite").counts() == {"running": 1}
    assert SweepState(tmp_path / "s.sqlite", resume=True).counts() == {"queued": 1}


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


def test_pushed_filter_is_in_the_query_and_survives_splits():
    p = datetime(2026, 7, 16, tzinfo=timezone.utc)
    sl = Slice(100, 199, T0, T1, p)
    assert sl.query() == ("stars:100..199 pushed:>=2026-07-16 created:2007-01-01T00:00:00Z..2026-10-12T00:00:00Z "
                          "fork:false archived:false")
    assert all(s.pushed_lo == p for s in sl.split())
    assert all(s.pushed_lo == p for s in initial_slices(100, T0, T1, p))


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


def test_empty_body_counts_as_too_heavy():
    from mergescout.github.graphql import too_heavy
    assert too_heavy(GraphQLError("GitHub returned a body that is not JSON: "))


def test_sweep_clients_share_one_rate_limit_wait(tmp_path, monkeypatch):
    from mergescout.discover import __main__ as cli
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    seen = {}
    monkeypatch.setattr(cli, "run_sweep", lambda make_gql, *a, **k: seen.update(clients=[f() for f in make_gql]))
    assert cli.main(["sweep", "--min-stars", "100", "--workers", "4"]) == 0
    clients = seen["clients"]
    assert len(clients) == 4 and len({id(c) for c in clients}) == 4 and len({id(c.gate) for c in clients}) == 1


def test_sweep_skips_the_unused_open_pr_count():
    from mergescout.discover.sweep import REPO_QUERY
    assert "pullRequests" not in REPO_QUERY and "opp:" in REPO_QUERY and "issues(states: OPEN)" in REPO_QUERY


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
