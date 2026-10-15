from datetime import datetime, timedelta, timezone

from mergescout.crawl.state import CrawlState

NOW = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)


def rows(n, prio=0):
    return [{"repo_id": i, "name": f"o/r{i}", "stars": 100 + i, "pushed_at": "2026-10-10T00:00:00Z",
             "priority": prio} for i in range(1, n + 1)]


def test_upsert_is_idempotent_and_keeps_status(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    assert s.upsert_repos(rows(3)) == 3
    s.start_run("r1", "crawl")
    claimed = s.claim(1, "r1")
    assert len(claimed) == 1
    s.done(claimed[0]["repo_id"], "r1", points=5, prs=10, issues=2)
    assert s.upsert_repos(rows(3)) == 0
    assert s.counts() == {"queued": 2, "done": 1}


def test_claim_is_best_first_and_exclusive(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    s.upsert_repos(rows(2, prio=1) + [{"repo_id": 9, "name": "o/big", "stars": 5000,
                                        "pushed_at": "2026-10-10T00:00:00Z", "priority": 10}])
    s.start_run("r1", "crawl")
    first = s.claim(1, "r1")
    assert first[0]["repo_id"] == 9
    second = s.claim(5, "r1")
    assert {c["repo_id"] for c in second} == {1, 2}
    assert s.claim(5, "r1") == []


def test_requeue_stale_running(tmp_path):
    clock = [NOW]
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: clock[0])
    s.upsert_repos(rows(1))
    s.start_run("r1", "crawl")
    s.claim(1, "r1")
    assert s.requeue_stale(older_than_minutes=120) == 0
    clock[0] = NOW + timedelta(hours=3)
    assert s.requeue_stale(older_than_minutes=120) == 1
    assert s.counts() == {"queued": 1}


def test_failed_retries_then_gives_up(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    s.upsert_repos(rows(1))
    s.start_run("r1", "crawl")
    for attempt in range(3):
        s.claim(1, "r1")
        s.fail(1, "r1", "boom")
        assert s.requeue_failed(max_attempts=3) == (1 if attempt < 2 else 0)
    assert s.counts() == {"failed": 1}


def test_gone_is_never_requeued(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    s.upsert_repos(rows(1))
    s.start_run("r1", "crawl")
    s.claim(1, "r1")
    s.gone(1, "r1", "NOT_FOUND")
    assert s.requeue_failed() == 0 and s.requeue_stale(0) == 0
    assert s.counts() == {"gone": 1}


def test_done_repo_becomes_due_again(tmp_path):
    clock = [NOW]
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: clock[0])
    s.upsert_repos(rows(1))
    s.start_run("r1", "crawl")
    s.claim(1, "r1")
    s.done(1, "r1", points=3, prs=1, issues=0, refresh_days=1)
    assert s.claim(1, "r1") == []
    clock[0] = NOW + timedelta(days=2)
    assert len(s.claim(1, "r2")) == 1


def test_issue_state_log_keeps_latest_per_issue(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    s.log_issue_state([{"repo_id": 1, "number": 7, "state": "OPEN", "assignees": 0, "linked_open_prs": 0,
                        "linked_merged_prs": 0, "comments": 1, "labels": "good first issue"}], "2026-10-13")
    s.log_issue_state([{"repo_id": 1, "number": 7, "state": "OPEN", "assignees": 1, "linked_open_prs": 1,
                        "linked_merged_prs": 0, "comments": 3, "labels": "good first issue"}], "2026-10-14")
    latest = s.latest_issue_state(1)
    assert latest == [{"repo_id": 1, "number": 7, "observed_at": "2026-10-14", "state": "OPEN", "assignees": 1,
                       "linked_open_prs": 1, "linked_merged_prs": 0, "comments": 3, "labels": "good first issue",
                       "updated_at": None}]
    assert s.open_issue_numbers(1) == [7]
    s.forget_issues(1, [7])
    assert s.latest_issue_state(1) == [] and s.open_issue_numbers(1) == []


def test_release_puts_repo_back_without_spending_an_attempt(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    s.upsert_repos(rows(1))
    s.start_run("r1", "crawl")
    s.claim(1, "r1")
    s.release(1)
    assert s.counts() == {"queued": 1}
    assert s.db.execute("SELECT attempts FROM repos WHERE repo_id = 1").fetchone()[0] == 0


def test_old_state_files_gain_the_full_crawl_column(tmp_path):
    import sqlite3
    db = sqlite3.connect(tmp_path / "c.sqlite")
    db.execute("CREATE TABLE repos (repo_id INTEGER PRIMARY KEY, name TEXT NOT NULL, stars INTEGER, pushed_at TEXT, "
               "priority INTEGER DEFAULT 0, status TEXT NOT NULL DEFAULT 'queued', attempts INTEGER DEFAULT 0, "
               "run_id TEXT, claimed_at TEXT, last_done_at TEXT, next_due TEXT, last_points INTEGER, "
               "last_prs INTEGER, last_issues INTEGER, error TEXT)")
    db.execute("INSERT INTO repos (repo_id, name, status, last_done_at, next_due) "
               "VALUES (1, 'o/r', 'done', '2026-10-13T00:00:00+00:00', '2026-10-13T00:00:00+00:00')")
    db.commit()
    db.close()
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    claimed = s.claim(1, "r1")
    assert claimed[0]["last_full_at"] == claimed[0]["last_done_at"] == "2026-10-13T00:00:00+00:00"


def test_done_records_whether_the_crawl_was_full(tmp_path):
    clock = [NOW]
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: clock[0])
    s.upsert_repos(rows(1))
    s.claim(1, "r1")
    s.done(1, "r1", points=5, prs=1, issues=1, refresh_days=1, full=True)
    clock[0] = NOW + timedelta(days=2)
    first = s.claim(1, "r2")[0]
    assert first["last_full_at"] == NOW.isoformat(timespec="seconds")
    s.done(1, "r2", points=2, prs=0, issues=0, refresh_days=1, full=False)
    clock[0] = NOW + timedelta(days=4)
    second = s.claim(1, "r3")[0]
    assert second["last_full_at"] == NOW.isoformat(timespec="seconds")
    assert second["last_done_at"] == (NOW + timedelta(days=2)).isoformat(timespec="seconds")


def test_hot_tracked_issues_keep_a_repo_hot(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    s.log_issue_state([{"repo_id": 1, "number": 7, "state": "OPEN", "assignees": 0, "linked_open_prs": 0,
                        "linked_merged_prs": 0, "comments": 0, "labels": "bug|Good First Issue"}], "2026-10-13")
    assert s.has_hot_tracked(1) and not s.has_hot_tracked(2)


def test_latest_seed_sets_the_priority(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    s.upsert_repos(rows(1, prio=107))
    s.upsert_repos(rows(1, prio=40))
    assert s.claim(1, "r1")[0]["priority"] == 40
