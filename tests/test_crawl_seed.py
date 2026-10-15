from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mergescout.crawl.seed import active_repos_from_sweep, expected_cost, priority, seed_queue
from mergescout.crawl.state import CrawlState
from mergescout.discover.sweep import SCHEMA

NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)


def repo(repo_id, name, stars, pushed=NOW, open_issues=1, opp=0, has_issues=True):
    return {"repo_id": repo_id, "name": name, "stars": stars, "forks": 1, "created_at": NOW, "pushed_at": pushed,
            "language": "Python", "has_issues": has_issues, "license": None, "open_issues": open_issues,
            "open_prs": 0, "opportunity_issues": opp}


def write_basics(data_dir: Path, rows, name="a.parquet", schema=SCHEMA):
    out = data_dir / "discover" / "basics"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), out / name)


def test_active_filter_dedupes_and_sorts(tmp_path):
    write_basics(tmp_path, [repo(1, "o/active", 150), repo(2, "o/stale", 150, pushed=NOW.replace(year=2025)),
                            repo(3, "o/small", 60), repo(4, "o/big", 9000)])
    write_basics(tmp_path, [repo(1, "o/active", 151)], name="b.parquet")
    repos = active_repos_from_sweep(tmp_path, min_stars=100, active_days=90, now=NOW)
    assert [r["name"] for r in repos] == ["o/big", "o/active"]


def test_old_sweep_files_without_the_opportunity_column_still_load(tmp_path):
    old = pa.schema([f for f in SCHEMA if f.name != "opportunity_issues"])
    row = repo(1, "o/r", 200)
    del row["opportunity_issues"]
    write_basics(tmp_path, [row], schema=old)
    assert active_repos_from_sweep(tmp_path, 100, 90, NOW)[0]["opportunity_issues"] is None


def test_priority_is_beginner_issues_per_expected_point():
    giant = {"stars": 190000, "has_issues": True, "open_issues": 18000, "opportunity_issues": 300}
    mid = {"stars": 3000, "has_issues": True, "open_issues": 40, "opportunity_issues": 5}
    assert expected_cost(giant) == 15 and priority(giant) == 1333
    assert expected_cost(mid) == 6 and priority(mid) == 833
    assert priority({"stars": 200, "has_issues": True, "open_issues": 3, "opportunity_issues": 1}) == 250
    assert priority({"stars": 150, "has_issues": True, "open_issues": 3}) == 125
    assert priority({"stars": 150, "has_issues": False, "open_issues": 0}) == 25


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
