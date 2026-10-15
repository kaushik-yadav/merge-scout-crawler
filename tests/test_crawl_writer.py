from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from mergescout.crawl.writer import SCHEMAS, BatchWriter

T = datetime(2026, 10, 1, tzinfo=timezone.utc)


def pr(number, merged_at=None):
    return {"repo_id": 1, "number": number, "created_at": T, "merged_at": merged_at, "closing_issue_numbers": [1]}


def test_flush_writes_one_file_per_table_with_fixed_schema(tmp_path):
    w = BatchWriter(tmp_path, "r1")
    w.add("prs", [pr(2)])
    w.add("prs", [pr(3)])
    w.add("issues", [])
    files = w.flush()
    assert set(files) == {"prs"} and files["prs"].name == "run=r1-000.parquet"
    t = pq.read_table(files["prs"])
    assert t.num_rows == 2 and t.schema.field("merged_at").type == pa.timestamp("us", tz="UTC")
    assert t.column("run_id").to_pylist() == ["r1", "r1"] and t.column("author_login").null_count == 2
    assert not list(tmp_path.rglob("*.part"))


def test_each_flush_is_a_new_file_and_empty_flush_writes_nothing(tmp_path):
    w = BatchWriter(tmp_path, "r1")
    w.add("prs", [pr(1)])
    w.flush()
    assert w.flush() == {}
    w.add("prs", [pr(2, merged_at=T)])
    assert w.flush()["prs"].name == "run=r1-001.parquet"
    assert len(list((tmp_path / "crawl" / "prs").glob("*.parquet"))) == 2


def test_every_table_has_a_schema():
    assert {"repo_meta", "prs", "issues", "comments", "issue_events", "closed_issues"} <= set(SCHEMAS)


def test_rows_are_stamped_with_fetch_time(tmp_path):
    w = BatchWriter(tmp_path, "r1", now=lambda: T)
    w.add("closed_issues", [{"repo_id": 1, "number": 2, "labels": ["good first issue"]}])
    t = pq.read_table(w.flush()["closed_issues"])
    assert t.column("fetched_at").to_pylist() == [T] and "fix_author" in t.column_names
