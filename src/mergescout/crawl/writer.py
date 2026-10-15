import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TS = pa.timestamp("us", tz="UTC")

# Fixed schemas so every batch file of a table can be read together later.
SCHEMAS = {
    "repo_meta": pa.schema([
        ("repo_id", pa.int64()), ("name", pa.string()), ("stars", pa.int32()), ("forks", pa.int32()),
        ("pushed_at", TS), ("language", pa.string()), ("archived", pa.bool_()), ("has_issues", pa.bool_()),
        ("has_discussions", pa.bool_()), ("has_projects", pa.bool_()), ("license", pa.string()),
        ("default_branch", pa.string()), ("contributing_sha", pa.string()), ("ai_policy_sha", pa.string()),
        ("pr_template_sha", pa.string()), ("readme_excerpt", pa.string()), ("last_release_at", TS),
        ("releases_12m", pa.int32()), ("open_prs", pa.int32()), ("open_issues", pa.int32()),
        ("contributing_excerpt", pa.string()), ("label_names", pa.list_(pa.string())),
        ("owner_logins", pa.list_(pa.string())),
        ("run_id", pa.string()), ("fetched_at", TS)]),
    "prs": pa.schema([
        ("repo_id", pa.int64()), ("number", pa.int32()), ("created_at", TS), ("merged_at", TS), ("closed_at", TS),
        ("state", pa.string()), ("author_login", pa.string()), ("author_type", pa.string()),
        ("author_association", pa.string()), ("merged_by", pa.string()), ("head_ref", pa.string()),
        ("additions", pa.int32()), ("deletions", pa.int32()), ("changed_files", pa.int32()),
        ("first_review_at", TS), ("first_review_by", pa.string()), ("first_review_association", pa.string()),
        ("first_human_review_at", TS), ("first_human_review_by", pa.string()),
        ("first_human_review_association", pa.string()),
        ("closing_issue_numbers", pa.list_(pa.int32())), ("ai_tag", pa.string()), ("run_id", pa.string()), ("fetched_at", TS)]),
    "issues": pa.schema([
        ("repo_id", pa.int64()), ("number", pa.int32()), ("title", pa.string()), ("body_excerpt", pa.string()),
        ("state", pa.string()), ("state_reason", pa.string()), ("created_at", TS), ("updated_at", TS),
        ("closed_at", TS), ("author_login", pa.string()), ("author_association", pa.string()),
        ("labels", pa.list_(pa.string())),
        ("assignees", pa.int32()), ("linked_open_prs", pa.int32()), ("linked_merged_prs", pa.int32()),
        ("comments_total", pa.int32()), ("linked_closed_prs", pa.int32()), ("fix_pr", pa.int32()),
        ("fix_pr_state", pa.string()), ("fix_author", pa.string()), ("fix_association", pa.string()), ("run_id", pa.string()), ("fetched_at", TS)]),
    "comments": pa.schema([
        ("repo_id", pa.int64()), ("number", pa.int32()), ("comment_id", pa.int64()), ("created_at", TS),
        ("author_login", pa.string()), ("author_type", pa.string()), ("author_association", pa.string()),
        ("body_excerpt", pa.string()), ("run_id", pa.string()), ("fetched_at", TS)]),
    "issue_events": pa.schema([
        ("repo_id", pa.int64()), ("number", pa.int32()), ("event", pa.string()), ("detail", pa.string()),
        ("observed_at", pa.string()), ("run_id", pa.string()), ("fetched_at", TS)]),
    "closed_issues": pa.schema([
        ("repo_id", pa.int64()), ("number", pa.int32()), ("created_at", TS), ("closed_at", TS),
        ("state_reason", pa.string()), ("author_login", pa.string()), ("author_association", pa.string()),
        ("labels", pa.list_(pa.string())), ("fix_pr", pa.int32()), ("fix_pr_state", pa.string()),
        ("fix_author", pa.string()), ("fix_association", pa.string()), ("run_id", pa.string()),
        ("fetched_at", TS)]),
}


class BatchWriter:
    def __init__(self, data_dir: Path, run_id: str, now=lambda: datetime.now(timezone.utc)):
        self.base = data_dir / "crawl"
        self.run_id = run_id
        self.now = now
        self.rows: dict[str, list[dict]] = {}
        self.seq = 0

    def add(self, table: str, rows: list[dict]) -> None:
        if table not in SCHEMAS:
            raise KeyError(f"unknown table {table}")
        if rows:
            # Every row says when it was fetched, so later layers can keep the newest copy.
            stamp = self.now()
            self.rows.setdefault(table, []).extend({**r, "run_id": self.run_id, "fetched_at": stamp} for r in rows)

    def flush(self) -> dict[str, Path]:
        # Each flush is a new file per table; a file is complete or absent (written as .part, then renamed).
        out = {}
        for table, rows in self.rows.items():
            schema = SCHEMAS[table]
            path = self.base / table / f"run={self.run_id}-{self.seq:03d}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            part = path.with_name(path.name + ".part")
            cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
            pq.write_table(pa.table(cols, schema=schema), part, compression="zstd")
            os.replace(part, path)
            out[table] = path
        if out:
            self.seq += 1
        self.rows = {}
        return out
