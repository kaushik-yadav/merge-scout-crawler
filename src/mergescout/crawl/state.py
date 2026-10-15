import sqlite3
import threading
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mergescout.labels import has_opportunity

# Bump when full crawls start collecting new fields: repos fully crawled by an older version are re-crawled in full.
CRAWL_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS repos (
  repo_id INTEGER PRIMARY KEY, name TEXT NOT NULL, stars INTEGER, pushed_at TEXT, priority INTEGER DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'queued', attempts INTEGER DEFAULT 0, run_id TEXT, claimed_at TEXT,
  last_done_at TEXT, next_due TEXT, last_points INTEGER, last_prs INTEGER, last_issues INTEGER, error TEXT,
  last_full_at TEXT, crawl_version INTEGER DEFAULT 1);
CREATE INDEX IF NOT EXISTS repos_claim ON repos(status, priority DESC, stars DESC);
CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, kind TEXT, started_at TEXT, finished_at TEXT,
  points INTEGER, repos INTEGER, reason TEXT);
CREATE TABLE IF NOT EXISTS issue_state (repo_id INTEGER, number INTEGER, observed_at TEXT, state TEXT,
  assignees INTEGER, linked_open_prs INTEGER, linked_merged_prs INTEGER, comments INTEGER, labels TEXT,
  updated_at TEXT, PRIMARY KEY (repo_id, number));
"""


class CrawlState:
    def __init__(self, path: Path, now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.now = now
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.executescript(SCHEMA)
            # State files saved before refreshes became incremental: their crawls were all full ones.
            columns = {r[1] for r in self.db.execute("PRAGMA table_info(repos)")}
            if "last_full_at" not in columns:
                self.db.execute("ALTER TABLE repos ADD COLUMN last_full_at TEXT")
                self.db.execute("UPDATE repos SET last_full_at = last_done_at WHERE last_done_at IS NOT NULL")
            if "crawl_version" not in columns:
                self.db.execute("ALTER TABLE repos ADD COLUMN crawl_version INTEGER DEFAULT 1")

    def _ts(self) -> str:
        return self.now().isoformat(timespec="seconds")

    def upsert_repos(self, rows: list[dict]) -> int:
        # New repos are queued; known repos get fresher metadata and the latest seed's priority.
        added = 0
        with self.lock, self.db:
            for r in rows:
                known = self.db.execute("SELECT 1 FROM repos WHERE repo_id = ?", (r["repo_id"],)).fetchone()
                if known:
                    self.db.execute("UPDATE repos SET name = ?, stars = ?, pushed_at = ?, priority = ? "
                                    "WHERE repo_id = ?",
                                    (r["name"], r.get("stars"), r.get("pushed_at"), r.get("priority", 0), r["repo_id"]))
                else:
                    self.db.execute("INSERT INTO repos (repo_id, name, stars, pushed_at, priority) VALUES (?, ?, ?, ?, ?)",
                                    (r["repo_id"], r["name"], r.get("stars"), r.get("pushed_at"), r.get("priority", 0)))
                    added += 1
        return added

    def claim(self, n: int, run_id: str) -> list[dict]:
        now = self._ts()
        with self.lock, self.db:
            rows = self.db.execute(
                "SELECT repo_id, name, stars, priority, last_done_at, last_full_at, crawl_version FROM repos "
                "WHERE status = 'queued' OR (status = 'done' AND ((next_due IS NOT NULL AND next_due <= ?) "
                "OR crawl_version < ?)) ORDER BY priority DESC, stars DESC LIMIT ?",
                (now, CRAWL_VERSION, n)).fetchall()
            for r in rows:
                self.db.execute("UPDATE repos SET status = 'running', run_id = ?, claimed_at = ?, "
                                "attempts = attempts + 1 WHERE repo_id = ?", (run_id, now, r["repo_id"]))
        return [dict(r) for r in rows]

    def done(self, repo_id: int, run_id: str, points: int, prs: int, issues: int, refresh_days: int = 7,
             full: bool = True) -> None:
        due = (self.now() + timedelta(days=refresh_days)).isoformat(timespec="seconds")
        now = self._ts()
        with self.lock, self.db:
            self.db.execute("UPDATE repos SET status = 'done', last_done_at = ?, next_due = ?, last_points = ?, "
                            "last_prs = ?, last_issues = ?, error = NULL, attempts = 0, "
                            "last_full_at = CASE WHEN ? THEN ? ELSE last_full_at END, "
                            "crawl_version = CASE WHEN ? THEN ? ELSE crawl_version END WHERE repo_id = ?",
                            (now, due, points, prs, issues, full, now, full, CRAWL_VERSION, repo_id))

    def fail(self, repo_id: int, run_id: str, error: str) -> None:
        with self.lock, self.db:
            self.db.execute("UPDATE repos SET status = 'failed', error = ? WHERE repo_id = ?", (error[:300], repo_id))

    def gone(self, repo_id: int, run_id: str, reason: str) -> None:
        with self.lock, self.db:
            self.db.execute("UPDATE repos SET status = 'gone', error = ? WHERE repo_id = ?", (reason[:300], repo_id))

    def release(self, repo_id: int) -> None:
        # Back to the queue without counting an attempt (the run stopped, the repo did not fail).
        with self.lock, self.db:
            self.db.execute("UPDATE repos SET status = 'queued', attempts = MAX(attempts - 1, 0) WHERE repo_id = ?",
                            (repo_id,))

    def requeue_stale(self, older_than_minutes: int = 120) -> int:
        cutoff = (self.now() - timedelta(minutes=older_than_minutes)).isoformat(timespec="seconds")
        with self.lock, self.db:
            cur = self.db.execute("UPDATE repos SET status = 'queued' WHERE status = 'running' AND claimed_at < ?",
                                  (cutoff,))
        return cur.rowcount

    def requeue_failed(self, max_attempts: int = 3) -> int:
        with self.lock, self.db:
            cur = self.db.execute("UPDATE repos SET status = 'queued' WHERE status = 'failed' AND attempts < ?",
                                  (max_attempts,))
        return cur.rowcount

    def start_run(self, run_id: str, kind: str) -> None:
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO runs (run_id, kind, started_at) VALUES (?, ?, ?)",
                            (run_id, kind, self._ts()))

    def finish_run(self, run_id: str, points: int, repos: int, reason: str) -> None:
        with self.lock, self.db:
            self.db.execute("UPDATE runs SET finished_at = ?, points = ?, repos = ?, reason = ? WHERE run_id = ?",
                            (self._ts(), points, repos, reason, run_id))

    def due_count(self) -> int:
        # Same rule as claim(): queued, due for a refresh, or crawled by an older crawler.
        with self.lock:
            return self.db.execute(
                "SELECT COUNT(*) FROM repos WHERE status = 'queued' OR (status = 'done' AND "
                "((next_due IS NOT NULL AND next_due <= ?) OR crawl_version < ?))",
                (self._ts(), CRAWL_VERSION)).fetchone()[0]

    def counts(self) -> dict:
        with self.lock:
            return dict(self.db.execute("SELECT status, COUNT(*) FROM repos GROUP BY status").fetchall())

    def log_issue_state(self, rows: list[dict], observed_at: str) -> None:
        # Only the latest state per tracked issue lives here; the history is in the Parquet batches.
        with self.lock, self.db:
            self.db.executemany(
                "INSERT OR REPLACE INTO issue_state (repo_id, number, observed_at, state, assignees, linked_open_prs, "
                "linked_merged_prs, comments, labels, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(r["repo_id"], r["number"], observed_at, r["state"], r["assignees"], r["linked_open_prs"],
                  r["linked_merged_prs"], r["comments"], r["labels"], r.get("updated_at")) for r in rows])

    def latest_issue_state(self, repo_id: int) -> list[dict]:
        with self.lock:
            rows = self.db.execute("SELECT * FROM issue_state WHERE repo_id = ? ORDER BY number", (repo_id,)).fetchall()
        return [dict(r) for r in rows]

    def open_issue_numbers(self, repo_id: int) -> list[int]:
        with self.lock:
            rows = self.db.execute("SELECT number FROM issue_state WHERE repo_id = ? AND state = 'OPEN' ORDER BY number",
                                   (repo_id,)).fetchall()
        return [r[0] for r in rows]

    def has_hot_tracked(self, repo_id: int) -> bool:
        # A refresh only sees changed issues, so hotness also comes from the beginner issues already tracked.
        with self.lock:
            rows = self.db.execute("SELECT labels FROM issue_state WHERE repo_id = ? AND state = 'OPEN'",
                                   (repo_id,)).fetchall()
        return any(has_opportunity((r[0] or "").split("|")) for r in rows)

    def forget_issues(self, repo_id: int, numbers: list[int]) -> None:
        with self.lock, self.db:
            self.db.executemany("DELETE FROM issue_state WHERE repo_id = ? AND number = ?",
                                [(repo_id, n) for n in numbers])

    def close(self) -> None:
        self.db.close()
