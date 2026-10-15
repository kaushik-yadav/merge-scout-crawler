import io
import tarfile
from datetime import datetime, timezone

import pytest

from mergescout.crawl.state import CrawlState
from mergescout.store.sync import pull, push, safe_extract

NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)
QUIET = {"log": lambda *a: None}


class MemReleases:
    def __init__(self, fail=False):
        self.files, self.fail = {}, fail

    def upload(self, tag, path, name=None):
        if self.fail:
            raise RuntimeError("network down")
        self.files[(tag, name or path.name)] = path.read_bytes()
        return {}

    def download(self, tag, name, dest):
        if (tag, name) not in self.files:
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.files[(tag, name)])
        return True


def make_data(d):
    for table in ("prs", "issues"):
        (d / "crawl" / table).mkdir(parents=True, exist_ok=True)
        (d / "crawl" / table / "run=111-1-000.parquet").write_bytes(b"a")
        (d / "crawl" / table / "run=111-1-001.parquet").write_bytes(b"b")
        (d / "crawl" / table / "run=222-1-000.parquet").write_bytes(b"c")
    CrawlState(d / "crawl.sqlite").upsert_repos([{"repo_id": 1, "name": "o/r", "stars": 100, "pushed_at": None,
                                                   "priority": 0}])


def test_push_uploads_one_bundle_per_run_then_clears_local_batches(tmp_path):
    make_data(tmp_path)
    rel = MemReleases()
    out = push(tmp_path, rel, NOW, **QUIET)
    assert out == {"runs": 2, "state": True, "discover": False}
    assert set(rel.files) == {("crawl-2026-W42", "crawl-111-1.tar"), ("crawl-2026-W42", "crawl-222-1.tar"),
                              ("crawl-state", "crawl.sqlite.gz")}
    names = tarfile.open(fileobj=io.BytesIO(rel.files[("crawl-2026-W42", "crawl-111-1.tar")])).getnames()
    assert sorted(names) == ["crawl/issues/run=111-1-000.parquet", "crawl/issues/run=111-1-001.parquet",
                             "crawl/prs/run=111-1-000.parquet", "crawl/prs/run=111-1-001.parquet"]
    assert not list((tmp_path / "crawl").rglob("*.parquet")) and not list((tmp_path / "outbox").glob("*"))


def test_failed_upload_keeps_local_batches_for_the_next_run(tmp_path):
    make_data(tmp_path)
    with pytest.raises(RuntimeError):
        push(tmp_path, MemReleases(fail=True), NOW, **QUIET)
    assert len(list((tmp_path / "crawl").rglob("*.parquet"))) == 6


def test_pull_restores_state_and_discover_into_an_empty_folder(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    make_data(src)
    (src / "discover" / "basics").mkdir(parents=True)
    (src / "discover" / "basics" / "a.parquet").write_bytes(b"basics")
    rel = MemReleases()
    assert push(src, rel, NOW, with_discover=True, **QUIET)["discover"] is True
    assert pull(dst, rel, **QUIET) == {"state": True, "discover": True}
    assert CrawlState(dst / "crawl.sqlite").counts() == {"queued": 1}
    assert (dst / "discover" / "basics" / "a.parquet").read_bytes() == b"basics"


def test_pull_keeps_local_state_when_present(tmp_path):
    rel = MemReleases()
    rel.files[("crawl-state", "crawl.sqlite.gz")] = b"not used"
    CrawlState(tmp_path / "crawl.sqlite")
    (tmp_path / "discover").mkdir()
    assert pull(tmp_path, rel, **QUIET) == {"state": False, "discover": False}


def test_safe_extract_refuses_paths_outside_the_folder(tmp_path):
    bad = tmp_path / "bad.tar"
    with tarfile.open(bad, "w") as tar:
        info = tarfile.TarInfo("../escape.txt")
        info.size = 1
        tar.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(ValueError):
        safe_extract(bad, tmp_path / "out")
    assert not (tmp_path / "escape.txt").exists()


def test_an_empty_state_is_never_backed_up(tmp_path):
    CrawlState(tmp_path / "crawl.sqlite")
    rel = MemReleases()
    out = push(tmp_path, rel, NOW, **QUIET)
    assert out["state"] is False and ("crawl-state", "crawl.sqlite.gz") not in rel.files
