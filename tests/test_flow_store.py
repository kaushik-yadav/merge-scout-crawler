# Backups end to end: data and state go to releases and come back; a bad state never replaces a good one.
import io
import tarfile
from datetime import datetime, timezone

import httpx
import pytest

from mergescout.crawl.state import CrawlState
from mergescout.github.releases import Releases
from mergescout.setup_check import check_store
from mergescout.store import __main__ as cli
from mergescout.store.sync import pull, push, safe_extract


QUIET = {"log": lambda *a: None}
NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)
REL = {"id": 7, "tag_name": "crawl-state"}


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


def test_failed_upload_keeps_local_batches_for_the_next_run(tmp_path):
    make_data(tmp_path)
    with pytest.raises(RuntimeError):
        push(tmp_path, MemReleases(fail=True), NOW, **QUIET)
    assert len(list((tmp_path / "crawl").rglob("*.parquet"))) == 6


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


class ListingReleases(MemReleases):
    def get(self, tag):
        return {"id": tag} if any(t == tag for t, _ in self.files) else None

    def list_releases(self):
        return [{"id": tag, "tag_name": tag} for tag in sorted({t for t, _ in self.files})]

    def assets(self, release):
        return [{"id": (release["id"], n), "name": n} for t, n in self.files if t == release["id"]]

    def download_asset(self, asset, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.files[asset["id"]])


def test_fetch_bronze_reads_every_crawl_release_once(tmp_path):
    from mergescout.store.sync import fetch_bronze
    src = tmp_path / "src"
    make_data(src)
    rel = ListingReleases()
    push(src, rel, NOW, **QUIET)
    rel.files[("crawl-2026-10", "crawl-1-1.tar")] = rel.files[("crawl-2026-W42", "crawl-111-1.tar")]
    dst = tmp_path / "dst"
    assert fetch_bronze(rel, dst, **QUIET) == 3
    assert not (dst / "bronze" / "done" / "crawl.sqlite.gz").exists()
    assert fetch_bronze(rel, dst, **QUIET) == 0


def test_push_then_pull_through_the_command_line(monkeypatch, tmp_path):
    monkeypatch.setenv("MS_RELEASE_TOKEN", "t")
    monkeypatch.setenv("GITHUB_REPOSITORY", "me/repo")
    rel = MemReleases()
    made = []
    monkeypatch.setattr(cli, "Releases", lambda token, repo: made.append((token, repo)) or rel)
    src, dst = tmp_path / "src", tmp_path / "dst"
    make_data(src)
    monkeypatch.setenv("MS_DATA_DIR", str(src))
    assert cli.main(["push", "--with-discover"]) == 0
    monkeypatch.setenv("MS_DATA_DIR", str(dst))
    assert cli.main(["pull"]) == 0
    assert (dst / "crawl.sqlite").exists() and made[0] == ("t", "me/repo")


def fake(routes):
    # routes: list of (method, url-substring, response or callable); every request is recorded.
    seen = []

    def handler(req: httpx.Request):
        seen.append(req)
        for method, part, resp in routes:
            if req.method == method and part in str(req.url):
                return resp(req) if callable(resp) else resp
        return httpx.Response(500, text=f"unexpected {req.method} {req.url}")
    return Releases("tok", "me/repo", client=httpx.Client(transport=httpx.MockTransport(handler),
                                                         follow_redirects=True)), seen


def test_upload_replaces_an_asset_with_the_same_name(tmp_path):
    f = tmp_path / "crawl.sqlite.gz"
    f.write_bytes(b"x" * 10)
    rel, seen = fake([("GET", "/releases/tags/crawl-state", httpx.Response(200, json=REL)),
                      ("GET", "/releases/7/assets", httpx.Response(200, json=[{"id": 3, "name": "crawl.sqlite.gz"}])),
                      ("DELETE", "/releases/assets/3", httpx.Response(204)),
                      ("POST", "uploads.github.com", httpx.Response(201, json={"id": 4, "name": "crawl.sqlite.gz"}))])
    assert rel.upload("crawl-state", f)["id"] == 4
    up = seen[-1]
    assert up.url.host == "uploads.github.com" and up.url.params["name"] == "crawl.sqlite.gz"
    assert up.headers["content-type"] == "application/octet-stream" and up.headers["content-length"] == "10"
    assert [r.method for r in seen].count("DELETE") == 1


def test_list_releases_pages_through_everything():
    page1 = [{"id": i, "tag_name": f"crawl-{i}"} for i in range(100)]
    rel, seen = fake([("GET", "&page=1", httpx.Response(200, json=page1)),
                      ("GET", "&page=2", httpx.Response(200, json=[{"id": 100, "tag_name": "crawl-state"}]))])
    assert len(rel.list_releases()) == 101 and len(seen) == 2


class Store:
    def __init__(self, release=True, can_write=True):
        self.release, self.can_write, self.deleted = release, can_write, []

    def get(self, tag):
        return {"id": 1, "tag_name": tag} if self.release else None

    def upload(self, tag, path, name=None):
        if not self.can_write:
            raise RuntimeError("uploading setup-check.txt failed: HTTP 403")
        return {"id": 9, "name": name or path.name}

    def delete_asset(self, asset):
        self.deleted.append(asset["id"])


def test_store_check_reads_then_writes_and_cleans_up():
    store = Store()
    ok, detail = check_store(store)
    assert ok and store.deleted == [9] and "write" in detail
    assert check_store(Store(release=False))[0] is False
    ok, detail = check_store(Store(can_write=False))
    assert not ok and "403" in detail


def test_bronze_download_works_with_the_real_release_client(tmp_path):
    # The signals build reads every crawl data release through the real client; only crawl data bundles count.
    from mergescout.store.sync import fetch_bronze
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        info = tarfile.TarInfo("crawl/prs/run=1-000.parquet")
        info.size = 5
        tar.addfile(info, io.BytesIO(b"bytes"))
    listed = [{"id": 1, "tag_name": "crawl-2026-W42"}, {"id": 2, "tag_name": "crawl-state"},
              {"id": 3, "tag_name": "signals-latest"}]
    rel, seen = fake([("GET", "/releases/1/assets", httpx.Response(200, json=[{"id": 11, "name": "crawl-1-1.tar"}])),
                      ("GET", "/releases/assets/11", httpx.Response(200, content=buf.getvalue())),
                      ("GET", "/repos/me/repo/releases", httpx.Response(200, json=listed))])
    assert fetch_bronze(rel, tmp_path, log=lambda *a: None) == 1
    assert (tmp_path / "bronze" / "crawl" / "prs" / "run=1-000.parquet").read_bytes() == b"bytes"
    assert fetch_bronze(rel, tmp_path, log=lambda *a: None) == 0
    assert not any("/releases/2/" in str(r.url) or "/releases/3/" in str(r.url) for r in seen)
