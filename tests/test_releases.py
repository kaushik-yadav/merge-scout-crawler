import json

import httpx
import pytest

from mergescout.github.releases import ReleaseError, Releases

REL = {"id": 7, "tag_name": "crawl-state"}


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


def test_ensure_creates_a_missing_release_as_a_prerelease():
    rel, seen = fake([("GET", "/releases/tags/crawl-state", httpx.Response(404)),
                      ("POST", "/repos/me/repo/releases", httpx.Response(201, json=REL))])
    assert rel.ensure("crawl-state")["id"] == 7
    body = json.loads(seen[-1].content)
    assert body["tag_name"] == "crawl-state" and body["prerelease"] is True and body["make_latest"] == "false"
    assert seen[-1].headers["authorization"] == "Bearer tok"


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


def test_download_writes_the_asset_and_reports_missing_ones(tmp_path):
    rel, _ = fake([("GET", "/releases/tags/crawl-state", httpx.Response(200, json=REL)),
                   ("GET", "/releases/7/assets", httpx.Response(200, json=[{"id": 3, "name": "a.bin"}])),
                   ("GET", "/releases/assets/3", httpx.Response(200, content=b"payload"))])
    assert rel.download("crawl-state", "a.bin", tmp_path / "out" / "a.bin") is True
    assert (tmp_path / "out" / "a.bin").read_bytes() == b"payload"
    assert rel.download("crawl-state", "missing.bin", tmp_path / "m.bin") is False
    gone, _ = fake([("GET", "/releases/tags/crawl-state", httpx.Response(404))])
    assert gone.download("crawl-state", "a.bin", tmp_path / "b.bin") is False


def test_errors_are_clear():
    rel, _ = fake([("GET", "/releases/tags/x", httpx.Response(403, text="Resource not accessible by integration"))])
    with pytest.raises(ReleaseError, match="HTTP 403"):
        rel.ensure("x")
