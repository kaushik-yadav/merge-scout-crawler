import os
import time
from pathlib import Path

import httpx

API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"


class ReleaseError(RuntimeError):
    pass


class Releases:
    # Release assets in our own repo are the durable store for crawl data and state backups.
    def __init__(self, token: str | None, repo: str, client: httpx.Client | None = None, sleep=time.sleep, clock=time.time,
                 max_wait: float = 600):  # the crawl job has 70 min: 52 crawling, a few to push
        self.repo, self.sleep, self.clock, self.max_wait = repo, sleep, clock, max_wait
        self.client = client or httpx.Client(timeout=httpx.Timeout(300.0, connect=15.0), follow_redirects=True)
        self.headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28",
                        "User-Agent": "mergescout"}
        # Public data downloads need no token; private repos and uploads do.
        if token and token.strip():
            self.headers["Authorization"] = f"Bearer {token.strip()}"

    @staticmethod
    def _check(resp: httpx.Response, what: str) -> httpx.Response:
        if resp.status_code >= 400:
            raise ReleaseError(f"{what} failed: HTTP {resp.status_code} {resp.text[:200]}")
        return resp

    def _limited_wait(self, resp: httpx.Response) -> float | None:
        # Seconds until the account's hourly limit resets, when that is why the request failed and the wait is short.
        if resp.status_code not in (403, 429) or resp.headers.get("x-ratelimit-remaining") != "0":
            return None
        wait = float(resp.headers.get("x-ratelimit-reset") or 0) - self.clock() + 5
        return wait if 0 < wait <= self.max_wait else None

    def get(self, tag: str) -> dict | None:
        resp = self.client.get(f"{API}/repos/{self.repo}/releases/tags/{tag}", headers=self.headers)
        if resp.status_code == 404:
            return None
        return self._check(resp, f"reading release {tag}").json()

    def ensure(self, tag: str) -> dict:
        found = self.get(tag)
        if found:
            return found
        body = {"tag_name": tag, "name": tag, "body": "Automated MergeScout data store.", "prerelease": True,
                "make_latest": "false"}
        resp = self.client.post(f"{API}/repos/{self.repo}/releases", headers=self.headers, json=body)
        return self._check(resp, f"creating release {tag}").json()

    def assets(self, release: dict) -> list[dict]:
        out, page = [], 1
        while True:
            resp = self.client.get(f"{API}/repos/{self.repo}/releases/{release['id']}/assets", headers=self.headers,
                                   params={"per_page": 100, "page": page})
            batch = self._check(resp, "listing release assets").json()
            out.extend(batch)
            if len(batch) < 100:
                return out
            page += 1

    def upload(self, tag: str, path: Path, name: str | None = None) -> dict:
        release = self.ensure(tag)
        name = name or path.name
        existing = {a["name"]: a for a in self.assets(release)}
        # A file being replaced is uploaded under a temporary name first, so a failed upload keeps the old copy.
        target = f"{name}.new" if name in existing else name
        if target in existing:
            self.delete_asset(existing[target])
        headers = {**self.headers, "Content-Type": "application/octet-stream",
                   "Content-Length": str(path.stat().st_size)}
        for attempt in range(2):
            with path.open("rb") as f:
                resp = self.client.post(f"{UPLOADS}/repos/{self.repo}/releases/{release['id']}/assets",
                                        params={"name": target}, headers=headers, content=f)
            # The hour's work is in this file: wait for the reset once rather than lose it (real failure 2026-10-20).
            wait = self._limited_wait(resp) if attempt == 0 else None
            if wait is None:
                break
            self.sleep(wait)
        uploaded = self._check(resp, f"uploading {name}").json()
        if target == name:
            return uploaded
        self.delete_asset(existing[name])
        resp = self.client.patch(f"{API}/repos/{self.repo}/releases/assets/{uploaded['id']}", headers=self.headers,
                                 json={"name": name})
        return self._check(resp, f"renaming {target}").json()

    def list_releases(self) -> list[dict]:
        out, page = [], 1
        while True:
            resp = self.client.get(f"{API}/repos/{self.repo}/releases", headers=self.headers,
                                   params={"per_page": 100, "page": page})
            batch = self._check(resp, "listing releases").json()
            out.extend(batch)
            if len(batch) < 100:
                return out
            page += 1

    def delete_asset(self, asset: dict) -> None:
        self._check(self.client.delete(f"{API}/repos/{self.repo}/releases/assets/{asset['id']}", headers=self.headers),
                    f"deleting {asset.get('name', asset['id'])}")

    def delete_release(self, release: dict) -> None:
        self._check(self.client.delete(f"{API}/repos/{self.repo}/releases/{release['id']}", headers=self.headers),
                    f"deleting release {release['tag_name']}")
        # The tag stays behind otherwise; a missing tag is fine.
        resp = self.client.delete(f"{API}/repos/{self.repo}/git/refs/tags/{release['tag_name']}", headers=self.headers)
        if resp.status_code not in (204, 404, 422):
            self._check(resp, f"deleting tag {release['tag_name']}")

    def download(self, tag: str, name: str, dest: Path) -> bool:
        release = self.get(tag)
        asset = next((a for a in self.assets(release) if a["name"] == name), None) if release else None
        if asset is None:
            return False
        self.download_asset(asset, dest)
        return True

    def download_asset(self, asset: dict, dest: Path) -> None:
        name = asset.get("name", asset["id"])
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        headers = {**self.headers, "Accept": "application/octet-stream"}
        with self.client.stream("GET", f"{API}/repos/{self.repo}/releases/assets/{asset['id']}",
                                headers=headers) as resp:
            if resp.status_code >= 400:
                resp.read()
                self._check(resp, f"downloading {name}")
            with part.open("wb") as f:
                for chunk in resp.iter_bytes():
                    f.write(chunk)
        os.replace(part, dest)
