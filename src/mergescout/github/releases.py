import os
from pathlib import Path

import httpx

API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"


class ReleaseError(RuntimeError):
    pass


class Releases:
    # Release assets in our own repo are the durable store for crawl data and state backups.
    def __init__(self, token: str, repo: str, client: httpx.Client | None = None):
        self.repo = repo
        self.client = client or httpx.Client(timeout=httpx.Timeout(300.0, connect=15.0), follow_redirects=True)
        self.headers = {"Authorization": f"Bearer {token.strip()}", "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "mergescout"}

    @staticmethod
    def _check(resp: httpx.Response, what: str) -> httpx.Response:
        if resp.status_code >= 400:
            raise ReleaseError(f"{what} failed: HTTP {resp.status_code} {resp.text[:200]}")
        return resp

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
        # GitHub refuses a second asset with the same name, so an older copy is removed first.
        for asset in self.assets(release):
            if asset["name"] == name:
                self._check(self.client.delete(f"{API}/repos/{self.repo}/releases/assets/{asset['id']}",
                                               headers=self.headers), f"replacing {name}")
        headers = {**self.headers, "Content-Type": "application/octet-stream",
                   "Content-Length": str(path.stat().st_size)}
        with path.open("rb") as f:
            resp = self.client.post(f"{UPLOADS}/repos/{self.repo}/releases/{release['id']}/assets",
                                    params={"name": name}, headers=headers, content=f)
        return self._check(resp, f"uploading {name}").json()

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
