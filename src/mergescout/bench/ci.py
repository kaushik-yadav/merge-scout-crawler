import hashlib
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import httpx

API = "https://api.github.com"
FAIL = {"failure", "timed_out"}
OK = {"success", "neutral"}
MIN_RUNS = 5
PAGE = 50
MIN_GROUP = 20


class OutOfBudget(RuntimeError):
    pass


def _t(text: str | None) -> datetime | None:
    return datetime.fromisoformat(text.replace("Z", "+00:00")) if text else None


def _pct(values: list[float], q: float) -> float:
    # Nearest-rank percentile: always a value that was really seen.
    s = sorted(values)
    return s[min(len(s) - 1, max(0, math.ceil(q * len(s)) - 1))]


def _median(values: list[float]) -> float:
    s, n = sorted(values), len(values)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def repo_stats(runs: list[dict], since: datetime | None = None, page_full: bool = False) -> dict | None:
    """CI of one repo per push: how long until all its checks finished, and how they ended."""
    # GitHub-managed workflows (Dependabot, CodeQL default setup) are not the repo's own CI.
    own = [r for r in runs if not (r.get("path") or "").startswith("dynamic/")
           and r.get("created_at") and r.get("updated_at") and (since is None or (_t(r.get("created_at")) or since) >= since)]
    pushes = {}
    for r in own:
        pushes.setdefault(r.get("head_sha") or r.get("id"), []).append(r)
    if page_full and pushes:
        # The page ends somewhere inside its oldest push, which may have more runs on the next page.
        pushes.pop(min(pushes, key=lambda k: min(r.get("created_at") or "" for r in pushes[k])))
    done = []
    for group in pushes.values():
        if any(r.get("status") != "completed" for r in group):
            continue
        # Runs cancelled by a newer push (or skipped by a condition) did not give an answer.
        group = [r for r in group if r.get("conclusion") not in ("cancelled", "skipped")]
        if group:
            done.append(group)
    if len(done) < MIN_RUNS:
        return None
    rerun = lambda g: any((r.get("run_attempt") or 1) > 1 for r in g)
    durations = []
    for g in done:
        start, end = min(_t(r.get("created_at")) for r in g), max(_t(r.get("updated_at")) for r in g)
        # The wait a developer sees: queue plus run, up to the last check; re-runs would add human delay.
        if not rerun(g) and start and end and end >= start:
            durations.append((end - start).total_seconds())
    if not durations:
        return None
    n = len(done)
    return {"pushes": n, "median_s": round(_median(durations)), "p90_s": round(_pct(durations, 0.9)),
            "fail_share": round(sum(any(r.get("conclusion") in FAIL for r in g) for g in done) / n, 4),
            "first_try_share": round(sum(not rerun(g) and all(r.get("conclusion") in OK for r in g)
                                         for g in done) / n, 4),
            "rerun_share": round(sum(rerun(g) for g in done) / n, 4),
            "workflows": len({r.get("path") for g in done for r in g})}


def todays_share(names: list[str], now: datetime, slices: int = 28) -> list[str]:
    # A stable hash splits the repos into 28 parts, one per day, so every repo is read once every four weeks.
    day = now.toordinal() % slices
    return [n for n in names if int(hashlib.sha1(n.lower().encode()).hexdigest(), 16) % slices == day]


class TokenPool:
    """Several identities' REST limits (separate from the crawl's GraphQL points); stops before all run low."""

    def __init__(self, tokens: list[str], client: httpx.Client | None = None, reserve: int = 300,
                 sleep=time.sleep, clock=time.time, max_wait: float = 0):
        seen = []
        for t in tokens:
            if t and t.strip() and t.strip() not in seen:
                seen.append(t.strip())
        self.tokens = {t: 5000 for t in seen}
        self.resets = {t: 0.0 for t in seen}
        self.client = client or httpx.Client(timeout=httpx.Timeout(60.0, connect=15.0), follow_redirects=True)
        self.reserve, self.sleep, self.clock, self.lock, self.requests = reserve, sleep, clock, threading.Lock(), 0
        self.max_wait, self.waited = max_wait, 0.0

    def _pick(self) -> str:
        with self.lock:
            live = {t: left for t, left in self.tokens.items() if left > self.reserve}
            if not self.tokens:
                raise OutOfBudget("no token can read other repos' Actions runs")
            if not live:
                # Everyone is low: wait for the first hourly reset if allowed (other workers wait on the lock).
                first = min(self.resets, key=self.resets.get)
                wait = max(0.0, (self.resets[first] or self.clock() + 3600) - self.clock()) + 5
                if self.waited + wait > self.max_wait:
                    raise OutOfBudget("every token is near its hourly REST limit")
                self.waited += wait
                self.sleep(wait)
                self.tokens[first] = 5000
                live = {first: 5000}
            self.requests += 1
            return max(live, key=live.get)

    def get(self, path: str, params: dict) -> httpx.Response | None:
        for attempt in range(3):
            token = self._pick()
            try:
                resp = self.client.get(f"{API}{path}", params=params,
                                       headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                                                "User-Agent": "mergescout-bench"})
            except httpx.TransportError:
                self.sleep(2 * (attempt + 1))
                continue
            left, reset = resp.headers.get("x-ratelimit-remaining"), resp.headers.get("x-ratelimit-reset")
            with self.lock:
                if left is not None:
                    self.tokens[token] = int(left)
                if reset is not None:
                    self.resets[token] = float(reset)
            if resp.status_code == 403 and "not accessible by integration" in resp.text.lower():
                # App and Actions tokens may be limited to their own repos: drop the token, never the repo.
                with self.lock:
                    self.tokens.pop(token, None)
                    self.resets.pop(token, None)
                continue
            if resp.status_code in (403, 429) and left == "0":
                continue
            if resp.status_code in (403, 429) and ("retry-after" in resp.headers or "secondary" in resp.text.lower()):
                self.sleep(min(float(resp.headers.get("retry-after", 60)), 300))
                continue
            if resp.status_code >= 500:
                self.sleep(3 * (attempt + 1))
                continue
            return resp
        return None


def _one(pool: TokenPool, repo: dict, since: datetime) -> dict | None:
    # A date filter makes GitHub's query 3x slower and costlier; the newest runs are filtered here instead.
    resp = pool.get(f"/repos/{repo['name']}/actions/runs",
                    {"event": "push", "per_page": PAGE, "exclude_pull_requests": "true"})
    # 404: gone or renamed; 403: Actions turned off. Neither is a CI to compare.
    if resp is None or resp.status_code >= 400:
        return None
    runs = resp.json().get("workflow_runs") or []
    stats = repo_stats(runs, since, page_full=len(runs) >= PAGE)
    return {"repo": repo["name"], "language": repo.get("language"), "stars": repo.get("stars"), **stats} if stats else None


def fetch_all(pool: TokenPool, repos: list[dict], now: datetime, days: int = 30, workers: int = 10, log=None) -> dict:
    since = now - timedelta(days=days)
    records, complete, day = [], True, now.strftime("%Y-%m-%d")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_one, pool, r, since) for r in repos]
        for i, fut in enumerate(futures):
            try:
                rec = fut.result()
            except OutOfBudget:
                complete = False
                continue
            if rec:
                records.append({**rec, "day": day})
            if log and (i + 1) % 1000 == 0:
                log(f"{i + 1}/{len(repos)} repos read, {len(records)} with enough CI runs")
    return {"records": records, "complete": complete, "requests": pool.requests}


def tier(stars: int | None) -> str:
    stars = stars or 0
    return "10k+" if stars >= 10_000 else "1k-10k" if stars >= 1000 else "100-1k"


def _summary(values: list[float]) -> dict:
    return {f"p{int(q * 100)}": _pct(values, q) for q in (0.1, 0.25, 0.5, 0.75, 0.9)}


def aggregate(records: list[dict], now: datetime, top_languages: int = 25) -> dict:
    """Benchmarks by language and size; a group is published only with at least 20 repos."""
    latest = {}
    for r in sorted(records, key=lambda r: r.get("day") or ""):
        latest[r["repo"].lower()] = r
    recs = list(latest.values())
    counts = {}
    for r in recs:
        counts[r.get("language") or "Other"] = counts.get(r.get("language") or "Other", 0) + 1
    langs = {name for name, _ in sorted(counts.items(), key=lambda kv: -kv[1])[:top_languages]}
    groups = {}
    for r in recs:
        lang = r.get("language") if r.get("language") in langs else "Other"
        for key in (f"{lang}|{tier(r.get('stars'))}", f"{lang}|all", f"all|{tier(r.get('stars'))}", "all|all"):
            groups.setdefault(key, []).append(r)
    out = {}
    for key, items in groups.items():
        if len(items) < MIN_GROUP:
            continue
        out[key] = {"repos": len(items), "median_s": _summary([r["median_s"] for r in items]),
                    "p90_s": _summary([r["p90_s"] for r in items]),
                    "first_try_share": _summary([r["first_try_share"] for r in items]),
                    "fail_share": _summary([r["fail_share"] for r in items]),
                    "rerun_share": _summary([r["rerun_share"] for r in items])}
    return {"version": 1, "built_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "repos": len(recs),
            "definition": "per push of the last 30 days (newest 50 push runs): time from push until all its checks finished, and how they ended; GitHub-managed workflows excluded",
            "groups": out}
