import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from mergescout.timeutil import parse_time


class GraphQLError(RuntimeError):
    pass


class NotFound(GraphQLError):
    pass


class RateLimited(GraphQLError):
    pass


class TokenRejected(GraphQLError):
    # A token that stopped working (expired App token, revoked token): that identity steps aside.
    pass


class Gate:
    # Shared by every client on one token: when GitHub says "wait", all of them wait, not just the one told.
    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self.lock = threading.Lock()
        self.until = 0.0
        self.stats = {"requests": 0, "timeouts": 0, "rate_waits": 0, "waited_s": 0.0}

    def note(self, key: str, amount: float = 1) -> None:
        with self.lock:
            self.stats[key] += amount

    def summary(self) -> str:
        s = self.stats
        return (f"requests={s['requests']} timeouts={s['timeouts']} rate-limit waits={s['rate_waits']} "
                f"waited={s['waited_s']:.0f}s")

    def hold(self, seconds: float) -> None:
        with self.lock:
            self.until = max(self.until, self.clock() + seconds)

    def remaining(self) -> float:
        with self.lock:
            return max(0.0, self.until - self.clock())


def too_heavy(exc: Exception) -> bool:
    # GitHub's ways of saying a page was too big or too slow; callers retry with a smaller page.
    msg = str(exc).lower()
    return any(s in msg for s in ("resource limits", "timeout", "timed out", "kept failing (http 502",
                                  "something went wrong", "not json"))


@dataclass
class GraphQLResult:
    data: dict
    cost: int
    remaining: int
    reset_at: str


class GitHubGraphQL:
    URL = "https://api.github.com/graphql"
    RETRY_STATUSES = (500, 502, 503, 504)
    TIMEOUT_STATUSES = (502, 504)

    def __init__(self, token: str, *, client: httpx.Client | None = None, max_retries: int = 5,
                 reserve: int = 200, sleep: Callable[[float], None] = time.sleep,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                 max_wait: float | Callable[[], float] | None = None,
                 timeout_retries: int = 1, gate: Gate | None = None):
        self.client = client or httpx.Client(timeout=httpx.Timeout(60.0, connect=15.0))
        # Tokens pasted into settings often carry a stray space or line break.
        self.headers = {"Authorization": f"Bearer {token.strip()}", "User-Agent": "mergescout"}
        self.max_retries = max_retries
        self.reserve = reserve
        self.sleep = sleep
        self.now = now
        # A crawl on paid runner minutes stops instead of sleeping until the hourly reset.
        self.max_wait = max_wait
        self.timeout_retries = timeout_retries
        self.last_remaining: int | None = None
        self.gate = gate or Gate()

    def query(self, query: str, variables: dict | None = None, *, partial: bool = False) -> GraphQLResult:
        timeouts = 0
        for attempt in range(self.max_retries + 1):
            shared = self.gate.remaining()
            if shared > 0:
                self._pause(shared)
            self.gate.note("requests")
            try:
                resp = self.client.post(self.URL, json={"query": query, "variables": variables or {}},
                                        headers=self.headers)
            except httpx.LocalProtocolError as exc:
                # The request itself is malformed (usually a bad character in the token); retrying cannot help.
                raise GraphQLError(f"GitHub request could not be sent ({exc}); check the token for spaces or "
                                   f"line breaks") from exc
            except httpx.TransportError as exc:
                # Wi-Fi drops, DNS hiccups and timeouts are retried like server errors.
                if attempt == self.max_retries:
                    raise GraphQLError(f"GitHub unreachable (network error: {exc}) after {attempt} retries") from exc
                self.sleep(min(60, 2 ** attempt * 2))
                continue
            if resp.status_code == 401:
                raise TokenRejected("GitHub rejected the token (401). Check GITHUB_TOKEN.")
            if resp.status_code in self.TIMEOUT_STATUSES:
                self.gate.note("timeouts")
                # GitHub stops queries after 10 s; retry once, then let the caller ask for a smaller page.
                if timeouts >= self.timeout_retries:
                    raise GraphQLError(f"GitHub timed out (HTTP {resp.status_code}); the query may be too heavy")
                timeouts += 1
            if resp.status_code in self.RETRY_STATUSES or self._is_rate_limited(resp):
                if attempt == self.max_retries:
                    raise GraphQLError(f"GitHub kept failing (HTTP {resp.status_code}) after {attempt} retries")
                wait = self._wait_for(resp, attempt)
                if self._is_rate_limited(resp):
                    self.gate.hold(wait)
                    self.gate.note("rate_waits")
                self._pause(wait)
                continue
            if resp.status_code != 200:
                raise GraphQLError(f"GitHub returned HTTP {resp.status_code}: {resp.text[:200]}")
            try:
                body = resp.json()
            except ValueError as exc:
                raise GraphQLError(f"GitHub returned a body that is not JSON: {resp.text[:200]}") from exc
            errors = body.get("errors") or []
            data = body.get("data") or {}
            if errors:
                types = {e.get("type") for e in errors}
                if "RATE_LIMITED" in types and attempt < self.max_retries:
                    wait = self._seconds_until((data.get("rateLimit") or {}).get("resetAt"), fallback=60)
                    self.gate.hold(wait)
                    self.gate.note("rate_waits")
                    self._pause(wait)
                    continue
                top = [e for e in errors if e.get("type") == "NOT_FOUND" and e.get("path") == ["repository"]]
                if top and (partial or len(top) == len(errors)):
                    raise NotFound(top[0].get("message", "not found"))
                # With partial, items that no longer exist come back as null instead of failing the whole page.
                if not (partial and all(e.get("type") == "NOT_FOUND" for e in errors)):
                    raise GraphQLError("; ".join(sorted({e.get("message", "unknown error") for e in errors})))
            rate = data.get("rateLimit")
            if rate is None:
                raise ValueError("Every query must request rateLimit { cost remaining resetAt }")
            result = GraphQLResult(data, rate["cost"], rate["remaining"], rate["resetAt"])
            self.last_remaining = result.remaining
            if result.remaining < self.reserve:
                wait = self._seconds_until(result.reset_at, fallback=0)
                limit = self._wait_limit()
                if limit is None or wait <= limit:
                    self.gate.hold(wait)
                    self.sleep(wait)
            return result
        raise GraphQLError("unreachable")

    @staticmethod
    def _is_rate_limited(resp: httpx.Response) -> bool:
        if resp.status_code == 429:
            return True
        return resp.status_code == 403 and (
            "rate limit" in resp.text.lower() or resp.headers.get("x-ratelimit-remaining") == "0")

    def _wait_for(self, resp: httpx.Response, attempt: int) -> float:
        retry_after = resp.headers.get("retry-after")
        if retry_after and retry_after.isdigit():
            return int(retry_after)
        if resp.headers.get("x-ratelimit-remaining") == "0" and resp.headers.get("x-ratelimit-reset", "").isdigit():
            reset = datetime.fromtimestamp(int(resp.headers["x-ratelimit-reset"]), tz=timezone.utc)
            return max(0.0, (reset - self.now()).total_seconds() + 5)
        if self._is_rate_limited(resp):
            # GitHub asks for at least a minute after a secondary limit.
            return 60
        return min(60, 2 ** attempt * 2)

    def _wait_limit(self) -> float | None:
        # max_wait may be a function, so a crawl can sit out any wait that still fits in its time window.
        return self.max_wait() if callable(self.max_wait) else self.max_wait

    def _pause(self, seconds: float) -> None:
        limit = self._wait_limit()
        if limit is not None and seconds > limit:
            raise RateLimited(f"GitHub asks to wait {int(seconds)} s for the rate limit (allowed {int(limit)} s)")
        self.gate.note("waited_s", seconds)
        self.sleep(seconds)

    def _seconds_until(self, reset_at: str | None, fallback: float) -> float:
        reset = parse_time(reset_at)
        if reset is None:
            return fallback
        return max(0.0, (reset - self.now()).total_seconds() + 5)
