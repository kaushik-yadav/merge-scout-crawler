# GitHub client rules that keep long runs alive: shared waits, time-aware limits, retries, clean token errors.
from datetime import datetime, timezone

import httpx
import pytest

from mergescout.github.graphql import GitHubGraphQL, GraphQLError
from mergescout.github.identities import identities_from_env


NOW = datetime(2026, 10, 11, 0, 0, 0, tzinfo=timezone.utc)
OK = {"data": {"rateLimit": {"cost": 3, "remaining": 4000, "resetAt": "2026-10-11T01:00:00Z"}, "x": 1}}


def make(responses, **kw):
    from mergescout.github.graphql import Gate
    queue = list(responses)
    client = httpx.Client(transport=httpx.MockTransport(lambda req: queue.pop(0)))
    sleeps, clock = [], [0.0]

    def sleep(seconds):
        # A fake sleep still moves time forward, as a real one does.
        sleeps.append(seconds)
        clock[0] += seconds
    kw.setdefault("gate", Gate(clock=lambda: clock[0]))
    gql = GitHubGraphQL("t", client=client, sleep=sleep, now=lambda: NOW, **kw)
    return gql, sleeps


def test_secondary_limit_uses_retry_after():
    limited = httpx.Response(403, headers={"retry-after": "30"},
                             json={"message": "You have exceeded a secondary rate limit"})
    gql, sleeps = make([limited, httpx.Response(200, json=OK)])
    gql.query("q")
    assert sleeps == [30]


def test_retries_network_drops_then_gives_up_cleanly():
    calls = []

    def handler(req):
        calls.append(1)
        if len(calls) <= 2:
            raise httpx.ConnectError("getaddrinfo failed", request=req)
        return httpx.Response(200, json=OK)

    sleeps = []
    gql = GitHubGraphQL("t", client=httpx.Client(transport=httpx.MockTransport(handler)),
                        sleep=sleeps.append, now=lambda: NOW)
    assert gql.query("q").cost == 3
    assert sleeps == [2, 4]

    def always_down(req):
        raise httpx.ReadTimeout("timed out", request=req)

    gql = GitHubGraphQL("t", client=httpx.Client(transport=httpx.MockTransport(always_down)),
                        sleep=sleeps.append, now=lambda: NOW, max_retries=1)
    with pytest.raises(GraphQLError, match="network"):
        gql.query("q")


def test_gateway_timeout_retries_once_then_reports_too_heavy():
    from mergescout.github.graphql import too_heavy
    gql, sleeps = make([httpx.Response(504), httpx.Response(502)])
    with pytest.raises(GraphQLError, match="timed out") as err:
        gql.query("q")
    assert sleeps == [2] and too_heavy(err.value)
    gql, sleeps = make([httpx.Response(504), httpx.Response(200, json=OK)])
    assert gql.query("q").cost == 3 and sleeps == [2]


def test_rate_limit_wait_is_shared_by_every_client_on_the_token():
    from mergescout.github.graphql import Gate
    clock = [0.0]
    gate = Gate(clock=lambda: clock[0])
    limited = httpx.Response(403, headers={"retry-after": "60"}, json={"message": "secondary rate limit"})
    b_requests = []
    b = GitHubGraphQL("t", client=httpx.Client(transport=httpx.MockTransport(
        lambda req: b_requests.append(req) or httpx.Response(200, json=OK))), sleep=lambda s: b_sleeps.append(s),
        gate=gate)
    b_sleeps, a_sleeps = [], []

    def a_sleep(seconds):
        # While client a waits out the limit, client b must wait too instead of hitting GitHub.
        a_sleeps.append(seconds)
        b.query("q")
        clock[0] += seconds
    queue = [limited, httpx.Response(200, json=OK)]
    a = GitHubGraphQL("t", client=httpx.Client(transport=httpx.MockTransport(lambda req: queue.pop(0))),
                      sleep=a_sleep, gate=gate)
    assert a.query("q").cost == 3
    assert a_sleeps == [60] and b_sleeps == [60] and len(b_requests) == 1


def test_shared_wait_longer_than_max_wait_stops_without_a_request():
    from mergescout.github.graphql import Gate, RateLimited
    gate = Gate(clock=lambda: 0.0)
    gate.hold(300)
    gql, sleeps = make([], gate=gate, max_wait=120)
    with pytest.raises(RateLimited):
        gql.query("q")
    assert sleeps == []


def test_rejected_token_has_its_own_error():
    from mergescout.github.graphql import TokenRejected
    gql, sleeps = make([httpx.Response(401, json={"message": "Bad credentials"})])
    with pytest.raises(TokenRejected, match="rejected the token"):
        gql.query("q")
    assert sleeps == []


def test_token_spaces_and_line_breaks_are_trimmed():
    seen = []
    client = httpx.Client(transport=httpx.MockTransport(lambda req: seen.append(req.headers["authorization"])
                                                        or httpx.Response(200, json=OK)))
    GitHubGraphQL("  abc123\n", client=client, sleep=lambda s: None).query("q")
    assert seen == ["Bearer abc123"]


def test_wait_limit_can_follow_the_time_left_in_the_run():
    from mergescout.github.graphql import RateLimited
    limited = httpx.Response(403, headers={"retry-after": "300"}, json={"message": "secondary rate limit"})
    gql, sleeps = make([limited, httpx.Response(200, json=OK)], max_wait=lambda: 600)
    assert gql.query("q").cost == 3 and sleeps == [300]
    gql, sleeps = make([limited], max_wait=lambda: 200)
    with pytest.raises(RateLimited, match="allowed 200 s"):
        gql.query("q")
    assert sleeps == []


def test_identities_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("MS_APP_TOKEN", "app")
    monkeypatch.setenv("MS_ACTIONS_TOKEN", "act")
    made = []
    ids = identities_from_env("pat", 120, client=lambda token, **kw: made.append((token, kw)) or token)
    assert [i.name for i in ids] == ["personal", "app", "actions"] and [i.share for i in ids] == [1.0, 1.0, 1.0]
    assert [i.make_gql() for i in ids] == ["pat", "app", "act"]
    assert len({id(kw["gate"]) for _, kw in made}) == 3 and all(kw["max_wait"] == 120 for _, kw in made)


def test_the_same_token_with_a_line_break_is_still_the_same_token(monkeypatch):
    monkeypatch.delenv("MS_APP_TOKEN", raising=False)
    monkeypatch.setenv("MS_ACTIONS_TOKEN", "pat\n")
    assert [i.name for i in identities_from_env("pat", None, client=lambda t, **kw: t)] == ["personal"]
