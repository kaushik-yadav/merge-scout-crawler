from datetime import datetime, timezone

import httpx
import pytest

from mergescout.github.graphql import GitHubGraphQL, GraphQLError, NotFound

OK = {"data": {"rateLimit": {"cost": 3, "remaining": 4000, "resetAt": "2026-10-11T01:00:00Z"}, "x": 1}}
NOW = datetime(2026, 10, 11, 0, 0, 0, tzinfo=timezone.utc)


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


def test_success_reports_cost():
    gql, sleeps = make([httpx.Response(200, json=OK)])
    r = gql.query("query { rateLimit { cost remaining resetAt } }")
    assert (r.cost, r.remaining, r.data["x"], sleeps) == (3, 4000, 1, [])


def test_retries_server_errors_with_backoff():
    gql, sleeps = make([httpx.Response(503), httpx.Response(503), httpx.Response(200, json=OK)])
    assert gql.query("q").cost == 3
    assert sleeps == [2, 4]


def test_secondary_limit_uses_retry_after():
    limited = httpx.Response(403, headers={"retry-after": "30"},
                             json={"message": "You have exceeded a secondary rate limit"})
    gql, sleeps = make([limited, httpx.Response(200, json=OK)])
    gql.query("q")
    assert sleeps == [30]


def test_gives_up_after_max_retries():
    gql, sleeps = make([httpx.Response(503)] * 3, max_retries=2)
    with pytest.raises(GraphQLError, match="kept failing"):
        gql.query("q")
    assert sleeps == [2, 4]


def test_not_found_and_other_errors():
    nf = {"data": {"repository": None}, "errors": [{"type": "NOT_FOUND", "path": ["repository"], "message": "gone"}]}
    gql, _ = make([httpx.Response(200, json=nf)])
    with pytest.raises(NotFound):
        gql.query("q")
    bad = {"errors": [{"type": "SOMETHING", "message": "boom"}]}
    gql, _ = make([httpx.Response(200, json=bad)])
    with pytest.raises(GraphQLError, match="boom"):
        gql.query("q")


def test_query_without_rate_limit_field_is_a_bug():
    gql, _ = make([httpx.Response(200, json={"data": {"x": 1}})])
    with pytest.raises(ValueError, match="rateLimit"):
        gql.query("q")


def test_waits_for_reset_when_budget_runs_low():
    low = {"data": {"rateLimit": {"cost": 1, "remaining": 50, "resetAt": "2026-10-11T00:01:40Z"}}}
    gql, sleeps = make([httpx.Response(200, json=low)])
    gql.query("q")
    assert sleeps == [105.0]


def test_bad_token_message():
    gql, _ = make([httpx.Response(401, json={"message": "Bad credentials"})])
    with pytest.raises(GraphQLError, match="rejected the token"):
        gql.query("q")


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


def test_retries_500_and_wraps_non_json_body():
    gql, sleeps = make([httpx.Response(500), httpx.Response(200, json=OK)])
    assert gql.query("q").cost == 3
    assert sleeps == [2]
    gql, _ = make([httpx.Response(200, content=b"<html>oops</html>")])
    with pytest.raises(GraphQLError, match="not JSON"):
        gql.query("q")
    gql, _ = make([httpx.Response(418)])
    with pytest.raises(GraphQLError, match="HTTP 418"):
        gql.query("q")


def test_primary_limit_waits_until_reset_header():
    reset = str(int(NOW.timestamp()) + 90)
    limited = httpx.Response(403, headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": reset},
                             json={"message": "API rate limit exceeded"})
    gql, sleeps = make([limited, httpx.Response(200, json=OK)])
    gql.query("q")
    assert sleeps == [95.0]


def test_secondary_limit_without_retry_after_waits_a_minute():
    limited = httpx.Response(403, json={"message": "You have exceeded a secondary rate limit"})
    gql, sleeps = make([limited, httpx.Response(200, json=OK)])
    gql.query("q")
    assert sleeps == [60]


def test_rate_limited_in_body_waits_until_reset_at():
    body = {"data": {"rateLimit": {"cost": 1, "remaining": 0, "resetAt": "2026-10-11T00:02:00Z"}},
            "errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]}
    gql, sleeps = make([httpx.Response(200, json=body), httpx.Response(200, json=OK)])
    gql.query("q")
    assert sleeps == [125.0]


def test_not_found_only_for_top_level_repository_path():
    nested = {"data": {"repository": {"x": None}},
              "errors": [{"type": "NOT_FOUND", "path": ["repository", "x"], "message": "missing field"}]}
    gql, _ = make([httpx.Response(200, json=nested)])
    with pytest.raises(GraphQLError) as excinfo:
        gql.query("q")
    assert not isinstance(excinfo.value, NotFound)
    top = {"data": {"repository": None},
           "errors": [{"type": "NOT_FOUND", "path": ["repository"], "message": "gone"}]}
    gql, _ = make([httpx.Response(200, json=top)])
    with pytest.raises(NotFound):
        gql.query("q")


def test_max_wait_raises_instead_of_sleeping_past_reset():
    from mergescout.github.graphql import RateLimited
    limited = {"data": {"rateLimit": {"cost": 1, "remaining": 0, "resetAt": "2026-10-11T00:50:00Z"}},
               "errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]}
    gql, sleeps = make([httpx.Response(200, json=limited)], max_wait=120)
    with pytest.raises(RateLimited):
        gql.query("q")
    assert sleeps == []


def test_max_wait_keeps_short_waits_and_skips_reserve_sleep():
    low = {"data": {"rateLimit": {"cost": 1, "remaining": 100, "resetAt": "2026-10-11T00:50:00Z"}}}
    limited = httpx.Response(403, headers={"retry-after": "30"}, json={"message": "secondary rate limit"})
    gql, sleeps = make([limited, httpx.Response(200, json=low)], max_wait=120)
    r = gql.query("q")
    assert (r.remaining, gql.last_remaining, sleeps) == (100, 100, [30])


def test_partial_keeps_data_when_only_nested_items_are_missing():
    body = {"data": {"rateLimit": {"cost": 1, "remaining": 4000, "resetAt": "2026-10-11T01:00:00Z"},
                     "repository": {"i1": {"number": 1}, "i2": None}},
            "errors": [{"type": "NOT_FOUND", "path": ["repository", "i2"], "message": "no issue 2"}]}
    gql, _ = make([httpx.Response(200, json=body)])
    r = gql.query("q", partial=True)
    assert r.data["repository"] == {"i1": {"number": 1}, "i2": None}
    gql, _ = make([httpx.Response(200, json=body)])
    with pytest.raises(GraphQLError, match="no issue 2"):
        gql.query("q")
    gone = {"data": {"repository": None}, "errors": [{"type": "NOT_FOUND", "path": ["repository"], "message": "x"}]}
    gql, _ = make([httpx.Response(200, json=gone)])
    with pytest.raises(NotFound):
        gql.query("q", partial=True)


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


def test_gate_counts_requests_timeouts_and_waits():
    limited = httpx.Response(403, headers={"retry-after": "30"}, json={"message": "secondary rate limit"})
    gql, _ = make([limited, httpx.Response(504), httpx.Response(200, json=OK)])
    gql.query("q")
    gate = gql.gate
    assert gate.stats == {"requests": 3, "timeouts": 1, "rate_waits": 1, "waited_s": 34.0}
    assert gate.summary() == "requests=3 timeouts=1 rate-limit waits=1 waited=34s"


def test_rejected_token_has_its_own_error():
    from mergescout.github.graphql import TokenRejected
    gql, sleeps = make([httpx.Response(401, json={"message": "Bad credentials"})])
    with pytest.raises(TokenRejected, match="rejected the token"):
        gql.query("q")
    assert sleeps == []
