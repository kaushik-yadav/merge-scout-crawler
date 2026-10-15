from datetime import datetime, timezone

import pytest

from mergescout.crawl.fetch import fetch_repo
from mergescout.github.graphql import GraphQLError, NotFound

NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)
META = {"repository": {"databaseId": 1, "nameWithOwner": "o/r", "stargazerCount": 200, "releases": {"nodes": []}}}


def pr(n, created="2026-10-01T00:00:00Z"):
    return {"number": n, "createdAt": created, "state": "OPEN", "author": {"__typename": "User", "login": "a"},
            "reviews": {"nodes": []}, "closingIssuesReferences": {"nodes": []}}


def issue(n, updated="2026-10-01T00:00:00Z"):
    return {"number": n, "title": "t", "state": "OPEN", "updatedAt": updated, "labels": {"nodes": []},
            "assignees": {"totalCount": 0}, "closedByPullRequestsReferences": {"nodes": []},
            "comments": {"totalCount": 0, "nodes": []}}


def page(conn, nodes, has_next, cursor=None):
    return {"repository": {conn: {"pageInfo": {"hasNextPage": has_next, "endCursor": cursor}, "nodes": nodes}}}


def responder_factory(pr_pages, issue_pages, meta=META):
    def responder(query, variables):
        if "pullRequests(first:" in query:
            return pr_pages[variables["cursor"]], 1
        if "issues(first:" in query:
            return issue_pages[variables["cursor"]], 2
        return meta, 1
    return responder


def test_fetch_caps_prs_and_counts_points(fake_gql):
    pr_pages = {None: page("pullRequests", [pr(i) for i in range(100)], True, "c1"),
                "c1": page("pullRequests", [pr(i) for i in range(100, 200)], True, "c2"),
                "c2": page("pullRequests", [pr(i) for i in range(200, 300)], True, "c3"),
                "c3": page("pullRequests", [pr(i) for i in range(300, 400)], True, "c4")}
    gql = fake_gql(responder_factory(pr_pages, {None: page("issues", [], False)}))
    b = fetch_repo(gql, 1, "o/r", NOW, pr_cap=300)
    assert b.status == "ok" and len(b.prs) == 300 and b.meta["name"] == "o/r"
    assert b.points == 1 + 3 + 2
    assert [c[1]["cursor"] for c in gql.calls if "pullRequests(first:" in c[0]] == [None, "c1", "c2"]


def test_issues_stop_at_cutoff_and_use_since(fake_gql):
    issue_pages = {None: page("issues", [issue(1, "2026-10-10T00:00:00Z"), issue(2, "2026-01-01T00:00:00Z")],
                              True, "i1"),
                   "i1": page("issues", [issue(3)], False)}
    gql = fake_gql(responder_factory({None: page("pullRequests", [], False)}, issue_pages))
    b = fetch_repo(gql, 1, "o/r", NOW, issue_days=90)
    assert [i["number"] for i in b.issues] == [1]
    since = [c[1]["since"] for c in gql.calls if "issues(first:" in c[0]]
    assert since == ["2026-07-16T00:00:00Z"]


def test_gone_repo_is_marked_not_retried(fake_gql):
    def responder(query, variables):
        raise NotFound("Could not resolve to a Repository")
    b = fetch_repo(fake_gql(responder), 1, "o/r", NOW)
    assert b.status == "gone" and b.prs == [] and b.meta is None and "resolve" in b.error


def test_heavy_page_halves_then_succeeds(fake_gql):
    calls = []

    def responder(query, variables):
        if "pullRequests(first:" in query:
            calls.append(variables["n"])
            if variables["n"] > 12:
                raise GraphQLError("Resource limits for this query exceeded")
            return page("pullRequests", [pr(1)], False), 1
        if "issues(first:" in query:
            return page("issues", [], False), 1
        return META, 1
    b = fetch_repo(fake_gql(responder), 1, "o/r", NOW)
    assert calls == [50, 25, 12] and len(b.prs) == 1


def test_default_pages_are_50_to_stay_under_githubs_10_second_limit(fake_gql):
    gql = fake_gql(responder_factory({None: page("pullRequests", [], False)}, {None: page("issues", [], False)}))
    fetch_repo(gql, 1, "o/r", NOW)
    assert [v["n"] for q, v in gql.calls if "pullRequests(first:" in q or "issues(first:" in q] == [50, 50]


def test_heavy_at_smallest_page_raises(fake_gql):
    def responder(query, variables):
        if "pullRequests(first:" in query:
            raise GraphQLError("Resource limits for this query exceeded")
        return META, 1
    with pytest.raises(GraphQLError):
        fetch_repo(fake_gql(responder), 1, "o/r", NOW)


def test_fetch_other_errors_propagate(fake_gql):
    def responder(query, variables):
        raise GraphQLError("boom")
    with pytest.raises(GraphQLError):
        fetch_repo(fake_gql(responder), 1, "o/r", NOW)


def test_tracked_issues_missing_from_the_open_list_are_checked_by_number(fake_gql):
    base = responder_factory({None: page("pullRequests", [], False)},
                             {None: page("issues", [issue(1, "2026-10-10T00:00:00Z")], False)})
    closed = {**issue(2, "2026-10-12T00:00:00Z"), "state": "CLOSED", "stateReason": "COMPLETED",
              "closedByPullRequestsReferences": {"nodes": [{"number": 9, "state": "MERGED"}]}}

    def responder(query, variables):
        if "i2: issue(number: 2)" in query:
            return {"repository": {"i2": closed, "i3": None}}, 1
        return base(query, variables)
    gql = fake_gql(responder)
    b = fetch_repo(gql, 1, "o/r", NOW, track=[1, 2, 3])
    by_num = {i["number"]: i for i in b.issues}
    assert set(by_num) == {1, 2} and b.deleted_issues == [3]
    assert by_num[2]["state_reason"] == "COMPLETED" and by_num[2]["linked_merged_prs"] == 1
    assert {"partial": True} in gql.kwargs and b.points == 1 + 1 + 2 + 1


def test_first_crawl_stops_pr_history_at_one_year(fake_gql):
    pr_pages = {None: page("pullRequests", [pr(1, "2026-10-01T00:00:00Z"), pr(2, "2025-06-01T00:00:00Z")], True, "c1"),
                "c1": page("pullRequests", [pr(3, "2025-01-01T00:00:00Z")], False)}
    gql = fake_gql(responder_factory(pr_pages, {None: page("issues", [], False)}))
    b = fetch_repo(gql, 1, "o/r", NOW)
    assert [p["number"] for p in b.prs] == [1] and b.full is True
    assert [v["cursor"] for q, v in gql.calls if "pullRequests(first:" in q] == [None]
    assert [v["order"] for q, v in gql.calls if "pullRequests(first:" in q] == ["CREATED_AT"]
    assert [v["states"] for q, v in gql.calls if "issues(first:" in q] == [["OPEN"]]


def test_refresh_asks_only_for_what_changed(fake_gql):
    changed = {**pr(5, "2025-01-01T00:00:00Z"), "updatedAt": "2026-10-12T00:00:00Z"}
    stale = {**pr(6, "2025-01-01T00:00:00Z"), "updatedAt": "2026-09-01T00:00:00Z"}
    closed = {**issue(8, "2026-10-12T00:00:00Z"), "state": "CLOSED", "stateReason": "COMPLETED"}
    gql = fake_gql(responder_factory({None: page("pullRequests", [changed, stale], True, "c1")},
                                     {None: page("issues", [closed], False)}))
    since = datetime(2026, 10, 10, tzinfo=timezone.utc)
    b = fetch_repo(gql, 1, "o/r", NOW, track=[8, 9, 10], changed_since=since)
    assert [p["number"] for p in b.prs] == [5] and b.full is False
    assert [i["number"] for i in b.issues] == [8] and b.issues[0]["state"] == "CLOSED"
    assert [v["order"] for q, v in gql.calls if "pullRequests(first:" in q] == ["UPDATED_AT"]
    issue_calls = [v for q, v in gql.calls if "issues(first:" in q]
    assert issue_calls[0]["states"] == ["OPEN", "CLOSED"] and issue_calls[0]["since"] == "2026-10-10T00:00:00Z"
    assert not any("issue(number:" in q for q, _ in gql.calls) and b.deleted_issues == []
    # Settings are only re-read on full crawls, and a refresh starts with a small first page.
    assert b.meta is None and not any("stargazerCount" in q for q, _ in gql.calls)
    assert [v["n"] for q, v in gql.calls if "pullRequests(first:" in q or "issues(first:" in q] == [20, 20]


def test_refresh_pages_grow_after_the_first(fake_gql):
    changed = [{**pr(i, "2025-01-01T00:00:00Z"), "updatedAt": "2026-10-12T00:00:00Z"} for i in range(20)]
    gql = fake_gql(responder_factory({None: page("pullRequests", changed, True, "c1"),
                                      "c1": page("pullRequests", [], False)}, {None: page("issues", [], False)}))
    fetch_repo(gql, 1, "o/r", NOW, changed_since=datetime(2026, 10, 10, tzinfo=timezone.utc))
    assert [v["n"] for q, v in gql.calls if "pullRequests(first:" in q] == [20, 50]


def test_first_crawl_reads_at_most_200_open_issues(fake_gql):
    issue_pages = {None if i == 0 else f"i{i}": page("issues", [issue(i * 50 + k, "2026-10-12T00:00:00Z")
                                                              for k in range(50)], True, f"i{i + 1}")
                   for i in range(6)}
    gql = fake_gql(responder_factory({None: page("pullRequests", [], False)}, issue_pages))
    b = fetch_repo(gql, 1, "o/r", NOW)
    assert len(b.issues) == 200 and sum("issues(first:" in q for q, _ in gql.calls) == 4
