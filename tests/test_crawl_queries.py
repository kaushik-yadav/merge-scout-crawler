import json
from pathlib import Path

from mergescout.crawl.queries import ai_tag, parse_issues, parse_prs, parse_repo_meta

FIX = Path(__file__).parent / "fixtures"


def test_parse_prs_from_fixture():
    data = json.loads((FIX / "pr_page.json").read_text(encoding="utf-8"))
    rows = parse_prs(1, data["repository"]["pullRequests"]["nodes"])
    assert len(rows) == 3
    r = rows[0]
    assert set(r) >= {"number", "created_at", "merged_at", "state", "author_login", "author_type",
                      "author_association", "merged_by", "head_ref", "first_review_at", "closing_issue_numbers", "ai_tag"}
    assert r["repo_id"] == 1 and isinstance(r["closing_issue_numbers"], list)
    assert "body" not in r


def test_pr_rows_tolerate_missing_fields():
    nodes = [None, {"number": 5, "createdAt": "2026-10-01T00:00:00Z", "state": "OPEN", "author": None,
                    "mergedBy": None, "reviews": {"nodes": []}, "closingIssuesReferences": {"nodes": []}}]
    rows = parse_prs(1, nodes)
    assert len(rows) == 1
    assert rows[0]["author_login"] is None and rows[0]["merged_by"] is None and rows[0]["first_review_at"] is None
    assert rows[0]["head_ref"] is None and rows[0]["ai_tag"] is None


def test_ai_tag_rules():
    assert ai_tag({"author_login": "dependabot", "author_type": "Bot"}) == "bot"
    assert ai_tag({"author_login": "x", "author_type": "User", "head_ref": "copilot/fix-123"}) == "copilot"
    assert ai_tag({"author_login": "x", "author_type": "User", "head_ref": "codex/abc"}) == "codex"
    assert ai_tag({"author_login": "devin-ai-integration[bot]", "author_type": "Bot"}) == "devin"
    assert ai_tag({"author_login": "x", "author_type": "User",
                   "body": "Co-Authored-By: Claude <noreply@anthropic.com>"}) == "claude"
    assert ai_tag({"author_login": "x", "author_type": "User", "head_ref": "feature/x", "body": "fix"}) is None


def test_parse_issues_and_comments_from_fixture():
    data = json.loads((FIX / "issue_page.json").read_text(encoding="utf-8"))
    issues, comments = parse_issues(1, data["repository"]["issues"]["nodes"])
    assert 1 <= len(issues) <= 3
    i = issues[0]
    assert set(i) >= {"number", "title", "body_excerpt", "state", "labels", "assignees", "linked_open_prs",
                      "linked_merged_prs", "comments_total"}
    assert len(i["body_excerpt"]) <= 1000
    for c in comments:
        assert c["repo_id"] == 1 and len(c["body_excerpt"]) <= 500


def test_parse_repo_meta_from_fixture():
    data = json.loads((FIX / "repo_meta.json").read_text(encoding="utf-8"))
    m = parse_repo_meta(1, data)
    assert m["name"] == "psf/requests" and m["stars"] > 1000
    assert "contributing_sha" in m and len(m["readme_excerpt"]) <= 2000 and m["releases_12m"] >= 0


def test_linked_pr_counts_and_labels():
    node = {"number": 9, "title": "t", "bodyText": "b", "state": "OPEN", "createdAt": "2026-10-01T00:00:00Z",
            "updatedAt": "2026-10-02T00:00:00Z", "author": {"login": "a"}, "authorAssociation": "NONE",
            "labels": {"nodes": [{"name": "good first issue"}, None, {"name": "bug"}]},
            "assignees": {"totalCount": 0},
            "closedByPullRequestsReferences": {"nodes": [{"number": 1, "state": "OPEN"}, {"number": 2, "state": "MERGED"},
                                                         {"number": 3, "state": "CLOSED"}, None]},
            "comments": {"totalCount": 7, "nodes": [{"databaseId": 11, "createdAt": "2026-10-02T00:00:00Z",
                                                     "author": None, "authorAssociation": "NONE", "bodyText": "I'll take this"}]}}
    issues, comments = parse_issues(4, [node])
    assert (issues[0]["linked_open_prs"], issues[0]["linked_merged_prs"]) == (1, 1)
    assert issues[0]["labels"] == ["good first issue", "bug"] and issues[0]["comments_total"] == 7
    assert comments == [{"repo_id": 4, "number": 9, "comment_id": 11, "created_at": comments[0]["created_at"],
                         "author_login": None, "author_association": "NONE", "body_excerpt": "I'll take this"}]


def test_issues_by_number_query_and_state_reason():
    from mergescout.crawl.queries import issues_by_number_query
    q = issues_by_number_query([12, 40])
    assert "i12: issue(number: 12)" in q and "i40: issue(number: 40)" in q and "stateReason" in q
    assert "rateLimit" in q
    issues, _ = parse_issues(1, [{"number": 12, "state": "CLOSED", "stateReason": "NOT_PLANNED"}])
    assert issues[0]["state_reason"] == "NOT_PLANNED"


def test_repo_meta_keeps_open_backlog_counts():
    meta = parse_repo_meta(1, {"repository": {"openPrs": {"totalCount": 412}, "openIssues": {"totalCount": 90}}})
    assert (meta["open_prs"], meta["open_issues"]) == (412, 90)
    assert parse_repo_meta(1, {"repository": {}})["open_prs"] is None


def test_label_normalization_and_opportunity():
    from mergescout.labels import has_opportunity, norm_label
    assert norm_label("Good-First-Issue :wave:") == "good first issue"
    assert norm_label("help_wanted") == "help wanted"
    assert has_opportunity(["bug", "Good First Issue"]) and not has_opportunity(["bug"])


def test_closed_unmerged_links_and_contributing_text():
    issues, _ = parse_issues(1, [{"number": 3, "closedByPullRequestsReferences": {"nodes": [
        {"number": 1, "state": "CLOSED"}, {"number": 2, "state": "MERGED"}, {"number": 4, "state": "CLOSED"}]}}])
    assert issues[0]["linked_closed_prs"] == 2
    meta = parse_repo_meta(1, {"repository": {"contributing2": {"oid": "abc", "text": "Sign the CLA. " * 500}}})
    assert meta["contributing_sha"] == "abc" and len(meta["contributing_excerpt"]) == 4000


def test_parse_closed_issues_prefers_the_merged_fix():
    from mergescout.crawl.queries import parse_closed_issues
    rows = parse_closed_issues(1, [None, {"number": 9, "stateReason": "COMPLETED", "closedAt": "2026-09-01T00:00:00Z",
        "labels": {"nodes": [{"name": "good first issue"}]},
        "closedByPullRequestsReferences": {"nodes": [
            {"number": 20, "state": "CLOSED", "author": {"login": "a"}, "authorAssociation": "NONE"},
            {"number": 21, "state": "MERGED", "author": {"login": "b"}, "authorAssociation": "CONTRIBUTOR"}]}}])
    assert len(rows) == 1
    r = rows[0]
    assert (r["fix_pr"], r["fix_pr_state"], r["fix_author"], r["fix_association"]) == (21, "MERGED", "b", "CONTRIBUTOR")
    assert r["labels"] == ["good first issue"] and r["state_reason"] == "COMPLETED"


def test_refreshed_closed_issue_carries_its_fix():
    # Refreshes deliver closed issues through the normal issue query, so issue rows keep the fixing PR too.
    from mergescout.crawl.queries import ISSUE_FIELDS
    assert "nodes { number state author { login } authorAssociation }" in " ".join(ISSUE_FIELDS.split())
    issues, _ = parse_issues(1, [{"number": 9, "state": "CLOSED", "stateReason": "COMPLETED",
        "closedByPullRequestsReferences": {"nodes": [
            {"number": 20, "state": "CLOSED", "author": {"login": "a"}, "authorAssociation": "NONE"},
            {"number": 21, "state": "MERGED", "author": {"login": "b"}, "authorAssociation": "CONTRIBUTOR"}]}}])
    r = issues[0]
    assert (r["fix_pr"], r["fix_pr_state"], r["fix_author"], r["fix_association"]) == (21, "MERGED", "b", "CONTRIBUTOR")


def test_first_human_review_skips_review_bots():
    from mergescout.crawl.queries import PR_PAGE_QUERY
    assert "reviews(first: 5) { nodes { submittedAt authorAssociation author { __typename login } } }" in " ".join(
        PR_PAGE_QUERY.split())
    rows = parse_prs(1, [{"number": 1, "state": "OPEN", "reviews": {"nodes": [
        {"submittedAt": "2026-10-01T00:05:00Z", "authorAssociation": "NONE",
         "author": {"__typename": "Bot", "login": "coderabbitai"}},
        {"submittedAt": "2026-10-02T00:00:00Z", "authorAssociation": "MEMBER",
         "author": {"__typename": "User", "login": "maint"}}]}}])
    r = rows[0]
    assert r["first_review_by"] == "coderabbitai"
    assert (r["first_human_review_by"], r["first_human_review_association"]) == ("maint", "MEMBER")
    assert r["first_human_review_at"].day == 2
