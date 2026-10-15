import re

from mergescout.timeutil import parse_time

RL = "rateLimit { cost remaining resetAt }"

PR_PAGE_QUERY = f"""
query($owner: String!, $name: String!, $cursor: String, $n: Int!, $order: IssueOrderField!) {{
  {RL}
  repository(owner: $owner, name: $name) {{
    pullRequests(first: $n, after: $cursor, orderBy: {{field: $order, direction: DESC}}) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{
        number createdAt updatedAt mergedAt closedAt state headRefName additions deletions changedFiles
        author {{ __typename login }} authorAssociation mergedBy {{ login }} bodyText
        reviews(first: 1) {{ nodes {{ submittedAt authorAssociation author {{ login }} }} }}
        closingIssuesReferences(first: 5) {{ nodes {{ number }} }}
      }}
    }}
  }}
}}
"""

ISSUE_FIELDS = """
        number title bodyText state stateReason createdAt updatedAt closedAt
        author { login } authorAssociation
        labels(first: 10) { nodes { name } }
        assignees(first: 5) { totalCount }
        closedByPullRequestsReferences(first: 5, includeClosedPrs: true) { nodes { number state author { login } authorAssociation } }
        comments(last: 5) { totalCount nodes { databaseId createdAt author { login } authorAssociation bodyText } }
"""

ISSUE_PAGE_QUERY = f"""
query($owner: String!, $name: String!, $cursor: String, $n: Int!, $since: DateTime!, $states: [IssueState!]) {{
  {RL}
  repository(owner: $owner, name: $name) {{
    issues(first: $n, after: $cursor, states: $states, filterBy: {{since: $since}},
           orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{ {ISSUE_FIELDS} }}
    }}
  }}
}}
"""


def issues_by_number_query(numbers: list[int]) -> str:
    # One query re-checks up to 50 tracked issues that dropped out of the open list.
    parts = "\n".join(f"    i{int(n)}: issue(number: {int(n)}) {{ {ISSUE_FIELDS} }}" for n in numbers)
    return f"query($owner: String!, $name: String!) {{\n  {RL}\n  repository(owner: $owner, name: $name) {{\n{parts}\n  }}\n}}"

REPO_META_QUERY = f"""
query($owner: String!, $name: String!) {{
  {RL}
  repository(owner: $owner, name: $name) {{
    databaseId nameWithOwner stargazerCount forkCount pushedAt isArchived
    hasIssuesEnabled hasDiscussionsEnabled hasProjectsEnabled
    primaryLanguage {{ name }} licenseInfo {{ spdxId }} defaultBranchRef {{ name }}
    openPrs: pullRequests(states: OPEN) {{ totalCount }} openIssues: issues(states: OPEN) {{ totalCount }}
    contributing: object(expression: "HEAD:CONTRIBUTING.md") {{ oid ... on Blob {{ text }} }}
    contributing2: object(expression: "HEAD:.github/CONTRIBUTING.md") {{ oid ... on Blob {{ text }} }}
    contributing3: object(expression: "HEAD:docs/CONTRIBUTING.md") {{ oid ... on Blob {{ text }} }}
    aipolicy: object(expression: "HEAD:AI_POLICY.md") {{ oid }}
    prtemplate: object(expression: "HEAD:.github/PULL_REQUEST_TEMPLATE.md") {{ oid }}
    readme: object(expression: "HEAD:README.md") {{ ... on Blob {{ text }} }}
    releases(first: 20, orderBy: {{field: CREATED_AT, direction: DESC}}) {{ totalCount nodes {{ createdAt }} }}
  }}
}}
"""

AI_BRANCH = re.compile(r"^(copilot|codex|cursor|devin|claude)[/-]", re.I)
AI_TRAILER = re.compile(r"co-authored-by:\s*(claude|codex|copilot|cursor|devin)", re.I)


def ai_tag(pr: dict) -> str | None:
    login = (pr.get("author_login") or "").lower()
    if "devin" in login:
        return "devin"
    if pr.get("author_type") == "Bot" or login.endswith("[bot]"):
        return "bot"
    m = AI_BRANCH.match(pr.get("head_ref") or "")
    if m:
        return m.group(1).lower()
    m = AI_TRAILER.search(pr.get("body") or "")
    if m:
        return m.group(1).lower()
    return None


def parse_prs(repo_id: int, nodes: list) -> list[dict]:
    rows = []
    for n in nodes:
        if not n:
            continue
        author = n.get("author") or {}
        review = ((n.get("reviews") or {}).get("nodes") or [None])[0] or {}
        row = {
            "repo_id": repo_id, "number": n.get("number"),
            "created_at": parse_time(n.get("createdAt")), "merged_at": parse_time(n.get("mergedAt")),
            "closed_at": parse_time(n.get("closedAt")), "state": n.get("state"),
            "author_login": author.get("login"), "author_type": author.get("__typename"),
            "author_association": n.get("authorAssociation"), "merged_by": (n.get("mergedBy") or {}).get("login"),
            "head_ref": n.get("headRefName"), "additions": n.get("additions"), "deletions": n.get("deletions"),
            "changed_files": n.get("changedFiles"),
            "first_review_at": parse_time(review.get("submittedAt")),
            "first_review_by": (review.get("author") or {}).get("login"),
            "first_review_association": review.get("authorAssociation"),
            "closing_issue_numbers": [c["number"] for c in (n.get("closingIssuesReferences") or {}).get("nodes") or []
                                      if c],
        }
        row["ai_tag"] = ai_tag({**row, "body": n.get("bodyText") or ""})
        rows.append(row)
    return rows


def _fix_fields(refs: list[dict]) -> dict:
    # The merged PR is the fix; otherwise the first linked PR shows who tried.
    fix = next((p for p in refs if p.get("state") == "MERGED"), None) or (refs[0] if refs else {})
    return {"fix_pr": fix.get("number"), "fix_pr_state": fix.get("state"),
            "fix_author": (fix.get("author") or {}).get("login"), "fix_association": fix.get("authorAssociation")}


def parse_issues(repo_id: int, nodes: list) -> tuple[list[dict], list[dict]]:
    issues, comments = [], []
    for n in nodes:
        if not n:
            continue
        linked = [p for p in (n.get("closedByPullRequestsReferences") or {}).get("nodes") or [] if p]
        cm = n.get("comments") or {}
        issues.append({
            "repo_id": repo_id, "number": n.get("number"), "title": n.get("title"),
            "body_excerpt": (n.get("bodyText") or "")[:1000], "state": n.get("state"),
            "state_reason": n.get("stateReason"),
            "created_at": parse_time(n.get("createdAt")), "updated_at": parse_time(n.get("updatedAt")),
            "closed_at": parse_time(n.get("closedAt")), "author_login": (n.get("author") or {}).get("login"),
            "author_association": n.get("authorAssociation"),
            "labels": [lb["name"] for lb in (n.get("labels") or {}).get("nodes") or [] if lb and lb.get("name")],
            "assignees": (n.get("assignees") or {}).get("totalCount"),
            "linked_open_prs": sum(p.get("state") == "OPEN" for p in linked),
            "linked_merged_prs": sum(p.get("state") == "MERGED" for p in linked),
            "linked_closed_prs": sum(p.get("state") == "CLOSED" for p in linked),
            **_fix_fields(linked),
            "comments_total": cm.get("totalCount"),
        })
        for c in cm.get("nodes") or []:
            if c:
                comments.append({"repo_id": repo_id, "number": n.get("number"), "comment_id": c.get("databaseId"),
                                 "created_at": parse_time(c.get("createdAt")),
                                 "author_login": (c.get("author") or {}).get("login"),
                                 "author_association": c.get("authorAssociation"),
                                 "body_excerpt": (c.get("bodyText") or "")[:500]})
    return issues, comments


def parse_repo_meta(repo_id: int, data: dict) -> dict:
    r = data.get("repository") or {}
    rel = r.get("releases") or {}
    dates = [d for d in (parse_time(x.get("createdAt")) for x in rel.get("nodes") or [] if x) if d]
    newest = max(dates) if dates else None
    contrib = r.get("contributing") or r.get("contributing2") or r.get("contributing3") or {}
    return {
        "repo_id": repo_id, "name": r.get("nameWithOwner"), "stars": r.get("stargazerCount"),
        "forks": r.get("forkCount"), "pushed_at": parse_time(r.get("pushedAt")),
        # Backlog sizes: cheap here (one repo per query), slow in the 50-repo sweep.
        "open_prs": (r.get("openPrs") or {}).get("totalCount"),
        "open_issues": (r.get("openIssues") or {}).get("totalCount"),
        "language": (r.get("primaryLanguage") or {}).get("name"), "archived": r.get("isArchived"),
        "has_issues": r.get("hasIssuesEnabled"), "has_discussions": r.get("hasDiscussionsEnabled"),
        "has_projects": r.get("hasProjectsEnabled"), "license": (r.get("licenseInfo") or {}).get("spdxId"),
        "default_branch": (r.get("defaultBranchRef") or {}).get("name"),
        "contributing_sha": contrib.get("oid"), "contributing_excerpt": (contrib.get("text") or "")[:4000],
        "ai_policy_sha": (r.get("aipolicy") or {}).get("oid"), "pr_template_sha": (r.get("prtemplate") or {}).get("oid"),
        "readme_excerpt": ((r.get("readme") or {}).get("text") or "")[:2000],
        "last_release_at": newest,
        "releases_12m": sum(1 for d in dates if newest and (newest - d).days <= 365),
    }


CLOSED_ISSUES_QUERY = f"""
query($owner: String!, $name: String!, $cursor: String, $n: Int!, $labels: [String!]) {{
  {RL}
  repository(owner: $owner, name: $name) {{
    issues(first: $n, after: $cursor, states: CLOSED, labels: $labels, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{
        number createdAt updatedAt closedAt stateReason author {{ login }} authorAssociation
        labels(first: 10) {{ nodes {{ name }} }}
        closedByPullRequestsReferences(first: 3, includeClosedPrs: true) {{
          nodes {{ number state author {{ login }} authorAssociation }}
        }}
      }}
    }}
  }}
}}
"""


def parse_closed_issues(repo_id: int, nodes: list) -> list[dict]:
    rows = []
    for n in nodes:
        if not n:
            continue
        refs = [p for p in (n.get("closedByPullRequestsReferences") or {}).get("nodes") or [] if p]
        rows.append({
            "repo_id": repo_id, "number": n.get("number"), "created_at": parse_time(n.get("createdAt")),
            "closed_at": parse_time(n.get("closedAt")), "state_reason": n.get("stateReason"),
            "author_login": (n.get("author") or {}).get("login"), "author_association": n.get("authorAssociation"),
            "labels": [lb["name"] for lb in (n.get("labels") or {}).get("nodes") or [] if lb and lb.get("name")],
            **_fix_fields(refs),
        })
    return rows
