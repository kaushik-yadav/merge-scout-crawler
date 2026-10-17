from dataclasses import dataclass, field
from datetime import datetime, timedelta

from mergescout.crawl.queries import (CLOSED_ISSUES_QUERY, ISSUE_PAGE_QUERY, PR_PAGE_QUERY, REPO_META_QUERY,
                                      is_dep_bot, issues_by_number_query, parse_closed_issues, parse_issues, parse_prs,
                                      parse_repo_meta, prs_by_number_query)
from mergescout.github.graphql import GraphQLError, NotFound, RateLimited, too_heavy
from mergescout.labels import HOT, OPPORTUNITY_LABELS, has_opportunity, norm_label

# Pages of 100 take 5-9 s on big repos and hit GitHub's 10 s limit under load; 50 costs the same points.
MIN_PAGE = 10
# GitHub charges by the page size asked for, so a refresh starts small: most repos change little between crawls.
REFRESH_FIRST_PAGE = 20
BY_NUMBER = 50


def cutoff(now: datetime, days: int) -> str:
    return (now - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class RepoBundle:
    repo_id: int
    name: str
    status: str = "ok"
    meta: dict | None = None
    prs: list[dict] = field(default_factory=list)
    issues: list[dict] = field(default_factory=list)
    comments: list[dict] = field(default_factory=list)
    points: int = 0
    error: str | None = None
    full: bool = True
    deleted_issues: list[int] = field(default_factory=list)
    closed_issues: list[dict] = field(default_factory=list)


def _walk(gql, query: str, variables: dict, connection: str, cap: int, page: int, bundle: RepoBundle,
          stop=None, first: int | None = None) -> list[dict]:
    # Pages through one connection, shrinking the page when GitHub says it is too heavy.
    cursor, nodes = None, []
    while len(nodes) < cap:
        size = min(first, page) if first and cursor is None else page
        try:
            result = gql.query(query, {**variables, "cursor": cursor, "n": min(size, cap - len(nodes))})
        except NotFound:
            raise
        except GraphQLError as exc:
            if too_heavy(exc) and page > MIN_PAGE:
                page = max(MIN_PAGE, page // 2)
                continue
            raise
        bundle.points += result.cost
        conn = (result.data.get("repository") or {}).get(connection) or {}
        batch = [n for n in conn.get("nodes") or [] if n]
        nodes.extend(batch)
        info = conn.get("pageInfo") or {}
        if not batch or (stop and stop(batch[-1])) or not info.get("hasNextPage") or not info.get("endCursor"):
            break
        cursor = info["endCursor"]
    return nodes[:cap]


def fetch_repo(gql, repo_id: int, name: str, now: datetime, pr_cap: int = 300, issue_cap: int = 200,
               issue_days: int = 90, page: int = 50, track: list[int] = (), track_cap: int = 200,
               pr_days: int = 365, changed_since: datetime | None = None, closed_cap: int = 100,
               closed_days: int = 365) -> RepoBundle:
    owner, repo = name.split("/", 1)
    bundle = RepoBundle(repo_id, name, full=changed_since is None)
    base = {"owner": owner, "name": repo}
    if changed_since is None:
        # First or periodic full crawl: PRs from the last year, open issues active in the last 90 days.
        pr_vars, pr_key, pr_since = {**base, "order": "CREATED_AT"}, "createdAt", cutoff(now, pr_days)
        since, states = cutoff(now, issue_days), ["OPEN"]
    else:
        # Refresh: only PRs and issues (open or closed) that changed since the last crawl; settings wait for the
        # next full crawl.
        pr_since = changed_since.strftime("%Y-%m-%dT%H:%M:%SZ")
        pr_vars, pr_key, since, states = {**base, "order": "UPDATED_AT"}, "updatedAt", pr_since, ["OPEN", "CLOSED"]
        track = ()
    first = REFRESH_FIRST_PAGE if changed_since else None
    try:
        if changed_since is None:
            meta = gql.query(REPO_META_QUERY, base)
            bundle.points += meta.cost
            bundle.meta = parse_repo_meta(repo_id, meta.data)
        pr_nodes = _walk(gql, PR_PAGE_QUERY, pr_vars, "pullRequests", pr_cap, page, bundle,
                         stop=lambda n: (n.get(pr_key) or "") < pr_since, first=first)
        pr_nodes = [n for n in pr_nodes if (n.get(pr_key) or "") >= pr_since]
        # Issues come newest-updated first; stop once a page reaches the cutoff.
        issue_nodes = _walk(gql, ISSUE_PAGE_QUERY, {**base, "since": since, "states": states}, "issues", issue_cap,
                            page, bundle, stop=lambda n: (n.get("updatedAt") or "") < since, first=first)
        fresh = [n for n in issue_nodes if (n.get("updatedAt") or "") >= since]
        # Tracked issues missing from the open list were closed, deleted or went quiet: look them up by number.
        seen = {n.get("number") for n in fresh}
        missing = [n for n in track if n not in seen][:track_cap]
        for i in range(0, len(missing), BY_NUMBER):
            chunk = missing[i:i + BY_NUMBER]
            result = gql.query(issues_by_number_query(chunk), base, partial=True)
            bundle.points += result.cost
            found = result.data.get("repository") or {}
            for number in chunk:
                node = found.get(f"i{number}")
                if node:
                    fresh.append(node)
                else:
                    bundle.deleted_issues.append(number)
        open_labels = [lb.get("name") for n in fresh if n.get("state", "OPEN") == "OPEN"
                       for lb in (n.get("labels") or {}).get("nodes") or [] if lb]
        if changed_since is None and closed_cap and has_opportunity(open_labels):
            # Label trust needs past beginner issues and who fixed them; only full crawls of hot repos pay for it.
            closed_since = cutoff(now, closed_days)
            # GitHub matches label names exactly, so ask with this repo's own spellings ("good first issue :wave:").
            hot_names = [n for n in (bundle.meta or {}).get("label_names") or [] if norm_label(n) in HOT]
            closed = _walk(gql, CLOSED_ISSUES_QUERY, {**base, "labels": hot_names or list(OPPORTUNITY_LABELS)},
                           "issues", closed_cap, page, bundle,
                           stop=lambda n: (n.get("updatedAt") or "") < closed_since)
            bundle.closed_issues = parse_closed_issues(
                repo_id, [n for n in closed if (n.get("updatedAt") or "") >= closed_since])
    except NotFound as exc:
        bundle.status, bundle.meta, bundle.error = "gone", None, str(exc)
        return bundle
    bundle.prs = parse_prs(repo_id, pr_nodes)
    bundle.issues, bundle.comments = parse_issues(repo_id, fresh)
    add_bot_outcomes(gql, base, bundle)
    return bundle


def add_bot_outcomes(gql, base: dict, bundle: RepoBundle) -> None:
    """Check result and commit count of finished dependency-bot PRs, 50 per query; only these need them (upgrade outcomes)."""
    rows = {r["number"]: r for r in bundle.prs if r["state"] in ("MERGED", "CLOSED")
            and is_dep_bot(r["author_login"], r["head_ref"])}
    numbers = sorted(rows)
    for i in range(0, len(numbers), BY_NUMBER):
        chunk = numbers[i:i + BY_NUMBER]
        try:
            result = gql.query(prs_by_number_query(chunk), base, partial=True)
        except (GraphQLError, RateLimited):
            # Outcomes are an extra: the PRs are kept without them and the next full crawl tries again.
            return
        bundle.points += result.cost
        found = result.data.get("repository") or {}
        for n in chunk:
            commits = (found.get(f"pr{n}") or {}).get("commits") or {}
            last = ((commits.get("nodes") or [{}])[-1] or {}).get("commit") or {}
            rows[n]["ci_state"] = (last.get("statusCheckRollup") or {}).get("state")
            rows[n]["commit_count"] = commits.get("totalCount")
