from mergescout.crawl.state import CrawlState


def _iso(value) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def issue_state_rows(issues: list[dict]) -> list[dict]:
    return [{"repo_id": i["repo_id"], "number": i["number"], "state": i.get("state"),
             "state_reason": i.get("state_reason"), "assignees": i.get("assignees") or 0,
             "linked_open_prs": i.get("linked_open_prs") or 0, "linked_merged_prs": i.get("linked_merged_prs") or 0,
             "comments": i.get("comments_total") or 0, "labels": "|".join(i.get("labels") or []),
             "updated_at": _iso(i.get("updated_at"))} for i in issues]


def diff_events(before: list[dict], after: list[dict]) -> list[dict]:
    prev = {(r["repo_id"], r["number"]): r for r in before}
    events = []
    for r in after:
        p = prev.get((r["repo_id"], r["number"]))
        if p is None:
            continue

        def add(kind, detail=None):
            events.append({"repo_id": r["repo_id"], "number": r["number"], "event": kind, "detail": detail,
                           "observed_at": r.get("observed_at")})
        if r["state"] == "DELETED":
            add("gone", "deleted or transferred")
            continue
        if p["assignees"] == 0 and r["assignees"] > 0:
            add("claimed")
        if p["assignees"] > 0 and r["assignees"] == 0:
            add("unclaimed")
        if r["linked_open_prs"] > p["linked_open_prs"]:
            add("pr_opened")
        if r["linked_merged_prs"] > p["linked_merged_prs"]:
            add("pr_merged")
        if p["state"] == "OPEN" and r["state"] == "CLOSED":
            add("closed", r.get("state_reason"))
    return events


def plan_outcomes(state: CrawlState, repo_id: int, issues: list[dict], observed_at: str, since: str | None = None,
                  deleted: list[int] = ()) -> tuple[list[dict], list[dict], list[int]]:
    # Read-only: the runner applies the new state only after the repo's rows are on disk.
    before = state.latest_issue_state(repo_id)
    after = [{**r, "observed_at": observed_at} for r in issue_state_rows(issues)]
    gone = [{"repo_id": repo_id, "number": n, "state": "DELETED", "observed_at": observed_at} for n in deleted]
    events = diff_events(before, after + gone)
    # Closed, deleted and quiet-for-90-days issues stop being tracked.
    forget = [r["number"] for r in after if r["state"] != "OPEN"
              or (since and r["updated_at"] and r["updated_at"] < since)] + list(deleted)
    keep = [r for r in after if r["number"] not in set(forget)]
    return events, keep, forget


def apply_outcomes(state: CrawlState, repo_id: int, after: list[dict], forget: list[int]) -> None:
    if after:
        state.log_issue_state(after, after[0]["observed_at"])
    state.forget_issues(repo_id, forget)


def events_for_repo(state: CrawlState, repo_id: int, issues: list[dict], observed_at: str, since: str | None = None,
                    deleted: list[int] = ()) -> list[dict]:
    events, after, forget = plan_outcomes(state, repo_id, issues, observed_at, since, deleted)
    apply_outcomes(state, repo_id, after, forget)
    return events
