from datetime import datetime, timezone

from mergescout.crawl.outcomes import apply_outcomes, diff_events, events_for_repo, issue_state_rows, plan_outcomes
from mergescout.crawl.state import CrawlState

NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)
RECENT = datetime(2026, 10, 10, tzinfo=timezone.utc)
OLD = datetime(2026, 6, 1, tzinfo=timezone.utc)
SINCE = "2026-07-16T00:00:00Z"


def issue(number, assignees=0, open_prs=0, merged=0, state="OPEN", labels=("good first issue",), comments=0,
          updated=RECENT, reason=None):
    return {"repo_id": 1, "number": number, "state": state, "state_reason": reason, "updated_at": updated,
            "assignees": assignees, "linked_open_prs": open_prs, "linked_merged_prs": merged,
            "comments_total": comments, "labels": list(labels)}


def test_issue_state_rows_shape():
    rows = issue_state_rows([issue(7, labels=("a", "b"))])
    assert rows == [{"repo_id": 1, "number": 7, "state": "OPEN", "state_reason": None, "assignees": 0,
                     "linked_open_prs": 0, "linked_merged_prs": 0, "comments": 0, "labels": "a|b",
                     "updated_at": "2026-10-10T00:00:00Z"}]


def test_diff_events():
    before = issue_state_rows([issue(1), issue(2), issue(3, assignees=1)])
    after = issue_state_rows([issue(1, assignees=1), issue(2, open_prs=1),
                              issue(3, assignees=0, merged=1, state="CLOSED", reason="COMPLETED")])
    events = diff_events(before, after)
    assert {(e["number"], e["event"]) for e in events} == {(1, "claimed"), (2, "pr_opened"), (3, "unclaimed"),
                                                          (3, "pr_merged"), (3, "closed")}
    assert [e["detail"] for e in events if e["event"] == "closed"] == ["COMPLETED"]


def test_events_for_repo_uses_previous_snapshot(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    assert events_for_repo(s, 1, [issue(5)], "2026-10-13") == []
    ev = events_for_repo(s, 1, [issue(5, open_prs=1)], "2026-10-14")
    assert [(e["number"], e["event"], e["observed_at"]) for e in ev] == [(5, "pr_opened", "2026-10-14")]
    assert s.latest_issue_state(1)[0]["linked_open_prs"] == 1


def test_closed_deleted_and_aged_out_issues_stop_being_tracked(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    events_for_repo(s, 1, [issue(5), issue(6), issue(7)], "2026-10-13")
    ev = events_for_repo(s, 1, [issue(5, state="CLOSED", reason="NOT_PLANNED"), issue(6, updated=OLD)],
                         "2026-10-14", since=SINCE, deleted=[7])
    assert {(e["number"], e["event"], e["detail"]) for e in ev} == {(5, "closed", "NOT_PLANNED"),
                                                                  (7, "gone", "deleted or transferred")}
    assert s.open_issue_numbers(1) == []


def test_plan_does_not_touch_state_until_applied(tmp_path):
    s = CrawlState(tmp_path / "c.sqlite", now=lambda: NOW)
    events_for_repo(s, 1, [issue(5)], "2026-10-13")
    ev, after, forget = plan_outcomes(s, 1, [issue(5, assignees=1)], "2026-10-14")
    assert [e["event"] for e in ev] == ["claimed"] and s.latest_issue_state(1)[0]["assignees"] == 0
    apply_outcomes(s, 1, after, forget)
    assert s.latest_issue_state(1)[0]["assignees"] == 1
