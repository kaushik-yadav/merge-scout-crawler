import re

# Label names that mark an issue as open to newcomers; GitHub matches any of them, ignoring case.
OPPORTUNITY_LABELS = ("good first issue", "good-first-issue", "help wanted", "help-wanted", "beginner",
                      "beginner friendly", "easy", "first-timers-only", "starter", "good first contribution")


def norm_label(name: str) -> str:
    # "Good-First-Issue :wave:" and "good_first_issue" both become "good first issue".
    text = re.sub(r":[a-z0-9_+-]+:", " ", (name or "").lower())
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


HOT = {norm_label(label) for label in OPPORTUNITY_LABELS}
# "help wanted" invites anyone; these labels promise a task sized for a first contribution.
BEGINNER = HOT - {"help wanted"}


def has_opportunity(labels: list[str]) -> bool:
    return any(norm_label(label) in HOT for label in labels or [])
