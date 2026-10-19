"""Star growth of crawled repos from GitHub's weekly star history (no stargazer identities), as percentiles by language
and size: the peer line next to a repo's own star trend."""
from datetime import datetime

from mergescout.bench.ci import TokenPool, _summary, tier

WEEKS = 12
MIN_GROUP = 20
DEFINITION = ("per repo: stars gained in the last 12 complete weeks, and that gain as a share of the stars it had "
              "before them, from GitHub's weekly star history")


def star_stats(rows: list, stars: int | None, now: datetime) -> dict | None:
    """Gain over the last 12 complete weeks; the running week is left out, and younger repos get no figure."""
    rows = sorted((r for r in rows or [] if isinstance(r, dict) and isinstance(r.get("week"), (int, float))),
                  key=lambda r: -r["week"])
    done = [r for r in rows if r["week"] + 7 * 86400 <= now.timestamp()]
    if len(done) < WEEKS:
        return None
    gained = sum(int(r.get("total") or 0) for r in done[:WEEKS])
    running = sum(int(r.get("total") or 0) for r in rows if r not in done)
    before = max((stars or 0) - gained - running, 1)
    return {"gained_12w": gained, "growth_12w": round(gained / before, 4)}


def read_stars(pool: TokenPool, repo: dict, now: datetime) -> dict | None:
    # One request: the first page holds the newest 30 weeks.
    resp = pool.get(f"/repos/{repo['name']}/stargazers/history", {})
    if resp is None:
        raise RuntimeError("GitHub gave no answer after retries")
    if resp.status_code >= 400:
        return None
    stats = star_stats(resp.json(), repo.get("stars"), now)
    return {"repo": repo["name"], "language": repo.get("language"), "stars": repo.get("stars"), **stats} if stats else None


def aggregate_stars(records: list[dict], now: datetime, top_languages: int = 25) -> dict:
    """Percentiles by language and size; a group is published only with MIN_GROUP repos."""
    latest = {}
    for r in sorted(records, key=lambda r: r.get("day") or ""):
        latest[r["repo"].lower()] = r
    recs = list(latest.values())
    counts = {}
    for r in recs:
        counts[r.get("language") or "Other"] = counts.get(r.get("language") or "Other", 0) + 1
    langs = {name for name, _ in sorted(counts.items(), key=lambda kv: -kv[1])[:top_languages]}
    groups = {}
    for r in recs:
        lang = r.get("language") if r.get("language") in langs else "Other"
        for key in (f"{lang}|{tier(r.get('stars'))}", f"{lang}|all", f"all|{tier(r.get('stars'))}", "all|all"):
            groups.setdefault(key, []).append(r)
    out = {key: {"repos": len(items), "gained_12w": _summary([r["gained_12w"] for r in items]),
                 "growth_12w": _summary([r["growth_12w"] for r in items])}
           for key, items in groups.items() if len(items) >= MIN_GROUP}
    return {"version": 1, "built_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "repos": len(recs), "definition": DEFINITION,
            "groups": out}
