"""How often public repos ship: two REST requests per sampled repo (its deployments, its releases). Production deployments
count first, else stable releases (pre-releases and alpha/beta/rc tags left out); deployment days per week and per month
over 90 days with DevLake's counting (weeks with none count as zero, lower median) and DORA's 2023 frequency tiers."""
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from mergescout.bench.ci import quantiles, tier

DAYS = 90
PAGE = 100
MIN_GROUP = 20
PRODUCTION = {"production", "prod", "live"}
PRE_TAG = re.compile(r"(?i)[-.](alpha|beta|rc|canary|next|dev|pre|preview|nightly|snapshot|insiders)([-.\d]|$)")
DEFINITION = ("per repo, the last 90 days: days with a successful-or-pending deployment to a production environment "
              "(GitHub's Deployments API, an environment named production, prod or live, or starting with it), else days "
              "with a published stable release (no pre-releases, no alpha/beta/rc tags); deployment days per week and per "
              "month are DevLake's lower medians over the calendar (weeks with none count as zero); tiers are DORA 2023's "
              "(elite 5+ days a week, high 1+ a week, medium 1+ a month, else low)")
POPULATION = "public GitHub repos with 100+ stars that pushed in the last 30 days, a slice of them read each day"
GAPS = ("deployments made outside GitHub (another repo, a hosting service without GitHub deployments, ArgoCD) are not seen, "
        "nor deploy workflows without an environment; the newest 100 deployments and releases are read, so the busiest repos "
        "are measured over the days those cover; deployment state is not read, so a failed deployment can count")


def _first_word(env: str | None) -> str:
    return re.split(r"[\s\-–_/:]+", (env or "").strip().lower(), maxsplit=1)[0]


def _lower_median(values: list[int]) -> int:
    # DevLake: the largest value whose percent rank is at most 0.5 (ties share the lowest rank).
    v = sorted(values)
    if not v:
        return 0
    if len(v) == 1:
        return v[0]
    return max(x for x in v if v.index(x) / (len(v) - 1) <= 0.5)


def frequency(days: set[str], now: datetime, start: datetime) -> tuple[int, int, str]:
    """(deployment days per week, per month, DORA 2023 tier) over the calendar days after `start` up to today."""
    weeks, months = defaultdict(int), defaultdict(int)
    k = 0
    while now - timedelta(days=k) > start:
        day = (now - timedelta(days=k)).date()
        hit = day.isoformat() in days
        weeks[day - timedelta(days=day.weekday())] += hit
        months[day.replace(day=1)] += hit
        k += 1
    per_week, per_month = _lower_median(list(weeks.values())), _lower_median(list(months.values()))
    level = "elite" if per_week >= 5 else "high" if per_week >= 1 else "medium" if per_month >= 1 else "low"
    return per_week, per_month, level


def _get(pool, path: str):
    resp = pool.get(path, {"per_page": PAGE})
    if resp is None:
        raise RuntimeError("GitHub gave no answer after retries")
    return resp.json() if resp.status_code < 400 else []


def repo_deploys(pool, repo: dict, now: datetime, days: int = DAYS) -> dict:
    """One repo: where its shipping is seen, on how many days, and its frequency (two requests)."""
    since = now - timedelta(days=days)
    stamp = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    deployments = _get(pool, f"/repos/{repo['name']}/deployments")
    releases = _get(pool, f"/repos/{repo['name']}/releases")
    found = [d["created_at"] for d in deployments if isinstance(d, dict) and _first_word(d.get("environment")) in PRODUCTION
             and (d.get("created_at") or "") >= stamp]
    source, page = "deployments api", deployments
    if not found:
        found = [r["published_at"] for r in releases if isinstance(r, dict) and r.get("published_at") and not r.get("draft")
                 and not r.get("prerelease") and not PRE_TAG.search(r.get("tag_name") or "") and r["published_at"] >= stamp]
        source, page = ("releases", releases) if found else (None, [])
    start = since
    if len(page) >= PAGE and found:
        # A full page that ends inside the window: measure only the days it covers.
        start = max(since, datetime.fromisoformat(min(found).replace("Z", "+00:00")) - timedelta(days=1))
    per_week, per_month, level = frequency({t[:10] for t in found}, now, start) if found else (0, 0, None)
    return {"repo": repo["name"], "language": repo.get("language"), "stars": repo.get("stars"), "source": source,
            "deploy_days": len({t[:10] for t in found}), "days_per_week": per_week, "days_per_month": per_month, "tier": level,
            "capped": len(page) >= PAGE, "requests": 2, "day": now.strftime("%Y-%m-%d")}


def aggregate_deploys(records: list[dict], now: datetime, top_languages: int = 25) -> dict:
    """Groups by language and stars (20+ repos): where shipping is seen, and how often among repos where it is."""
    latest = {}
    for r in sorted(records, key=lambda r: r.get("day") or ""):
        latest[r["repo"].lower()] = r
    recs = list(latest.values())
    counts = Counter(r.get("language") or "Other" for r in recs)
    langs = {name for name, _ in counts.most_common(top_languages)}
    groups = defaultdict(list)
    for r in recs:
        lang = r.get("language") if r.get("language") in langs else "Other"
        for key in (f"{lang}|{tier(r.get('stars'))}", f"{lang}|all", f"all|{tier(r.get('stars'))}", "all|all"):
            groups[key].append(r)
    out = {}
    for key, items in groups.items():
        if len(items) < MIN_GROUP:
            continue
        n = len(items)
        shipping = [r for r in items if r.get("source")]
        entry = {"repos": n, "sources": {s: round(sum(r.get("source") == s for r in items) / n, 4)
                                         for s in ("deployments api", "releases")}}
        entry["sources"]["none"] = round(1 - sum(entry["sources"].values()), 4)
        if len(shipping) >= MIN_GROUP:
            entry["days_per_week"] = {"repos": len(shipping), **quantiles([r["days_per_week"] for r in shipping])}
            entry["days_per_month"] = {"repos": len(shipping), **quantiles([r["days_per_month"] for r in shipping])}
            entry["tiers"] = {t: round(sum(r.get("tier") == t for r in shipping) / len(shipping), 4)
                              for t in ("elite", "high", "medium", "low")}
        out[key] = entry
    return {"version": 1, "built_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "repos": len(recs),
            "requests": sum(r.get("requests") or 0 for r in recs), "definition": DEFINITION, "population": POPULATION,
            "gaps": GAPS, "groups": out}
