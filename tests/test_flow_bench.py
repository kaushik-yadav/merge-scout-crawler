import gzip
import json
from datetime import datetime, timedelta, timezone

import httpx

from mergescout.bench.__main__ import publish
from mergescout.bench.ci import TokenPool, aggregate, fetch_all, repo_stats, todays_share

NOW = datetime(2026, 10, 16, 12, tzinfo=timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def run(k, secs, conclusion="success", attempt=1, path=".github/workflows/ci.yml", days_ago=1, sha=None, wait=0,
        status="completed"):
    created = NOW - timedelta(days=days_ago, hours=k)
    start = created + timedelta(seconds=wait)
    return {"id": k * 100 + len(path), "head_sha": sha or f"s{k}", "path": path, "conclusion": conclusion,
            "run_attempt": attempt, "status": status, "created_at": iso(created), "run_started_at": iso(start),
            "updated_at": iso(start + timedelta(seconds=secs))}


def repos(n):
    return [{"name": f"o/r{i}", "language": "Go", "stars": 500} for i in range(n)]


# Real data: per-run medians mixed 10 s label bots with 30 min test suites (scikit-learn came out at 12 s).
def test_repo_stats_measure_each_push_until_all_its_checks_finish():
    lint = ".github/workflows/lint.yml"
    runs = []
    for k in range(8):
        runs += [run(k, 280 + k * 10, wait=20), run(k, 15, path=lint)]
    runs += [run(20, 900, "failure"), run(20, 30, path=lint), run(21, 400, attempt=2),
             run(22, 50, "failure", path="dynamic/dependabot/x"), run(23, 5000, "failure", days_ago=40),
             run(24, 60, status="in_progress"), run(24, 10, path=lint), run(25, 60, "cancelled")]
    s = repo_stats(runs, since=NOW - timedelta(days=30))
    assert s["pushes"] == 10 and s["median_s"] == 340 and s["fail_share"] == 0.1
    assert s["first_try_share"] == 0.8 and s["rerun_share"] == 0.1 and s["workflows"] == 2
    assert repo_stats(runs[:8]) is None


def test_four_weeks_of_daily_shares_cover_every_repo_once():
    names = [f"o/r{i}" for i in range(2800)]
    seen = []
    for d in range(28):
        seen += todays_share(names, NOW + timedelta(days=d))
    assert sorted(seen) == sorted(names)


# A token that cannot read, or ran out, must never make a repo look like it has no CI.
def test_a_token_that_cannot_read_other_repos_drops_out_instead_of_the_repos():
    seen = []

    def handler(request):
        seen.append(request.headers["authorization"])
        if request.headers["authorization"] == "Bearer tok-app":
            return httpx.Response(403, json={"message": "Resource not accessible by integration"},
                                  headers={"x-ratelimit-remaining": "4999"})
        return httpx.Response(200, json={"workflow_runs": [run(k, 300) for k in range(6)]},
                              headers={"x-ratelimit-remaining": "3000"})
    pool = TokenPool(["tok-app", "tok-pat"], client=httpx.Client(transport=httpx.MockTransport(handler)), reserve=100)
    out = fetch_all(pool, repos(4), NOW, workers=1)
    assert len(out["records"]) == 4 and seen.count("Bearer tok-app") == 1


def test_a_repo_that_hit_a_used_up_token_is_read_again_with_another():
    def handler(request):
        if request.headers["authorization"] == "Bearer tok-a":
            return httpx.Response(403, json={"message": "API rate limit exceeded"}, headers={"x-ratelimit-remaining": "0"})
        return httpx.Response(200, json={"workflow_runs": [run(k, 300) for k in range(6)]},
                              headers={"x-ratelimit-remaining": "3000"})
    pool = TokenPool(["tok-a", "tok-b"], client=httpx.Client(transport=httpx.MockTransport(handler)), reserve=100)
    assert len(fetch_all(pool, repos(3), NOW, workers=1)["records"]) == 3


def test_aggregates_group_by_language_and_size_with_enough_repos():
    recs = [{"repo": f"o/p{i}", "language": "Python", "stars": 500, "median_s": 100 + i, "p90_s": 200,
             "fail_share": 0.1, "first_try_share": 0.9, "rerun_share": 0.02, "pushes": 20} for i in range(25)]
    recs += [{"repo": f"o/g{i}", "language": "Go", "stars": 50_000, "median_s": 900, "p90_s": 990,
              "fail_share": 0.2, "first_try_share": 0.7, "rerun_share": 0.05, "pushes": 20} for i in range(5)]
    agg = aggregate(recs, NOW)
    py = agg["groups"]["Python|100-1k"]
    assert py["repos"] == 25 and py["median_s"]["p50"] == 112 and py["median_s"]["p25"] == 106
    assert "Go|10k+" not in agg["groups"] and agg["groups"]["all|all"]["repos"] == 30


class FakeReleases:
    def __init__(self, files=None):
        self.files = dict(files or {})

    def upload(self, tag, path, name=None):
        self.files[(tag, name or path.name)] = path.read_bytes()

    def get(self, tag):
        return {"tag_name": tag} if any(t == tag for t, _ in self.files) else None

    def assets(self, release):
        return [{"name": n, "tag": t} for t, n in self.files if t == release["tag_name"]]

    def download_asset(self, asset, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.files[(asset["tag"], asset["name"])])


def daily_gz(records):
    return gzip.compress("".join(json.dumps(r) + "\n" for r in records).encode())


def test_daily_run_stores_todays_sample_and_rebuilds_benchmarks_from_recent_days(tmp_path):
    rec = lambda i, day, m: {"repo": f"o/p{i}", "language": "Python", "stars": 500, "median_s": m, "p90_s": m,  # noqa: E731
                             "fail_share": 0.1, "first_try_share": 0.9, "rerun_share": 0.0, "pushes": 10, "day": day}
    old = [rec(i, "2026-10-09", 100) for i in range(15)]
    rel = FakeReleases({("bench-2026-W41", "ci-2026-10-09.ndjson.gz"): daily_gz(old),
                        ("bench-2026-W31", "ci-2026-08-01.ndjson.gz"): daily_gz([rec(99, "2026-08-01", 5)])})
    publish(tmp_path, rel, [rec(i, "2026-10-16", 200) for i in range(10, 25)], NOW, log=lambda m: None)
    stored = gzip.decompress(rel.files[("bench-2026-W42", "ci-2026-10-16.ndjson.gz")]).decode().splitlines()
    assert len(stored) == 15
    agg = json.loads(rel.files[("bench-latest", "ci-benchmarks.json")])
    # 25 repos: 10 seen only last week, 15 re-read today (their newest numbers win); August is too old.
    assert agg["groups"]["Python|100-1k"]["repos"] == 25 and agg["groups"]["Python|100-1k"]["median_s"]["p50"] == 200


# Review findings: tags pushed later on an old commit, broken answers and same-day re-runs.
def test_a_tag_push_of_an_old_commit_is_its_own_push():
    def r(k, branch, hours_ago, sha=None):
        cr = NOW - timedelta(hours=hours_ago)
        return {"id": k, "head_sha": sha or f"s{k}", "head_branch": branch, "path": "ci.yml", "conclusion": "success",
                "run_attempt": 1, "status": "completed", "created_at": iso(cr), "run_started_at": iso(cr),
                "updated_at": iso(cr + timedelta(seconds=300))}
    runs = [r(k, "main", k + 1) for k in range(5)] + [r(99, "main", 80, "same"), r(100, "v1", 2, "same")]
    s = repo_stats(runs)
    assert s["pushes"] == 7 and s["p90_s"] == 300


def test_one_broken_repo_answer_never_loses_the_days_sample():
    def handler(request):
        if "bad" in request.url.path:
            return httpx.Response(200, content=b"not json")
        return httpx.Response(200, json={"workflow_runs": [run(k, 300) for k in range(6)]}, headers={"x-ratelimit-remaining": "4000"})
    pool = TokenPool(["tok"], client=httpx.Client(transport=httpx.MockTransport(handler)), reserve=100)
    out = fetch_all(pool, [{"name": n, "language": "Go", "stars": 500} for n in ("o/bad", "o/ok")], NOW, workers=1)
    assert out["complete"] is False and [r["repo"] for r in out["records"]] == ["o/ok"]


def test_a_second_run_on_the_same_day_adds_to_that_days_file(tmp_path):
    rec = lambda i: {"repo": f"o/p{i}", "language": "Go", "stars": 500, "median_s": 1, "p90_s": 1, "fail_share": 0,  # noqa: E731
                     "first_try_share": 1, "rerun_share": 0, "pushes": 9, "day": "2026-10-16"}
    rel = FakeReleases()
    publish(tmp_path / "a", rel, [rec(i) for i in range(30)], NOW, log=lambda m: None)
    publish(tmp_path / "b", rel, [rec(99)], NOW, log=lambda m: None)
    assert len(gzip.decompress(rel.files[("bench-2026-W42", "ci-2026-10-16.ndjson.gz")]).decode().splitlines()) == 31


def test_star_growth_leaves_out_the_running_week_and_young_repos():
    # Real data: GitHub's weekly star history lists the running, partial week first.
    from mergescout.bench.stars import star_stats
    start = int((NOW - timedelta(days=3)).timestamp())
    rows = [{"week": start, "total": 5}] + [{"week": start - (i + 1) * 604800, "total": 10} for i in range(13)]
    assert star_stats(rows, stars=1125, now=NOW) == {"gained_12w": 120, "growth_12w": 0.12}
    assert star_stats(rows[:9], stars=40, now=NOW) is None


def test_shipping_is_read_from_production_deployments_first_then_stable_releases():
    # Measured on 300 repos (2026-10-21): 2.00 requests each; 4% ship through production deployments, 44% through
    # releases, 52% through neither, which the benchmark says beside its numbers.
    from datetime import datetime, timedelta, timezone

    import httpx

    from mergescout.bench.ci import TokenPool
    from mergescout.bench.deploys import repo_deploys
    now = datetime(2026, 10, 21, 12, tzinfo=timezone.utc)

    def iso(days):
        return (now - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    answers = {"/repos/o/app/deployments": [{"environment": "Production – web", "created_at": iso(d)} for d in range(0, 90, 2)],
               "/repos/o/app/releases": [],
               "/repos/o/lib/deployments": [{"environment": "Preview", "created_at": iso(1)}],
               "/repos/o/lib/releases": [{"tag_name": f"v1.{k}.0", "published_at": iso(k * 14), "prerelease": False}
                                         for k in range(6)] + [{"tag_name": "v2.0.0-rc.1", "published_at": iso(2)}]}
    pool = TokenPool(["t"], client=httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, json=answers.get(r.url.path, []), headers={"x-ratelimit-remaining": "4000"}))))
    app = repo_deploys(pool, {"name": "o/app"}, now)
    lib = repo_deploys(pool, {"name": "o/lib"}, now)
    assert (app["source"], app["days_per_week"], app["tier"]) == ("deployments api", 3, "high")
    assert (lib["source"], lib["deploy_days"], lib["tier"]) == ("releases", 6, "medium") and pool.requests == 4
