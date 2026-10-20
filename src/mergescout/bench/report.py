"""A public report from the crawl's published benchmark files: aggregates only (no repo or person named), and every
number in a CSV beside it so anyone can recompute it."""
import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

MIN_REPOS = 50
LICENCE = "CC BY 4.0"
QS = ("p10", "p25", "p50", "p75", "p90", "p50_low", "p50_high")
METRICS = {"ci": ("median_s", "p90_s", "first_try_share", "rerun_share", "fail_share"),
           "flow": ("first_review_hours", "merge_hours", "outsider_merge_rate", "size_lines"),
           "delivery": ("pickup_p75_hours", "review_p75_hours", "cycle_to_merge_p75_hours"),
           "deploys": ("days_per_week", "days_per_month"),
           "community": ("first_answer_hours", "answered_1d_share", "newcomer_answered_share", "newcomer_return_rate"),
           "stars": ("gained_12w", "growth_12w")}
RECOMPUTE_SQL = """```sql
-- DuckDB, in the folder with the CSV files: the typical repo's pickup P75 by language, with its range
SELECT language, repos, pickup_p75_hours_p50, pickup_p75_hours_p50_low, pickup_p75_hours_p50_high,
       pickup_p75_hours_linearb_good
FROM read_csv_auto('delivery.csv') WHERE size = 'all' ORDER BY repos DESC;
```"""
RECOMPUTE = """```python
import csv
rows = list(csv.DictReader(open("ci.csv", encoding="utf-8")))
rust = next(r for r in rows if r["language"] == "Rust" and r["size"] == "all")
print(float(rust["median_s_p50"]) / 60, "minutes")  # the typical Rust repo's push wait
```"""


def _load(folder: Path, name: str) -> dict | None:
    try:
        return json.loads((folder / f"{name}-benchmarks.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _rows(data: dict, metrics) -> list[dict]:
    out = []
    for key, g in sorted((data.get("groups") or {}).items()):
        if not isinstance(g, dict) or (g.get("repos") or 0) < MIN_REPOS:
            continue
        lang, size = key.split("|", 1)
        row = {"language": lang, "size": size, "repos": g["repos"]}
        for kind in ("sources", "tiers"):
            if isinstance(g.get(kind), dict):
                row.update({f"{kind}_{k.replace(' ', '_')}": v for k, v in g[kind].items()})
        for m in metrics:
            q = g.get(m) or {}
            row[f"{m}_repos"] = q.get("repos", "")
            row.update({f"{m}_{p}": q.get(p, "") for p in QS})
            if isinstance(q.get("linearb"), dict):
                row.update({f"{m}_linearb_{band}": q["linearb"].get(band, "") for band in ("elite", "good", "fair")})
        out.append(row)
    return out


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.unlink(missing_ok=True)  # an earlier build's file would no longer match the report
        return
    # Groups can carry different measures (a band share only where the group had the measure): every column, in order.
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, restval="")
        w.writeheader()
        w.writerows(rows)


def _min(secs) -> str:
    return "-" if secs in ("", None) else f"{float(secs) / 60:.1f} min"


def _hrs(h) -> str:
    return "-" if h in ("", None) else (f"{float(h):.0f} h" if float(h) < 48 else f"{float(h) / 24:.1f} days")


def _hrs1(h) -> str:
    return "-" if h in ("", None) else (f"{float(h):.1f} h" if float(h) < 48 else f"{float(h) / 24:.1f} days")


def _pct(x) -> str:
    return "-" if x in ("", None) else f"{float(x):.0%}"


def _table(rows: list[dict], by: str, cols: list[tuple[str, str, object]]) -> list[str]:
    pick = [r for r in rows if (r["size"] == "all") != (by == "size") and (r["language"] == "all") == (by == "size")]
    if not pick:
        return []
    head = ["Language" if by == "language" else "Stars", "Repos"] + [c[0] for c in cols]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    tiers = ["100-1k", "1k-10k", "10k+"]
    order = (lambda r: -int(r["repos"])) if by == "language" else (lambda r: tiers.index(r["size"]) if r["size"] in tiers else 9)
    for r in sorted(pick, key=order):
        name = r["language"] if by == "language" else r["size"]
        lines.append(f"| {name} | {int(r['repos']):,} | " + " | ".join(fmt(r[key]) for _, key, fmt in cols) + " |")
    return lines


def build_report(bench: Path, out: Path) -> Path:
    """report.md plus one CSV per benchmark file in `out`."""
    bench, out = Path(bench), Path(out)
    out.mkdir(parents=True, exist_ok=True)
    data = {name: _load(bench, name) for name in METRICS}
    rows = {}
    for name, d in data.items():
        if d:
            rows[name] = _rows(d, METRICS[name])
            _write_csv(out / f"{name}.csv", rows[name])
    ci, flow, com = data.get("ci") or {}, data.get("flow") or {}, data.get("community") or {}
    built = max((d.get("built_at") or "" for d in data.values() if d), default="")[:10] or "date unknown"
    month = datetime.fromisoformat(built).strftime("%B %Y") if built[:1].isdigit() else ""
    newcomers = com.get("newcomers") or {}
    lines = [f"# State of open-source CI and review flow{f' ({month})' if month else ''}", "",
             "From a daily crawl of public GitHub repos with 100+ stars: "
             + ", ".join(x for x in (f"CI from {ci['repos']:,} repos" if ci else "",
                                    f"review flow from {flow['repos']:,} repos" if flow else "",
                                    f"first answers from {com['repos']:,} repos" if com else "",
                                    f"{newcomers['newcomers']:,} newcomers in {newcomers['repos']:,} repos" if newcomers else "") if x)
             + f". Aggregates only: no repo or person is named. Every number below is in the CSV files next to this report, "
               f"under {LICENCE}. Groups with fewer than {MIN_REPOS} repos are left out. Built {built}.", ""]
    all_ci = next((r for r in rows.get("ci", []) if r["language"] == "all" and r["size"] == "all"), None)
    if all_ci:
        lines += ["## CI: how long a push waits for its checks", "",
                  f"The typical repo (the median repo) waits {_min(all_ci['median_s_p50'])} from a push until all its checks "
                  f"finish; one in ten repos waits more than {_min(all_ci['median_s_p90'])}. The typical repo's pushes are green "
                  f"on the first try {_pct(all_ci['first_try_share_p50'])} of the time ({int(all_ci['repos']):,} repos).", ""]
        cols = [("Typical push wait", "median_s_p50", _min), ("Slowest 10% of repos", "median_s_p90", _min),
                ("Green on first try", "first_try_share_p50", _pct)]
        lines += _table(rows["ci"], "language", cols) + [""] + _table(rows["ci"], "size", cols) + [""]
    all_flow = next((r for r in rows.get("flow", []) if r["language"] == "all" and r["size"] == "all"), None)
    if all_flow:
        lines += ["## Review flow", "",
                  f"In the typical repo, a pull request that gets reviewed waits {_hrs(all_flow['first_review_hours_p50'])} for its "
                  f"first review by a maintainer other than its author. Merged pull requests are merged a median "
                  f"{_hrs(all_flow['merge_hours_p50'])} after opening"
                  + (" (this counts PRs merged with no review, and comes from a different set of repos)"
                     if all_flow["merge_hours_p50"] not in ("", None) and all_flow["first_review_hours_p50"] not in ("", None)
                     and float(all_flow["merge_hours_p50"]) < float(all_flow["first_review_hours_p50"]) else "")
                  + f". The typical repo merges {_pct(all_flow['outsider_merge_rate_p50'])} of outside contributors' decided PRs "
                  f"({int(all_flow['repos']):,} repos).", ""]
        cols = [("First review (reviewed PRs)", "first_review_hours_p50", _hrs), ("To merge (all merged PRs)", "merge_hours_p50", _hrs),
                ("Outside PRs merged", "outsider_merge_rate_p50", _pct)]
        lines += _table(rows["flow"], "language", cols) + [""] + _table(rows["flow"], "size", cols) + [""]
        agents = (flow.get("agent_prs") or {}).get("all|all")
        if agents:
            lines += [f"**Coding agents:** in the typical repo that gets both, {_pct(agents['agent']['merged'])} of pull requests "
                      f"opened by coding agents (often started by the repo's own maintainers) were merged, against {_pct(agents['outsider']['merged'])} of people's outside PRs "
                      f"({agents['repos_with_agent_prs']:,} repos; {agents['agent']['decided']:,} agent PRs and "
                      f"{agents['outsider']['decided']:,} outside PRs decided in the last 180 days).", ""]
        bt = flow.get("odds_backtest") or {}
        if (bt.get("mae") or {}).get("calibrated") is not None:
            lines += [f"**Can a repo's record predict a new outside PR?** A repo's past merge rate for outside PRs runs high for "
                      f"its next month's outside PRs, because many stay open. Corrected by what happened on other repos with a "
                      f"similar record, it was off by {bt['mae']['calibrated'] * 100:.0f} points on average ({bt['repos']:,} repos, "
                      f"{bt['prs']:,} PRs; checked on repos the correction was not built from); the raw rate was off by "
                      f"{bt['mae']['raw'] * 100:.0f} points and the typical repo's rate alone by {bt['mae']['typical_only'] * 100:.0f}.", ""]
    all_com = next((r for r in rows.get("community", []) if r["language"] == "all" and r["size"] == "all"), None)
    if all_com or newcomers:
        lines += ["## Outside contributors and newcomers", ""]
        if all_com:
            lines += [f"The typical repo first answers an outside contributor's PR in {_hrs(all_com['first_answer_hours_p50'])}; "
                      f"it answers {_pct(all_com['answered_1d_share_p50'])} of them within a day ({int(all_com['repos']):,} repos).", ""]
        if newcomers.get("by_answer"):
            lines += [f"Of {newcomers['newcomers']:,} newcomers (a first PR in a repo), the share who came back with new work "
                      f"within {newcomers.get('return_days', 90)} days, by how long their first PR waited for an answer:", "",
                      "| First answer | Newcomers | Came back | First PR merged |", "|---|---|---|---|"]
            lines += [f"| {b['bucket']} | {b['newcomers']:,} | {_pct(b['returned'])} | {_pct(b['merged'])} |" for b in newcomers["by_answer"]]
            lines += ["", "This is a correlation, not a promise.", ""]
    all_del = next((r for r in rows.get("delivery", []) if r["language"] == "all" and r["size"] == "all"), None)
    if all_del:
        def mid(m):
            # One decimal: P75s of a few hours lose their meaning at whole hours.
            value, low, high = (all_del.get(m + k) for k in ("_p50", "_p50_low", "_p50_high"))
            if value in ("", None):
                return "-"
            if float(value) >= 48:
                return _hrs(value)
            span = f" ({float(low):.1f}-{float(high):.1f})" if low not in ("", None) and high not in ("", None) else ""
            return f"{float(value):.1f} h{span}"
        dl = data.get("delivery") or {}
        lines += ["## Delivery: pickup and review against LinearB's bands", "",
                  f"The typical repo's P75 pickup (opening to the first maintainer review) is {mid('pickup_p75_hours')}, its P75 "
                  f"review (first review to merge) {mid('review_p75_hours')} and its P75 from opening to merge "
                  f"{mid('cycle_to_merge_p75_hours')} ({int(all_del['repos']):,} repos; ranges are 95% bootstrap ranges of the "
                  f"median). Inside LinearB's good band (pickup up to 4 h, review up to 14 h): "
                  f"{_pct(all_del.get('pickup_p75_hours_linearb_good'))} of repos for pickup and "
                  f"{_pct(all_del.get('review_p75_hours_linearb_good'))} for review. {dl.get('bands_note', '')}.", ""]
        cols = [("Pickup P75 (typical)", "pickup_p75_hours_p50", _hrs1), ("Review P75 (typical)", "review_p75_hours_p50", _hrs1),
                ("Pickup inside LinearB's good band", "pickup_p75_hours_linearb_good", _pct)]
        lines += _table(rows["delivery"], "language", cols) + [""] + _table(rows["delivery"], "size", cols) + [""]
    dep = data.get("deploys") or {}
    top = (dep.get("groups") or {}).get("all|all") or {}
    if top.get("sources"):
        src, tiers = top["sources"], top.get("tiers") or {}
        lines += ["## Shipping: how often public repos release", "",
                  f"Of {int(top['repos']):,} sampled repos, {_pct(src.get('releases'))} show their shipping through GitHub "
                  f"releases, {_pct(src.get('deployments api'))} through production deployments, and {_pct(src.get('none'))} "
                  "through neither (they ship from another repo or a service GitHub can't see)."
                  + (f" Among those that show it, by DORA 2023's frequency tiers: {_pct(tiers.get('elite'))} elite, "
                     f"{_pct(tiers.get('high'))} high, {_pct(tiers.get('medium'))} medium, {_pct(tiers.get('low'))} low."
                     if tiers else ""), ""]
    lines += ["## Method and files", ""]
    for name, d in data.items():
        if d:
            lines.append(f"- **{name}.csv** ({d.get('repos', 0):,} repos, built {(d.get('built_at') or 'date unknown')[:10]}): "
                         f"{d.get('definition', '')}")
            if d.get("population") or d.get("gaps"):
                lines.append(f"  Who is counted: {d.get('population') or '-'}. Not covered: {d.get('gaps') or '-'}.")
    lines += ["", "Each CSV row is a group of repos (language and star range, or `all`); for each measure it gives the number of "
                  "repos measured, the 10th, 25th, 50th, 75th and 90th percentiles across them, and a 95% range for the median "
                  "(the 50th) from 400 bootstrap resamples of the group's repos with a fixed seed, so a rebuild gives the same "
                  "numbers. A public crawl is not every team: private and enterprise repos are not in it. To recompute a number:",
              "", RECOMPUTE_SQL, "", RECOMPUTE, ""]
    (out / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return out / "report.md"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m mergescout.bench.report", description="Build the public report from benchmark files.")
    parser.add_argument("bench", help="folder with the *-benchmarks.json files")
    parser.add_argument("out", help="folder for report.md and the CSV files")
    args = parser.parse_args(argv)
    print(build_report(Path(args.bench), Path(args.out)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
